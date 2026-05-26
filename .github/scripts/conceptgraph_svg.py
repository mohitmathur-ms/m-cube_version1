"""Render the AI-authored concept ("knowledge") graph for a push.

Distinct from ``depgraph_svg`` (which draws the deterministic file-import graph
from on-disk AST scanning). This module draws the *semantic* graph the model
returns: the key entities a change touches — classes, functions, configs, or
domain concepts such as ``ManagedExitStrategy`` / ``ExitConfig`` / ``on_bar`` —
and the relationships between them.

No CDN, no JavaScript, no third-party deps. Layout is a deterministic radial
placement (nodes evenly spaced on a circle) so it renders offline and never
needs a graph-layout engine. Consumes the normalized shape:

    nodes: list[{"id": str, "kind": str}]
    edges: list[{"source": str, "relation": str, "target": str}]

Both renderers are robust to empty / malformed input and cap their size so a
chatty model can't blow up the SVG.
"""

from __future__ import annotations

import math
from html import escape

# Caps + geometry.
_MAX_NODES = 10
_MAX_EDGES = 14
_WIDTH = 940
_HEIGHT = 460
_CX = _WIDTH // 2
_CY = _HEIGHT // 2
_RADIUS = 165
_NODE_RX = 92          # node half-width
_NODE_RY = 17          # node half-height

# Node fill/stroke by kind. Unknown kinds fall back to "concept".
_KIND_STROKE = {
    "class": "var(--orange)",
    "function": "var(--blue)",
    "config": "var(--green)",
    "concept": "var(--purple)",
    "file": "var(--muted)",
}


def _truncate(text: str, limit: int = 22) -> str:
    text = str(text)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _stroke_for(kind: str) -> str:
    return _KIND_STROKE.get(str(kind).lower().strip(), _KIND_STROKE["concept"])


def _collect_nodes(nodes: list, edges: list) -> list[dict]:
    """Return a capped, de-duplicated, ordered node list.

    Uses the model's ``nodes`` first (preserving its order + kind), then adds any
    edge endpoint not already present (kind ``concept``) so no edge dangles.
    """
    seen: dict[str, dict] = {}
    for n in nodes or []:
        if isinstance(n, dict):
            nid = str(n.get("id", "")).strip()
            kind = str(n.get("kind", "concept")).strip() or "concept"
        else:
            nid, kind = str(n).strip(), "concept"
        if nid and nid not in seen:
            seen[nid] = {"id": nid, "kind": kind}
    for e in edges or []:
        if not isinstance(e, dict):
            continue
        for key in ("source", "target"):
            nid = str(e.get(key, "")).strip()
            if nid and nid not in seen:
                seen[nid] = {"id": nid, "kind": "concept"}
    return list(seen.values())[:_MAX_NODES]


def render_concept_svg(nodes: list, edges: list) -> str:
    """Return an ``<svg>`` for the radial concept graph (or a muted note)."""
    node_list = _collect_nodes(nodes, edges)
    if not node_list:
        return (
            "<p class='muted'>No concept graph was produced for this push "
            "(the change may be configuration- or documentation-only).</p>"
        )

    ids = [n["id"] for n in node_list]
    idset = set(ids)
    count = len(node_list)

    # Place nodes evenly on a circle; a single node sits in the center.
    pos: dict[str, tuple[float, float]] = {}
    if count == 1:
        pos[ids[0]] = (_CX, _CY)
    else:
        for i, nid in enumerate(ids):
            angle = -math.pi / 2 + (2 * math.pi * i / count)
            pos[nid] = (_CX + _RADIUS * math.cos(angle),
                        _CY + _RADIUS * math.sin(angle))

    marker = (
        '<defs><marker id="carrow" markerWidth="9" markerHeight="9" refX="7" refY="3" '
        'orient="auto" markerUnits="strokeWidth">'
        '<path d="M0,0 L7,3 L0,6 Z" fill="var(--muted)"/></marker></defs>'
    )

    edge_parts: list[str] = []
    label_parts: list[str] = []
    drawn = 0
    for e in edges or []:
        if drawn >= _MAX_EDGES:
            break
        if not isinstance(e, dict):
            continue
        src = str(e.get("source", "")).strip()
        tgt = str(e.get("target", "")).strip()
        rel = str(e.get("relation", "")).strip()
        if src not in idset or tgt not in idset or src == tgt:
            continue
        x1, y1 = pos[src]
        x2, y2 = pos[tgt]
        # Shorten the segment so the arrowhead stops at the node edge, not center.
        dx, dy = x2 - x1, y2 - y1
        dist = math.hypot(dx, dy) or 1.0
        ux, uy = dx / dist, dy / dist
        sx, sy = x1 + ux * _NODE_RX, y1 + uy * _NODE_RY
        ex, ey = x2 - ux * (_NODE_RX + 6), y2 - uy * (_NODE_RY + 6)
        edge_parts.append(
            f'<line x1="{sx:.1f}" y1="{sy:.1f}" x2="{ex:.1f}" y2="{ey:.1f}" '
            f'stroke="var(--muted)" stroke-width="1.2" marker-end="url(#carrow)" opacity="0.7"/>'
        )
        if rel:
            mx, my = (x1 + x2) / 2, (y1 + y2) / 2
            label_parts.append(
                f'<text x="{mx:.1f}" y="{my:.1f}" text-anchor="middle" font-size="10.5" '
                f'fill="var(--muted)" stroke="var(--bg)" stroke-width="3" '
                f'paint-order="stroke">{escape(_truncate(rel, 18))}</text>'
            )
        drawn += 1

    node_parts: list[str] = []
    for n in node_list:
        nid = n["id"]
        x, y = pos[nid]
        stroke = _stroke_for(n["kind"])
        node_parts.append(
            f'<g><title>{escape(nid)} ({escape(n["kind"])})</title>'
            f'<rect x="{x - _NODE_RX:.1f}" y="{y - _NODE_RY:.1f}" '
            f'width="{_NODE_RX * 2}" height="{_NODE_RY * 2}" rx="9" '
            f'fill="var(--surface2)" stroke="{stroke}" stroke-width="1.5"/>'
            f'<text x="{x:.1f}" y="{y + 4:.1f}" text-anchor="middle" font-size="12" '
            f'fill="var(--text)" font-family="SFMono-Regular,Consolas,monospace">'
            f'{escape(_truncate(nid))}</text></g>'
        )

    # Edges + labels under nodes so nodes paint on top.
    body = marker + "".join(edge_parts) + "".join(label_parts) + "".join(node_parts)
    return (
        f'<svg viewBox="0 0 {_WIDTH} {_HEIGHT}" width="100%" role="img" '
        f'aria-label="concept graph" style="max-width:{_WIDTH}px">{body}</svg>'
    )


def render_concept_table(edges: list) -> str:
    """A reliable Source / Relationship / Target table beneath the SVG."""
    rows: list[str] = []
    for e in (edges or [])[:_MAX_EDGES]:
        if not isinstance(e, dict):
            continue
        src = str(e.get("source", "")).strip()
        tgt = str(e.get("target", "")).strip()
        rel = str(e.get("relation", "")).strip()
        if not src or not tgt:
            continue
        rel_cell = escape(rel) if rel else "<span class='muted'>relates to</span>"
        rows.append(
            f"<tr><td><code>{escape(src)}</code></td>"
            f"<td>{rel_cell}</td>"
            f"<td><code>{escape(tgt)}</code></td></tr>"
        )
    if not rows:
        return ""
    return (
        "<table class='dep'><thead><tr>"
        "<th>Source</th><th>Relationship</th><th>Target</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    )
