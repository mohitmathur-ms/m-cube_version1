"""Render the push dependency graph as a self-contained inline SVG + dark tables.

No CDN, no JavaScript, no third-party deps — the output is plain SVG/HTML that
renders offline. Consumes the dict produced by ``depgraph.neighbors_for`` /
``depgraph.graph_for_push``.

Layout is a fixed three-column diagram:

    dependents            changed files            dependencies
    (import the   ──▶   (this push's .py files)  ──▶   (imported by the
     changed files)        highlighted                  changed files)

Arrows always point importer → imported ("depends on"), so an arrow into the
middle column means "this file is depended on by the left node", and an arrow
out of the middle means "this changed file depends on the right node".
"""

from __future__ import annotations

from html import escape

# Geometry / caps.
_WIDTH = 940
_COL_X = {"left": 150, "mid": 470, "right": 790}
_NODE_W = 250
_NODE_H = 34
_V_GAP = 14
_TOP = 70
_MAX_PER_COL = 12


def _basename(path: str) -> str:
    """Compact label: keep the last package + file, e.g. 'core/models/__init__.py'
    -> 'models/__init__.py', 'server.py' -> 'server.py'."""
    parts = path.split("/")
    return "/".join(parts[-2:]) if len(parts) > 1 else path


def _truncate(text: str, limit: int = 30) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _node_svg(x: int, y: int, label: str, title: str, kind: str) -> str:
    """One rounded-rect node centered horizontally on x."""
    fill = {
        "mid": "var(--surface2)",
        "left": "var(--surface)",
        "right": "var(--surface)",
    }[kind]
    stroke = {"mid": "var(--orange)", "left": "var(--blue)", "right": "var(--green)"}[kind]
    rx = x - _NODE_W // 2
    return (
        f'<g><title>{escape(title)}</title>'
        f'<rect x="{rx}" y="{y}" width="{_NODE_W}" height="{_NODE_H}" rx="7" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>'
        f'<text x="{x}" y="{y + _NODE_H // 2 + 4}" text-anchor="middle" '
        f'font-size="12.5" fill="var(--text)" '
        f'font-family="SFMono-Regular,Consolas,monospace">{escape(_truncate(label))}</text>'
        f"</g>"
    )


def _edge_svg(x1: int, y1: int, x2: int, y2: int) -> str:
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="var(--muted)" stroke-width="1.2" marker-end="url(#arrow)" opacity="0.75"/>'
    )


def render_svg(graph: dict) -> str:
    """Return an <svg> string for the three-column dependency diagram."""
    changed = graph.get("changed", [])
    per_file = graph.get("per_file", {})

    # Collect the left (dependents) and right (dependencies) columns across all changed files.
    dependents: list[str] = []
    dependencies: list[str] = []
    for f in changed:
        dependents.extend(per_file.get(f, {}).get("depended_on_by", []))
        dependencies.extend(per_file.get(f, {}).get("depends_on", []))
    dependents = sorted(set(dependents))
    dependencies = sorted(set(dependencies))

    if not changed:
        return (
            "<p class='muted'>No first-party Python files changed in this push, so there is "
            "no import graph to draw. See the table below for the full changed-file list.</p>"
        )

    left = dependents[:_MAX_PER_COL]
    mid = changed[:_MAX_PER_COL]
    right = dependencies[:_MAX_PER_COL]

    def _col_y(items: list[str]) -> dict[str, int]:
        return {name: _TOP + i * (_NODE_H + _V_GAP) for i, name in enumerate(items)}

    y_left, y_mid, y_right = _col_y(left), _col_y(mid), _col_y(right)
    rows = max(len(left), len(mid), len(right), 1)
    height = _TOP + rows * (_NODE_H + _V_GAP) + 20

    parts: list[str] = []
    # Edges first so nodes paint on top.
    for importer, imported in graph.get("edges", []):
        if imported in y_mid and importer in y_left:        # dependent -> changed
            parts.append(_edge_svg(
                _COL_X["left"] + _NODE_W // 2, y_left[importer] + _NODE_H // 2,
                _COL_X["mid"] - _NODE_W // 2, y_mid[imported] + _NODE_H // 2))
        if importer in y_mid and imported in y_right:        # changed -> dependency
            parts.append(_edge_svg(
                _COL_X["mid"] + _NODE_W // 2, y_mid[importer] + _NODE_H // 2,
                _COL_X["right"] - _NODE_W // 2, y_right[imported] + _NODE_H // 2))

    for name in left:
        parts.append(_node_svg(_COL_X["left"], y_left[name], _basename(name), name, "left"))
    for name in mid:
        parts.append(_node_svg(_COL_X["mid"], y_mid[name], _basename(name), name, "mid"))
    for name in right:
        parts.append(_node_svg(_COL_X["right"], y_right[name], _basename(name), name, "right"))

    # Column headers + "+N more" notes.
    def _hdr(x: int, text: str, total: int, shown: int) -> str:
        extra = f" (+{total - shown} more)" if total > shown else ""
        return (
            f'<text x="{x}" y="38" text-anchor="middle" font-size="12" '
            f'fill="var(--muted)" font-weight="600">{escape(text)}{escape(extra)}</text>'
        )

    headers = (
        _hdr(_COL_X["left"], "Depended on by", len(dependents), len(left))
        + _hdr(_COL_X["mid"], "Changed files", len(changed), len(mid))
        + _hdr(_COL_X["right"], "Depends on", len(dependencies), len(right))
    )

    marker = (
        '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="7" refY="3" '
        'orient="auto" markerUnits="strokeWidth">'
        '<path d="M0,0 L7,3 L0,6 Z" fill="var(--muted)"/></marker></defs>'
    )

    return (
        f'<svg viewBox="0 0 {_WIDTH} {height}" width="100%" '
        f'role="img" aria-label="dependency graph" '
        f'style="max-width:{_WIDTH}px">{marker}{headers}{"".join(parts)}</svg>'
    )


def _list_cell(items: list[str]) -> str:
    if not items:
        return "<span class='muted'>&mdash;</span>"
    return "<br>".join(f"<code>{escape(i)}</code>" for i in items)


def render_tables(graph: dict) -> str:
    """Dark adjacency tables: one row per changed .py file, plus a non-.py note."""
    changed = graph.get("changed", [])
    per_file = graph.get("per_file", {})
    non_py = graph.get("non_py", [])

    blocks: list[str] = []

    if changed:
        rows = []
        for f in changed:
            info = per_file.get(f, {})
            rows.append(
                f"<tr><td><code>{escape(f)}</code></td>"
                f"<td>{_list_cell(info.get('depended_on_by', []))}</td>"
                f"<td>{_list_cell(info.get('depends_on', []))}</td></tr>"
            )
        blocks.append(
            "<table class='dep'><thead><tr>"
            "<th>Changed file</th><th>Depended on by &larr;</th><th>Depends on &rarr;</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
        )

    if non_py:
        items = "".join(f"<li><code>{escape(p)}</code></li>" for p in non_py)
        blocks.append(
            "<p class='sub' style='margin-top:14px'>Changed files not in the Python import "
            f"graph (config, docs, workflows, JSON, etc.):</p><ul class='risks'>{items}</ul>"
        )

    return "".join(blocks) or "<p class='muted'>No changed files reported.</p>"
