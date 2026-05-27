"""Render push-report graphs as Mermaid diagrams.

Mermaid (https://mermaid.js.org) turns a small text DSL into an SVG diagram in
the browser at view time. This module emits that DSL text for the change
report's knowledge graph. Two sources are supported, in priority order:

  1. concept graph    -- the AI-authored *semantic* graph: the key entities a
     change touches (classes, functions, configs, domain concepts) and the
     relationships between them. Preferred, because it reads like a true
     "knowledge graph" rather than a file-import map.
  2. dependency graph -- the deterministic first-party import graph from
     ``depgraph.py``. Used as a fallback so EVERY report still carries a Mermaid
     graph even when the AI backend is unavailable (degraded mode).

Pure stdlib, never raises: malformed input yields ``""`` (an empty-string
sentinel) so the report still renders. Real node ids (file paths, dotted
symbols) contain ``/`` ``.`` and other characters Mermaid cannot use in an
identifier, so each is remapped to a safe token (``n0``, ``n1``, ...) and the
human-readable text lives in the quoted node label instead.
"""

from __future__ import annotations

# Keep graphs legible: a chatty model or a hot module with many importers must
# not produce an unreadable hairball. Mirrors the caps in conceptgraph_svg.py.
_MAX_NODES = 16
_MAX_EDGES = 22

# Concept-node kind -> Mermaid classDef name (defined in _CLASSDEFS).
_KIND_CLASS = {
    "class": "kclass",
    "function": "kfunc",
    "config": "kconfig",
    "concept": "kconcept",
    "file": "kfile",
}

# classDef lines injected once per graph. Colors mirror the GitHub-dark palette
# in _style.py so the diagram feels native against the dark report background.
_CLASSDEFS = [
    "classDef kclass fill:#3a2a12,stroke:#f0883e,color:#ffd9b3;",
    "classDef kfunc fill:#10243f,stroke:#58a6ff,color:#cfe6ff;",
    "classDef kconfig fill:#11261a,stroke:#3fb950,color:#c9f5d4;",
    "classDef kconcept fill:#241a3a,stroke:#bc8cff,color:#e6d6ff;",
    "classDef kfile fill:#1c2230,stroke:#8b949e,color:#d6deea;",
    "classDef kchanged fill:#11261a,stroke:#3fb950,color:#c9f5d4,stroke-width:2px;",
    "classDef kdep fill:#1c2230,stroke:#8b949e,color:#d6deea;",
]


def _mlabel(text: str, limit: int = 34) -> str:
    """Sanitize text for use inside a Mermaid quoted label.

    Strips characters that break the ``["..."]`` / ``|"..."|`` parsers (or that
    the browser would interpret as HTML inside the ``<div class="mermaid">``),
    collapses whitespace, and truncates so a node stays a node, not a wall.
    """
    s = str(text).strip().replace('"', "'").replace("\n", " ")
    for ch in ("[", "]", "{", "}", "|", "<", ">", "`", "&"):
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    if len(s) > limit:
        s = s[: limit - 1] + "…"
    return s or "?"


def _finish(lines: list[str], node_count: int, classes: dict[str, str]) -> str:
    """Append per-node class assignments + classDefs and join, or '' if empty."""
    if node_count == 0:
        return ""
    for tok, cls in classes.items():
        lines.append(f"  class {tok} {cls};")
    lines.extend(f"  {c}" for c in _CLASSDEFS)
    return "\n".join(lines)


def from_concept(nodes: list, edges: list) -> str:
    """Mermaid ``graph LR`` source for the AI concept (knowledge) graph, or ''.

    ``nodes``: list of ``{"id": str, "kind": str}``.
    ``edges``: list of ``{"source": str, "relation": str, "target": str}``.
    """
    safe: dict[str, str] = {}      # real id -> mermaid token
    classes: dict[str, str] = {}   # mermaid token -> classDef name
    lines = ["graph LR"]

    def _tok(real_id, kind: str = "concept") -> str | None:
        real_id = str(real_id).strip()
        if not real_id:
            return None
        if real_id not in safe:
            if len(safe) >= _MAX_NODES:
                return None
            tok = f"n{len(safe)}"
            safe[real_id] = tok
            classes[tok] = _KIND_CLASS.get(str(kind).lower().strip(), "kconcept")
            lines.append(f'  {tok}["{_mlabel(real_id)}"]')
        return safe[real_id]

    for n in nodes or []:
        if isinstance(n, dict):
            _tok(n.get("id", ""), n.get("kind", "concept"))
        else:
            _tok(n)

    drawn = 0
    for e in edges or []:
        if drawn >= _MAX_EDGES or not isinstance(e, dict):
            continue
        s = _tok(e.get("source", ""))
        t = _tok(e.get("target", ""))
        if not s or not t or s == t:
            continue
        rel = _mlabel(e.get("relation", ""), limit=22)
        lines.append(f'  {s} -->|"{rel}"| {t}' if rel != "?" else f"  {s} --> {t}")
        drawn += 1

    return _finish(lines, len(safe), classes)


def from_depgraph(graph: dict | None) -> str:
    """Mermaid ``graph LR`` source for the deterministic import graph, or ''.

    Consumes ``depgraph.neighbors_for(...)`` output: ``changed`` (list) and
    ``edges`` (list of ``[importer, imported]``). Changed files are highlighted;
    edge direction is importer --> imported ("depends on").
    """
    if not isinstance(graph, dict):
        return ""
    edges = graph.get("edges") or []
    if not edges:
        return ""
    changed = set(graph.get("changed") or [])

    safe: dict[str, str] = {}
    classes: dict[str, str] = {}
    lines = ["graph LR"]

    def _tok(path) -> str | None:
        path = str(path).strip()
        if not path:
            return None
        if path not in safe:
            if len(safe) >= _MAX_NODES:
                return None
            tok = f"n{len(safe)}"
            safe[path] = tok
            classes[tok] = "kchanged" if path in changed else "kdep"
            lines.append(f'  {tok}["{_mlabel(path)}"]')
        return safe[path]

    drawn = 0
    for edge in edges:
        if drawn >= _MAX_EDGES:
            break
        try:
            importer, imported = edge[0], edge[1]
        except (TypeError, IndexError, KeyError):
            continue
        s = _tok(importer)
        t = _tok(imported)
        if not s or not t or s == t:
            continue
        lines.append(f"  {s} -->|depends on| {t}")
        drawn += 1

    return _finish(lines, len(safe), classes)


if __name__ == "__main__":  # tiny self-check
    demo_nodes = [
        {"id": "ManagedExitStrategy", "kind": "class"},
        {"id": "ExitConfig", "kind": "config"},
        {"id": "on_bar", "kind": "function"},
    ]
    demo_edges = [
        {"source": "ManagedExitStrategy", "relation": "reads", "target": "ExitConfig"},
        {"source": "ManagedExitStrategy", "relation": "runs", "target": "on_bar"},
    ]
    print(from_concept(demo_nodes, demo_edges))
    print("---")
    print(from_depgraph({
        "changed": ["core/managed_strategy.py"],
        "edges": [["core/backtest_runner/runner.py", "core/managed_strategy.py"]],
    }))