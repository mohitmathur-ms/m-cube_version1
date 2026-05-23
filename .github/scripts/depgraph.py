"""Deterministic first-party import scanner for the push-report dependency graph.

Pure stdlib (no third-party deps). Walks the repo's Python files, parses their
`import` / `from ... import` statements with the `ast` module, and resolves each
to an *in-repo* target file. From that it builds a directed import graph using
the convention:

    edge (importer -> imported)  == "importer DEPENDS ON imported"

`neighbors_for(...)` then slices that graph down to the files changed in a single
push plus their direct (1-hop) neighbours, which is what the report renders.

Nothing here calls out to the network or the AI backend, so the dependency graph
is produced even when the report degrades to metadata-only mode.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path

# Directories we never treat as first-party source.
_SKIP_DIRS = {
    "venv", ".venv", "env", ".git", "__pycache__", "node_modules",
    "html_reports", ".github", "catalog", ".pytest_cache", ".mypy_cache",
    "build", "dist", ".idea", ".vscode",
}


def _iter_py_files(repo_root: Path):
    """Yield every first-party .py file under repo_root (pruning _SKIP_DIRS)."""
    for dirpath, dirnames, filenames in os.walk(repo_root):
        # Prune skipped directories in place so os.walk doesn't descend into them.
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for name in filenames:
            if name.endswith(".py"):
                yield Path(dirpath) / name


def _rel(repo_root: Path, path: Path) -> str:
    """Repo-relative POSIX path string (stable across OSes)."""
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def _module_name_for(repo_root: Path, path: Path) -> str:
    """Dotted module name for a file, e.g. core/models/__init__.py -> 'core.models',
    core/backtest_runner.py -> 'core.backtest_runner'."""
    rel = path.resolve().relative_to(repo_root.resolve())
    parts = list(rel.parts)
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1][:-3]  # strip ".py"
    return ".".join(parts)


def build_module_index(repo_root: str | Path) -> dict[str, str]:
    """Map dotted module name -> repo-relative file path for all first-party modules.

    Both the package (``core.models``) and its ``__init__`` file are indexed so
    ``from core.models import X`` and ``import core.models`` both resolve.
    """
    repo_root = Path(repo_root)
    index: dict[str, str] = {}
    for path in _iter_py_files(repo_root):
        try:
            mod = _module_name_for(repo_root, path)
        except ValueError:
            continue  # outside repo_root; ignore
        if mod:
            index[mod] = _rel(repo_root, path)
    return index


def _resolve(module: str, index: dict[str, str]) -> str | None:
    """Resolve a dotted module string to an in-repo file path, or None.

    Tries the exact name first, then walks up parents so that
    ``core.models.exit_config.ExitConfig`` still resolves to the deepest module
    that actually exists in the index (handles ``from pkg.mod import Symbol``).
    """
    parts = module.split(".")
    for i in range(len(parts), 0, -1):
        candidate = ".".join(parts[:i])
        if candidate in index:
            return index[candidate]
    return None


def _abs_module_for_relative(pkg_parts: list[str], node: ast.ImportFrom) -> str:
    """Resolve a relative import (``from . import x`` / ``from ..pkg import y``)
    to an absolute dotted module, using the importing file's package parts."""
    # level 1 == current package; each extra level pops one parent.
    base = pkg_parts[: len(pkg_parts) - (node.level - 1)] if node.level >= 1 else pkg_parts
    if node.module:
        base = base + node.module.split(".")
    return ".".join(base)


def _imports_in(path: Path, repo_root: Path) -> list[str]:
    """Return the list of resolvable absolute dotted modules a file imports."""
    try:
        # utf-8-sig transparently strips a leading BOM (several repo scripts have one).
        source = path.read_text(encoding="utf-8-sig")
        tree = ast.parse(source, filename=str(path))
    except (SyntaxError, UnicodeDecodeError, OSError) as exc:
        print(f"[depgraph] skipped {path}: {exc}")
        return []

    # Package parts of the importing file, used to resolve relative imports.
    rel_parts = path.resolve().relative_to(repo_root.resolve()).parts
    pkg_parts = list(rel_parts[:-1])  # drop the filename -> the containing package

    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                modules.append(_abs_module_for_relative(pkg_parts, node))
            elif node.module:
                # Also probe ``module.symbol`` so symbol-style imports resolve.
                modules.append(node.module)
                modules.extend(f"{node.module}.{a.name}" for a in node.names)
    return modules


def build_edges(repo_root: str | Path, index: dict[str, str]) -> set[tuple[str, str]]:
    """Build the set of (importer_path, imported_path) edges across the repo.

    Self-edges and unresolved (third-party / stdlib) imports are dropped.
    """
    repo_root = Path(repo_root)
    edges: set[tuple[str, str]] = set()
    for path in _iter_py_files(repo_root):
        src = _rel(repo_root, path)
        for module in _imports_in(path, repo_root):
            target = _resolve(module, index)
            if target and target != src:
                edges.add((src, target))
    return edges


def neighbors_for(
    changed_files: list[str],
    edges: set[tuple[str, str]],
    *,
    index: dict[str, str] | None = None,
) -> dict:
    """Slice the full edge set down to the changed .py files + their 1-hop neighbours.

    Returns a dict consumed by depgraph_svg:

        {
          "changed": [<changed .py paths that are in the import graph>],
          "non_py":  [<changed paths that aren't .py / not first-party>],
          "per_file": {
              path: {"depends_on": [...], "depended_on_by": [...]}
          },
          "nodes": [...],                # changed + neighbours, de-duplicated
          "edges": [[importer, imported], ...],  # only edges touching a changed file
        }
    """
    known = set(index.values()) if index is not None else None
    changed_py, non_py = [], []
    for f in changed_files:
        f = f.strip()
        if not f:
            continue
        if f.endswith(".py") and (known is None or f in known):
            changed_py.append(f)
        else:
            non_py.append(f)

    changed_set = set(changed_py)
    per_file: dict[str, dict[str, list[str]]] = {
        f: {"depends_on": [], "depended_on_by": []} for f in changed_py
    }
    sliced: set[tuple[str, str]] = set()

    for importer, imported in edges:
        if imported in changed_set:                 # someone depends on a changed file
            per_file[imported]["depended_on_by"].append(importer)
            sliced.add((importer, imported))
        if importer in changed_set:                 # a changed file depends on someone
            per_file[importer]["depends_on"].append(imported)
            sliced.add((importer, imported))

    for f in per_file.values():
        f["depends_on"] = sorted(set(f["depends_on"]))
        f["depended_on_by"] = sorted(set(f["depended_on_by"]))

    nodes: set[str] = set(changed_py)
    for importer, imported in sliced:
        nodes.add(importer)
        nodes.add(imported)

    return {
        "changed": sorted(changed_py),
        "non_py": sorted(non_py),
        "per_file": per_file,
        "nodes": sorted(nodes),
        "edges": sorted([list(e) for e in sliced]),
    }


def graph_for_push(repo_root: str | Path, changed_files: list[str]) -> dict:
    """Convenience one-shot: index -> edges -> sliced neighbour graph for a push."""
    index = build_module_index(repo_root)
    edges = build_edges(repo_root, index)
    return neighbors_for(changed_files, edges, index=index)


if __name__ == "__main__":  # tiny self-check
    import sys

    root = sys.argv[1] if len(sys.argv) > 1 else "."
    idx = build_module_index(root)
    ed = build_edges(root, idx)
    print(f"[depgraph] {len(idx)} modules, {len(ed)} edges")
