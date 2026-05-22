"""Scanner for one-file-per-side consolidated FX CSVs."""

from __future__ import annotations

from pathlib import Path

from core.csv_loader.constants import _FLAT_FX_FILE_PATTERN, _SYMBOL_NORMALIZE


def _scan_fx_consolidated_files(root: Path) -> list[dict]:
    """Discover one-file-per-side consolidated FX CSVs.

    Each match produces a single entry that flows through the inline
    single-file path in :func:`core.nautilus_loader.load_csv_and_store`
    (no ``aggregated`` flag, no ``ask_files`` / ``bid_files`` lists), so
    ingest stays in-process and skips the background-job machinery
    entirely. The downstream loader picks ``price_type`` straight from
    ``entry["side"]``.

    Searched scope is intentionally shallow — the root and one level
    below — so we don't traverse a daily-layout tree (thousands of
    nested files) hunting for files that only ever live near the top.
    """
    seen: set[Path] = set()
    candidates: list[Path] = []
    for path in root.glob("*.csv"):
        if path.is_file():
            candidates.append(path)
            seen.add(path)
    for path in root.glob("*/*.csv"):
        if path.is_file() and path not in seen:
            candidates.append(path)

    entries: list[dict] = []
    auto_id = 0
    for path in sorted(candidates):
        match = _FLAT_FX_FILE_PATTERN.match(path.name)
        if not match:
            continue
        symbol_raw, side = match.group(1), match.group(2)
        symbol = _SYMBOL_NORMALIZE.get(symbol_raw, symbol_raw)
        auto_id -= 1
        entries.append({
            "path": str(path),
            "filename": path.name,
            "id": auto_id,
            "symbol": symbol,
            "side": side,
            "name": f"{symbol} {side} (consolidated)",
        })
    return entries
