"""Scanner for the daily commodity layout (<COMMODITY>/YYYY/MM/DD/(ASK|BID).csv)."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from core.csv_loader.constants import _DAILY_COMMODITY_PATTERN
from core.csv_loader.scan_cache import (
    _COMMODITY_SCAN_CACHE,
    _COMMODITY_SCAN_CACHE_LOCK,
    _COMMODITY_SCAN_CACHE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)


def _scan_commodity_daily_layout(root: Path) -> list[dict]:
    """Aggregate daily commodity CSVs (<root>/<COMMODITY>/YYYY/MM/DD/(ASK|BID).csv)
    into three entries per symbol: ASK, BID and MID.

    Mirrors :func:`_scan_fx_daily_layout` so the produced entry shape feeds
    cleanly into :func:`core.nautilus_loader.load_csv_and_store` (it dispatches
    on ``entry["side"]`` + ``ask_files`` / ``bid_files`` + ``aggregated``).
    The only structural differences from the FX scanner:

    * The filename pattern (:data:`_DAILY_COMMODITY_PATTERN`) captures only
      the side — the date lives in the parent dirs instead of the filename.
    * No symbol normalisation: commodities don't share the FX misspelling
      quirks that :data:`_SYMBOL_NORMALIZE` handles.
    """
    key = str(root)
    try:
        root_mtime = root.stat().st_mtime
    except OSError:
        return []

    now = time.monotonic()
    cached = _COMMODITY_SCAN_CACHE.get(key)
    if (cached is not None
            and cached[1] == root_mtime
            and now - cached[0] < _COMMODITY_SCAN_CACHE_TTL_SECONDS):
        return list(cached[2])

    aggregated: dict[tuple[str, str], list[str]] = {}
    for csv_file in root.rglob("*.csv"):
        match = _DAILY_COMMODITY_PATTERN.match(csv_file.name)
        if not match:
            continue
        try:
            rel_parts = csv_file.relative_to(root).parts
        except ValueError:
            continue
        if len(rel_parts) < 2:
            # File directly in root — not a daily-layout file.
            continue
        symbol = rel_parts[0].upper()
        side = match.group(1).upper()
        aggregated.setdefault((symbol, side), []).append(str(csv_file))

    pairs: dict[str, dict[str, list[str]]] = {}
    for (symbol, side), files in aggregated.items():
        pairs.setdefault(symbol, {})[side] = sorted(files)

    entries: list[dict] = []
    auto_id = 0
    for symbol in sorted(pairs):
        sides = pairs[symbol]
        ask_files = sides.get("ASK", [])
        bid_files = sides.get("BID", [])
        if not ask_files or not bid_files:
            missing = "ASK" if not ask_files else "BID"
            logger.warning("commodity_scan skip %s: missing %s side", symbol, missing)
            continue
        total_files = len(ask_files) + len(bid_files)
        for entry_side in ("ASK", "BID", "MID"):
            auto_id -= 1
            # Shape the synthetic filename like the consolidated FX naming so the
            # commodity.json filename_pattern (and the FX-style UI filter) still
            # accept it. The date range is a display placeholder — actual files
            # are loaded via ask_files / bid_files.
            synth_filename = (
                f"{symbol}_{symbol}_01JAN2015_31DEC2025_{entry_side}_OHLCV.csv"
            )
            nominal_path = (bid_files if entry_side == "BID" else ask_files)[0]
            entries.append({
                "path": nominal_path,
                "filename": synth_filename,
                "id": auto_id,
                "symbol": symbol,
                "side": entry_side,
                "name": f"{symbol} {entry_side} ({total_files:,} daily files)",
                "ask_files": ask_files,
                "bid_files": bid_files,
                "aggregated": True,
            })

    with _COMMODITY_SCAN_CACHE_LOCK:
        _COMMODITY_SCAN_CACHE[key] = (now, root_mtime, list(entries))
    return entries
