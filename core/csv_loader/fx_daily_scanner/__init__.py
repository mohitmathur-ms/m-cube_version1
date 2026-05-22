"""Scanner for the daily FX layout (<PAIR>/YYYY/MM/DD/DD.MM.YYYY_(BID|ASK)_OHLCV.csv)."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from core.csv_loader.constants import _DAILY_FX_PATTERN, _SYMBOL_NORMALIZE
from core.csv_loader.scan_cache import (
    _FX_SCAN_CACHE,
    _FX_SCAN_CACHE_LOCK,
    _FX_SCAN_CACHE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)


def _scan_fx_daily_layout(root: Path) -> list[dict]:
    """Aggregate daily FX CSVs (<root>/<PAIR>/YYYY/MM/DD/DD.MM.YYYY_(BID|ASK)_OHLCV.csv)
    into three entries per pair: ASK, BID and MID.

    Each entry carries both sides' file lists so the downstream loader can
    either concatenate one side directly (ASK or BID) or synthesize MID
    from ASK+BID via :func:`_merge_ask_bid_to_mid`. Pairs missing either
    side are skipped so the downstream merge always has something to join.

    Result is cached under :data:`_FX_SCAN_CACHE` with a TTL — repeated UI
    refreshes hit the cache instead of re-walking thousands of daily files.
    """
    key = str(root)
    try:
        root_mtime = root.stat().st_mtime
    except OSError:
        return []

    now = time.monotonic()
    cached = _FX_SCAN_CACHE.get(key)
    if (cached is not None
            and cached[1] == root_mtime
            and now - cached[0] < _FX_SCAN_CACHE_TTL_SECONDS):
        return list(cached[2])

    aggregated: dict[tuple[str, str], list[str]] = {}
    for csv_file in root.rglob("*.csv"):
        match = _DAILY_FX_PATTERN.match(csv_file.name)
        if not match:
            continue
        try:
            rel_parts = csv_file.relative_to(root).parts
        except ValueError:
            continue
        if len(rel_parts) < 2:
            # File directly in root — skip, handled by flat scan.
            continue
        pair_dir = rel_parts[0].upper()
        symbol = _SYMBOL_NORMALIZE.get(pair_dir, pair_dir)
        side = match.group(4).upper()
        aggregated.setdefault((symbol, side), []).append(str(csv_file))

    # Reshape into per-pair maps with both sides.
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
            logger.warning("fx_scan skip %s: missing %s side", symbol, missing)
            continue
        total_files = len(ask_files) + len(bid_files)
        for entry_side in ("ASK", "BID", "MID"):
            auto_id -= 1
            # Synthetic filename shaped like the consolidated format so the
            # fx.json filename_pattern still accepts it for UI filtering.
            synth_filename = (
                f"{symbol}_{symbol}_01JAN2015_31DEC2025_{entry_side}_OHLCV.csv"
            )
            nominal_path = (bid_files if entry_side == "BID" else ask_files)[0]
            entries.append({
                "path": nominal_path,  # nominal — loader uses ask_files / bid_files
                "filename": synth_filename,
                "id": auto_id,
                "symbol": symbol,
                "side": entry_side,
                "name": f"{symbol} {entry_side} ({total_files:,} daily files)",
                "ask_files": ask_files,
                "bid_files": bid_files,
                "aggregated": True,
            })

    with _FX_SCAN_CACHE_LOCK:
        _FX_SCAN_CACHE[key] = (now, root_mtime, list(entries))
    return entries
