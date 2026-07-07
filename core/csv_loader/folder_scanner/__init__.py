"""Folder-level scan orchestration.

``scan_csv_folder`` tries each layout scanner in fallback order and returns the
first non-empty result; ``get_display_label`` formats an entry for the UI.
"""

from __future__ import annotations

import re
from pathlib import Path

from core.csv_loader.commodity_daily_scanner import _scan_commodity_daily_layout
from core.csv_loader.constants import DEFAULT_CSV_FOLDER
from core.csv_loader.crypto_nested_scanner import _scan_crypto_nested_layout
from core.csv_loader.fx_consolidated_scanner import _scan_fx_consolidated_files
from core.csv_loader.fx_daily_scanner import _scan_fx_daily_layout
from core.csv_loader.index_daily_scanner import _scan_index_daily_layout


def scan_csv_folder(folder: str = DEFAULT_CSV_FOLDER) -> list[dict]:
    """
    Scan a folder for all CSV files and return metadata for each.

    Tries to parse the crypto naming pattern {id}_{SYMBOL}_{Name}.csv
    for backward compatibility. For files that don't match, basic
    metadata is still returned so the frontend can apply its own
    pattern matching based on the selected asset class.

    When the folder contains no direct CSV files but does contain the
    FX daily-file layout (<PAIR>/YYYY/MM/DD/*.csv), files are aggregated
    by pair and returned as **three entries per pair** (ASK, BID, MID)
    so each side is independently selectable in the UI. The same
    aggregation also applies to the commodity daily-file layout
    (<COMMODITY>/YYYY/MM/DD/(ASK|BID).csv), which is tried as a final
    fallback after the FX scanners return empty.

    Returns
    -------
    list[dict]
        Each dict has keys: path, filename, id, symbol, name.
        Aggregated FX/commodity entries also carry:
        side ("ASK"|"BID"|"MID"), ask_files (list), bid_files (list),
        aggregated (True).
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        return []

    # Try the consolidated FX layout first — one CSV per pair per side at
    # or near the root. Inline ingest, no background jobs. If anything
    # matches, return immediately so this layout never gets shadowed by
    # the legacy crypto-flat heuristic for filenames like
    # "EURUSD_EURUSD_..._ASK_OHLCV.csv".
    fx_consolidated = _scan_fx_consolidated_files(folder_path)
    if fx_consolidated:
        return fx_consolidated

    results = []
    crypto_pattern = re.compile(r"^(\d+)_([A-Z]+)_(.+)\.csv$")
    auto_id = 0

    for csv_file in sorted(folder_path.glob("*.csv")):
        match = crypto_pattern.match(csv_file.name)
        if match:
            results.append({
                "path": str(csv_file),
                "filename": csv_file.name,
                "id": int(match.group(1)),
                "symbol": match.group(2),
                "name": match.group(3),
            })
        else:
            auto_id -= 1  # negative IDs for non-crypto files to avoid collisions
            stem = csv_file.stem
            results.append({
                "path": str(csv_file),
                "filename": csv_file.name,
                "id": auto_id,
                "symbol": stem.split("_")[0] if "_" in stem else stem,
                "name": stem,
            })

    # Fall back to the daily-layout aggregator only when the flat scan
    # found nothing — otherwise a directory like Fx_single_file wouldn't
    # behave as before.
    if not results:
        results = _scan_fx_daily_layout(folder_path)

    # Final fallback: commodity daily layout (<COMMODITY>/YYYY/MM/DD/(ASK|BID).csv).
    # Runs only when neither flat nor FX-daily produced anything, so existing
    # roots stay on their current code path.
    if not results:
        results = _scan_commodity_daily_layout(folder_path)

    # Index daily layout
    # (<root>/YYYY/MM/DD.MM.YYYY_complete_df_OHLCV.csv). Three-deep, no symbol
    # directory, single consolidated stream.
    if not results:
        results = _scan_index_daily_layout(folder_path)

    # Last-resort fallback: nested crypto layout where the scanned root is the
    # venue folder (<root>/<BASE-QUOTE>/YYYY/MM/<daily>.csv; venue = root name).
    # Runs only when every earlier scanner came up empty — their filename
    # regexes never match crypto daily files, and the 4-deep tree doesn't
    # collide with the FX daily (5-deep) or index (3-deep) layouts, so a crypto
    # tree falls through to here while the others are claimed before reaching it.
    if not results:
        results = _scan_crypto_nested_layout(folder_path)

    return results


def get_display_label(entry: dict) -> str:
    """Create a display label like 'BTC - Bitcoin (1_BTC_Bitcoin.csv)'."""
    return f"{entry['symbol']} - {entry['name']} ({entry['filename']})"
