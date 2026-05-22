"""Load OHLCV data from local CSV files.

Scans a folder of CSV files and loads them into clean pandas DataFrames ready
for NautilusTrader wrangling. Two layouts are supported:

* Flat crypto layout: `{id}_{SYMBOL}_{Name}.csv` directly under the folder.
* FX daily layout: `<root>/<PAIR>/YYYY/MM/DD/DD.MM.YYYY_(BID|ASK)_OHLCV.csv`.
  For the FX layout the scanner emits **three entries per pair** — one each
  for ASK, BID and MID — so each side becomes its own selectable instrument
  in the UI and its own BarType key in the parquet catalog. MID is
  synthesized from ASK+BID at load time.

This package decomposes the former monolithic ``core/csv_loader.py`` into
single-responsibility components (see README.md). The public API is unchanged:
``scan_csv_folder``, ``get_display_label``, ``load_csv``, ``concat_side``,
``load_pair_mid``, ``clear_fx_scan_cache``, plus ``DEFAULT_CSV_FOLDER`` and
``QUANTITY_MAX``. The scanners and ``_merge_ask_bid_to_mid`` / ``_parse_timestamps``
helpers are re-exported here too for backward compatibility (e.g.
``tests/test_perf_regression.py`` calls ``csv_loader._scan_fx_daily_layout``).
"""

from __future__ import annotations

from core.csv_loader.commodity_daily_scanner import _scan_commodity_daily_layout
from core.csv_loader.constants import DEFAULT_CSV_FOLDER, QUANTITY_MAX
from core.csv_loader.crypto_nested_scanner import _scan_crypto_nested_layout
from core.csv_loader.csv_reader import load_csv
from core.csv_loader.folder_scanner import get_display_label, scan_csv_folder
from core.csv_loader.fx_consolidated_scanner import _scan_fx_consolidated_files
from core.csv_loader.fx_daily_scanner import _scan_fx_daily_layout
from core.csv_loader.index_daily_scanner import (
    _index_symbol_from_root,
    _scan_index_daily_layout,
)
from core.csv_loader.mid_merge import _merge_ask_bid_to_mid, load_pair_mid
from core.csv_loader.scan_cache import clear_fx_scan_cache
from core.csv_loader.side_concat import concat_side
from core.csv_loader.timestamp_parser import _parse_timestamps

__all__ = [
    # Public API
    "DEFAULT_CSV_FOLDER",
    "QUANTITY_MAX",
    "scan_csv_folder",
    "get_display_label",
    "load_csv",
    "concat_side",
    "load_pair_mid",
    "clear_fx_scan_cache",
    # Re-exported internals (back-compat for tests / introspection)
    "_parse_timestamps",
    "_merge_ask_bid_to_mid",
    "_scan_fx_consolidated_files",
    "_scan_fx_daily_layout",
    "_scan_commodity_daily_layout",
    "_scan_index_daily_layout",
    "_scan_crypto_nested_layout",
    "_index_symbol_from_root",
]
