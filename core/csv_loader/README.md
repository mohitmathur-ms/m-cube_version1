# `core/csv_loader`

Loads OHLCV data from local CSV files: discovers the layout of a data folder,
reads/cleans individual CSVs into pandas DataFrames, concatenates daily files
per side, and synthesizes MID from ASK+BID — ready for NautilusTrader
wrangling. This package was split out of the former monolithic
`core/csv_loader.py`; each subdirectory owns one logical concern, with that
concern's logic living directly in the directory's `__init__.py`.

## Public API (unchanged after the split)

```python
from core.csv_loader import (
    scan_csv_folder, get_display_label,      # discovery
    load_csv, concat_side, load_pair_mid,    # loading
    session_window_from_df,                  # session window derivation
    clear_fx_scan_cache,                     # cache control
    DEFAULT_CSV_FOLDER, QUANTITY_MAX,        # constants
)
```

External consumers: `server.py` (`scan_csv_folder`, `get_display_label`,
`DEFAULT_CSV_FOLDER`), `core/nautilus_loader.py` (`load_csv`, `concat_side`,
`load_pair_mid`, `QUANTITY_MAX`), `scripts/ingest_fx_bulk.py` (`load_csv`),
and tests (`QUANTITY_MAX`, `clear_fx_scan_cache`, `_scan_fx_daily_layout`).

The scanners and `_merge_ask_bid_to_mid` / `_parse_timestamps` are also
re-exported from the package root for backward compatibility with tests and
introspection.

## Components

| Directory | Responsibility | Key symbols |
|---|---|---|
| `constants/` | Tunables, volume cap, symbol-normalisation, regex/dir patterns, month table, OHLCV column names. | `DEFAULT_CSV_FOLDER`, `QUANTITY_MAX`, `_OHLCV_LOWER`, `_SYMBOL_NORMALIZE`, `_*_PATTERN`, `_MONTH_ABBR` |
| `scan_cache/` | The four mtime+TTL scan caches (FX/commodity/index/crypto) and the function that clears them all. | `_FX_SCAN_CACHE` (+ siblings), `clear_fx_scan_cache` |
| `fx_consolidated_scanner/` | Discover one-file-per-side consolidated FX CSVs (shallow scan). | `_scan_fx_consolidated_files` |
| `fx_daily_scanner/` | Aggregate the FX daily tree into ASK/BID/MID entries per pair. | `_scan_fx_daily_layout` |
| `commodity_daily_scanner/` | Same as FX daily, for the commodity layout (bare `ASK.csv`/`BID.csv`). | `_scan_commodity_daily_layout` |
| `index_daily_scanner/` | Aggregate single-stream daily index CSVs into one entry. | `_scan_index_daily_layout`, `_index_symbol_from_root` |
| `crypto_nested_scanner/` | Aggregate nested crypto CSVs (root = venue folder) into one entry per pair. | `_scan_crypto_nested_layout` |
| `folder_scanner/` | Orchestrator: try each layout scanner in fallback order; format display labels. | `scan_csv_folder`, `get_display_label` |
| `timestamp_parser/` | Parse a timestamp column to UTC (fast PyArrow path for the FX GMT format). | `_parse_timestamps` |
| `csv_reader/` | Read a single CSV into a clean OHLCV DataFrame (PyArrow read, volume cap). | `load_csv` |
| `side_concat/` | Concatenate one side's daily files in timestamp order (parallel thread pool). | `concat_side` |
| `mid_merge/` | Row-wise MID from ASK+BID; load a pair's MID from its file lists. | `_merge_ask_bid_to_mid`, `load_pair_mid` |
| `session_window/` | Derive a venue's daily session window (UTC time-of-day extremes) from loaded bars; persisted per venue at ingest by `core.venue_config.update_venue_session_window`. | `session_window_from_df` |

## Dependency graph (acyclic)

```
constants ◄── (everything)
scan_cache ◄── fx_daily / commodity_daily / index_daily / crypto_nested scanners
   fx_consolidated / fx_daily / commodity_daily / index_daily / crypto_nested
        └──► folder_scanner ──► __init__ (public API)
timestamp_parser ──► csv_reader ──► side_concat ──► mid_merge ──► __init__
```

The scan caches are shared mutable dicts: each daily scanner imports the same
cache/lock objects from `scan_cache/` and mutates them in place (never rebinds),
so `clear_fx_scan_cache()` drops every scanner's cache.
