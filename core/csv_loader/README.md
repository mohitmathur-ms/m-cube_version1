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

## Hardcoded values (no UI field feeds these)

Values below are baked into the package's `__init__.py` files. None has a box on
the main Load-Data UI that lets a user supply the real value; the override column
notes whether *any* indirect path exists. "None" means source-edit only.

The classification was derived by tracing how loader parameters reach this
package: the main caller `core/nautilus_loader.py:load_csv_and_store` pulls
`timestamp_column`, `required_columns`, `optional_columns` and `delimiter` from
the asset-class `adapter_admin/data_formats/<class>.json` config (an
admin-editable file, **not** an end-user UI box). The data **folder** is the
only value the Load-Data page truly supplies. Everything else listed here has no
UI path at all.

Override-path legend:
- **None** — only changeable by editing source.
- **kwarg** — overridable via a function argument, but nothing in the app passes it.
- **data_format JSON** — overridable via `adapter_admin/data_formats/<class>.json` (admin file, no end-user UI box).
- **UI folder field** — the one value the Load-Data page supplies.

### `constants/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| `_CONCAT_SIDE_MAX_WORKERS` | `16` | Thread-pool size for parallel daily-file reads in `concat_side`. | kwarg (`max_workers`) only |
| `DEFAULT_CSV_FOLDER` | `r"D:\Data_all\Fx"` | Folder scanned when none is given. | UI folder field |
| `QUANTITY_MAX` | `16_000_000_000.0` | Volume cap (below Nautilus `QUANTITY_RAW_MAX`) applied in `load_csv` and the MID merge. | None |
| `_OHLCV_LOWER` | `("open","high","low","close","volume")` | Canonical output column names. | None |
| `_SYMBOL_NORMALIZE` | `{"EUROUSD":"EURUSD","GPBUSD":"GBPUSD"}` | Fixed misspelling-fix map for source pair names. | None |
| `_DAILY_FX_PATTERN` | `DD.MM.YYYY_(BID\|ASK)_OHLCV.csv` | FX daily filename regex. | None |
| `_DAILY_COMMODITY_PATTERN` | `(ASK\|BID).csv` | Commodity daily filename regex. | None |
| `_DAILY_INDEX_PATTERN` | `DD.MM.YYYY_complete_df_OHLCV.csv` | Index daily filename regex. | None |
| `_CRYPTO_YEAR_DIR_PATTERN` / `_CRYPTO_MONTH_DIR_PATTERN` | `^\d{4}$` / `^\d{1,2}$` | Year/month directory-name gates for the crypto layout. | None |
| `_FLAT_FX_FILE_PATTERN` | `{PAIR}_{PAIR}_DDMMMYYYY_DDMMMYYYY_(ASK\|BID\|MID)_OHLCV.csv` | Consolidated-FX filename regex. | None |
| `_MONTH_ABBR` | `JAN…DEC` | Month-abbreviation table for synthetic display filenames. | None |

### `scan_cache/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| `_FX_SCAN_CACHE_TTL_SECONDS` and the COMMODITY / INDEX / CRYPTO siblings | `600.0` each | Safety-net TTL for each daily-layout scan cache (mtime is the real key). | None (call `clear_fx_scan_cache()` to force refresh) |

### `fx_daily_scanner/` and `commodity_daily_scanner/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| Synthetic date range | `01JAN2015_31DEC2025` | Display-only placeholder embedded in the synthetic filename; actual data is loaded from `ask_files`/`bid_files`, not this range. | None |
| Side iteration order | `("ASK","BID","MID")` | Order the three per-pair entries are emitted. | None |
| `rglob` glob | `*.csv` | Files considered during the walk. | None |
| `name` strings | `"{symbol} {side} ({n} daily files)"` | UI display label. | None |

### `index_daily_scanner/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| Fallback symbol | `"INDEX"` | Used when the folder name cleans to empty (symbol is otherwise derived from the folder name). | None (rename folder to change derived symbol) |
| Depth gate | `len(rel_parts) != 3` | Requires exactly `<root>/YYYY/MM/<file>`. | None |
| Synthetic price-type token | `LAST` | Token in the synthetic display filename (downstream uses `price_type="LAST"`). | None |

### `crypto_nested_scanner/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| Depth gate | `len(rel_parts) != 4` | Requires `<root>/<BASE-QUOTE>/YYYY/MM/<file>`. | None |
| Pair separator | `"-"` | Splits the pair dir into base/quote. | None |
| Venue source | `root.name.upper()` | Venue is the scanned folder name. | UI folder field (pick which venue folder) |
| Synthetic filename / `LAST` token | `{base}-{quote}_{VENUE}_LAST_OHLCV.csv` | Display-only filename. | None |

### `fx_consolidated_scanner/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| Scan depth | root `*.csv` + one level `*/*.csv` | Intentionally shallow; deeper trees are ignored here. | None |
| `name` string | `"{symbol} {side} (consolidated)"` | UI display label. | None |

### `folder_scanner/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| Legacy crypto-flat regex | `^(\d+)_([A-Z]+)_(.+)\.csv$` | `{id}_{SYMBOL}_{Name}.csv` flat layout match. | None |
| Non-match symbol rule | `stem.split("_")[0]` | Symbol guess for files that don't match any pattern. | None |
| Scanner fallback order | consolidated → flat → FX-daily → commodity → index → crypto | Order layouts are tried; first non-empty wins. | None |

### `timestamp_parser/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| FX-format detection heuristic | `"GMT" in sample and "." in sample.split(" ")[0]` | Decides whether to use the fast PyArrow path. | None |
| Character-slice offsets | day `0:2`, month `3:5`, year `6:10`, time `11:-9`, offset `-5:None` | Fixed positions assuming `DD.MM.YYYY HH:MM:SS[.fff] GMT±HHMM`. | None |

### `csv_reader/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| `timestamp_column` default | `"ts"` | Source timestamp column name. | data_format JSON |
| `delimiter` default | `","` | CSV delimiter. | data_format JSON |
| `required_columns` default | `[ts, *OHLCV]` | Columns that must exist. | data_format JSON |
| OHLCV column dtype | `pa.float64()` | Forced numeric type for OHLCV + optional columns. | None |

### `side_concat/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| Worker default | `min(_CONCAT_SIDE_MAX_WORKERS, len(files))` | Pool size (caps at 16). | kwarg only |
| Duplicate policy | `keep="first"` | Which row survives on duplicate timestamps. | None |

### `mid_merge/`
| Value | Literal | Meaning | Override |
|---|---|---|---|
| MID OHLC formula | `(ask + bid) * 0.5` | Mid price per O/H/L/C. | None |
| MID volume formula | `ask + bid` (capped at `QUANTITY_MAX`) | Mid volume. | None |
| Collision rule | ASK side wins | For non-OHLCV extras present on both sides. | None |
| Join | inner on timestamp index | One-sided rows are dropped. | None |
