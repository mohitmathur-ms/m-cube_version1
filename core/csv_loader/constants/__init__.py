"""Shared constants for the csv_loader package.

Holds the tunables, the volume cap, symbol-normalisation map, the filename /
directory regex patterns each scanner gates on, and the month-abbreviation
table used to shape synthetic display filenames.
"""

from __future__ import annotations

import re

# Worker pool size for parallel daily-file reads in :func:`concat_side`.
# 16 is a good default for SSDs/NVMe (pandas releases the GIL inside the
# C parser, so threads overlap cleanly). Lower it for HDDs; raise for
# arrays. Tunable per-call via the ``max_workers`` kwarg.
_CONCAT_SIDE_MAX_WORKERS = 16

# Default path to the user's crypto CSV data
DEFAULT_CSV_FOLDER = r"D:\Data_all\Fx"

# NautilusTrader QUANTITY_RAW_MAX is 18_446_744_072.999999488
# Use a safe value well below this to avoid Rust panics
QUANTITY_MAX = 16_000_000_000.0

# OHLCV column names (lower-case) shared by the reader and the MID merge.
_OHLCV_LOWER = ("open", "high", "low", "close", "volume")

# Common symbol-name misspellings in the source data we've observed.
# Normalize so downstream instrument creation uses ISO-like pair names.
_SYMBOL_NORMALIZE = {"EUROUSD": "EURUSD", "GPBUSD": "GBPUSD"}

# FX daily-file naming: "DD.MM.YYYY_{BID|ASK}_OHLCV.csv"
_DAILY_FX_PATTERN = re.compile(
    r"^(\d{2})\.(\d{2})\.(\d{4})_(BID|ASK)_OHLCV\.csv$",
    re.IGNORECASE,
)

# Commodity daily-file naming: bare "ASK.csv" / "BID.csv". The date is encoded
# in the parent directory path (<COMMODITY>/YYYY/MM/DD/), not the filename, so
# the regex captures only the side.
_DAILY_COMMODITY_PATTERN = re.compile(r"^(ASK|BID)\.csv$", re.IGNORECASE)

# Index daily-file naming: "DD.MM.YYYY_complete_df_OHLCV.csv" — one consolidated
# file per trading day (no ASK/BID split — the index level itself is the bar
# stream). Layout is three-deep with no symbol directory:
# <root>/YYYY/MM/<DD.MM.YYYY>_complete_df_OHLCV.csv
_DAILY_INDEX_PATTERN = re.compile(
    r"^(\d{2})\.(\d{2})\.(\d{4})_complete_df_OHLCV\.csv$",
    re.IGNORECASE,
)

# Nested-crypto layout gates: <root>/<venue>/<BASE-QUOTE>/YYYY/MM/<daily>.csv.
# The date lives entirely in the parent directories (a 4-digit YEAR dir then a
# 1-2 digit MONTH dir), so the daily filename itself is unconstrained — these
# two patterns match the year/month directory names instead. This is also what
# distinguishes the layout from the FX daily tree (<PAIR>/YYYY/MM/DD/...), where
# the directory at depth index 2 is a 2-digit month, not a 4-digit year.
_CRYPTO_YEAR_DIR_PATTERN = re.compile(r"^\d{4}$")
_CRYPTO_MONTH_DIR_PATTERN = re.compile(r"^\d{1,2}$")

# Months → 3-letter abbreviation used in the synthetic display filename produced
# by :func:`_scan_index_daily_layout`. Matches the convention in the consolidated
# FX naming (e.g. "01JAN2020_31DEC2025") so the index display filename slots
# cleanly into the asset-class filter the UI applies.
_MONTH_ABBR = (
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
)

# Consolidated-FX naming: "{PAIR}_{PAIR}_DDMMMYYYY_DDMMMYYYY_{ASK|BID|MID}_OHLCV.csv"
# (matches the pattern documented in adapter_admin/data_formats/fx.json). One file
# per pair per side, replacing the thousands-of-daily-files layout. Captured groups
# are (pair, side).
_FLAT_FX_FILE_PATTERN = re.compile(
    r"^([A-Z]+)_\1_\d{2}[A-Z]{3}\d{4}_\d{2}[A-Z]{3}\d{4}_(ASK|BID|MID)_OHLCV\.csv$"
)
