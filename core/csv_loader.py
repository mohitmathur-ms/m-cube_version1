"""
Load crypto OHLCV data from local CSV files.

Scans a folder of CSV files (format: {id}_{SYMBOL}_{Name}.csv) and loads them
into clean pandas DataFrames ready for NautilusTrader wrangling.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


# Default path to the user's crypto CSV data
DEFAULT_CSV_FOLDER = r"C:\Users\HP\Desktop\MS\Dataset\id_name_all_symbols"

# NautilusTrader QUANTITY_RAW_MAX is 18_446_744_072.999999488
# Use a safe value well below this to avoid Rust panics
QUANTITY_MAX = 16_000_000_000.0


# Common symbol-name misspellings in the source data we've observed.
# Normalize so downstream instrument creation uses ISO-like pair names.
_SYMBOL_NORMALIZE = {"EUROUSD": "EURUSD", "GPBUSD": "GBPUSD"}

# FX daily-file naming: "DD.MM.YYYY_{BID|ASK}_OHLCV.csv"
_DAILY_FX_PATTERN = re.compile(
    r"^(\d{2})\.(\d{2})\.(\d{4})_(BID|ASK)_OHLCV\.csv$",
    re.IGNORECASE,
)


def _scan_fx_daily_layout(root: Path) -> list[dict]:
    """Aggregate daily FX CSVs (<root>/<PAIR>/YYYY/MM/DD/DD.MM.YYYY_(BID|ASK)_OHLCV.csv)
    into one virtual entry per (pair, side).

    Each entry carries a `files` list consumed by load_csv_and_store. The
    synthetic `filename` is shaped to match fx.json's single-file
    filename_pattern so the UI filter accepts it without special casing.
    """
    aggregated: dict[tuple[str, str], list[str]] = {}
    for csv_file in root.rglob("*.csv"):
        if not _DAILY_FX_PATTERN.match(csv_file.name):
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
        side = _DAILY_FX_PATTERN.match(csv_file.name).group(4).upper()
        aggregated.setdefault((symbol, side), []).append(str(csv_file))

    entries = []
    auto_id = 0
    for (symbol, side), files in sorted(aggregated.items()):
        files.sort()
        auto_id -= 1
        # Synthetic filename shaped like the consolidated format
        # (e.g. "EURUSD_EURUSD_01JAN2015_27JUN2025_ASK_OHLCV.csv") so the
        # fx.json filename_pattern accepts it without configuration changes.
        synth_filename = f"{symbol}_{symbol}_01JAN2015_31DEC2025_{side}_OHLCV.csv"
        entries.append({
            "path": files[0],  # nominal — loader uses the full list from `files`
            "filename": synth_filename,
            "id": auto_id,
            "symbol": symbol,
            "name": f"{symbol} {side} ({len(files):,} daily files)",
            "files": files,
            "aggregated": True,
        })
    return entries


def scan_csv_folder(folder: str = DEFAULT_CSV_FOLDER) -> list[dict]:
    """
    Scan a folder for all CSV files and return metadata for each.

    Tries to parse the crypto naming pattern {id}_{SYMBOL}_{Name}.csv
    for backward compatibility. For files that don't match, basic
    metadata is still returned so the frontend can apply its own
    pattern matching based on the selected asset class.

    When the folder contains no direct CSV files but does contain the
    FX daily-file layout (<PAIR>/YYYY/MM/DD/*.csv), files are aggregated
    by (pair, side) and returned as one entry per group so the UI can
    ingest the whole tree in one click.

    Returns
    -------
    list[dict]
        Each dict has keys: path, filename, id, symbol, name.
        Aggregated entries also carry: files (list), aggregated (True).
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        return []

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

    return results


def get_unique_symbols(folder: str = DEFAULT_CSV_FOLDER) -> list[dict]:
    """
    Get unique legitimate crypto symbols from the folder.

    When multiple files share the same ticker (e.g. many PEPE tokens),
    returns all of them so the user can pick the right one.
    Groups by symbol for display.

    Returns
    -------
    list[dict]
        Sorted by symbol, then by id (lower id = more established coin).
    """
    all_files = scan_csv_folder(folder)
    return sorted(all_files, key=lambda x: (x["symbol"], x["id"]))


def get_display_label(entry: dict) -> str:
    """Create a display label like 'BTC - Bitcoin (1_BTC_Bitcoin.csv)'."""
    return f"{entry['symbol']} - {entry['name']} ({entry['filename']})"


def _parse_timestamps(series: pd.Series) -> pd.Series:
    """Parse a timestamp column to UTC datetime, handling various formats."""
    sample = str(series.iloc[0])

    # Detect "DD.MM.YYYY ... GMT+XXXX" format (e.g., FX data)
    if "GMT" in sample and "." in sample.split(" ")[0]:
        cleaned = series.str.replace("GMT", "", regex=False)
        return pd.to_datetime(cleaned, format="%d.%m.%Y %H:%M:%S.%f %z", utc=True)

    # Default: let pandas auto-parse with UTC
    return pd.to_datetime(series, utc=True)


def load_csv(csv_path: str, timestamp_column: str = "ts",
             required_columns: list[str] | None = None,
             delimiter: str = ",") -> pd.DataFrame:
    """
    Load a CSV file and return a clean OHLCV DataFrame.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file.
    timestamp_column : str
        Name of the timestamp column in the CSV (default: "ts").
    required_columns : list[str] | None
        Columns that must exist. Defaults to [ts_col, open, high, low, close, volume].
    delimiter : str
        CSV delimiter (default: ",").

    Returns
    -------
    pd.DataFrame
        Columns: open, high, low, close, volume
        Index: timestamp (UTC datetime)
    """
    df = pd.read_csv(csv_path, delimiter=delimiter)

    # Case-insensitive column matching. Some FX daily-file batches mix
    # title-case ("Open") with lowercase ("open") across pairs — this
    # normalizes once up front so every downstream reader can rely on
    # lowercase OHLCV names.
    lower_map = {c: c.lower() for c in df.columns}
    df = df.rename(columns=lower_map)
    timestamp_column = timestamp_column.lower() if timestamp_column else timestamp_column

    # Validate required columns
    if required_columns is None:
        required_columns = [timestamp_column, "open", "high", "low", "close", "volume"]
    else:
        required_columns = [c.lower() for c in required_columns]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}. Found: {list(df.columns)}")

    # Select OHLCV + timestamp columns
    ohlcv_cols = [timestamp_column, "open", "high", "low", "close", "volume"]
    result = df[ohlcv_cols].copy()
    result[timestamp_column] = _parse_timestamps(result[timestamp_column])
    result = result.rename(columns={timestamp_column: "timestamp"})
    result = result.set_index("timestamp")

    # Convert to numeric
    for col in ["open", "high", "low", "close", "volume"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    # Drop NaN rows
    result = result.dropna()

    # Sort by timestamp
    result = result.sort_index()

    # Cap volume to QUANTITY_MAX to avoid NautilusTrader overflow
    result["volume"] = result["volume"].clip(upper=QUANTITY_MAX)

    return result


def parse_symbol_from_entry(entry: dict) -> tuple[str, str]:
    """
    Parse a CSV entry into (base_currency, quote_currency).

    Since these CSVs are priced in USD, we always use USD as quote.
    """
    return entry["symbol"], "USD"
