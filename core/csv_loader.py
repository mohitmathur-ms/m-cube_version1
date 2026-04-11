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
DEFAULT_CSV_FOLDER = r"C:\Users\ADMIN\Desktop\id_name_all_symbols\id_name_all_symbols"

# NautilusTrader QUANTITY_RAW_MAX is 18_446_744_072.999999488
# Use a safe value well below this to avoid Rust panics
QUANTITY_MAX = 16_000_000_000.0


def scan_csv_folder(folder: str = DEFAULT_CSV_FOLDER) -> list[dict]:
    """
    Scan a folder for crypto CSV files and return metadata for each.

    Files are expected to be named like: {id}_{SYMBOL}_{Name}.csv

    Returns
    -------
    list[dict]
        Each dict has keys: path, filename, id, symbol, name
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        return []

    results = []
    pattern = re.compile(r"^(\d+)_([A-Z]+)_(.+)\.csv$")

    for csv_file in sorted(folder_path.glob("*.csv")):
        match = pattern.match(csv_file.name)
        if match:
            results.append({
                "path": str(csv_file),
                "filename": csv_file.name,
                "id": int(match.group(1)),
                "symbol": match.group(2),
                "name": match.group(3),
            })

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


def load_csv(csv_path: str) -> pd.DataFrame:
    """
    Load a crypto CSV file and return a clean OHLCV DataFrame.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        Columns: open, high, low, close, volume
        Index: timestamp (UTC datetime)
    """
    df = pd.read_csv(csv_path)

    # Validate required columns
    required = ["ts", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}. Found: {list(df.columns)}")

    # Select and rename columns
    result = df[["ts", "open", "high", "low", "close", "volume"]].copy()
    result["ts"] = pd.to_datetime(result["ts"], utc=True)
    result = result.rename(columns={"ts": "timestamp"})
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
