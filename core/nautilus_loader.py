"""
Convert local CSV OHLCV DataFrames to NautilusTrader Bar objects and store in ParquetDataCatalog.

This module bridges local CSV data → NautilusTrader native types → Parquet catalog.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler

from core.csv_loader import QUANTITY_MAX
from core.csv_loader import load_csv
from core.csv_loader import parse_symbol_from_entry
from core.instrument_factory import create_instrument


DEFAULT_CATALOG_PATH = "./catalog"


def make_bar_type_str(instrument: CurrencyPair, timeframe: str = "1-DAY") -> str:
    """
    Build a BarType string like "BTCUSD.CRYPTO-1-DAY-LAST-EXTERNAL".

    Parameters
    ----------
    instrument : CurrencyPair
        The instrument.
    timeframe : str, default "1-DAY"
        Timeframe string like "1-DAY", "1-MINUTE", "1-HOUR".

    Returns
    -------
    str
    """
    return f"{instrument.id}-{timeframe}-LAST-EXTERNAL"


def wrangle_bars(
    df: pd.DataFrame,
    instrument: CurrencyPair,
    timeframe: str = "1-DAY",
) -> list[Bar]:
    """
    Convert an OHLCV DataFrame into a list of NautilusTrader Bar objects.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns: open, high, low, close, volume.
        Index must be a datetime index named 'timestamp'.
        Volume must already be capped to QUANTITY_MAX.
    instrument : CurrencyPair
        The instrument definition.
    timeframe : str, default "1-DAY"
        Timeframe string.

    Returns
    -------
    list[Bar]
    """
    bar_type_str = make_bar_type_str(instrument, timeframe)
    bar_type = BarType.from_str(bar_type_str)

    # Ensure volume is capped (safety check)
    df_copy = df.copy()
    df_copy["volume"] = df_copy["volume"].clip(upper=QUANTITY_MAX)

    wrangler = BarDataWrangler(bar_type, instrument)
    bars = wrangler.process(df_copy)
    return bars


def save_to_catalog(
    bars: list[Bar],
    instrument: CurrencyPair,
    catalog_path: str = DEFAULT_CATALOG_PATH,
) -> ParquetDataCatalog:
    """
    Write instrument and bar data to a ParquetDataCatalog.

    Parameters
    ----------
    bars : list[Bar]
        The bar data to write.
    instrument : CurrencyPair
        The instrument definition to write.
    catalog_path : str
        Path to the catalog directory.

    Returns
    -------
    ParquetDataCatalog
        The catalog instance.
    """
    Path(catalog_path).mkdir(parents=True, exist_ok=True)
    catalog = ParquetDataCatalog(catalog_path)
    catalog.write_data([instrument])
    catalog.write_data(bars)
    return catalog


def load_catalog(catalog_path: str = DEFAULT_CATALOG_PATH) -> ParquetDataCatalog:
    """Load an existing ParquetDataCatalog."""
    return ParquetDataCatalog(catalog_path)


def load_csv_and_store(
    csv_entry: dict,
    catalog_path: str = DEFAULT_CATALOG_PATH,
    venue: str = "CRYPTO",
) -> dict:
    """
    Full pipeline: load local CSV → wrangle → store in catalog.

    Parameters
    ----------
    csv_entry : dict
        Entry from csv_loader.scan_csv_folder() with keys: path, symbol, name, id.
    catalog_path : str
        Path to store the parquet catalog.
    venue : str
        Venue name for instrument creation.

    Returns
    -------
    dict
        Summary with keys: symbol, name, instrument, bar_type, num_bars, catalog_path, dataframe.
    """
    # Step 1: Load CSV
    df = load_csv(csv_entry["path"])

    # Step 2: Create instrument
    base, quote = parse_symbol_from_entry(csv_entry)
    instrument = create_instrument(base, quote, venue)

    # Step 3: Wrangle into Nautilus Bar objects
    bars = wrangle_bars(df, instrument)

    # Step 4: Save to catalog
    save_to_catalog(bars, instrument, catalog_path)

    bar_type_str = make_bar_type_str(instrument)

    return {
        "symbol": csv_entry["symbol"],
        "name": csv_entry["name"],
        "instrument": instrument,
        "bar_type": bar_type_str,
        "num_bars": len(bars),
        "catalog_path": catalog_path,
        "dataframe": df,
    }
