"""
Convert local CSV OHLCV DataFrames to NautilusTrader Bar objects and store in ParquetDataCatalog.

This module bridges local CSV data → NautilusTrader native types → Parquet catalog.
"""

from __future__ import annotations

import re
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


def make_bar_type_str(instrument: CurrencyPair, timeframe: str = "1-DAY", price_type: str = "LAST") -> str:
    """
    Build a BarType string like "BTCUSD.CRYPTO-1-DAY-LAST-EXTERNAL".

    Parameters
    ----------
    instrument : CurrencyPair
        The instrument.
    timeframe : str, default "1-DAY"
        Timeframe string like "1-DAY", "1-MINUTE", "1-HOUR".
    price_type : str, default "LAST"
        Price type like "LAST", "BID", "ASK".

    Returns
    -------
    str
    """
    return f"{instrument.id}-{timeframe}-{price_type}-EXTERNAL"


def wrangle_bars(
    df: pd.DataFrame,
    instrument: CurrencyPair,
    timeframe: str = "1-DAY",
    price_type: str = "LAST",
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
    price_type : str, default "LAST"
        Price type like "LAST", "BID", "ASK".

    Returns
    -------
    list[Bar]
    """
    bar_type_str = make_bar_type_str(instrument, timeframe, price_type=price_type)
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
    venue: str = "BINANCE",
    data_format: dict | None = None,
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
    data_format : dict | None
        Data format config from data_formats/<asset_class>.json.
        Contains csv, instrument, and trading sections.

    Returns
    -------
    dict
        Summary with keys: symbol, name, instrument, bar_type, num_bars, catalog_path, dataframe.
    """
    csv_config = (data_format or {}).get("csv", {})
    inst_config = (data_format or {}).get("instrument", {})

    # Step 1: Load CSV(s). Entries from the daily-FX aggregator carry a
    # `files` list; we concat them in timestamp order and drop duplicates
    # (overlapping day files occasionally cross midnight).
    ts_col = csv_config.get("timestamp_column") or "ts"
    req_cols = csv_config.get("required_columns") or None
    delimiter = csv_config.get("delimiter") or ","

    file_list = csv_entry.get("files")
    if file_list:
        parts = []
        for p in file_list:
            try:
                parts.append(load_csv(p, timestamp_column=ts_col,
                                      required_columns=req_cols, delimiter=delimiter))
            except Exception as e:
                # Best-effort: skip individual bad files but keep going so a
                # transient parse error on one day doesn't lose a full year.
                print(f"[csv_load] skip {Path(p).name}: {e}")
        if not parts:
            raise ValueError(f"No loadable CSVs in aggregated entry for {csv_entry.get('symbol')}")
        df = pd.concat(parts).sort_index()
        df = df[~df.index.duplicated(keep="first")]
    else:
        df = load_csv(csv_entry["path"], timestamp_column=ts_col,
                      required_columns=req_cols, delimiter=delimiter)

    # Step 2: Create instrument
    quote = inst_config.get("quote_currency") or "USD"
    base = csv_entry["symbol"]

    # For forex: split combined pair symbol (e.g., "EURUSD" → base="EUR", quote="USD")
    base_len = inst_config.get("base_currency_length")
    if base_len and len(base) > base_len:
        quote = base[base_len:]
        base = base[:base_len]

    instrument = create_instrument(
        base, quote, venue,
        price_precision=inst_config.get("price_precision"),
        size_precision=inst_config.get("size_precision"),
    )

    # Step 3: Wrangle into Nautilus Bar objects
    timeframe = inst_config.get("timeframe") or "1-DAY"

    # Extract price type (BID/ASK) from filename if configured
    price_type = "LAST"
    if inst_config.get("price_type_from_filename"):
        filename = csv_entry.get("filename", "")
        pt_match = re.search(r"_(BID|ASK)_", filename)
        if pt_match:
            price_type = pt_match.group(1)

    bars = wrangle_bars(df, instrument, timeframe=timeframe, price_type=price_type)

    # Step 4: Save to catalog
    save_to_catalog(bars, instrument, catalog_path)

    bar_type_str = make_bar_type_str(instrument, timeframe=timeframe, price_type=price_type)

    # Post-ingest sanity: if > 30% of a sample of bars have open == close,
    # the instrument precision is almost certainly too low for this data
    # source and every bar has been rounded into a doji. Callers surface
    # this as a user-visible warning so the issue gets caught at ingest
    # time rather than at backtest time.
    doji_rate = 0.0
    if bars:
        sample_size = min(1000, len(bars))
        # Stride-sample across the full bar list so we catch early/mid/late
        # discrepancies, not just the first 1000 (which may be pre-market quiet).
        stride = max(1, len(bars) // sample_size)
        sample = bars[::stride][:sample_size]
        doji = sum(1 for b in sample if float(b.open) == float(b.close))
        doji_rate = doji / len(sample)

    return {
        "symbol": csv_entry["symbol"],
        "name": csv_entry["name"],
        "instrument": instrument,
        "bar_type": bar_type_str,
        "num_bars": len(bars),
        "catalog_path": catalog_path,
        "dataframe": df,
        "doji_rate": doji_rate,
        "price_precision": instrument.price_precision,
    }
