"""Single-CSV reader producing a clean OHLCV DataFrame."""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

from core.csv_loader.constants import _OHLCV_LOWER, QUANTITY_MAX
from core.csv_loader.timestamp_parser import _parse_timestamps


def load_csv(csv_path: str, timestamp_column: str = "ts",
             required_columns: list[str] | None = None,
             optional_columns: list[str] | None = None,
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
    optional_columns : list[str] | None
        Extra columns to include if present in the source file. Missing
        optional columns are silently skipped (no validation error). Used
        e.g. for commodity ASK/BID files where `ask_vwap` exists on ASK files
        and `bid_vwap` exists on BID files — declaring both as optional lets
        the same config load either side without per-side branching.
    delimiter : str
        CSV delimiter (default: ",").

    Returns
    -------
    pd.DataFrame
        Columns: open, high, low, close, volume (+ any optional_columns present).
        Index: timestamp (UTC datetime)
    """
    ts_col_lc = (timestamp_column or "ts").lower()

    # Read header only (cheap) to discover the source case-mixed names, then
    # re-read with usecols + dtype so the C parser only materializes the
    # columns we need with the right types — skips ~10 extra columns the
    # FX daily files ship with and removes the post-hoc to_numeric pass.
    header_df = pd.read_csv(csv_path, delimiter=delimiter, nrows=0)
    case_map = {c.lower(): c for c in header_df.columns}

    needed_lc = [c.lower() for c in (required_columns or [ts_col_lc, *_OHLCV_LOWER])]
    missing = [c for c in needed_lc if c not in case_map]
    if missing:
        raise ValueError(
            f"CSV missing required columns: {missing}. Found: {list(header_df.columns)}"
        )

    src_ts = case_map[ts_col_lc]
    src_ohlcv = [case_map[c] for c in _OHLCV_LOWER]

    # Resolve optional columns against the actual header. Anything not present
    # is silently dropped from the include list — that's the whole point of
    # the field. Preserves config order and de-dupes against OHLCV/timestamp.
    optional_lc = [c.lower() for c in (optional_columns or [])]
    extra_lc = [c for c in optional_lc
                if c in case_map and c != ts_col_lc and c not in _OHLCV_LOWER]
    src_extra = [case_map[c] for c in extra_lc]

    use_cols = [src_ts, *src_ohlcv, *src_extra]

    # Use PyArrow for the data read — ~35x faster than pandas on the
    # 4M-row OHLCV files (benchmarked). Header probe above stays in pandas
    # since it's a one-row read and gives us the case-insensitive column
    # mapping PyArrow's include_columns needs as exact case.
    table = pacsv.read_csv(
        csv_path,
        parse_options=pacsv.ParseOptions(delimiter=delimiter),
        convert_options=pacsv.ConvertOptions(
            include_columns=use_cols,
            column_types={c: pa.float64() for c in [*src_ohlcv, *src_extra]},
        ),
    )
    df = table.to_pandas()
    rename_map = {src_ts: "timestamp"}
    rename_map.update({src: lc for src, lc in zip(src_ohlcv, _OHLCV_LOWER)})
    rename_map.update({src: lc for src, lc in zip(src_extra, extra_lc)})
    df = df.rename(columns=rename_map)

    df["timestamp"] = _parse_timestamps(df["timestamp"])
    df = df.dropna(subset=["timestamp"]).set_index("timestamp")
    # Only OHLCV NaNs disqualify a row — an optional column like ask_vwap may
    # legitimately be NaN on individual bars and shouldn't drop the row.
    df = df.dropna(subset=list(_OHLCV_LOWER))

    # Each daily file is already monotonic — only sort if a concatenation
    # upstream broke that invariant.
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    # Cap volume to QUANTITY_MAX to avoid NautilusTrader overflow.
    df["volume"] = df["volume"].clip(upper=QUANTITY_MAX)

    return df
