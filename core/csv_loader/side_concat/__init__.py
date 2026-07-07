"""Concatenate daily CSVs for one side (ASK or BID) in timestamp order."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from core.csv_loader.constants import _CONCAT_SIDE_MAX_WORKERS
from core.csv_loader.csv_reader import load_csv

logger = logging.getLogger(__name__)


def concat_side(files: list[str], timestamp_column: str = "ts",
                required_columns: list[str] | None = None,
                optional_columns: list[str] | None = None,
                delimiter: str = ",",
                max_workers: int | None = None) -> pd.DataFrame:
    """Concatenate daily CSVs for one side (ASK or BID) in timestamp order,
    dropping overlapping-midnight duplicates.

    Files are loaded in parallel through a thread pool — pandas releases
    the GIL inside the C parser, so I/O on N files overlaps cleanly.
    ``ThreadPoolExecutor.map`` preserves input order, so the chronological
    ordering from the scanner flows through to the concat and the global
    sort is usually a no-op (only kicks in if concatenation broke
    monotonicity, e.g. overlapping ranges across files).

    Parameters
    ----------
    max_workers : int | None
        Override the pool size (default: capped at the module constant
        :data:`_CONCAT_SIDE_MAX_WORKERS`). Pass ``1`` to force serial
        loading — useful for benchmarks and disk-bound HDD setups.
    """
    if not files:
        raise ValueError("No loadable CSVs for side")

    def _safe_load(path: str) -> pd.DataFrame | None:
        try:
            return load_csv(path, timestamp_column=timestamp_column,
                            required_columns=required_columns,
                            optional_columns=optional_columns,
                            delimiter=delimiter)
        except Exception as e:
            logger.warning("csv_load skip %s: %s", Path(path).name, e)
            return None

    workers = (max_workers if max_workers is not None
               else min(_CONCAT_SIDE_MAX_WORKERS, len(files)))

    if workers <= 1 or len(files) == 1:
        loaded: list[pd.DataFrame | None] = [_safe_load(p) for p in files]
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="csv_load") as ex:
            loaded = list(ex.map(_safe_load, files))

    parts = [df for df in loaded if df is not None]
    if not parts:
        raise ValueError("No loadable CSVs for side")
    df = pd.concat(parts, copy=False)
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()
    df = df[~df.index.duplicated(keep="first")]
    return df
