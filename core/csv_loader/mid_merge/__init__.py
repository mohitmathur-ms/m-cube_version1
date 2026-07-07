"""Synthesize MID OHLCV from ASK + BID, and load a pair's MID from file lists."""

from __future__ import annotations

import pandas as pd

from core.csv_loader.constants import QUANTITY_MAX
from core.csv_loader.side_concat import concat_side


def _merge_ask_bid_to_mid(ask_df: pd.DataFrame, bid_df: pd.DataFrame) -> pd.DataFrame:
    """Row-wise MID from ASK + BID OHLCV frames.

    O/H/L/C = (ask + bid) / 2, volume = ask + bid. Inner-join on the
    timestamp index, so rows present on only one side are dropped.

    Any extra (non-OHLCV) columns present on either side are carried through
    to the MID frame with their original per-side values — e.g. for
    commodity data this preserves ``ask_vwap`` (from the ASK side) and
    ``bid_vwap`` (from the BID side) on the merged result.
    """
    common_idx = ask_df.index.intersection(bid_df.index)
    ask_aligned = ask_df.loc[common_idx]
    bid_aligned = bid_df.loc[common_idx]
    ohlc = ["open", "high", "low", "close"]
    mid = (ask_aligned[ohlc] + bid_aligned[ohlc]) * 0.5
    mid["volume"] = (ask_aligned["volume"] + bid_aligned["volume"]).clip(upper=QUANTITY_MAX)
    # Carry per-side extras through unchanged. ASK-side wins on name collisions
    # (none expected: ask_vwap vs bid_vwap are distinctly named in practice).
    base_cols = {*ohlc, "volume"}
    for col in ask_aligned.columns:
        if col not in base_cols:
            mid[col] = ask_aligned[col]
    for col in bid_aligned.columns:
        if col not in base_cols and col not in mid.columns:
            mid[col] = bid_aligned[col]
    return mid


def load_pair_mid(entry: dict, timestamp_column: str = "ts",
                  required_columns: list[str] | None = None,
                  optional_columns: list[str] | None = None,
                  delimiter: str = ",") -> pd.DataFrame:
    """Load one pair's ASK and BID file lists and return a merged MID OHLCV frame."""
    ask_files = entry.get("ask_files") or []
    bid_files = entry.get("bid_files") or []
    if not ask_files or not bid_files:
        raise ValueError(
            f"load_pair_mid requires both ask_files and bid_files in the entry "
            f"(got ask={len(ask_files)}, bid={len(bid_files)})"
        )
    ask_df = concat_side(ask_files, timestamp_column, required_columns,
                         optional_columns, delimiter)
    bid_df = concat_side(bid_files, timestamp_column, required_columns,
                         optional_columns, delimiter)
    return _merge_ask_bid_to_mid(ask_df, bid_df)
