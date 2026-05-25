"""Derive a venue's daily trading session window from loaded OHLCV data.

A single-responsibility component of the :mod:`core.csv_loader` package: given a
cleaned OHLCV DataFrame (timestamp index, UTC — see ``timestamp_parser``), report
the earliest and latest *time-of-day* observed across all bars. That pair is the
data-derived daily session window persisted per venue at ingest time by
:func:`core.venue_config.update_venue_session_window`.
"""

from __future__ import annotations

import pandas as pd


def session_window_from_df(df: pd.DataFrame) -> tuple[str, str] | None:
    """Derive the daily trading session window from a loaded OHLCV frame.

    Returns ``(session_start_time, session_end_time)`` as zero-padded
    ``"HH:MM:SS"`` strings, computed as the earliest and latest *time-of-day*
    observed across every bar. The index is UTC, so the returned times are UTC
    too — callers persist them verbatim.

    For ~24h markets (FX) this collapses to roughly ``00:00:00``–``23:59:00``;
    for fixed-window markets (index/futures) it captures the real daily window
    (e.g. an exchange that trades a single intraday block). Returns ``None`` for
    an empty frame or a non-datetime index so callers can skip the write.
    """
    if df is None or df.empty or not isinstance(df.index, pd.DatetimeIndex):
        return None
    # Zero-padded "HH:MM:SS" sorts lexicographically the same as chronologically,
    # so plain min/max over the formatted strings gives the time-of-day extremes.
    tod = df.index.strftime("%H:%M:%S")
    return str(tod.min()), str(tod.max())
