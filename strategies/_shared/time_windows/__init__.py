"""Time-window helpers shared by time-gated strategies.

Pure functions extracted from ``range_breakout.py`` so any strategy that needs
intraday session windows (range start/end, squareoff, etc.) can reuse the same
HHMM math instead of re-implementing it.

``hhmm_to_min`` is the integer-HHMM form used inside strategy *configs*
(e.g. ``930`` → 570). It is intentionally separate from
``core.backtest_runner._hhmm_to_minute`` which parses ``"HH:MM"`` *strings*
from the portfolio entry-window UI — different input type, different caller.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


def hhmm_to_min(hhmm: int) -> int:
    """Convert an integer HHMM clock value to minutes since midnight.

    ``930`` -> 570, ``1515`` -> 915. No validation: callers pass config values
    already bounded by the UI (0..2359).
    """
    return (hhmm // 100) * 60 + (hhmm % 100)


def bar_to_local(ts_event_ns: int, tz_offset_min: int) -> tuple[int, date]:
    """Resolve a bar's UTC nanosecond timestamp to local minute-of-day + date.

    ``ts_event_ns`` is ``Bar.ts_event`` (UTC nanoseconds). ``tz_offset_min`` is
    the strategy's ``timezone_offset_min`` (e.g. 330 for IST, 0 for UTC).
    Returns ``(local_minute_of_day, local_date)``.
    """
    local_dt = datetime.fromtimestamp(ts_event_ns / 1e9, tz=timezone.utc) + timedelta(
        minutes=tz_offset_min
    )
    return local_dt.hour * 60 + local_dt.minute, local_dt.date()
