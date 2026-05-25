"""Bar-list filters used by every execution path: run_on_days weekday gate,
intraday entry-window time-of-day gate, ReExecute replay cutoff, and the
HH:MM / intraday-granularity parsing helpers they share."""

from __future__ import annotations


_DAY_NAME_TO_WEEKDAY = {
    "MON": 0, "MONDAY": 0,
    "TUE": 1, "TUESDAY": 1,
    "WED": 2, "WEDNESDAY": 2,
    "THU": 3, "THURSDAY": 3,
    "FRI": 4, "FRIDAY": 4,
    "SAT": 5, "SATURDAY": 5,
    "SUN": 6, "SUNDAY": 6,
}


def _allowed_weekdays(run_on_days) -> set | None:
    """Resolve a portfolio's run_on_days into a set of weekday integers.

    Returns None when no filter should apply (input is None — meaning
    "all 7 days are fine"). Returns a set of int weekdays (0=Mon..6=Sun)
    when the filter is active. Returns an empty set when the input is
    a non-None list whose entries don't match any known day name —
    callers should treat that as "no days are allowed" and short-circuit.
    """
    if run_on_days is None:
        return None
    if not isinstance(run_on_days, (list, tuple, set)):
        return None
    allowed: set = set()
    for day in run_on_days:
        if not isinstance(day, str):
            continue
        wd = _DAY_NAME_TO_WEEKDAY.get(day.strip().upper())
        if wd is not None:
            allowed.add(wd)
    return allowed


# 1970-01-01 (UNIX epoch) was a Thursday — Python weekday() = 3.
_NANOS_PER_DAY = 86_400_000_000_000
_EPOCH_WEEKDAY = 3


def _filter_bars_by_weekday(bars: list, allowed_weekdays: set | None) -> tuple[list, int]:
    """Drop bars whose UTC weekday isn't in allowed_weekdays.

    Returns (kept_bars, dropped_count). When allowed_weekdays is None,
    returns the input list unchanged with dropped=0. Uses an integer
    modulo on ts_event nanoseconds to avoid the per-bar pandas-timestamp
    cost — for a year of 1-min FX bars that's the difference between a
    20 ms filter and a 2 second filter.
    """
    if allowed_weekdays is None:
        return bars, 0
    if not allowed_weekdays:
        return [], len(bars)
    if not bars:
        return bars, 0

    kept = []
    for bar in bars:
        days = bar.ts_event // _NANOS_PER_DAY
        weekday = (_EPOCH_WEEKDAY + days) % 7
        if weekday in allowed_weekdays:
            kept.append(bar)
    return kept, len(bars) - len(kept)


def _filter_bars_after_ns(bars: list, cutoff_ns: int) -> tuple[list, int]:
    """Drop bars with ``ts_event < cutoff_ns`` — keep only bars at/after it.

    Used by the portfolio ReExecute replay (spec §1.2 / §2.4): a replay
    segment re-runs every slot from the clip timestamp onward, so each slot
    starts flat at ``cutoff_ns``. ``cutoff_ns <= 0`` is a no-op (the normal
    full-range run). Returns ``(kept_bars, dropped_count)``.
    """
    if cutoff_ns <= 0 or not bars:
        return bars, 0
    kept = [b for b in bars if b.ts_event >= cutoff_ns]
    return kept, len(bars) - len(kept)


_NANOS_PER_MINUTE = 60_000_000_000


def _hhmm_to_minute(s: str | None) -> int | None:
    """Parse a 'HH:MM' or 'HH:MM:SS' string into a minute-of-day integer.

    Returns None for None/empty/malformed input. Doesn't raise — bad input
    just disables that endpoint of the filter. Range is 0..1439 inclusive.
    """
    if not s or not isinstance(s, str):
        return None
    parts = s.strip().split(":")
    if len(parts) < 2:
        return None
    try:
        h = int(parts[0])
        m = int(parts[1])
    except ValueError:
        return None
    if not (0 <= h <= 23) or not (0 <= m <= 59):
        return None
    return h * 60 + m


def _is_intraday_bar_type(bt_str: str) -> bool:
    """Return True if the bar type is intraday granularity (minute / hour / second).

    Bar type format: ``INSTRUMENT.VENUE-N-AGGREGATION-PRICE-SOURCE``
    e.g. ``BTCUSD.BINANCE-1-DAY-LAST-EXTERNAL`` -> DAY (not intraday)
         ``EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL`` -> MINUTE (intraday)

    The intra-day entry window (entry_start_time / entry_end_time) only
    makes sense for intraday bars — daily/weekly/monthly bars have a single
    ts_event per period (typically 00:00 UTC of the period start) which would
    be unconditionally inside or outside any HH:MM window. Applying the filter
    to daily bars on an equity-hours window (e.g. 09:30..16:15) drops every
    bar.
    """
    if not bt_str:
        return False
    upper = bt_str.upper()
    return ("-SECOND-" in upper) or ("-MINUTE-" in upper) or ("-HOUR-" in upper)


def _filter_bars_by_time_of_day(
    bars: list,
    start_hhmm: str | None,
    end_hhmm: str | None,
) -> tuple[list, int]:
    """Drop bars whose UTC time-of-day falls outside [start_hhmm, end_hhmm].

    Both endpoints are inclusive. Either may be None — in which case that
    side of the window is unbounded (start=00:00 or end=23:59 effectively).
    When both are None, returns input unchanged.

    Window is in UTC. To use a non-UTC window, the caller would convert
    bar timestamps first; we don't pull pandas in here for the same
    perf reason as ``_filter_bars_by_weekday``.

    Returns (kept_bars, dropped_count).
    """
    start_min = _hhmm_to_minute(start_hhmm)
    end_min = _hhmm_to_minute(end_hhmm)
    if start_min is None and end_min is None:
        return bars, 0
    if not bars:
        return bars, 0

    # Normalize unbounded sides to full-day extremes
    lo = start_min if start_min is not None else 0
    hi = end_min if end_min is not None else (24 * 60 - 1)

    if lo > hi:
        # Inverted window (e.g. start=22:00, end=02:00) — treat as wrap-around
        # i.e. keep bars in [lo, 24*60) ∪ [0, hi].
        kept = []
        for bar in bars:
            intra = (bar.ts_event % _NANOS_PER_DAY) // _NANOS_PER_MINUTE
            if intra >= lo or intra <= hi:
                kept.append(bar)
        return kept, len(bars) - len(kept)

    kept = []
    for bar in bars:
        intra = (bar.ts_event % _NANOS_PER_DAY) // _NANOS_PER_MINUTE
        if lo <= intra <= hi:
            kept.append(bar)
    return kept, len(bars) - len(kept)
