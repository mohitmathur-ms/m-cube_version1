"""Low-level timeframe parsing for bar-type strings.

The unit→seconds table and two parsers used to compare bar-type resolutions
(e.g. is a strategy-subscribe timeframe coarser than the data feed?). Consumed
by ``composite_bar_type`` to validate/repair strategy-subscribe bar types.
"""

from __future__ import annotations

import re


_UNIT_SECONDS = {
    "SECOND": 1,
    "MINUTE": 60,
    "HOUR": 3600,
    "DAY": 86400,
    "WEEK": 604800,
    "MONTH": 2592000,  # nominal 30-day month; used only for ordering
}


def _timeframe_seconds(timeframe: str | None) -> int | None:
    """``"1-MINUTE"`` → 60, ``"30-MINUTE"`` → 1800. ``None`` if unparseable."""
    m = re.match(r"^\s*(\d+)\s*-\s*([A-Za-z]+)\s*$", str(timeframe or ""))
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2).upper()
    base = _UNIT_SECONDS.get(unit)
    return n * base if base else None


def _bar_type_timeframe(bar_type_str: str | None) -> str | None:
    """Pull the leading ``"<step>-<unit>"`` timeframe out of a bar type string,
    e.g. ``"EURUSD.FOREX_MS-30-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL"`` → ``"30-MINUTE"``."""
    parts = str(bar_type_str or "").split("-")
    if len(parts) < 3:
        return None
    return f"{parts[1]}-{parts[2]}"
