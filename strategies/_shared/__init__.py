"""Shared, pure helpers extracted from the strategy files.

The underscore prefix keeps this package out of the strategy auto-discovery
scan in ``strategies/__init__.py`` (``_build_registry`` skips names starting
with ``_``), so adding helpers here never accidentally registers a "strategy".

Submodules:
  - ``time_windows`` — HHMM/intraday clock math (``hhmm_to_min``, ``bar_to_local``)
  - ``entry_tags``   — orderbook "ENTRY DETAILED REASON" string builders
  - ``leg_state``    — ``LegState`` dataclass for multi-leg/pyramid strategies
"""

from __future__ import annotations

from strategies._shared.entry_tags import (
    bollinger_reason,
    ema_reason,
    four_ma_reason,
    rbo_reason,
    rsi_reason,
)
from strategies._shared.leg_state import LegState
from strategies._shared.time_windows import bar_to_local, hhmm_to_min

__all__ = [
    "hhmm_to_min",
    "bar_to_local",
    "LegState",
    "ema_reason",
    "rsi_reason",
    "bollinger_reason",
    "four_ma_reason",
    "rbo_reason",
]
