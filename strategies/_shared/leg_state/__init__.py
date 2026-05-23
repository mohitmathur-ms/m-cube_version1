"""Per-leg state container for pyramid/multi-leg strategies.

Extracted from ``range_breakout.py``. One ``LegState`` tracks a single leg of
one side (long or short): whether it is currently in the market, its modeling
entry price, whether it has ever hit target this day (gates pyramid promotion
of the next leg), and how many times it has (re-)entered today.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LegState:
    active: bool = False
    entry_price: float = 0.0
    ever_hit_target: bool = False
    entry_count: int = 0

    def reset(self) -> None:
        self.active = False
        self.entry_price = 0.0
        self.ever_hit_target = False
        self.entry_count = 0
