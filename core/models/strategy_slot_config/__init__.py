"""One strategy instance within a portfolio (the middle schema dataclass).

``StrategySlotConfig`` sits between the leg-level ``ExitConfig`` and the
portfolio-level ``PortfolioConfig``: it pairs a strategy + bar type + sizing
(lots / allocation) with an embedded ``ExitConfig``, plus per-slot date range
and square-off overrides.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional

from core.models.exit_config import ExitConfig


@dataclass
class StrategySlotConfig:
    """One strategy instance within a portfolio."""

    slot_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    strategy_name: str = "EMA Cross"
    strategy_params: dict = field(default_factory=dict)
    bar_type_str: str = ""
    # Strategy-subscribe bar types (NautilusTrader composite/aggregated bar
    # types). ``bar_type_str`` above is the BASE timeframe — the catalog data
    # fed to the engine, the resolution at which the matching engine fills
    # orders. This list holds the composite bar types the strategy subscribes
    # to, e.g. a 5-minute bar internally aggregated from the 1-minute base:
    #   "EURUSD.FOREX_MS-5-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL"
    # When non-empty, the strategy OPERATES on the first composite — its
    # signal, indicators and SL/TP run on that aggregated stream and orders
    # are submitted on its interval, while fills still happen on the base
    # data. Any further composites are subscribed-and-received (available to
    # the strategy) but the single-signal logic uses the first. Empty list →
    # strategy runs on the base bar type (the original, unchanged behaviour).
    strategy_bar_types: list[str] = field(default_factory=list)
    # Sizing multiplier (number of contracts). Combined with the instrument's
    # admin-configured lot_size at order-submit time:
    #   order_qty = min(instrument.lot_size × slot.lots, instrument.trade_size_cap)
    # FX example: lot_size=100_000 (one standard lot), lots=0.01 → 1000 base ccy.
    # Older portfolio JSON used a single ``trade_size`` field that conflated lot
    # size and multiplier; ``portfolio_from_dict`` migrates those values
    # automatically (see _migrate_legacy_trade_size).
    lots: float = 1.0
    allocation_pct: float = 0.0  # Capital allocation percentage (used when mode is "percentage")
    exit_config: ExitConfig = field(default_factory=ExitConfig)
    enabled: bool = True
    start_date: Optional[str] = None  # ISO date like "2024-01-01"
    end_date: Optional[str] = None    # ISO date like "2024-12-31"
    # Slot/strategy-level square-off override. Falls back to portfolio-level
    # if None. Leg-level (ExitConfig.squareoff_time) wins over both.
    squareoff_time: Optional[str] = None
    squareoff_tz: Optional[str] = None

    @property
    def display_name(self) -> str:
        instrument = self.bar_type_str.split(".")[0] if self.bar_type_str else "N/A"
        sl_desc = ""
        if self.exit_config.stop_loss_type == "percentage":
            sl_desc = f" | SL: {self.exit_config.stop_loss_value}%"
        elif self.exit_config.stop_loss_type == "points":
            sl_desc = f" | SL: {self.exit_config.stop_loss_value}pts"
        elif self.exit_config.stop_loss_type == "trailing":
            sl_desc = f" | SL: trailing {self.exit_config.trailing_sl_offset}%"
        tp_desc = ""
        if self.exit_config.target_type == "percentage":
            tp_desc = f" | TP: {self.exit_config.target_value}%"
        elif self.exit_config.target_type == "points":
            tp_desc = f" | TP: {self.exit_config.target_value}pts"
        return f"{self.strategy_name} on {instrument}{sl_desc}{tp_desc}"
