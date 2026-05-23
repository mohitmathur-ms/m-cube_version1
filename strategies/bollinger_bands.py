"""Bollinger Bands Strategy - Buy at lower band, sell at upper band."""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.indicators import BollingerBands
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy

from core.aggregating_strategy import AggregatingStrategyMixin
from strategies._shared.entry_tags import bollinger_reason


class BollingerConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    extra_bar_types: list[BarType] | None = None
    # Target EXTERNAL bar type to aggregate the base stream up to (custom
    # streaming aggregator). Empty ⇒ no aggregation (runs on base, unchanged).
    aggregate_to_bar_type: str = ""
    bb_period: PositiveInt = 20
    bb_std: float = 2.0


class BollingerBandsStrategy(AggregatingStrategyMixin, Strategy):
    """Buy when price touches lower band, sell when price touches upper band."""

    def __init__(self, config: BollingerConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument = None
        self.bb = BollingerBands(config.bb_period, config.bb_std)

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return
        aggregating = self._setup_aggregation(
            self.instrument,
            self.config.bar_type,
            self.config.aggregate_to_bar_type,
            [self.bb],
        )
        if not aggregating:
            self.register_indicator_for_bars(self.config.bar_type, self.bb)
        self.subscribe_bars(self.config.bar_type)
        if self.config.extra_bar_types:
            for bt in self.config.extra_bar_types:
                self.subscribe_bars(bt)

    def on_bar(self, bar: Bar) -> None:
        # Only trade on the primary bar; extra_bar_types (e.g. higher TFs)
        # feed indicators but must not drive order submission.
        if bar.bar_type != self.config.bar_type:
            return
        bar = self._route_bar(bar)
        if bar is None:
            return
        if not self._indicators_ready():
            return

        close = float(bar.close)

        p = int(self.config.bb_period)
        sd = float(self.config.bb_std)
        if close <= self.bb.lower:
            reason = bollinger_reason(OrderSide.BUY, close, self.bb.lower, sd, p)
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.BUY, reason)
            elif self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.BUY, reason)
        elif close >= self.bb.upper:
            reason = bollinger_reason(OrderSide.SELL, close, self.bb.upper, sd, p)
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.SELL, reason)
            elif self.portfolio.is_net_long(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.SELL, reason)

    def _submit_order(self, side: OrderSide, reason: str | None = None) -> None:
        kwargs = dict(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        if reason:
            kwargs["tags"] = [reason]
        order = self.order_factory.market(**kwargs)
        self.submit_order(order)

    def on_stop(self) -> None:
        self._flush_aggregator()
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


# ── Registry exports ──
STRATEGY_NAME = "Bollinger Bands"
STRATEGY_CLASS = BollingerBandsStrategy
CONFIG_CLASS = BollingerConfig
DESCRIPTION = "Buy at lower band, sell at upper band."
PARAMS = {
    "bb_period": {"label": "BB Period", "min": 5, "max": 100, "default": 20},
    "bb_std": {"label": "Std Deviations", "min": 0.5, "max": 4.0, "default": 2.0},
}
