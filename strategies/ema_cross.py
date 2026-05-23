"""EMA Cross Strategy - Buy when fast EMA crosses above slow EMA, sell on cross below."""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy

from core.aggregating_strategy import AggregatingStrategyMixin
from strategies._shared.entry_tags import ema_reason


class EMACrossConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    extra_bar_types: list[BarType] | None = None
    # Target EXTERNAL bar type to aggregate the base stream up to via the custom
    # streaming aggregator (e.g. "EURUSD.FOREX_MS-5-MINUTE-ASK-EXTERNAL"). Empty
    # ⇒ no aggregation (strategy runs on the base bar_type, unchanged behaviour).
    aggregate_to_bar_type: str = ""
    fast_ema_period: PositiveInt = 10
    slow_ema_period: PositiveInt = 20


class EMACrossStrategy(AggregatingStrategyMixin, Strategy):
    """Buy when fast EMA crosses above slow EMA, sell when it crosses below."""

    def __init__(self, config: EMACrossConfig) -> None:
        PyCondition.is_true(
            config.fast_ema_period < config.slow_ema_period,
            f"fast_ema_period ({config.fast_ema_period}) must be < slow_ema_period ({config.slow_ema_period})",
        )
        super().__init__(config)
        self.instrument: Instrument = None
        self.fast_ema = ExponentialMovingAverage(config.fast_ema_period)
        self.slow_ema = ExponentialMovingAverage(config.slow_ema_period)

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
            [self.fast_ema, self.slow_ema],
        )
        if not aggregating:
            # Passthrough: let the engine feed the indicators as before.
            self.register_indicator_for_bars(self.config.bar_type, self.fast_ema)
            self.register_indicator_for_bars(self.config.bar_type, self.slow_ema)
        self.subscribe_bars(self.config.bar_type)
        if self.config.extra_bar_types:
            for bt in self.config.extra_bar_types:
                self.subscribe_bars(bt)

    def on_bar(self, bar: Bar) -> None:
        # Only trade on the primary bar; extra_bar_types (e.g. higher TFs)
        # feed indicators but must not drive order submission.
        # Compare via .standard() so composite subscriptions like
        # `5-MIN-INTERNAL@1-MIN-EXTERNAL` (where the aggregator emits the
        # standard LHS form) still match config.bar_type.
        if bar.bar_type.standard() != self.config.bar_type.standard():
            return
        # Route through the streaming aggregator. None ⇒ window still open.
        bar = self._route_bar(bar)
        if bar is None:
            return
        if not self._indicators_ready():
            return

        if self.fast_ema.value >= self.slow_ema.value:
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.BUY)
            elif self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.BUY)
        elif self.fast_ema.value < self.slow_ema.value:
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.SELL)
            elif self.portfolio.is_net_long(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.SELL)

    def _submit_order(self, side: OrderSide) -> None:
        # Attach an indicator-aware tag so the orderbook's
        # "ENTRY DETAILED REASON" column carries the trigger detail.
        reason = ema_reason(
            side,
            int(self.config.fast_ema_period),
            int(self.config.slow_ema_period),
            self.fast_ema.value,
            self.slow_ema.value,
        )
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
            tags=[reason],
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        self._flush_aggregator()
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


# ── Registry exports ──
STRATEGY_NAME = "EMA Cross"
STRATEGY_CLASS = EMACrossStrategy
CONFIG_CLASS = EMACrossConfig
DESCRIPTION = "Buy when fast EMA crosses above slow EMA, sell on cross below."
PARAMS = {
    "fast_ema_period": {"label": "Fast EMA Period", "min": 2, "max": 100, "default": 10},
    "slow_ema_period": {"label": "Slow EMA Period", "min": 5, "max": 200, "default": 20},
}
