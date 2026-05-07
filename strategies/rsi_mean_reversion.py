"""RSI Mean Reversion Strategy - Buy when RSI is oversold, sell when overbought."""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.indicators import RelativeStrengthIndex
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class RSIConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    extra_bar_types: list[BarType] | None = None
    rsi_period: PositiveInt = 14
    overbought: float = 70.0
    oversold: float = 30.0


class RSIMeanReversionStrategy(Strategy):
    """Buy when RSI is oversold, sell when RSI is overbought."""

    def __init__(self, config: RSIConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument = None
        self.rsi = RelativeStrengthIndex(config.rsi_period)

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return
        self.register_indicator_for_bars(self.config.bar_type, self.rsi)
        self.subscribe_bars(self.config.bar_type)
        if self.config.extra_bar_types:
            for bt in self.config.extra_bar_types:
                self.subscribe_bars(bt)

    def on_bar(self, bar: Bar) -> None:
        # Only trade on the primary bar; extra_bar_types (e.g. higher TFs)
        # feed indicators but must not drive order submission.
        if bar.bar_type != self.config.bar_type:
            return
        if not self.indicators_initialized():
            return

        if self.rsi.value <= self.config.oversold:
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.BUY)
            elif self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.BUY)
        elif self.rsi.value >= self.config.overbought:
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.SELL)
            elif self.portfolio.is_net_long(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.SELL)

    def _submit_order(self, side: OrderSide) -> None:
        rv = self.rsi.value
        p = int(self.config.rsi_period)
        if side == OrderSide.BUY:
            reason = f"RSI({p})={rv:.2f} ≤ oversold({self.config.oversold})"
        else:
            reason = f"RSI({p})={rv:.2f} ≥ overbought({self.config.overbought})"
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
            tags=[reason],
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


# ── Registry exports ──
STRATEGY_NAME = "RSI Mean Reversion"
STRATEGY_CLASS = RSIMeanReversionStrategy
CONFIG_CLASS = RSIConfig
DESCRIPTION = "Buy when RSI is oversold, sell when overbought."
PARAMS = {
    "rsi_period": {"label": "RSI Period", "min": 2, "max": 50, "default": 14},
    "overbought": {"label": "Overbought Level", "min": 50.0, "max": 95.0, "default": 70.0},
    "oversold": {"label": "Oversold Level", "min": 5.0, "max": 50.0, "default": 30.0},
}
