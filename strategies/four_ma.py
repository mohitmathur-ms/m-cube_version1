"""4 Moving Averages Strategy - Buy on bullish MA alignment, sell on bearish."""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.indicators import ExponentialMovingAverage, SimpleMovingAverage
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class FourMAConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    extra_bar_types: list[BarType] | None = None
    use_ema: bool = False
    ma1_period: PositiveInt = 5
    ma2_period: PositiveInt = 10
    ma3_period: PositiveInt = 20
    ma4_period: PositiveInt = 50


class FourMAStrategy(Strategy):
    """Buy when MA1 > MA2 > MA3 > MA4 (bullish alignment), sell on bearish alignment."""

    def __init__(self, config: FourMAConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument = None
        ma_cls = ExponentialMovingAverage if config.use_ema else SimpleMovingAverage
        self.ma1 = ma_cls(config.ma1_period)
        self.ma2 = ma_cls(config.ma2_period)
        self.ma3 = ma_cls(config.ma3_period)
        self.ma4 = ma_cls(config.ma4_period)

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return
        for ma in (self.ma1, self.ma2, self.ma3, self.ma4):
            self.register_indicator_for_bars(self.config.bar_type, ma)
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

        v1, v2, v3, v4 = self.ma1.value, self.ma2.value, self.ma3.value, self.ma4.value
        p1, p2, p3, p4 = (int(self.config.ma1_period), int(self.config.ma2_period),
                          int(self.config.ma3_period), int(self.config.ma4_period))

        if v1 > v2 > v3 > v4:
            reason = (f"4MA BUY: ma{p1}>ma{p2}>ma{p3}>ma{p4} "
                      f"({v1:.4f}/{v2:.4f}/{v3:.4f}/{v4:.4f})")
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.BUY, reason)
            elif self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.BUY, reason)
        elif v1 < v2 < v3 < v4:
            reason = (f"4MA SELL: ma{p1}<ma{p2}<ma{p3}<ma{p4} "
                      f"({v1:.4f}/{v2:.4f}/{v3:.4f}/{v4:.4f})")
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
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


# ── Registry exports ──
STRATEGY_NAME = "4 Moving Averages"
STRATEGY_CLASS = FourMAStrategy
CONFIG_CLASS = FourMAConfig
DESCRIPTION = "Buy when 4 MAs align bullish (MA1>MA2>MA3>MA4), sell on bearish alignment. Toggle SMA/EMA."
PARAMS = {
    "use_ema": {"label": "Use EMA", "default": False},
    "ma1_period": {"label": "MA1 Period", "min": 2, "max": 200, "default": 5},
    "ma2_period": {"label": "MA2 Period", "min": 2, "max": 200, "default": 10},
    "ma3_period": {"label": "MA3 Period", "min": 2, "max": 200, "default": 20},
    "ma4_period": {"label": "MA4 Period", "min": 2, "max": 500, "default": 50},
}
