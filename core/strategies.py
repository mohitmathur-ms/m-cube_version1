"""
Strategy registry for the dashboard.

Provides multiple pre-built trading strategies that can be selected from the UI.
Each strategy wraps NautilusTrader's Strategy class with configurable parameters.
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.indicators import BollingerBands
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.indicators import SimpleMovingAverage
from nautilus_trader.indicators import RelativeStrengthIndex
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy


# =============================================================================
# EMA Cross Strategy
# =============================================================================

class EMACrossConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    fast_ema_period: PositiveInt = 10
    slow_ema_period: PositiveInt = 20


class EMACrossStrategy(Strategy):
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
        self.register_indicator_for_bars(self.config.bar_type, self.fast_ema)
        self.register_indicator_for_bars(self.config.bar_type, self.slow_ema)
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        if not self.indicators_initialized():
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
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


# =============================================================================
# RSI Mean Reversion Strategy
# =============================================================================

class RSIConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
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

    def on_bar(self, bar: Bar) -> None:
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
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


# =============================================================================
# Bollinger Bands Strategy
# =============================================================================

class BollingerConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    bb_period: PositiveInt = 20
    bb_std: float = 2.0


class BollingerBandsStrategy(Strategy):
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
        self.register_indicator_for_bars(self.config.bar_type, self.bb)
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        if not self.indicators_initialized():
            return

        close = float(bar.close)

        if close <= self.bb.lower:
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.BUY)
            elif self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.BUY)
        elif close >= self.bb.upper:
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.SELL)
            elif self.portfolio.is_net_long(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.SELL)

    def _submit_order(self, side: OrderSide) -> None:
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)

# =============================================================================
# 4 Moving Averages Strategy
# =============================================================================

class FourMAConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
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

    def on_bar(self, bar: Bar) -> None:
        if not self.indicators_initialized():
            return

        v1, v2, v3, v4 = self.ma1.value, self.ma2.value, self.ma3.value, self.ma4.value

        if v1 > v2 > v3 > v4:
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.BUY)
            elif self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.BUY)
        elif v1 < v2 < v3 < v4:
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.SELL)
            elif self.portfolio.is_net_long(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.SELL)

    def _submit_order(self, side: OrderSide) -> None:
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


# =============================================================================
# Strategy Registry
# =============================================================================

STRATEGY_REGISTRY = {
    "EMA Cross": {
        "strategy_class": EMACrossStrategy,
        "config_class": EMACrossConfig,
        "description": "Buy when fast EMA crosses above slow EMA, sell on cross below.",
        "params": {
            "fast_ema_period": {"label": "Fast EMA Period", "min": 2, "max": 100, "default": 10},
            "slow_ema_period": {"label": "Slow EMA Period", "min": 5, "max": 200, "default": 20},
        },
    },
    "RSI Mean Reversion": {
        "strategy_class": RSIMeanReversionStrategy,
        "config_class": RSIConfig,
        "description": "Buy when RSI is oversold, sell when overbought.",
        "params": {
            "rsi_period": {"label": "RSI Period", "min": 2, "max": 50, "default": 14},
            "overbought": {"label": "Overbought Level", "min": 50.0, "max": 95.0, "default": 70.0},
            "oversold": {"label": "Oversold Level", "min": 5.0, "max": 50.0, "default": 30.0},
        },
    },
    "Bollinger Bands": {
        "strategy_class": BollingerBandsStrategy,
        "config_class": BollingerConfig,
        "description": "Buy at lower band, sell at upper band.",
        "params": {
            "bb_period": {"label": "BB Period", "min": 5, "max": 100, "default": 20},
            "bb_std": {"label": "Std Deviations", "min": 0.5, "max": 4.0, "default": 2.0},
        },
    },
    "4 Moving Averages": {
        "strategy_class": FourMAStrategy,
        "config_class": FourMAConfig,
        "description": "Buy when 4 MAs align bullish (MA1>MA2>MA3>MA4), sell on bearish alignment. Toggle SMA/EMA.",
        "params": {
            "use_ema": {"label": "Use EMA", "default": False},
            "ma1_period": {"label": "MA1 Period", "min": 2, "max": 200, "default": 5},
            "ma2_period": {"label": "MA2 Period", "min": 2, "max": 200, "default": 10},
            "ma3_period": {"label": "MA3 Period", "min": 2, "max": 200, "default": 20},
            "ma4_period": {"label": "MA4 Period", "min": 2, "max": 500, "default": 50},
        },
    },
}
