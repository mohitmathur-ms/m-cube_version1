from __future__ import annotations

from decimal import Decimal

from nautilus_trader.indicators import RelativeStrengthIndex
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import ContingencyType, OrderSide, OrderType, PriceType, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.trading.strategy import Strategy, StrategyConfig

from price_selector import PriceSelector

PIP_SIZE = Decimal("0.0001")


class RsiMeanReversionConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    extra_bar_types: list[BarType] | None = None
    trade_size: Decimal = Decimal("10000")
    stop_loss_pips: int = 10
    target_pips: int = 20
    rsi_period: int = 14
    overbought: float = 70.0
    oversold: float = 30.0
    direction: str = "both"


class RsiMeanReversionStrategy(Strategy):
    def __init__(self, config: RsiMeanReversionConfig):
        super().__init__(config)
        self.instrument_id = config.instrument_id
        self.trade_size = config.trade_size
        self.stop_loss_pips = config.stop_loss_pips
        self.target_pips = config.target_pips
        self.period = config.rsi_period
        self.direction = config.direction

        if self.direction not in ("long", "short", "both"):
            raise ValueError("Invalid Direction Value")

        self.rsi = RelativeStrengthIndex(config.rsi_period)

        self.overbought = config.overbought / 100
        self.oversold = config.oversold / 100

        self._was_overbought: bool = False
        self._was_oversold: bool = False
        self._prev_val: float | None = None

        self.prices: PriceSelector | None = None

    def on_start(self) -> None:
        # Bars (BID/ASK/MID) are subscribed by the runner from config.bar_type +
        # config.extra_bar_types. PriceSelector reads them off self.cache.bar(...),
        # so no re-subscribe is needed here.
        self.prices = PriceSelector(self.cache, self.instrument_id)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.instrument_id)
        self.close_all_positions(self.instrument_id)

    def on_bar(self, bar: Bar):
        # Filter to MID bars — BID/ASK bars feed the cache for PriceSelector only.
        if bar.bar_type.spec.price_type != PriceType.MID:
            return

        self.rsi.handle_bar(bar)

        if not self.rsi.initialized:
            return

        if self.portfolio.is_net_short(self.instrument_id) or self.portfolio.is_net_long(self.instrument_id):
            self._prev_val = float(self.rsi.value)
            return

        wants_short = self._signal_says_short()
        wants_long = self._signal_says_long()

        if self.direction in ("short", "both") and wants_short:
            self._enter_short()
        elif self.direction in ("long", "both") and wants_long:
            self._enter_long()

        self._prev_val = float(self.rsi.value)

    def _signal_says_short(self) -> bool:
        current = float(self.rsi.value)

        if current >= self.overbought:
            self._was_overbought = True
            return False

        elif self._was_overbought and self._prev_val is not None and current < self._prev_val:
            self._was_overbought = False
            return True

        else:
            return False

    def _signal_says_long(self) -> bool:
        current = float(self.rsi.value)

        if current <= self.oversold:
            self._was_oversold = True
            return False

        elif self._was_oversold and self._prev_val is not None and current > self._prev_val:
            self._was_oversold = False
            return True

        else:
            return False

    def _enter_short(self):
        if self.prices is None:
            return

        entry_px = self.prices.entry_price(OrderSide.SELL)
        if entry_px is None:
            return

        instrument = self.cache.instrument(self.instrument_id)
        precision = instrument.price_precision

        stop_offset = self.stop_loss_pips * PIP_SIZE
        target_offset = self.target_pips * PIP_SIZE

        stop_px = Price(Decimal(str(entry_px)) + stop_offset, precision)
        target_px = Price(Decimal(str(entry_px)) - target_offset, precision)

        qty = Quantity.from_str(str(self.trade_size))

        order_list = self.order_factory.bracket(
            instrument_id=self.instrument_id,
            order_side=OrderSide.SELL,
            quantity=qty,
            contingency_type=ContingencyType.OUO,
            entry_order_type=OrderType.MARKET,
            sl_trigger_price=stop_px,
            tp_order_type=OrderType.MARKET_IF_TOUCHED,
            tp_trigger_price=target_px,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order_list(order_list)

    def _enter_long(self):
        if self.prices is None:
            return

        entry_px = self.prices.entry_price(OrderSide.BUY)
        if entry_px is None:
            return

        instrument = self.cache.instrument(self.instrument_id)
        precision = instrument.price_precision

        stop_offset = self.stop_loss_pips * PIP_SIZE
        target_offset = self.target_pips * PIP_SIZE

        stop_px = Price(Decimal(str(entry_px)) - stop_offset, precision)
        target_px = Price(Decimal(str(entry_px)) + target_offset, precision)

        qty = Quantity.from_str(str(self.trade_size))

        order_list = self.order_factory.bracket(
            instrument_id=self.instrument_id,
            order_side=OrderSide.BUY,
            quantity=qty,
            contingency_type=ContingencyType.OUO,
            entry_order_type=OrderType.MARKET,
            sl_trigger_price=stop_px,
            tp_order_type=OrderType.MARKET_IF_TOUCHED,
            tp_trigger_price=target_px,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order_list(order_list)


STRATEGY_NAME = "RSI Mean Reversion (BID/ASK/MID)"
CONFIG_CLASS = RsiMeanReversionConfig
STRATEGY_CLASS = RsiMeanReversionStrategy
DESCRIPTION = "RSI overbought/oversold reversion with BID/ASK/MID realistic-fill PriceSelector. Requires 3 bar types per instrument."

PARAMS = {
    "stop_loss_pips": {"label": "Stop Loss (pips)", "default": 10, "min": 1, "max": 500},
    "target_pips":    {"label": "Target (pips)",    "default": 20, "min": 1, "max": 1000},
    "rsi_period":     {"label": "RSI Period",       "default": 14, "min": 2,  "max": 200},
    "overbought":     {"label": "Overbought (%)",   "default": 70.0, "min": 50.0, "max": 99.0},
    "oversold":       {"label": "Oversold (%)",     "default": 30.0, "min": 1.0,  "max": 50.0},
}
