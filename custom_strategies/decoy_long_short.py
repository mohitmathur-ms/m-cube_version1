"""
Decoy Long / Decoy Short Strategy for NautilusTrader Crypto Dashboard
======================================================================

Ported from Pine Script by blockchainpcn.

Logic:
  - Decoy Long:  Higher High + Higher Low but RED candle   -> BUY
  - Decoy Short: Lower High  + Lower Low  but GREEN candle -> SELL

The idea is that the candle color "deceives" relative to structure:
  structure says UP but candle closes red  = buying opportunity,
  structure says DOWN but candle closes green = selling opportunity.

Required exports (do not remove):
    STRATEGY_NAME  - Display name for the UI
    CONFIG_CLASS   - Your config class (must inherit StrategyConfig)
    STRATEGY_CLASS - Your strategy class (must inherit Strategy)
    DESCRIPTION    - One-line description
    PARAMS         - Dict of configurable parameters for the UI
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


# =============================================================================
# 1. CONFIG CLASS
# =============================================================================

class DecoyConfig(StrategyConfig, frozen=True):
    # --- Required fields (do not remove) ---
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    extra_bar_types: list[BarType] | None = None

    # --- Custom parameters ---
    use_strict_color: bool = True
    ignore_doji: bool = True
    min_body_pct: float = 0.0


# =============================================================================
# 2. STRATEGY CLASS
# =============================================================================

class DecoyStrategy(Strategy):
    """Decoy Long / Decoy Short: trade structural deception candles."""

    def __init__(self, config: DecoyConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None
        # Store previous bar for structure comparison
        self.prev_bar: Bar | None = None

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return
        self.subscribe_bars(self.config.bar_type)
        if self.config.extra_bar_types:
            for bt in self.config.extra_bar_types:
                self.subscribe_bars(bt)

    def on_bar(self, bar: Bar) -> None:
        # Only trade on the primary bar; extra_bar_types (e.g. higher TFs)
        # feed state but must not drive signal generation — prev_bar should
        # only advance on real primary-bar minute boundaries.
        if bar.bar_type != self.config.bar_type:
            return

        if self.prev_bar is None:
            self.prev_bar = bar
            return

        cur_open = float(bar.open)
        cur_high = float(bar.high)
        cur_low = float(bar.low)
        cur_close = float(bar.close)
        prev_high = float(self.prev_bar.high)
        prev_low = float(self.prev_bar.low)
        prev_close = float(self.prev_bar.close)

        # --- Candle range and body ---
        rng = cur_high - cur_low
        body = abs(cur_close - cur_open)
        body_ok = True
        if self.config.min_body_pct > 0.0 and rng > 0:
            body_ok = (body / rng) * 100.0 >= self.config.min_body_pct

        # --- Candle color logic ---
        if self.config.use_strict_color:
            is_red = cur_close < cur_open
            is_green = cur_close > cur_open
            is_doji = cur_close == cur_open
            color_ok = not is_doji if self.config.ignore_doji else True
        else:
            is_red = cur_close < prev_close
            is_green = cur_close > prev_close
            color_ok = True

        # --- Structure conditions ---
        hh_hl = cur_high > prev_high and cur_low > prev_low
        lh_ll = cur_high < prev_high and cur_low < prev_low

        # --- Decoy signals ---
        decoy_long = hh_hl and is_red and color_ok and body_ok
        decoy_short = lh_ll and is_green and color_ok and body_ok

        # --- Trading logic ---
        if decoy_long:
            self.log.info("Decoy Long signal: HH+HL but red candle")
            if self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.BUY)

        elif decoy_short:
            self.log.info("Decoy Short signal: LH+LL but green candle")
            if self.portfolio.is_net_long(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.SELL)

        self.prev_bar = bar

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
# 3. REQUIRED EXPORTS (do not remove)
# =============================================================================

STRATEGY_NAME = "Decoy Long / Decoy Short"

CONFIG_CLASS = DecoyConfig

STRATEGY_CLASS = DecoyStrategy

DESCRIPTION = "Buy on structural highs with red candles (decoy long), sell on structural lows with green candles (decoy short)."

PARAMS = {
    "use_strict_color": {
        "label": "Strict Candle Color (Close vs Open)",
        "default": True,
    },
    "ignore_doji": {
        "label": "Ignore Doji Candles",
        "default": True,
    },
    "min_body_pct": {
        "label": "Min Body % of Range (0 = off)",
        "min": 0.0,
        "max": 100.0,
        "default": 0.0,
    },
}
