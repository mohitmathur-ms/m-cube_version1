"""
PriceSelector — thin dispatcher over NautilusTrader's native PriceType / BarType.

Design rules:
- No custom price math. All pricing comes from typed Bar streams (BID / ASK / MID)
  already in the Cache, or from the latest QuoteTick.
- Side-aware: every method takes OrderSide and routes to the correct book side.
- No look-ahead: only reads bars/quotes already published to the message bus.
"""
from __future__ import annotations

from decimal import Decimal

from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.data import Bar, BarType, QuoteTick
from nautilus_trader.model.enums import AggregationSource, BarAggregation, OrderSide, PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price


class PriceSelector:
    """
    Resolves the *correct* price for each backtest action using native PriceType bars.

    Conventions for a directional short-seller using market orders:
      - SELL  -> consumes BID side (you receive the bid)
      - BUY   -> consumes ASK side (you pay the ask)
      - SIGNAL/MONITOR -> MID (symmetric, no spread bias)
    """

    def __init__(
        self,
        cache: Cache,
        instrument_id: InstrumentId,
        bar_step: int = 1,
        bar_aggregation: BarAggregation = BarAggregation.MINUTE,
    ) -> None:
        self._cache = cache
        self._instrument_id = instrument_id

        # Build the three typed BarTypes once. NautilusTrader will look these up
        # from the cache directly — no manual bar storage needed.
        self._bar_type_bid = self._build_bar_type(bar_step, bar_aggregation, PriceType.BID)
        self._bar_type_ask = self._build_bar_type(bar_step, bar_aggregation, PriceType.ASK)
        self._bar_type_mid = self._build_bar_type(bar_step, bar_aggregation, PriceType.MID)

    # ---------- public API ----------

    @property
    def bar_types(self) -> tuple[BarType, BarType, BarType]:
        """Return the three BarTypes the strategy must subscribe to in on_start()."""
        return self._bar_type_bid, self._bar_type_ask, self._bar_type_mid

    def signal_price(self) -> Price | None:
        """MID close — for indicators, signals, and chart monitoring."""
        bar = self._cache.bar(self._bar_type_mid)
        return bar.close if bar is not None else None

    def entry_price(self, side: OrderSide) -> Price | None:
        """Realistic fill price for a market order opening a position."""
        return self._side_close(side)

    def exit_price(self, side: OrderSide) -> Price | None:
        """
        Realistic fill price for closing a position.
        `side` must be the CLOSING order side (opposite of position side).
        """
        return self._side_close(side)

    def stop_trigger_price(self, position_side: OrderSide) -> Price | None:
        """
        Reference price to test whether a stop should fire this bar.
          - SHORT position -> stop hit when ASK rises (use ASK.high)
          - LONG  position -> stop hit when BID falls (use BID.low)
        """
        if position_side == OrderSide.SELL:  # short position
            bar = self._cache.bar(self._bar_type_ask)
            return bar.high if bar is not None else None
        elif position_side == OrderSide.BUY:  # long position
            bar = self._cache.bar(self._bar_type_bid)
            return bar.low if bar is not None else None
        return None

    def target_trigger_price(self, position_side: OrderSide) -> Price | None:
        """
        Reference price to test whether a profit target should fire this bar.
          - SHORT position -> target hit when ASK falls (use ASK.low)
          - LONG  position -> target hit when BID rises (use BID.high)
        """
        if position_side == OrderSide.SELL:  # short position
            bar = self._cache.bar(self._bar_type_ask)
            return bar.low if bar is not None else None
        elif position_side == OrderSide.BUY:  # long position
            bar = self._cache.bar(self._bar_type_bid)
            return bar.high if bar is not None else None
        return None

    def latest_quote_price(self, side: OrderSide) -> Price | None:
        """
        For tick-level decisions: pull bid or ask from the latest QuoteTick.
        Useful when bars are too coarse for stop/target precision.
        """
        quote: QuoteTick | None = self._cache.quote_tick(self._instrument_id)
        if quote is None:
            return None
        return quote.bid_price if side == OrderSide.SELL else quote.ask_price

    # ---------- helpers ----------

    def _side_close(self, side: OrderSide) -> Price | None:
        bar_type = self._bar_type_bid if side == OrderSide.SELL else self._bar_type_ask
        bar = self._cache.bar(bar_type)
        return bar.close if bar is not None else None

    def _build_bar_type(
        self,
        step: int,
        aggregation: BarAggregation,
        price_type: PriceType,
    ) -> BarType:
        spec_str = f"{self._instrument_id}-{step}-{aggregation.name}-{price_type.name}-EXTERNAL"
        return BarType.from_str(spec_str)
