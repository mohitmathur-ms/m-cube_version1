"""Live conservative-fill model (single-pass §4.2 exit fills).

A bar backtest engine never fills you *worse* than the market, so the conservative
SL/TP exit fill (`MIN/MAX(vwap, hit)` — deliberately adverse) historically had to be
applied as a POST-RUN reprice. NautilusTrader's supported live hook is
``FillModel.get_orderbook_for_fill_simulation()``: returning a synthetic order book
makes the matching engine fill against THAT book. So a leg writes its computed
conservative price to the shared pending-fill-price bus before submitting its close,
and this model hands the engine a synthetic book at that price → the close fills at
the conservative price DURING the run, no post-run step.

Verified mechanism: a synthetic book with deep liquidity at price P on both sides
forces any market order to fill at exactly P.
"""
from __future__ import annotations

from nautilus_trader.backtest.config import FillModelConfig
from nautilus_trader.backtest.models.fill import FillModel
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.enums import BookType, OrderSide
from nautilus_trader.model.objects import Quantity


class ConservativeFillModelConfig(FillModelConfig, frozen=True):
    """Config form so a BacktestNode can build this model declaratively
    (``BacktestVenueConfig.fill_model = ImportableFillModelConfig(...)``). Carries the
    ``portfolio_id`` used to resolve the shared pending-fill-price bus — a plain
    string, so it serializes cleanly through the node's config pipeline."""

    portfolio_id: str = ""

# Exit closes that get a pinned conservative/directional fill (vs entries, which fill
# normally). SL/TP → §4.2 conservative (MIN/MAX(vwap, hit)); Squareoff/Portfolio →
# §8.1 directional close (bid_close/ask_close). The leg pins the right price per close
# type; the tag distinguishes these exits from entry orders.
_SL_PREFIXES = ("Stop Loss", "Trailing SL", "Reverse on SL")
_TP_PREFIXES = ("Take Profit", "Reverse on TP")
_MARKET_PREFIXES = ("Squareoff", "Portfolio")


class ConservativeFillModel(FillModel):
    """Forces a leg's exit close to fill at the price the leg pinned (per instrument)
    on the shared pending-fill-price bus: SL/TP → §4.2 conservative, Squareoff/
    Portfolio → §8.1 directional close. Scoped by tag to those exit closes so an
    entry on the same instrument can't grab the pin (an unscoped pin caused Format A
    divergence). One-shot: the pin is consumed on use so it can't bleed onto a later
    order."""

    def __init__(self, portfolio_id: str | None = None, config=None):
        super().__init__(config=config)
        # portfolio_id may arrive positionally (unified low-level path:
        # ``ConservativeFillModel(pid)``) OR inside the config (BacktestNode path:
        # ``ConservativeFillModel(config=ConservativeFillModelConfig(portfolio_id=pid))``,
        # built by FillModelFactory). Same shared bus either way.
        pid = portfolio_id
        if pid is None and config is not None:
            pid = getattr(config, "portfolio_id", None)
        # Lazy import avoids any import-time coupling with managed_strategy.
        from core.managed_strategy import get_pf_fill_px_bus
        self._fill_px = get_pf_fill_px_bus(pid or "")

    def get_orderbook_for_fill_simulation(self, instrument, order, best_bid, best_ask):
        iid = str(instrument.id)
        # The leg pins per (instrument, strategy) so two portfolios sharing a venue/
        # instrument can't collide. Look up the composite key first; fall back to the
        # bare-iid key (keeps single-portfolio and any legacy writer byte-identical,
        # since order.strategy_id == the pinning leg's id).
        sid = ""
        try:
            sid = str(order.strategy_id)
        except Exception:  # noqa: BLE001
            sid = ""
        matched = f"{iid}|{sid}" if sid else None
        px = self._fill_px.get(matched) if matched is not None else None
        if px is None:
            matched, px = iid, self._fill_px.get(iid)
        if px is None or px <= 0:
            return None  # no pinned conservative price → engine's standard fill
        # Apply ONLY to pinned exit closes (SL/TP + squareoff/portfolio, by tag);
        # entries and anything else fill normally.
        tags = getattr(order, "tags", None) or []
        tag0 = str(tags[0]) if tags else ""
        if not (tag0.startswith(_SL_PREFIXES) or tag0.startswith(_TP_PREFIXES)
                or tag0.startswith(_MARKET_PREFIXES)):
            return None
        self._fill_px.pop(matched, None)  # one-shot — consume so it can't bleed
        p = instrument.make_price(px)
        book = OrderBook(instrument_id=instrument.id, book_type=BookType.L2_MBP)
        # Deep liquidity at the conservative price on BOTH sides → any market order
        # (the SL/TP close, either side) fills at exactly that price.
        sz = Quantity(1_000_000, instrument.size_precision)
        book.add(BookOrder(side=OrderSide.BUY, price=p, size=sz, order_id=1), 0, 0)
        book.add(BookOrder(side=OrderSide.SELL, price=p, size=sz, order_id=2), 0, 0)
        return book
