---
name: per-strategy-pnl-base-ccy
description: NautilusTrader 1.224.0 — how to compute per-strategy-scoped combined P&L in account base currency; Portfolio has NO strategy-scoped PnL, must sum per-position via cache + get_xrate.
metadata:
  type: reference
---

How to get strategy-scoped combined (realized+unrealized) P&L in account base
currency from a monitor Strategy, v1.224.0. Source-verified against
`venv/Lib/site-packages/nautilus_trader/{portfolio/portfolio.pyx, cache/cache.pyx, model/position.pyx, model/position.pxd}` + Portfolio.txt / Cache.txt / Positions.txt.

## Portfolio has NO per-strategy PnL
Public Portfolio PnL/exposure methods are scoped only by venue/account_id/
instrument_id/target_currency — NEVER strategy_id:
- realized_pnl(InstrumentId, account_id=None, target_currency=None)  L1110
- unrealized_pnl(InstrumentId, price=None, account_id=None, target_currency=None) L1141
- total_pnl(InstrumentId, price=None, account_id=None, target_currency=None) L1186
- net_exposure(...) L1245; *_pnls/*_exposures dict variants take (venue, account_id, target_currency).
(The strategy_id=None hits in portfolio.pyx are its OWN internal cache.positions_open calls.)

## Cache positions DO filter by strategy_id (and instrument_id)
cache.positions / positions_open(venue, instrument_id, strategy_id, side, account_id);
positions_closed(venue, instrument_id, strategy_id, account_id). All return list[Position].
(cache.pyx L5327/5362/5397.)

## Position PnL
- Position.realized_pnl = readonly Money attr (settlement ccy), can be None until first realized fill (position.pxd L103).
- Position.unrealized_pnl(price: Price) -> Money in SETTLEMENT ccy; requires a Price; FLAT -> Money(0). (position.pyx L812)
- Position.total_pnl(price) = realized + unrealized, settlement ccy. (L842)

## Conversion to base ccy = replicate Portfolio internally
Portfolio._convert_money (portfolio.pyx L2750) does:
cache.get_xrate(venue, from=settlement_ccy, to=base_ccy, price_type=MID) then Money(money*xrate, base).
So from a Strategy: self.cache.get_xrate(venue, from_currency, to_currency, price_type=PriceType.MID) -> float|None.
Returns None + logs error if no rate (no silent 1.0). If use_mark_xrates: MARK first (cache.get_mark_xrate) then MID fallback.

## get_xrate price source (relevant to m-cube)
get_xrate (cache.pyx L3505) builds bid/ask table per venue from latest QUOTE TICK bid/ask;
if no quotes, FALLS BACK to latest BID bar close + ASK bar close (_bars_bid/_bars_ask). So m-cube's
MID+BID+ASK bar loading (_pair_bid_ask_bar_type) lets FX cross rates resolve from bar closes with no ticks.

## NETTING: summing per-position is the correct & only route
Under OmsType.NETTING positions are isolated per (instrument, strategy) (strategy in position id),
so cache.positions(strategy_id=...) is clean. No Portfolio aggregate exists -> sum
realized+unrealized(price), converted, over those positions. GOTCHA: closed-then-reopened
NETTING positions reset; prior-cycle realized PnL lives in cache.position_snapshots(position_id)
(Portfolio aggregates snapshots; manual sum of live Position objects misses earlier cycles).

Related: [[oms-types-position-ids]], [[streaming-batched-backtest]].
