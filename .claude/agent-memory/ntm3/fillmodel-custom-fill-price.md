---
name: fillmodel-custom-fill-price
description: NautilusTrader 1.224.0 FillModel can force arbitrary worse-than-market fill prices during the backtest pass via get_orderbook_for_fill_simulation() (synthetic order book). No explicit per-order fill-price setter exists.
metadata:
  type: project
---

In nautilus_trader 1.224.0 you CAN model an arbitrary, strategy-computed worse-than-market fill
price DURING the backtest pass (not just post-run repricing) — via a custom `FillModel` subclass.

**Why:** m-cube's SL/TP exit engine wants conservative `MIN/MAX(vwap, hit)` fills (adverse slippage)
applied in-run. Today it does post-run repricing of the positions report; user asked whether the
engine can do it natively.

**How to apply:**
- `FillModel` (`venv/Lib/site-packages/nautilus_trader/backtest/models/fill.pyx`) was expanded in
  1.224.0. Constructor params are ONLY `prob_fill_on_limit` (default 1.0), `prob_slippage`
  (default 0.0), `random_seed`, `config`. There is NO `prob_fill_on_stop` field (docs/older versions
  mention it; not present here). `prob_slippage` = probability of exactly ONE tick of adverse
  slippage (fixed +/-1 tick, not arbitrary), L1/bar data only.
- The base hooks `is_limit_filled()` / `is_slipped()` / `fill_limit_inside_spread()` return BOOLEANS
  only — they cannot set a price.
- The price-control hook is `cpdef OrderBook get_orderbook_for_fill_simulation(self, instrument,
  order, best_bid, best_ask)`. Returning a synthetic `OrderBook` makes the matching engine fill the
  order against THAT book's price levels — so you can place liquidity at ANY price (incl. worse than
  market). Returning None = default fill logic. Per docs Backtesting.txt L1109-1123: "matching engine
  calls get_orderbook_for_fill_simulation()... fills execute against that book's liquidity."
- 10 built-in example subclasses ship: BestPriceFillModel, OneTickSlippageFillModel, TwoTier/Three
  Tier, ProbabilisticFillModel, SizeAware, LimitOrderPartial, MarketHours, VolumeSensitive,
  CompetitionAware. None compute VWAP-conservative price — you'd subclass and inject your own level.
- Wiring: Path B / BacktestNode uses `BacktestVenueConfig.fill_model=ImportableFillModelConfig(
  fill_model_path=..., config_path=..., config={...})`. Path A (BacktestEngine) takes a FillModel
  instance on `add_venue(..., fill_model=...)`.
- LIMIT marketable fills: matching_core.pyx `is_limit_marketable` — a marketable LIMIT fills at the
  better market price, never worse than its own limit; a LIMIT cannot fill you worse than its price.
  So to model adverse exits you cannot rely on a LIMIT's own price; use the synthetic-book hook.
- CAVEAT: `get_orderbook_for_fill_simulation` receives `order` but is per-instrument liquidity, not a
  literal per-order price setter. To key the worse price off live strategy state (running VWAP) you
  must give the FillModel access to that state (custom subclass holding a reference / shared object),
  since the hook only gets instrument/order/best_bid/best_ask. OrderMatchingEngine itself is compiled
  (.pyd, no .pyx shipped) so the exact bar->OHLC tick fill path is not source-citable in this wheel.
