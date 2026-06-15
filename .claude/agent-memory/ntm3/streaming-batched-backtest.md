---
name: streaming-batched-backtest
description: NautilusTrader 1.224.0 memory-bounded/streaming backtest mechanics — which API the streaming primitives belong to, state persistence across batches, and what the docs do NOT specify. Source Backtesting.txt + Portfolio.txt.
metadata:
  type: reference
---

Per `.claude/agents/ntm3_docs/Backtesting.txt` ("Optimization strategies" / "Strategy 3: Use streaming API", pp. 3-4) and `Portfolio.txt`.

## Streaming primitives are documented on the LOW-LEVEL BacktestEngine
The two streaming mechanisms are shown as `BacktestEngine` methods, NOT
`BacktestNode`/`BacktestDataConfig`:
1. Automatic chunking: `engine.add_data_iterator(data_name=..., generator=gen)`
   then `engine.run()` — chunks pulled lazily during ONE run() call.
2. Manual chunking: loop `engine.add_data(batch); engine.run(streaming=True);
   engine.clear_data()` then `engine.end()`. Doc explicitly says this is "the
   pattern used internally by BacktestNode."
So BacktestNode streams by wrapping the SAME low-level streaming loop. The
"data exceeds memory -> use high-level API" advice (p.1) is realized via this
internal manual-chunk loop over the ParquetDataCatalog.

## State persists across batches (continuous run)
Streaming mode is ONE continuous run: only raw data is discarded via
`clear_data()` between batches; trading state survives. Contrast `reset()`
(p.5) which DOES wipe orders/positions/balances/strategies — streaming does
NOT call reset between batches. `engine.end()` finalizes: flushes remaining
timers, stops engines, produces results. Timer caveat (p.4): in streaming mode
timer advancement stops when each batch's data exhausts; timers past the last
data point (e.g. bar-aggregation intervals) are deferred until more data
arrives or end() is called.

## Cross-instrument ordering
add_data sorts Data into monotonic order by `ts_init` (p.2). Docs do NOT
explicitly describe how cross-INSTRUMENT/cross-batch merge ordering works in
catalog streaming — within a batch the engine sorts; global ordering across
batches relies on batches being supplied in time order. Not spelled out for
BacktestNode catalog reads.

## Multi-currency combined P&L (Portfolio.txt)
Portfolio.total_pnl()/total_pnls(), unrealized_pnl(s), realized_pnl(s),
net_exposure(s) all take optional `target_currency`. Single combined figure IS
built-in: pass target_currency to get one-currency dict; without it, different
base currencies return a multi-key dict (per account base ccy). Requires xrate
data available or position flagged unpriceable (no silent 1.0). equity()/
mark_values() give continuous MTM valuation. Caveat: portfolio_returns() series
needs single-currency balance history — multi-currency account silently falls
back to position_returns; tearsheet aggregates per-venue series.

## EXACT SIGNATURES (verified from source v1.224.0)
Source: `venv/Lib/site-packages/nautilus_trader/backtest/engine.pyx` + `node.py` + `backtest/config.py`.
- `engine.run(start=None, end=None, run_config_id=None, streaming: bool = False)`
  -> when streaming=False it AUTO-calls `end()`; when True it pauses w/o finalizing. (engine.pyx ~L1300)
- `engine.add_data_iterator(str data_name, generator: Generator[list[Data],None,None], client_id=None)` (engine.pyx ~L920)
- `engine.add_data(list data, client_id=None, validate=True, sort=True)` (engine.pyx ~L781)
- `engine.clear_data()` -> drops raw data + rebuilds BacktestDataIterator; KEEPS instruments. Does NOT reset trading state. (engine.pyx ~L1254)
- `engine.end()` -> flushes tail timers to end_ns, stops all engines, settles venues. Only needed after streaming runs. (engine.pyx ~L1366)
- `engine.reset()` -> wipes ALL stateful fields (orders/positions/balances/strategies), KEEPS data+instruments. Different from clear_data. (engine.pyx ~L1176)
- `BacktestRunConfig(..., chunk_size: int | None = None)` -> "number of DATA POINTS per chunk during streaming"; None = oneshot (load all). (backtest/config.py L378-426)
- Canonical loop lives in `node.py::BacktestNode._run_streaming` (L528-612): builds ONE `DataBackendSession(chunk_size=chunk_size)`, `catalog.backend_session(...)` per data config, then `for chunk in session.to_query_result(): engine.add_data(capsule_to_list(chunk), validate=False, sort=True); engine.run(streaming=True); engine.clear_data()` then `engine.end()`. NO BacktestEngineConfig streaming flag exists — streaming is driven by run()'s param + RunConfig.chunk_size only.

## DOC GAPS (cannot cite from concept .txt)
- BacktestDataConfig field schema NOT published in concept .txt; but `BacktestRunConfig.chunk_size`
  IS documented in `backtest/config.py` docstring (number of data points per chunk).
  No `streaming` flag on BacktestEngineConfig anywhere.
- Actors.txt + Startegies.txt are EMPTY — cannot cite Actor's ability to read
  Portfolio/Cache or submit orders from the concept docs. Cache.txt DOES show
  self.cache.positions(strategy_id=...)/orders(...)/account_for_venue() and
  states Cache "enables data sharing between different strategies." Portfolio
  query API is in Portfolio.txt. So the portfolio-monitor-actor capability is
  inferable from Cache+Portfolio docs but the Actor lifecycle/order-submit
  surface itself is NOT in the .txt set.

Related: [[backtest-api-choice-matrix]], [[project-docs-library-empty]].
