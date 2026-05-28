---
name: backtest-api-choice-matrix
description: NautilusTrader 1.224.0 official doc guidance on when to use BacktestEngine (low-level) vs BacktestNode (high-level), and capabilities each retains exclusively. Source: Backtesting.txt.
metadata:
  type: reference
---

Per `.claude/agents/ntm3_docs/Backtesting.txt` (single authoritative source for the API split).

## "Choosing an API level" (p. 1) and "Best practices" (p. 7)

Low-level `BacktestEngine` is recommended when:
- Data fits in RAM.
- Not using Parquet catalog format.
- Need fine-grained control (swap components, parameter optimisation against same data via `reset()`).

High-level `BacktestNode` + `BacktestRunConfig` is recommended when:
- Data exceeds memory (catalog streaming).
- Want ParquetDataCatalog convenience.
- Want to bundle many run configs and execute in one `node.run()`.

Production backtesting → BacktestNode. Parameter optimisation against same data → `BacktestEngine.reset()`. Quick experiments → either.

## Things `BacktestNode` adds (p. 5–6)

- `BacktestRunConfig` list bundling — many engines, one `node.run()`.
- Catalog-driven streaming via `BacktestDataConfig` (lazy chunk consumption).
- Fresh engine per run — no `reset()` needed; clean state guaranteed.
- `Importable{Actor,Strategy,ExecAlgorithm,Controller}Config` indirection.

## Things only `BacktestEngine` documents (p. 2, p. 4)

- Accepts raw lists of `Data` objects via `add_data(...)`.
- `sort=False` + final `sort_data()` optimisation for multi-instrument loads.
- `add_data_iterator(generator=...)` for lazy chunked single-run streaming.
- `engine.run(streaming=True)` + manual `clear_data()` loop for full control.

## What the docs do NOT confirm (gaps)

- `BacktestDataConfig` field schema is NOT published in the concept doc — cannot cite "it only accepts catalog_path/instrument_id/bar_types/start_time/end_time" from docs. Must check `nautilus_trader.config` source if a definitive list is needed.
- Per-strategy P&L recovery pattern is not in Backtesting.txt. Strategies.txt is empty in this checkout. Indirect doc support: `Execution.txt:134` (NETTING position IDs are `{instrument_id}-{strategy_id}`), `Cache.txt:253/:312` (cache filtered by `strategy_id`), `Reports.txt` (all report rows carry `strategy_id`). The `order_id_tag` pattern does NOT appear anywhere in the 28 concept .txt files.
- No documented state-carrying semantics across `BacktestRunConfig` sub-runs; "Each run gets a fresh engine with clean state" (p. 6) implies you start flat every sub-config — any continuity is application-level.

## Reset semantics (p. 5–6)

`BacktestEngine.reset()`: wipes trading state (orders/positions/balances), removes strategy instances (must re-add), keeps data and instruments (`CacheConfig.drop_instruments_on_reset=False` default).

## How to apply in m-cube

- Path A (per-slot or `_USE_GROUPING`) aligns with the "fine-grained control / param optimisation / data fits in RAM" branch.
- Path B (`_USE_BACKTEST_NODE=1`) aligns with the "production / catalog / multi-config" branch.
- The `_path_b_supports_filters` fallback (`core/backtest_runner/path_b/__init__.py`) for `run_on_days` / `entry_start_time` is consistent with the documented surface — docs do not expose a way to inject pre-filtered bars through `BacktestDataConfig`. Removing it would require an undocumented mechanism.

Related: [[oms-types-position-ids]] (strategy_id role in NETTING position IDs).
