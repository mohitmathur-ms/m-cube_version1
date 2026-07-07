---
name: live-trading-api-and-docs-gap
description: Live-trading node/factory/sandbox API location in nautilus_trader 1.224.0, and which parts the ntm3 concept docs do NOT cover
metadata:
  type: reference
---

Live trading question coverage for nautilus_trader==1.224.0.

**Concept docs DO cover:** `Live Trading.txt` (reconciliation is ~the whole doc),
`Adapters.txt` (adapter anatomy, InstrumentProviderConfig load_all/load_ids,
request/subscribe API), `Configuration.txt` (per-adapter config classes +
common fields table, LiveExecEngineConfig), `Cache.txt` (Redis DatabaseConfig +
LiveExecEngineConfig purge timers), `MessageBus.txt` (Redis external bus),
`Logger.txt` + `Execution.txt` (RiskEngine in all envs; allow_overfills).

**Concept docs do NOT cover (had to use package source):**
- `TradingNode` / `TradingNodeBuilder` and the factory-registration API.
  `Live Trading.txt` line 11 defers this to a "Configure a live trading node"
  how-to guide that is NOT in the ntm3 library. `Architecture.txt` is EMPTY.
- The SANDBOX execution client (not mentioned anywhere in the concept PDFs).
- Per-adapter credential/testnet field names (Configuration.txt defers to
  per-venue integration guides not in the library).

**Confirmed-from-source API (cite as source, not docs):**
- `nautilus_trader/live/node.py`: `class TradingNode` (L39); methods
  `add_data_client_factory(name, factory)` (L230), `add_exec_client_factory`
  (L251), `build()` (272), `run()`/`run_async()`, `stop()`/`stop_async()`,
  `dispose()`. Real asyncio loop, long-lived process.
- `nautilus_trader/live/node_builder.py`: `class TradingNodeBuilder` (L34) —
  same add_*_client_factory methods.
- `nautilus_trader/live/config.py`: `class TradingNodeConfig(NautilusKernelConfig)`
  (L313) with fields cache, data_engine=LiveDataEngineConfig,
  risk_engine=LiveRiskEngineConfig, exec_engine=LiveExecEngineConfig,
  data_clients/exec_clients dicts keyed by venue name.
- `nautilus_trader/adapters/sandbox/{config,execution,factory}.py`:
  `SandboxExecutionClientConfig` (venue, starting_balances, oms_type,
  account_type, bar_execution, etc.). SANDBOX = real live DATA client +
  the SAME simulated matching engine as backtest (confirmed by
  `Backtesting.txt` L1325/1340: "simulated exchange used by both backtest AND
  sandbox"). Lowest-risk paper-trading milestone; reuses backtest fill model.

**Verified 2026-06-01 (1.224.0) additional specifics:**
- All config classes re-exported from top-level `nautilus_trader.config`
  (config/__init__.py): TradingNodeConfig, CacheConfig (real def in
  cache/config.py L23), DatabaseConfig + MessageBusConfig (common/config.py
  L305/L360), InstrumentProviderConfig, LoggingConfig, LiveExecEngineConfig,
  LiveRiskEngineConfig.
- TradingNodeConfig fields (live/config.py L336): environment (default LIVE),
  trader_id, data_engine, risk_engine, exec_engine, data_clients/exec_clients
  (dict keyed by venue name; key MUST equal the name passed to
  add_*_client_factory). cache/message_bus/logging inherited from KernelConfig.
- node.build() must precede run(); node.run() is blocking (run_until_complete).
  Add strategies via node.trader.add_strategy(...) (same as backtest).
- Binance: BinanceLiveDataClientFactory (factories.py L247),
  BinanceLiveExecClientFactory (L358); BinanceDataClientConfig (config.py L72) /
  BinanceExecClientConfig (L127) key fields api_key/api_secret/account_type
  (BinanceAccountType, default SPOT)/environment (LIVE/TESTNET/DEMO; testnet=
  deprecated).
- SANDBOX: SandboxExecutionClientConfig REQUIRED fields = venue:str,
  starting_balances:list[str]; defaults oms_type=NETTING, account_type=MARGIN,
  bar_execution=True. Factory SandboxLiveExecClientFactory (factory.py L27).
  Paper-trading shape = Binance LIVE data client + SANDBOX exec client.
- LiveExecEngineConfig reconciliation knobs (config.py L199+): reconciliation
  (default True), reconciliation_lookback_mins, inflight_check_*,
  open_check_interval_secs/position_check_interval_secs (continuous recon, off
  unless set), generate_missing_orders.
- Strategy portability CONFIRMED: same Strategy subclass runs backtest->
  sandbox->live unchanged (shared NautilusKernel/Trader). Only behavioral
  caveat is async fills on the REAL exec client (react in on_order_filled,
  not inline) — sandbox fills near-synchronously like backtest.

**m-cube mapping:** live needs a NEW execution path; cannot reuse
core/backtest_runner.py per-slot ProcessPoolExecutor / grouping / Path B. But
L1 strategies/ and L2 core/managed_strategy.py (order_factory/submit_order/exit
engine) run UNCHANGED — that is the Nautilus promise (Live Trading.txt L8).
See [[backtest-api-choice-matrix]].
