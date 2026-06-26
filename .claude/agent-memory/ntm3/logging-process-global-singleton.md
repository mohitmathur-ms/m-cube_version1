---
name: logging-process-global-singleton
description: The ONE genuine process-global singleton in NautilusTrader is the Rust logging subsystem (init_logging/LogGuard); engines otherwise isolated. Corrects the over-broad "no process-global state" claim.
metadata:
  type: project
---

NautilusTrader's "global singleton" = the **Rust logging subsystem**, not the trading components.

**The singleton (per process):** logging is implemented in Rust, runs in a dedicated thread, fed by an MPSC channel. `init_logging()` (`common/component.pyx:1215`) can be called ONCE per process — `component.pyx:1298-1299` does `if logging_is_initialized(): raise RuntimeError("Logging subsystem already initialized")`. Returns a `LogGuard` (component.pyx:1203). Source confirms `Logger.txt` (Logging concept doc).

**LogGuard ref-counting (Logger.txt pp.9-11):** atomic counter; up to 255 concurrent LogGuards; when count hits 0 (last dropped) the logging thread is joined + buffers flushed. A `NautilusKernel` (so every `BacktestEngine`/`TradingNode`) initializes logging on construction and holds a LogGuard.

**The multi-engine gotcha (Logger.txt p.10, "Why use LogGuard?"):** running engines SEQUENTIALLY in one process: when engine #1 is `dispose()`d its LogGuard drops, the channel + Rust Logger close, and engine #2's logs fail with `Error sending log event: [INFO] ...`. Fix: grab `engine.get_log_guard()` from the FIRST engine and KEEP the reference alive across all runs (Logger.txt p.11 loop example). m-cube sidesteps this entirely by running each slot in its own PROCESS (ProcessPoolExecutor) — separate process = separate logging singleton, no LogGuard juggling needed. Only relevant if m-cube ever runs sequential engines in ONE process.

**Other once-per-process inits:** tracing subscriber (`init_tracing`/`use_tracing=True`) — once per process; subsequent kernel creations skip re-init, but direct repeat `init_tracing()` raises (Logger.txt p.13). `RUST_LOG`/`NAUTILUS_LOG` env vars are process-global config.

**NOT process-global (per-engine/per-kernel, isolated):** Cache, MessageBus, Portfolio, Clock, DataEngine, ExecEngine, component registry — all live on each engine's own `NautilusKernel`. See [[backtestengine-state-isolation]]. The clock is per-kernel (TestClock in backtest), NOT a global clock singleton.

**Correction:** [[backtestengine-state-isolation]] says "no module-level/global singletons" — that's true for TRADING state but the logging subsystem IS a genuine process-global singleton. The two coexist: trading components isolated, logging shared process-wide.

**Per-run reset (Backtesting.txt "Repeated runs" pp.5-6):** `BacktestEngine.reset()` clears orders/positions/balances/counters/timestamps and REMOVES strategies (must re-add); data + instruments + venues persist (`CacheConfig.drop_instruments_on_reset=False`). `BacktestNode` gives each run a fresh engine (clean state, no reset needed). TradeId/IdsGenerator collision-safe across resets (Backtesting.txt:1340).
