---
name: mcube-process-model-unified-vs-perslot
description: m-cube execution process model — default unified = 1 engine in-thread (NO ProcessPoolExecutor); per-slot fallback = 1 worker process per slot
metadata:
  type: project
---

How a single m-cube portfolio backtest maps to processes (verified in code 2026-06-26, branch merger_path_A_B).

**Default flags (`_USE_UNIFIED_ENGINE` on, per-slot/node/multi-pf off):** the WHOLE portfolio (all N legs) runs in ONE `BacktestEngine`, one shared multi-instrument timeline, IN-THREAD — NO ProcessPoolExecutor.
- `orchestration/__init__.py::_run_all_slots`: unified guard L300-303 → `return _run_portfolio_unified(...)` at L318, BEFORE the `with ProcessPoolExecutor(...)` block (L355). Pool never reached in unified path.
- `unified/__init__.py`: single `BacktestEngine`, `engine.run()`/`run(streaming=True)` at L894/934/956/961. Lazy 7-day windowed streaming (default `_USE_LAZY_STREAM`) bounds memory: load window → run(streaming=True) → clear_data() → end() (L895-943). Bars loaded ONCE.

**Per-slot Path A fallback (unified guard fails):** ProcessPoolExecutor, ONE `executor.submit(_run_single_slot)` per slot (L424-453) = one worker process per slot. `max_workers = min(n, cpu_count, 32)` (L174). Each worker builds its OWN engine + OWN copy of bars (N× bar memory). `_USE_GROUPING=1` collapses `(bar_type,start,end,dir)`-sharing slots into one shared-engine group (L361-422).

**What trips fallback to per-slot:** ReExecute replay pass (`replay_cutoff_ns`>0, L605-690), two-pass agg Move-SL (L532-548) unless live-monitor-enforced, portfolio ReExecute/agg Move-SL not live-enforced, entry-window when not all legs managed. See guard L300-303 + `_unified_active()` L67-78.

**Flask vs worker boundary:** portfolio endpoint (`server.py` L1783 `generate()`) runs `run_portfolio_backtest` in a `threading.Thread` (L1831-1848) for progress streaming. In DEFAULT unified path `engine.run()` executes IN THAT FLASK THREAD / Flask process (no child proc). Per-slot path: that thread owns the ProcessPoolExecutor → engine.run() in child procs. Server is `app.run(threaded=True, use_reloader=False)` dev server (L2367), one process, thread-per-request. Single `/backtest` (L1210) runs in the request thread directly.

**Scalability gotchas:** Flask is threaded (not multi-proc); if `engine.run()` doesn't release the GIL (UNVERIFIED — not in concept docs), concurrent users serialize. Per-call ProcessPoolExecutor has no global admission control → CPU oversubscription across concurrent portfolios. Logging is the one process-global singleton ([[logging-process-global-singleton]]). To scale: move engine.run off Flask into a bounded process-global pool/task queue; verify GIL release first.

See also [[backtestengine-state-isolation]] (each engine self-contained).
