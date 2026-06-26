---
name: backtest-engine-run-holds-gil
description: DEFINITIVE — BacktestEngine.run() holds the GIL for the whole bar loop (Cython, no nogil); thread-based concurrency across engines is a no-op, use processes
metadata:
  type: project
---

**Verified from installed nautilus_trader==1.224.0 Cython source (2026-06-26).** `BacktestEngine.run()` holds the Python GIL continuously for essentially the entire CPU-bound backtest loop. It does NOT release the GIL.

Evidence (`venv\Lib\site-packages\nautilus_trader\backtest\engine.pyx`):
- `run()` L1300-1364 is a thin wrapper: calls `self._run(...)` L1361, then `self.end()`.
- `_run()` L1457; MAIN BACKTEST LOOP is a plain Cython `while True:` at L1573-1660 with NO `with nogil:` block. Body dispatches under the GIL: `exchange.process_bar(data)` L1635, `self._data_engine.process(data)` L1643, `self._process_and_settle_venues(...)` L1646, `self._advance_time(...)` cdef L1611/L1684, `self._data_iterator.next()` L1648.
- Loop components are all Cython `cdef class`: `SimulatedExchange` (`backtest/engine.pxd:206`), `DataEngine(Component)` (`data/engine.pyx:151`), `TestClock`.
- DECISIVE: package-wide Grep for `with nogil`/`nogil` over `**/*.pyx` and `**/*.pxd` under nautilus_trader = ZERO matches. No GIL release anywhere in the backtest path. This is the v1 legacy Cython stack (Rust calls are GIL-held FFI, loop is Cython-with-GIL).

**Implication for m-cube:** In the default unified path, `engine.run()` runs in the Flask background thread (`server.py:1831-1848`); two concurrent users serialize on the GIL — threads do NOT parallelize backtests. Real concurrency requires SEPARATE PROCESSES (per-slot Path A ProcessPoolExecutor already gives each worker its own GIL). To scale concurrent users, move every engine.run() (unified included) into a bounded process-global PROCESS pool / task queue, not Flask threads. Resolves the GIL unknown flagged in [[mcube-process-model-unified-vs-perslot]].
