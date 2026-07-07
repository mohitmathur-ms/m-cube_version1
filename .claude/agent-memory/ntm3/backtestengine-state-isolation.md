---
name: backtestengine-state-isolation
description: BacktestEngine owns its Cache/MessageBus/Portfolio per-engine via its own NautilusKernel; two engines are fully isolated, no process-global state, no DB needed
metadata:
  type: project
---

BacktestEngine state isolation (nautilus_trader==1.224.0) — confirmed YES, two concurrent engines are fully isolated.

**Composition (package source):** `backtest/engine.pyx:265` — `BacktestEngine.__init__` constructs its OWN `NautilusKernel(name=..., config=config)`. Cache/msgbus/portfolio/clock/data_engine/exec_engine are all `self._kernel.*` instance members (engine.pyx:269/439/442/466 + 667-700). `system/kernel.py:343/354/359` — kernel constructs a fresh `MessageBus(...)`, `Cache(...)`, `Portfolio(...)` per instance. No module-level/global singletons; the MessageBus is per-kernel/per-engine, NOT process-global.

**Identity:** kernel.py:160 `instance_id = config.instance_id or UUID4()` → fresh UUIDv4 per engine. trader_id defaults to a TESTER id but two engines can share a trader_id without cross-talk because state lives in distinct in-memory objects (the trader_id/instance_id only matter for keying an EXTERNAL Redis/DB backing — see below).

**Lifecycle:** `engine.dispose()` (engine.pyx:1289 → `self._kernel.dispose()`:1298) tears down the kernel and its components; `reset()` keeps data+instruments (CacheConfig.drop_instruments_on_reset=False) per `Cache.txt` / `Backtesting.txt` (Repeated runs, pp.5-6). No persistence between engine instances UNLESS a `DatabaseConfig` (Redis) is explicitly configured — `Cache.txt` "Database configuration" p.3 and `MessageBus.txt` "External publishing" p.7 both state persistence/external state is opt-in (default in-memory only).

**Doc note:** docs say Cache is "central in-memory database... shared between strategies / accessible to all components" (`Cache.txt` p.1, p.13) — that "shared" scope is WITHIN one engine, not across engines. The literal "owns / instance member" claim comes from package source (above), not the concept PDFs. Architecture.txt is EMPTY so no architecture-doc citation available.

**Verdict:** same-process two engines = isolated (separate kernels). Separate-process (m-cube ProcessPoolExecutor / containers) = isolated by definition + no mandatory external state for backtests.
