---
name: live-trading-containerization-gaps
description: What the live/exec/cache/msgbus concept PDFs DO and DON'T cover for running a live node in Docker/k8s (persistence, single-writer, shutdown)
metadata:
  type: reference
---

Coverage map for "live node in containers" questions (nautilus_trader==1.224.0 concept PDFs). Related: [[live-trading-api-and-docs-gap]], [[backtestengine-state-isolation]], [[timestamps-timezones-sessions]].

DOCUMENTED in the concept PDFs:
- Cache persistence: `Cache.txt` *Database configuration* — `CacheConfig(database=DatabaseConfig(type="redis",...))`, `flush_on_start` (default False; MUST stay False in containers). "pick up exactly where you left off" on restart.
- MessageBus external backing: `MessageBus.txt` *External publishing* (Redis 6.2+ streams, MPSC->Rust thread), *External streams* (producer/consumer fan-out, `external_streams`/`external_clients`), key `trader:{trader_id}:{instance_id}:{streams_prefix}`, `autotrim_mins`, `types_filter`, `buffer_interval_ms`. NOTE: cache-Redis = state recovery; bus-Redis = distribution/audit (separate configs).
- Reconciliation cost cached vs cold: `Live Trading.txt` *Two scenarios* + tip "persist all execution events / cache all events locally"; `reconciliation_lookback_mins`. Cold restart = C=0 = Scenario B full rebuild. See [[per-strategy-pnl-base-ccy]]? no — see reconciliation analysis in this same doc family.
- Continuous reconciliation knobs: `Execution.txt` — `reconciliation_startup_delay_secs` (10s), `open_check_interval_secs`, `inflight_check_threshold_ms`/`retries`, `own_books_audit_interval_secs`; websocket-reconnect replay handled by `trade_id` dedup + deterministic reconciliation trade_ids (ID-determinism invariant).
- Time = UTC nanoseconds internal (`Reports.txt`/`Events.txt`/`DST_timezone.txt`); no TZ/session concept doc -> UTC-minimal base image is fine. See [[timestamps-timezones-sessions]].

NOT in the concept PDFs (inference / how-to-guide territory):
- TradingNode start/stop/dispose lifecycle, clean disconnect, SIGTERM/final-flush -> lives in the referenced "Configure a live trading node" HOW-TO guide, which is NOT in the ntm3_docs library. DST_timezone.txt only notes node run-loop shutdown is ctrl_c-driven and flags "drain races at shutdown".
- Explicit single-writer / one-node-per-account rule: NOT stated, but strongly IMPLIED by automatic EXTERNAL-order generation (a 2nd replica's real orders get reconciled as venue drift -> reconciliation civil war) + per-node instance UUID being for partitioning not co-writing. k8s: replicas=1 + Recreate/StatefulSet, never RollingUpdate overlap.
- Any mention of Docker/Kubernetes at all -> zero. All container-specific advice is inference.

Key verdict for m-cube: backtesting-only today => LiveExecutionEngine NEVER instantiates ("only LiveExecutionEngine reconciles, backtesting controls both sides"). Backtest containers are stateless/deterministic; none of the live concerns apply until an L4 live layer exists (it does not, per CLAUDE.md).
