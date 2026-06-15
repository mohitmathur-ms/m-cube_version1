---
name: timestamps-timezones-sessions
description: How NautilusTrader 1.224.0 handles time (UTC ns), bar ts_init=close convention, and the absence of any tz/session/calendar concept doc
metadata:
  type: project
---

NautilusTrader 1.224.0 time model, confirmed from concept docs:

- **Internal time = integer UTC nanoseconds since epoch**, NOT timezone-aware. Every
  data object/event carries `ts_event` (when it occurred) + `ts_init` (when created);
  events sorted/sequenced by `ts_init` to prevent look-ahead. Durations are `*_ns` ints.
  Source: `Events.txt` field lists (L94 order events, L286 position events, L323),
  `Backtesting.txt` L843.
- **Bar timestamp convention**: `ts_init` MUST equal bar CLOSE for correct execution.
  If source stamps bars at OPEN, rewrite or pass `ts_init_delta` = bar duration ns to
  `BarDataWrangler.process()` (e.g. 60_000_000_000 for 1-min). Source: `Backtesting.txt`
  L819-874 ("Bar timestamp convention").
- **Internal time-bar aggregation** uses timers closing at interval boundaries on the
  UTC-ns clock; `time_bars_build_delay` (DataEngineConfig) handles the at-boundary-tick
  edge case. No documented SESSION- or TZ-aligned bar boundary option. `Backtesting.txt`
  L945-965.

**Why this matters / gaps to flag:**
- There is NO dedicated timezone/DST/trading-calendar concept doc. The doc named
  `DST_timezone.txt` (= `dst.pdf`) is "Deterministic Simulation Testing" (seeded
  concurrency testing), NOT daylight-saving. Don't cite it for tz questions.
- No built-in exchange-session / trading-calendar / DST-aware session-time abstraction
  in the concept docs. "MarketHoursFillModel / Session-aware execution" (`Backtesting.txt`
  L1033-1037) is only a FILL MODEL (wider spreads in low liquidity), not a calendar.
- Could NOT confirm any built-in Strategy/Clock timezone-conversion helper from docs:
  `Startegies.txt`/`Actors.txt` are EMPTY (see [[project_docs_library_empty]]), and the
  `Clock` API (`set_time_alert`/`set_timer`/`utc_now`) lives in the compiled Rust/Cython
  core (`common/component.pyx`/.pyd) with no Python-readable signatures. Treat
  "enter at 10:00 NY" as manual `zoneinfo`/`pytz` conversion in strategy code.

**How to apply:** Session/entry-window/squareoff/DST logic is the Strategy/Actor's
responsibility — which is exactly why m-cube implements entry windows, `run_on_days`,
and `squareoff_time` itself in `core/managed_strategy.py` + `core/backtest_runner.py`
rather than delegating to Nautilus.
