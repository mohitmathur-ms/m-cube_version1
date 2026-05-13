# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**m-cube** — a backtesting and research platform for FX, crypto and other
asset classes built on **NautilusTrader**. The product is a single Flask
web app (Python backend + vanilla HTML/CSS/JS SPA frontend) for loading
historical data, running single-strategy and multi-slot portfolio
backtests with exit management (SL/TP/trailing), and viewing tearsheets.
A second Flask app under [adapter_admin/](adapter_admin/) administers
broker adapter configurations.

Primary docs to read when context is needed:
- [README_APP.md](README_APP.md) — user-facing app overview
- [PORTFOLIO_SETUP_GUIDE.md](PORTFOLIO_SETUP_GUIDE.md) — full design & schema of the portfolio system (most authoritative spec)
- [LOGICS_BACKEND_STATUS.md](LOGICS_BACKEND_STATUS.md) — which UI logics are wired vs UI-only; updated each session
- [docs/btsoftware_fx_clone_design.md](docs/btsoftware_fx_clone_design.md) — long-term design target (Options-style BTSoftware features mapped to FX)
- [how_to_use.txt](how_to_use.txt) — runtime env-var flags

## Commands

This is a Windows-first codebase. The default shell is PowerShell.

```powershell
# First-time setup or refresh of dependencies (creates ./venv, installs requirements,
# opens browser on http://localhost:5000)
.\start.bat

# Manual run (assumes venv activated or pip install -r requirements.txt done)
python server.py

# Adapter Admin Panel (separate Flask app, default port differs)
python adapter_admin\admin_server.py
```

Python **>= 3.11** is required (`start.bat` enforces this and rebuilds the venv
on mismatch). Major dep pins: `nautilus_trader==1.224.0`, `Flask==3.1.3`,
`pandas==2.3.3`, `numpy==2.4.3`, `pyarrow==23.0.1`.

### Tests

```powershell
# pytest test suite (custom strategy loader, perf regression, aggregator)
venv\Scripts\python.exe -m pytest tests\

# Single test file
venv\Scripts\python.exe -m pytest tests\test_aggregator.py -v

# Verification harness (parses 100+ checks against this session's wiring;
# exits non-zero on any failure — keep this green)
python verify_session_changes.py

# Smoke / stress tests for portfolio engine + user layer (each has a matching
# `*_report.py` companion that renders results as HTML)
python tests\smoke_tests\smoke_test_logics_audit.py
python tests\smoke_tests\stress_test_portfolios.py
python tests\smoke_tests\stress_test_stoploss_report.py
```

### Runtime flags (env vars, all default off)

| Flag | Effect |
|---|---|
| `_USE_GROUPING=1` | Slots that share `(bar_type, start_date, end_date, custom_strategies_dir)` run in **one** shared `BacktestEngine` (bars load once per group instead of once per slot). Falls back per-slot for group size 1. |
| `_USE_BACKTEST_NODE=1` | Routes through `BacktestNode` (Path B) instead of building a `BacktestEngine` per worker. Auto-falls-back to Path A when `run_on_days` or `entry_start_time/end_time` filters are set. Result dict gets `"path_b": True`. |
| `_PROFILE_PHASES=1` | Emits per-phase wall-time (`registry_load`, `bars_load`, `engine_run`, etc.) inside the slot/group helpers. Zero cost when off. |

Verify grouping parity for a specific portfolio before trusting `_USE_GROUPING=1`:
```powershell
python scripts\verify_grouping_parity.py --portfolio portfolios\_default\<file>.json
```
Exit 0 = parity OK; exit 1 = mismatch.

## Architecture

### Backend layers (from highest to lowest)

```
server.py  (Flask REST API — every endpoint takes an X-User-Id header)
  └─ core/backtest_runner.py       L3  Portfolio runner / engine orchestrator
        ├─ run_backtest()           single-strategy path
        ├─ run_portfolio_backtest() multi-slot path; dispatches to:
        │     ├─ _run_single_slot   (one engine per slot)
        │     └─ _run_slot_group    (shared engine, multiple slots, gated by _USE_GROUPING)
        │     plus `_node` variants for _USE_BACKTEST_NODE (Path B)
        └─ _merge_portfolio_results / _apply_portfolio_clip (post-run aggregation + portfolio SL/TP halt)
  └─ core/managed_strategy.py      L2  ManagedExitStrategy (the SL/TP/trailing/RBO engine)
  └─ strategies/                   L1  Pure entry strategies (EMA Cross, RSI, Bollinger, Four MA, Range Breakout)
  └─ core/{csv_loader,nautilus_loader,instrument_factory,aggregator}.py
                                   L0  Data ingest → ParquetDataCatalog
```

**Layer cheat-sheet** referenced throughout `LOGICS_BACKEND_STATUS.md`:
- L1 = `strategies/<name>.py` — pure signal logic, no exit management
- L2 = `core/managed_strategy.py` — wraps any signal with SL/TP/trailing/squareoff/RBO
- L3 = `core/backtest_runner.py` — portfolio-level orchestration, filters, halts
- L4 = "Execution layer" — does **not** exist yet (intentionally; live-trading-only fields are marked `pf-live-only` in the UI)

### The exit-management engine (`ManagedExitStrategy`)

This is the heart of the portfolio system and where most non-trivial work lands.
`core/managed_strategy.py` wraps a signal function from `core/signals.py:SIGNAL_REGISTRY`
with a full exit-management state machine. `on_bar` flow:

1. **If in position:** update highest profit → check target-lock → update
   trailing SL → check SL (with optional `sl_wait_bars` confirmation) → check TP
   → on exit, dispatch `on_sl_action` / `on_target_action` (`close` / `re_execute` / `reverse`).
2. **If flat:** run signal function; on signal, submit entry and compute initial SL/TP.

Square-off precedence (resolved at portfolio-load time, **leg > slot > portfolio**):
`ExitConfig.squareoff_time` > `StrategySlotConfig.squareoff_time` > `PortfolioConfig.squareoff_time`.
Force-closes at the configured local time and blocks re-entry until next session.

RBO (Range Breakout) state machine runs per-day; spec lives in
`5. Logics/rbo_logics.html`. The whole-portfolio RBO tab is **not** wired —
only per-strategy RBO via `strategies/range_breakout.py` is implemented.

### Strategy plug-in contract

Every file in [strategies/](strategies/) is auto-discovered by
`strategies/__init__.py` (`_build_registry`) at import time. To register a
strategy, export 5 module-level constants:

```python
STRATEGY_NAME: str
STRATEGY_CLASS: type   # subclass of nautilus_trader.trading.strategy.Strategy
CONFIG_CLASS: type     # subclass of StrategyConfig, must be frozen=True
DESCRIPTION: str
PARAMS: dict           # UI metadata: {param_name: {"type": ..., "default": ..., ...}}
```

Custom user-uploaded strategies follow the same contract via
[core/custom_strategy_loader.py](core/custom_strategy_loader.py); they live
under `custom_strategies/<user_id>/` and are merged with built-ins by
`get_merged_registry()`.

To make a strategy work inside `ManagedExitStrategy` (i.e. with exit management
in the portfolio system), also add a signal function to
`core/signals.py:SIGNAL_REGISTRY`. The strategy class itself only handles
raw single-shot signals.

### Path A vs Path B vs Grouping

There are three backtest execution paths that the runner can dispatch to:

1. **Path A, per-slot** (default, oldest, proven): each enabled slot runs in its own
   `BacktestEngine` inside its own worker `ProcessPoolExecutor`.
2. **Path A, grouped** (`_USE_GROUPING=1`): slots sharing
   `(bar_type, start_date, end_date, custom_strategies_dir)` are batched into
   one shared engine; per-slot P&L recovered post-run by filtering positions
   on `strategy_id`. NautilusTrader reassigns IDs internally — always read
   them back via `engine.trader.strategies()` after `engine.run()`.
3. **Path B** (`_USE_BACKTEST_NODE=1`): uses Nautilus's higher-level
   `BacktestNode` / `BacktestRunConfig` API. Auto-falls-back to Path A when
   bar-filtering features (`run_on_days`, intraday `entry_start_time/end_time`)
   are active, because filters cannot be injected into `BacktestDataConfig`.

These exist as feature flags for safe rollout. Don't remove the flag gates
without verifying parity with `scripts/verify_grouping_parity.py`.

### Portfolio data model

`core/models.py` is the schema. Three nested dataclasses:

```
PortfolioConfig (portfolio-level: capital, max_loss/profit, run_on_days,
   │              entry window, squareoff, RBO settings, portfolio SL/TP)
   └─ StrategySlotConfig (per-slot: strategy_name, bar_type_str, lots,
          │               allocation_pct, start/end_date, squareoff)
          └─ ExitConfig (per-leg: SL/TP type+value, trailing, target lock,
                         sl_wait_bars, on_sl/target_action, squareoff)
```

Saved as JSON under `portfolios/<user_id>/<name>.json`. The `_default`
user_id is reserved for legacy portfolios pre-multi-user (see
[core/migrate_users.py](core/migrate_users.py)). `_migrate_legacy_trade_size`
in `portfolio_from_dict` rewrites the old `trade_size` field into the new
`lots` × instrument-`lot_size` model — don't break this migration when
touching the schema.

### Multi-user model

[core/users.py](core/users.py) implements an **identity-only** layer (no auth).
Every API call requires an `X-User-Id` header that must match a `user_id` in
[config/users.json](config/users.json). Users carry a `multiplier`
(trade-size scalar) and optional `allowed_instruments` whitelist. The frontend
gates init on user selection (see `App._ensureUserSelected` in
[static/js/app.js](static/js/app.js)). Anyone on the network can spoof headers —
**this is only safe for trusted internal use** and the registry's `_meta`
description says so.

### FX-specific concerns

- **Bar type strings** are Nautilus-formatted: `"<symbol>.<venue>-<timeframe>-<price_type>-EXTERNAL"`
  (e.g. `"USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL"`).
- **MID bars are synthesized** from ASK+BID at CSV-load time
  ([core/csv_loader.py](core/csv_loader.py)). When a MID slot runs, the engine
  also needs the matching ASK and BID bar types loaded — see
  `_pair_bid_ask_bar_type` in `backtest_runner.py`.
- **Cross-currency PnL** is converted via [core/fx_rates.py](core/fx_rates.py).
  USDJPY trades produce JPY PnL; the resolver reads the venue's `fx_conversion`
  block from `adapter_admin/adapters_config/<venue>.json` and converts at the
  bar nearest (and not after) each trade timestamp. Without this, JPY PnL is
  silently dropped from the USD account.
- **Venue names must match the InstrumentId**: if your instrument ID is
  `BTCUSD.CRYPTO`, the engine needs `add_venue(Venue("CRYPTO"), ...)`.
  Don't use reserved Nautilus names like `SYNTH` — `SIM`, `CRYPTO`, `BINANCE`,
  `FOREX_MS`, `YAHOO`, `TEST` are all safe.
- **`flat_trades` / `decisive_win_rate`**: every result dict carries these.
  Many FX strategies at small `trade_size` produce thousands of $0.00 round-trips
  (sub-cent price moves); the basic `win_rate` becomes misleading without them.
  Don't strip these fields from results — the dashboard and HTML template both
  consume them.

### Frontend (vanilla JS SPA)

[static/js/app.js](static/js/app.js) is the router; each page is a module
(`Dashboard`, `LoadData`, `ViewData`, `Backtest`, `Tearsheet`, `Orderbook`,
`Portfolio`, `PortfolioTearsheet`). Pages are kept alive across navigations
(hidden via `display:none`) to preserve Plotly chart state and form values.

The portfolio UI in [static/js/portfolio.js](static/js/portfolio.js) renders
a tabbed editor whose tabs mirror the HTML specs in `5. Logics/`. Tabs and
fields are marked with two CSS classes that communicate backend wiring state:

- `pf-ui-only` (red) — field exists in UI but backend silently ignores it
- `pf-live-only` (gray) — field will only ever apply to live trading or
  options trading, not backtest (permanent)
- (no class) — field is wired end-to-end

The mapping of every UI tab/field → backend status is in
[LOGICS_BACKEND_STATUS.md](LOGICS_BACKEND_STATUS.md). **When you implement a
UI-only logic, remove the `pf-ui-only` class from its section AND update that
status document in the same change.**

Theme handling is done via `data-theme` on `<html>` with an inline
early-paint script in [static/index.html](static/index.html) to avoid a
flash of wrong theme; `App.setTheme` also repaints any already-rendered
Plotly chart so colors stay consistent.

### Adapter Admin Panel

A separate Flask app under [adapter_admin/](adapter_admin/) manages
broker/exchange configurations stored as JSON in
`adapter_admin/adapters_config/*.json` (one per venue). Built-in adapters
are auto-discovered from the installed `nautilus_trader` package
(`adapter_discovery.py`); custom adapters can be uploaded as Python files
to `adapter_admin/custom_adapters/`. The main server reads these configs at
backtest time via [core/venue_config.py](core/venue_config.py) — the venue
is parsed from the bar type string. Sensitive fields (`api_key`, `secret`,
etc.) are masked with `****<last-4>` when sent to the frontend.

### Catalog format

NautilusTrader's native `ParquetDataCatalog`, default at `./catalog/`:

```
catalog/
├── data/
│   ├── bar/<bar_type>/           # OHLCV parquet, filename = ts_start_ts_end.parquet
│   └── currency_pair/            # Instrument definitions
```

There is a fast-path in `server.py::_bar_type_range_from_files` that parses
date ranges from parquet **filenames** (`<start_iso>_<end_iso>.parquet`) instead
of opening rows — this is the difference between ~1 second and ~3 minutes for
the FX catalog with ~24M bars. Don't replace this with a `catalog.bars()` scan.

## Conventions specific to this repo

- **Phase profiling pattern**: `with _phase("label", phase_bag): ...` in
  `backtest_runner.py`. `phase_bag` is `None` in the hot path (zero cost);
  only allocated when `_PROFILE_PHASES=1`. Don't refactor this to use
  `time.perf_counter()` calls directly — the no-op `None` path is intentional.
- **Result-dict tags**: Every backtest result includes provenance tags like
  `path_b`, `bars_filtered_by_run_on_days`, `bars_filtered_by_entry_window`,
  `warning`, `entry_window_skipped`. Frontend tearsheets fall back to
  client-side derivation for new metrics so older backends still render —
  keep that compatibility unless you're cleaning a deprecated path.
- **No `pytest.ini`/`pyproject.toml`**: pytest discovery uses the `tests/`
  layout. `verify_session_changes.py` is a hand-rolled checker, not pytest —
  run it independently after big changes to wiring.
- **Don't bypass the venue's account-base-currency**: When in doubt about
  currency conversion, prefer adding to `fx_conversion` in the venue's
  adapter config over hard-coding in the runner.