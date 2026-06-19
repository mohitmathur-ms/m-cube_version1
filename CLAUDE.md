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

> **Branch note:** the active line of work is `merger_path_A_B`, which merges
> several feature branches. Most of `core/` has been **modularized**: the former
> monoliths `backtest_runner.py`, `models.py`, `csv_loader.py` and
> `instrument_factory.py` are now **packages** (directories) whose logic lives in
> each subdirectory's `__init__.py`. Their public APIs are unchanged — every
> former symbol is re-exported from the package root, so `from core.models import
> PortfolioConfig` and `core.backtest_runner.run_portfolio_backtest` still
> resolve. Each package ships a `README.md` mapping subdirectory → responsibility.

Primary docs to read when context is needed:
- [README_APP.md](README_APP.md) — user-facing app overview
- [PORTFOLIO_SETUP_GUIDE.md](PORTFOLIO_SETUP_GUIDE.md) — full design & schema of the portfolio system (most authoritative spec)
- [LOGICS_BACKEND_STATUS.md](LOGICS_BACKEND_STATUS.md) — which UI logics are wired vs UI-only; updated each session
- [docs/btsoftware_fx_clone_design.md](docs/btsoftware_fx_clone_design.md) — long-term design target (Options-style BTSoftware features mapped to FX)
- [how_to_use.txt](how_to_use.txt) — runtime env-var flags (the authoritative, full list)
- Per-package READMEs: [core/backtest_runner/README.md](core/backtest_runner/README.md),
  [core/models/README.md](core/models/README.md),
  [core/csv_loader/README.md](core/csv_loader/README.md),
  [core/instrument_factory/README.md](core/instrument_factory/README.md)

### NautilusTrader docs subagent (`ntm3`)

When a task touches a NautilusTrader API/class/concept (Strategy,
BacktestEngine/BacktestNode, OrderBook, Orders, Instruments, Data, Actors,
Cache, MessageBus, Portfolio, Accounts/Positions, Adapters, FillModel, …),
prefer the **`ntm3`** subagent — it answers from the 28 concept PDFs under
[.claude/agents/ntm3_docs/](.claude/agents/) for `nautilus_trader==1.224.0` and
cites them. See [.claude/agents/ntm3.md](.claude/agents/ntm3.md).

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
# pytest test suite (custom strategy loader, perf regression, aggregator,
# models product, portfolio tags, leg/underlying/user targets, reexec replay,
# run_on_days gating, three-format engine, VWAP/agg move-SL, …)
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

There are also many ad-hoc `verify_*.py` / `*_test.py` / `parity_*.py` scripts at
the repo root (e.g. `multiportfolio_parity_test.py`, `enforce_parity_test.py`,
`verify_leg_sl_formulas.py`, `stream_parity_test.py`) used to prove parity
between execution paths during the path-A/B/unified merge — run them directly
with the venv python.

### Runtime flags (env vars)

`how_to_use.txt` is the authoritative list; the conceptually important flags:

| Flag | Default | Effect |
|---|---|---|
| `_USE_UNIFIED_ENGINE` | **on** | One-pass unified path: run ALL enabled legs of a portfolio in **one** `BacktestEngine` (multi-venue/instrument, shared timeline); per-leg results recovered post-run by `strategy_id`. Auto-falls-back to per-slot Path A when portfolio-level features (combined SL/Target, ReExecute, agg Move-SL) or bar-filtering (`run_on_days`, intraday entry window) are active. |
| `_USE_PER_SLOT` | off | Force the proven per-slot Path A (one engine per slot), overriding the unified path. |
| `_USE_GROUPING` | off | (Path A) Slots that share `(bar_type, start_date, end_date, custom_strategies_dir)` run in **one** shared `BacktestEngine` (bars load once per group). Falls back per-slot for group size 1. |
| `_USE_BACKTEST_NODE` | off | Route through `BacktestNode` (Path B) instead of building a `BacktestEngine` per worker. Auto-falls-back to Path A when `run_on_days`/`entry_start_time/end_time` filters are set. Result dict gets `"path_b": True`. |
| `_USE_PF_ENFORCE` | **on** | Apply portfolio-level SL/Target/Move-SL enforcement (the post-run clip / two-pass machinery). |
| `_USE_POST_RUN_PF` | off | Use the legacy post-run portfolio enforcement codepath. |
| `_USE_PF_MONITOR` | off | Attach the live in-engine `core/portfolio_monitor.py` Strategy (unified path only). Currently a skeleton that only records the live combined-P&L curve. |
| `_USE_MULTI_PORTFOLIO` | off | Run several portfolios together in one session engine for live cross-portfolio actions (see `/api/portfolios/session-backtest`). |
| `_USE_PF_AGG_MOVE_SL` | off | Enable aggregate Move-SL-to-Cost (pass-1 → pass-2 coordination). |
| `_USE_PF_REEXEC_REPLAY` | off | Enable portfolio ReExecute replay pass. |
| `_USE_LIVE_FILL_MODEL` | **on** | Use the live `core/conservative_fill_model.py` synthetic-orderbook FillModel (unified path) so conservative SL/TP exits fill at the computed adverse price DURING the run instead of a post-run reprice. |
| `_USE_VWAP_FILL` / `_USE_DIRECTIONAL_FILL` | off | Post-run conservative exit-fill repricing models (VWAP session-cumulative / directional bid-ask close). |
| `_USE_LAZY_STREAM` / `_USE_UNIFIED_STREAMING` | on / off | Bound engine-side memory by streaming bars into the unified engine. |
| `_PROFILE_PHASES` | off | Emit per-phase wall-time (`registry_load`, `bars_load`, `engine_run`, …) in the slot/group/unified helpers. Zero cost when off. |

Verify grouping/unified parity for a specific portfolio before trusting a flag:
```powershell
python scripts\verify_grouping_parity.py --portfolio portfolios\_default\<file>.json
```
Exit 0 = parity OK; exit 1 = mismatch.

## Architecture

### Backend layers (from highest to lowest)

```
server.py  (Flask REST API — every endpoint takes an X-User-Id header)
  └─ core/backtest_runner/          L3  Portfolio runner / engine orchestrator (PACKAGE)
        ├─ single_backtest/         run_backtest / run_backtest_node (single-strategy, Path A/B)
        ├─ orchestration/           run_portfolio_backtest (multi-slot entry point)
        ├─ unified/                 _run_portfolio_unified (one-engine path, _USE_UNIFIED_ENGINE)
        ├─ session/                 run_session_backtest (several portfolios in one session engine)
        ├─ slot_execution/          _run_single_slot / _run_slot_group (+ _node variants)
        ├─ portfolio_clip/          portfolio SL/TP halt, Move-SL, agg coordination
        ├─ portfolio_results/       _merge_portfolio_results + per-strategy breakdown
        └─ {bar_filters,bar_types,path_b,rbo,other_settings,portfolio_exit_config,
            cross_portfolio,equity_curves,data_cache,results,exit_fill,profiling}/
  └─ core/managed_strategy.py       L2  ManagedExitStrategy (the SL/TP/trailing/RBO engine)
  └─ strategies/                    L1  Pure entry strategies (EMA Cross, RSI, Bollinger, Four MA, Range Breakout)
  └─ core/{csv_loader,nautilus_loader,instrument_factory,aggregator}      L0  Data ingest → ParquetDataCatalog
```

`server.py` imports `run_backtest`, `run_portfolio_backtest` and
`_run_single_backtest_task` straight from `core.backtest_runner`; session
backtests import `run_session_backtest` from `core.backtest_runner.session`.

**Layer cheat-sheet** referenced throughout `LOGICS_BACKEND_STATUS.md`:
- L1 = `strategies/<name>.py` — pure signal logic, no exit management
- L2 = `core/managed_strategy.py` — wraps any signal with SL/TP/trailing/squareoff/RBO
- L3 = `core/backtest_runner/` — portfolio-level orchestration, filters, halts
- L4 = "Execution layer" — does **not** exist yet (intentionally; live-trading-only fields are marked `pf-live-only` in the UI)

### Other notable `core/` modules

- `core/signals.py` — `SIGNAL_REGISTRY`: pure signal functions returning
  `(OrderSide | None, reason | None)`; the reason string feeds the orderbook's
  *ENTRY DETAILED REASON* column.
- `core/aggregator.py` + `core/aggregating_strategy.py` — in-process bar
  aggregation. The `AggregatingStrategy` mixin routes BASE-timeframe bars
  through a `BarAggregator` so strategy logic runs only on the emitted
  higher-timeframe bar (replaces Nautilus' `TimeBarAggregator` / composite
  `INTERNAL@` bar types). No-op passthrough when no `aggregate_to_bar_type`.
- `core/conservative_fill_model.py` — live FillModel returning a synthetic order
  book so conservative SL/TP closes fill at the precomputed adverse price during
  the run (`_USE_LIVE_FILL_MODEL`).
- `core/portfolio_monitor.py` — position-less Strategy that reads LIVE combined
  P&L across all legs in the unified single engine (`_USE_PF_MONITOR`, skeleton).
- `core/tags.py` — **Strategy Tags** (spec §11): an SL/Target tier between
  portfolio and user (`leg → portfolio → TAG → user`). Definitions in
  `config/tags.json`; a session-level aggregator clips every portfolio carrying
  a tag when the tag's combined PnL breaches its limit.
- `core/users.py` — identity-only multi-user layer (see below).
- `core/session_windows.py` — recomputes each venue's daily session window from
  the catalog (covers parquet dropped in out-of-band); called on startup and on
  catalog-status. Cached per parquet file by `(mtime, size)`.
- `core/runtime_history.py` — persistent per-`(bar_type, strategy)` wall-time
  history (`.runtime_history.json`) for LPT scheduling estimates.
- `core/templates.py` — predefined portfolio templates (`/api/portfolios/templates`).
- `core/report_generator.py` + `core/_pandas_utils.py` — HTML tearsheet rendering
  from `docs/report_template.html`.
- `core/strategies.py` — thin back-compat re-export of `strategies.STRATEGY_REGISTRY`.

### The exit-management engine (`ManagedExitStrategy`)

This is the heart of the portfolio system and where most non-trivial work lands.
`core/managed_strategy.py` wraps a signal function from `core/signals.py:SIGNAL_REGISTRY`
with a full exit-management state machine. `on_bar` flow:

1. **If in position:** update highest profit → check target-lock → update
   trailing SL → check SL (with optional `sl_wait_bars` confirmation) → check TP
   → on exit, dispatch `on_sl_action` / `on_target_action` (`close` / `re_execute` / `reverse`).
2. **If flat:** run signal function; on signal, submit entry and compute initial SL/TP.

Square-off precedence (resolved by `core/models/squareoff/`, **leg > slot > portfolio**):
`ExitConfig.squareoff_time` > `StrategySlotConfig.squareoff_time` > `PortfolioConfig.squareoff_time`,
with the portfolio level supplying a MIS-product default via
`effective_portfolio_squareoff`. Force-closes at the configured local time and
blocks re-entry until next session.

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

`_build_registry` skips names starting with `_`, so the
[strategies/_shared/](strategies/_shared/) package — pure helpers extracted from
the strategy files (`time_windows` HHMM math, `entry_tags` *ENTRY DETAILED
REASON* string builders, `leg_state.LegState` for pyramid/multi-leg
strategies) — never accidentally registers as a "strategy". The entry-tag
strings are reproduced byte-for-byte; changing them alters the orderbook column.

Custom user-uploaded strategies follow the same contract via
[core/custom_strategy_loader.py](core/custom_strategy_loader.py); they live
under `custom_strategies/<user_id>/` and are merged with built-ins by
`get_merged_registry()`.

To make a strategy work inside `ManagedExitStrategy` (i.e. with exit management
in the portfolio system), also add a signal function to
`core/signals.py:SIGNAL_REGISTRY`. The strategy class itself only handles
raw single-shot signals.

### Execution paths (per-slot / grouped / Path B / unified / session)

The runner can dispatch a portfolio backtest to several execution paths, all
gated by env flags for safe rollout. Don't remove the flag gates without
verifying parity (`scripts/verify_grouping_parity.py`, root `*_parity_test.py`):

1. **Path A, per-slot** (proven baseline; `_USE_PER_SLOT=1` forces it): each
   enabled slot runs in its own `BacktestEngine` inside its own worker
   `ProcessPoolExecutor`.
2. **Path A, grouped** (`_USE_GROUPING=1`): slots sharing
   `(bar_type, start_date, end_date, custom_strategies_dir)` batched into one
   shared engine; per-slot P&L recovered post-run by filtering positions on
   `strategy_id`. NautilusTrader reassigns IDs internally — always read them back
   via `engine.trader.strategies()` after `engine.run()`.
3. **Path B** (`_USE_BACKTEST_NODE=1`): Nautilus's higher-level `BacktestNode` /
   `BacktestRunConfig` API. Auto-falls-back to Path A when bar-filtering
   (`run_on_days`, intraday entry window) is active — filters cannot be injected
   into `BacktestDataConfig`. Per-day data-config chunking lives in
   `core/backtest_runner/path_b/`.
4. **Unified single-engine** (`_USE_UNIFIED_ENGINE`, **default on**;
   `core/backtest_runner/unified/`): ALL enabled legs share ONE
   `BacktestEngine` on a single multi-venue/instrument timeline; per-leg results
   recovered by `strategy_id`, returning a `{slot_id: result}` dict shape-
   identical to the per-slot path. Phase 1 = parity skeleton: used only when no
   portfolio-level feature (combined SL/Target, ReExecute, agg Move-SL) and no
   bar-filtering is active; otherwise it falls back to per-slot Path A.
5. **Session** (`core/backtest_runner/session/run_session_backtest`, exposed by
   `/api/portfolios/session-backtest`, `_USE_MULTI_PORTFOLIO`): several
   portfolios run together in one session engine so cross-portfolio actions
   (SqOff / Execute / Start Other Portfolio) fire LIVE, same-bar, between them,
   and Strategy-Tag limits aggregate across them.

Portfolio-level enforcement (combined SL/Target, ReExecute, aggregate Move-SL)
is applied either live (portfolio monitor, future) or via the post-run clip /
two-pass replay in `core/backtest_runner/portfolio_clip/` and
`portfolio_results/` (`_USE_PF_ENFORCE` on by default).

### Portfolio data model

`core/models/` (package) is the schema. Three nested dataclasses:

```
PortfolioConfig (portfolio-level: capital, max_loss/profit, run_on_days,
   │              entry window (+ overnight), product MIS/NRML, squareoff,
   │              RBO settings, winter-time adjust, portfolio SL/TP)
   └─ StrategySlotConfig (per-slot: strategy_name, bar_type_str, lots,
          │               allocation_pct, start/end_date, squareoff)
          └─ ExitConfig (per-leg: SL/TP type+value, trailing, target lock,
                         sl_wait_bars, on_sl/target_action, squareoff)
```

Package layout: `portfolio_config/`, `strategy_slot_config/`, `exit_config/`,
`squareoff/`, `leg_actions/`, `slot_sizing/`, `composite_bar_type/`,
`timeframe_utils/`, `serialization/`, `persistence/`.

- **Product type** (`product`): `"MIS"` supplies a default `squareoff_time`
  (`mis_squareoff_time`/`mis_squareoff_tz`) when none is set; `"NRML"` is a
  backtest no-op. Leverage/margin are not modeled.
- **Overnight entry window** (`entry_window_overnight`): lets the intraday entry
  window and/or square-off cross midnight (NRML carry / 24h FX/crypto). The
  runtime bar filter already wraps such windows; this flag mainly governs SAVE
  validation and UI intent (forced OFF for MIS). See `server.py` ~line 1340.
- **Winter Time Adjustment**: shifts every configured local time +1h across a
  DST boundary (`core/backtest_runner/rbo/_apply_winter_time`, `add_one_hour`).

Saved as JSON under `portfolios/<user_id>/<name>.json`. The `_default`
user_id is reserved for legacy portfolios pre-multi-user (see
[core/migrate_users.py](core/migrate_users.py)). `_migrate_legacy_trade_size`
in `portfolio_from_dict` (in `core/models/serialization/`) rewrites the old
`trade_size` field into the new `lots` × instrument-`lot_size` model — don't
break this migration when touching the schema.

### Multi-user model + Strategy Tags

[core/users.py](core/users.py) implements an **identity-only** layer (no auth).
Every API call requires an `X-User-Id` header that must match a `user_id` in
[config/users.json](config/users.json). Users carry a `multiplier`
(trade-size scalar, threaded through `effective_slot_qty`) and optional
`allowed_instruments` whitelist. The frontend gates init on user selection (see
`App._ensureUserSelected` in [static/js/app.js](static/js/app.js)). Anyone on
the network can spoof headers — **this is only safe for trusted internal use**.

[core/tags.py](core/tags.py) adds a **Strategy Tag** tier between portfolio and
user in the evaluation hierarchy (`leg → portfolio → TAG → user`). Tag limit
definitions live in `config/tags.json` (CRUD via `/api/tags/*`); when a tag's
combined PnL across a session breaches its limit, every portfolio carrying that
tag is squared off while other tags keep running. The user-level cap is the
ultimate ceiling.

### FX-specific concerns

- **Bar type strings** are Nautilus-formatted: `"<symbol>.<venue>-<timeframe>-<price_type>-EXTERNAL"`
  (e.g. `"USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL"`).
- **MID bars are synthesized** from ASK+BID at CSV-load time
  ([core/csv_loader/mid_merge/](core/csv_loader/)). When a MID slot runs, the
  engine also needs the matching ASK and BID bar types loaded — see
  `_pair_bid_ask_bar_type` in `core/backtest_runner/bar_types/`.
- **Cross-currency PnL** is converted via [core/fx_rates.py](core/fx_rates.py).
  USDJPY trades produce JPY PnL; the resolver reads the venue's `fx_conversion`
  block from `adapter_admin/adapters_config/<venue>.json` and converts at the
  bar nearest (and not after) each trade timestamp. Without this, JPY PnL is
  silently dropped from the USD account. Result extraction (`results/`) converts
  realized/unrealized PnL to the account base currency.
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
is parsed from the bar type string. Each venue config also stores a derived
daily session window (kept fresh by `core/session_windows.py`). Sensitive
fields (`api_key`, `secret`, etc.) are masked with `****<last-4>` when sent to
the frontend.

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

### CI / reports

`.github/workflows/` holds `python-app.yml` (test/lint on push) and
`push-report.yml` (auto-generates HTML reports into `html_reports/` on push,
committed as `report: <branch>@<sha> [skip ci]`). Generated reports and design
notes live under `html_reports/`, `reports/` and `docs/`.

## Conventions specific to this repo

- **Modular package layout**: `backtest_runner`, `models`, `csv_loader`,
  `instrument_factory` are packages whose logic lives in each subdirectory's
  `__init__.py`, with the package root re-exporting **every** former symbol
  (curated `__all__`). When adding a concern, give it its own subdirectory and
  re-export from the root — don't break the back-compat surface that
  `verify_session_changes.py`, `scripts/`, and `tests/` reach into.
- **Phase profiling pattern**: `with _phase("label", phase_bag): ...` in
  `core/backtest_runner/profiling/`. `phase_bag` is `None` in the hot path
  (zero cost); only allocated when `_PROFILE_PHASES=1`. Don't refactor this to
  use `time.perf_counter()` calls directly — the no-op `None` path is intentional.
- **Result-dict tags**: Every backtest result includes provenance tags like
  `path_b`, `bars_filtered_by_run_on_days`, `bars_filtered_by_entry_window`,
  `warning`, `entry_window_skipped`, `cross_portfolio_dispatch`. Frontend
  tearsheets fall back to client-side derivation for new metrics so older
  backends still render — keep that compatibility unless you're cleaning a
  deprecated path.
- **No `pytest.ini`/`pyproject.toml`**: pytest discovery uses the `tests/`
  layout. `verify_session_changes.py` is a hand-rolled checker, not pytest —
  run it independently after big changes to wiring.
- **Don't bypass the venue's account-base-currency**: When in doubt about
  currency conversion, prefer adding to `fx_conversion` in the venue's
  adapter config over hard-coding in the runner.
