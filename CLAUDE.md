# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**m-cube** — a backtesting and research platform built on **NautilusTrader**.
The product is a single Flask web app (Python backend + vanilla HTML/CSS/JS
SPA frontend) for loading historical data, running single-strategy and
multi-slot portfolio backtests with exit management (SL/TP/trailing), and
viewing tearsheets. It targets multiple asset classes: **FX**, **crypto**,
**commodities** and **indices** are wired end-to-end, with CSV/instrument
schema stubs already in place for **equity**, **debt** and **alternative**
assets (see `adapter_admin/data_formats/`). Every single-strategy backtest
also auto-generates an aggregation report (a self-rebuilding HTML catalog of
all runs plus per-stage bar-capture CSVs) under `aggregation_reports/`.

A second Flask app under [adapter_admin/](adapter_admin/) (port **5001**)
administers broker/venue adapter configurations, per-asset-class data-format
schemas, and the multi-user registry.

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

# Adapter Admin Panel (separate Flask app, http://localhost:5001)
python adapter_admin\admin_server.py
```

Python **>= 3.11** is required (`start.bat` enforces this and rebuilds the venv
on mismatch). Major dep pins: `nautilus_trader==1.224.0`, `Flask==3.1.3`,
`pandas==2.3.3`, `numpy==2.4.3`, `pyarrow==23.0.1`.

### Tests

```powershell
# pytest test suite (custom strategy loader, perf regression, leg/target features, ...)
venv\Scripts\python.exe -m pytest tests\

# Single test file
venv\Scripts\python.exe -m pytest tests\test_custom_strategy_loader.py -v

# NOTE: tests\test_aggregator.py still imports the removed `core/aggregator.py`
# (the aggregation logic was superseded by the reporting subsystem) and will
# fail at collection until that module is restored — skip it for now.

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
| `_USE_VWAP_FILL=1` | Reprices SL/Target exit fills to a VWAP-proxy fill (per spec §4.2). Builds a VWAP lookup from the ASK/BID bars instead of filling at the bar close. |
| `_USE_PF_AGG_MOVE_SL=1` | Two-pass portfolio-level **aggregated** Move-SL trigger: pass 1 discovers the combined-P&L timeline, pass 2 replays slots with the aggregate trigger injected. ~2× runtime; only active when an aggregate/cross-slot Move-SL is configured. |
| `_USE_PF_REEXEC_REPLAY=1` | Portfolio **ReExecute replay** (spec §2.4): when a portfolio SL/Target fires ReExecute, every slot is re-run flat from the clip timestamp and spliced onto the pre-clip trades. Recurses per configured ReExecute count (0 = unlimited, hard-capped at 50). |

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
  └─ core/{csv_loader/,nautilus_loader.py,instrument_factory/}
                                   L0  Data ingest → ParquetDataCatalog
                                       (csv_loader & instrument_factory are now packages)
  └─ core/{aggregation_report,sniffers,report_generator}.py
                                   --  Reporting: ledger + auto HTML + bar-capture CSVs
```

The two L0 ingest modules are **packages**, not single files (recently
modularized into single-responsibility sub-packages; each public API is
re-exported from the package `__init__.py`, so `from core.csv_loader import
scan_csv_folder` / `from core.instrument_factory import create_instrument`
keep working unchanged):
- `core/csv_loader/` — CSV scan → clean OHLCV → MID synthesis. Public API:
  `scan_csv_folder`, `load_csv`, `concat_side`, `load_pair_mid`,
  `get_display_label`, `clear_fx_scan_cache`, `DEFAULT_CSV_FOLDER`,
  `QUANTITY_MAX`. Sub-packages: per-layout scanners (`fx_daily_scanner`,
  `fx_consolidated_scanner`, `commodity_daily_scanner`, `index_daily_scanner`,
  `crypto_nested_scanner`) + `folder_scanner` orchestrator, `csv_reader`,
  `side_concat` (parallel one-side concat), `mid_merge` (ASK+BID → MID),
  `timestamp_parser` (vectorized), `scan_cache` (mtime+TTL), `constants`.
- `core/instrument_factory/` — bar-type → NautilusTrader `CurrencyPair`.
  Public API: `create_instrument`, `instrument_for_bar_type`, `VENUE`,
  `PRICE_PRECISION`, `BASE_PRICE_PRECISION`, `CURRENCY_PRECISION`.
  Sub-packages: `bar_type_resolver`, `instrument_builder`, `currency`,
  `price_size_precision`, `lot_size`, `catalog_lookup` (authoritative lookup
  from the ParquetDataCatalog), `data_format_config` (per-asset-class JSON).
  Construction is invoked from `core/nautilus_loader.py` after CSV load.

Supporting `core/` modules (not part of the L0–L3 stack but referenced throughout):
- `aggregation_report.py` — owns the `aggregation_reports/` path layout, the JSON
  ledger, and the single self-rebuilding `combined_report.html`.
- `sniffers.py` — capture-strategies that dump the bar stream at three pipeline
  stages (raw engine data, DataEngine dispatch, post-aggregation strategy boundary).
- `report_generator.py` — interpolates per-backtest tearsheet HTML (orderbook, fills,
  P&L, logs) from a NautilusTrader result; `_pandas_utils.py` provides its fast
  row iteration helper.
- `runtime_history.py` — persists EMA-weighted per-`(bar_type, strategy)` wall-times
  to `.runtime_history.json` for longest-processing-time scheduling heuristics.
- `templates.py` — built-in portfolio templates (Trend Following, Mean Reversion, etc.).
- `strategies.py` — thin backward-compat re-export of the `strategies/` package registry.

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

### Other gated execution behaviors

Three more flags (see the runtime-flags table) gate non-default exit/fill logic
in `backtest_runner.py`, all off by default and all additive:

- **VWAP fill** (`_USE_VWAP_FILL`): instead of filling SL/Target exits at the
  bar close, a VWAP-proxy lookup is built from the ASK/BID bars and the exit
  fill is repriced to it.
- **Aggregate Move-SL** (`_USE_PF_AGG_MOVE_SL`): a two-pass replay. Pass 1 runs
  to discover the combined cross-slot P&L timeline; pass 2 re-runs with the
  portfolio-aggregate Move-SL trigger injected (`move_sl_agg_pnl_*` fields on
  `PortfolioConfig`). Roughly doubles runtime, so it's gated.
- **ReExecute replay** (`_USE_PF_REEXEC_REPLAY`): when a portfolio SL/Target
  resolves to ReExecute, every slot is re-run flat starting at the clip
  timestamp and the new trades are spliced onto the pre-clip trades. It
  recurses per the configured ReExecute count (hard-capped at 50).

### Portfolio data model

`core/models.py` is the schema. Three nested dataclasses:

```
PortfolioConfig (~90 fields. portfolio-level: capital, max_loss/profit,
   │              run_on_days, entry window, squareoff, RBO settings;
   │              portfolio SL/Target + trailing; Move-SL-to-cost incl. the
   │              aggregate trigger (move_sl_agg_pnl_*); cross-portfolio SL/Target
   │              dispatch (pf_sl/tgt_target_portfolio); ReExecute tab; product/
   │              MIS squareoff; live-only monitoring fields)
   └─ StrategySlotConfig (per-slot: strategy_name, bar_type_str, lots,
          │               allocation_pct, start/end_date, squareoff,
          │               strategy_bar_types = composite/aggregated bar list)
          └─ ExitConfig (per-leg: SL/TP type+value incl. ATR-based (sl_atr_*,
                         tgt_atr_*), trailing, target lock, leg-level trailing
                         target (tgt_trail_*), sl_wait_bars/sl_wait_sec +
                         tgt_wait_sec, on_sl/target_action, cross-leg re-entry
                         (execute_target_leg_id, reentry_price), squareoff)
```

The schema is large and still growing; treat the field groups above as
categories, not an exhaustive list — read `core/models.py` for the authoritative
set and defaults.

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
  ([core/csv_loader/mid_merge/](core/csv_loader/mid_merge/) —
  `_merge_ask_bid_to_mid` / `load_pair_mid`). When a MID slot runs, the engine
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

A separate Flask app under [adapter_admin/](adapter_admin/), served on
**port 5001**, with its own vanilla-JS SPA in `adapter_admin/static/`
(Dashboard, Add Adapter, Custom Adapters, Data Formats, Users pages). It owns
three kinds of configuration:

**1. Venue/adapter configs** — stored as JSON in
`adapter_admin/adapters_config/*.json` (one per venue; currently `binance_ms`,
`coinbase_ms`, `commodities_ms`, `forex_ms`, `nifty_futures_ms`). Built-in
adapters are auto-discovered from the installed `nautilus_trader` package by
`adapter_discovery.py` (it introspects `*DataClientConfig` / `*ExecClientConfig`
+ factory classes and turns their fields into UI form definitions). Custom
adapters are uploaded as Python files to `adapter_admin/custom_adapters/` and
validated in a subprocess by `custom_adapter_loader.py` (must export
`ADAPTER_NAME`, a `DATA_CLIENT_CLASS`/`EXEC_CLIENT_CLASS`, `CONFIG_CLASS`,
`FACTORY_CLASS`, and optional `PARAMS`) — subprocess isolation prevents a bad
file from crashing the panel via the Rust extension. `adapter_registry.py`
handles masking: sensitive fields (`api_key`, `secret`, `passphrase`, tokens,
etc.) are sent to the frontend as `****<last-4>`, and on update an incoming
masked value preserves the stored secret instead of overwriting it. Each config
also carries `account_base_currency` and an `fx_conversion` block — these drive
cross-currency PnL conversion (see the FX-specific concerns above). The main
server reads these configs at backtest time via
[core/venue_config.py](core/venue_config.py) — the venue is parsed from the
bar type string.

**2. Per-asset-class data-format schemas** — `adapter_admin/data_formats/*.json`,
one per asset class (`cryptocurrency`, `commodity`, `equity`, `fx`, `debt`,
`index`, `alternative`). Each describes how that class's CSV input is parsed
(filename pattern, required/optional columns, timestamp format), how its
NautilusTrader instrument is constructed (type, quote currency, exposed
ASK/BID/MID sides, price/size precision, default timeframe), and default
trading params (maker/taker fees, init/maint margin). These are consumed at
data-load time; `equity`/`debt`/`alternative` are still mostly null stubs.

**3. The multi-user registry** — the panel also reads/writes the repo-root
[config/users.json](config/users.json) (see Multi-user model above).

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

### Auto-generated reports

Every single-strategy `run_backtest()` automatically records an aggregation
report after `engine.run()` —
[core/backtest_runner.py](core/backtest_runner.py) calls
`core.aggregation_report.record_run(...)`, which appends one row to
`aggregation_reports/_ledger.json` and re-renders the single global
`aggregation_reports/combined_report.html` from that ledger. In parallel,
`core/sniffers.py` captures the bar stream at three pipeline stages and writes
CSVs under `aggregation_reports/{asset_class}/{symbol}/{target_timeframe}/`
(the `{target_timeframe}` directory is the INTERNAL aggregation timeframe the
strategy trades on — e.g. `5MIN` for a 1-min→5-min run — or the EXTERNAL base
timeframe when no aggregation is used):

```
raw_engine_sniffer_<EXT_TF>_<DDMMMYYYY>_<DDMMMYYYY>.csv   # engine.data before run()
sniffer_data_engine_<EXT_TF>_<DDMMMYYYY>_<DDMMMYYYY>.csv  # what DataEngine dispatches
sniffer_strategy_<TARGET_TF>_<DDMMMYYYY>_<DDMMMYYYY>.csv  # what reaches the strategy
                                                          # (after any INTERNAL aggregation)
```

**Contract:** all report functions swallow and log their own errors — a
report-pipeline failure must **never** propagate out and kill a backtest. Keep
this invariant when touching the reporting code.

Both `aggregation_reports/` and the portfolio-level `reports/` directory are
runtime artifacts and are **gitignored** — don't commit their contents. The
portfolio-level HTML tearsheets in `reports/<user_id>/` are produced separately
by `core/report_generator.py` and are only generated on explicit request from
the relevant `server.py` endpoints, not auto-triggered.

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