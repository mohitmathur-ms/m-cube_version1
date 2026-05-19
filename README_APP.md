# m-cube — Application Overview & Repository Layout

**m-cube** is a backtesting and research platform for **FX, crypto, and commodities** built on **NautilusTrader**. It's a single Flask web app (Python backend + vanilla HTML/CSS/JS SPA) for loading historical bar data, aggregating it to higher timeframes, running single-strategy and multi-slot portfolio backtests with full exit management (SL / TP / trailing / squareoff / RBO), and viewing tearsheets. A second Flask app under [adapter_admin/](adapter_admin/) administers broker / venue adapter configurations.

For deeper docs see:
- [CLAUDE.md](CLAUDE.md) — architecture cheat-sheet for contributors
- [PORTFOLIO_SETUP_GUIDE.md](PORTFOLIO_SETUP_GUIDE.md) — full design & schema of the portfolio system (most authoritative spec)
- [LOGICS_BACKEND_STATUS.md](LOGICS_BACKEND_STATUS.md) — which UI logics are wired to the backend vs UI-only
- [docs/btsoftware_fx_clone_design.md](docs/btsoftware_fx_clone_design.md) — long-term design target
- [ipynb/README.md](ipynb/README.md) — tour of the aggregation & engine-dispatch verification notebooks
- [how_to_use.txt](how_to_use.txt) — runtime env-var flags

---

## Quick Start

```powershell
# Windows one-shot setup + launch (creates ./venv, installs requirements,
# opens browser at http://localhost:5000)
.\start.bat

# Manual run (venv activated or `pip install -r requirements.txt` already done)
python server.py

# Adapter Admin Panel (separate Flask app on its own port)
python adapter_admin\admin_server.py
```

Python **>= 3.11** required.

---

## Repository Layout

Folders are grouped by role. The two most important things to find your way around are highlighted at the bottom of this section: **where data aggregation happens** and **where aggregation is validated**.

### Backend code

| Folder / file | What it does |
|---|---|
| [server.py](server.py) | Flask REST API. Every endpoint requires an `X-User-Id` header. Routes for data load, view, backtest, portfolio, tearsheet, orderbook, and adapter config. |
| [core/](core/) | All backend business logic — see the file-level breakdown below. |
| [strategies/](strategies/) | Built-in entry strategies (`ema_cross.py`, `rsi_mean_reversion.py`, `bollinger_bands.py`, `four_ma.py`, `range_breakout.py`). Pure signal logic, no exit management. Auto-discovered by [strategies/__init__.py](strategies/__init__.py). |
| [custom_strategies/](custom_strategies/) | User-uploaded strategy `.py` files, organised as `custom_strategies/<user_id>/`. Loaded via [core/custom_strategy_loader.py](core/custom_strategy_loader.py) and merged with built-ins at runtime. |
| [adapter_admin/](adapter_admin/) | Separate Flask app that manages broker / venue adapter configurations. Per-venue JSON in [adapter_admin/adapters_config/](adapter_admin/adapters_config/); custom Python adapters in [adapter_admin/custom_adapters/](adapter_admin/custom_adapters/); data-format specs in [adapter_admin/data_formats/](adapter_admin/data_formats/). |

#### Inside `core/`

| File | Job |
|---|---|
| [core/aggregator.py](core/aggregator.py) | **THE data-aggregation engine.** Resamples 1-minute OHLCV → 5/15/30-min, 1/2-hour, 1-day, 1-week, 1-month bars using `closed='right', label='right'` bucketing. Also computes RSI / ATR / EMA / VWAP feature sidecars per aggregated bar. Public entry point: `aggregate_ohlcv(df_1min, timeframe)` and `aggregate_to_timeframes(...)`. |
| [core/csv_loader.py](core/csv_loader.py) | Parses raw vendor CSVs; synthesizes MID bars from ASK + BID at load time. |
| [core/nautilus_loader.py](core/nautilus_loader.py) | Converts DataFrames into Nautilus `Bar` objects and writes them into the Parquet catalog. |
| [core/instrument_factory.py](core/instrument_factory.py) | Constructs Nautilus `Instrument` definitions (currency pairs, commodities) for venues. |
| [core/backtest_runner.py](core/backtest_runner.py) | **L3 portfolio orchestrator.** Dispatches to per-slot or grouped engines; implements Path A / Path B / `_USE_GROUPING` execution paths; merges results; applies portfolio SL/TP halts. |
| [core/managed_strategy.py](core/managed_strategy.py) | **L2 exit-management state machine** — wraps any signal with SL / TP / trailing / squareoff / RBO and exit-action dispatch (`close` / `re_execute` / `reverse`). |
| [core/signals.py](core/signals.py) | `SIGNAL_REGISTRY` — pure signal functions used by `ManagedExitStrategy` to drive entries. |
| [core/models.py](core/models.py) | Portfolio schema: `PortfolioConfig` → `StrategySlotConfig` → `ExitConfig` dataclasses, plus serialization helpers. |
| [core/users.py](core/users.py) | Identity-only multi-user layer (no auth); reads [config/users.json](config/users.json). |
| [core/migrate_users.py](core/migrate_users.py) | One-time migration of legacy single-user portfolios into the `_default` bucket. |
| [core/fx_rates.py](core/fx_rates.py) | Cross-currency PnL conversion (e.g. USDJPY → USD account base) using the venue's `fx_conversion` block. |
| [core/venue_config.py](core/venue_config.py) | Reads per-venue JSON from `adapter_admin/adapters_config/` at backtest time. |
| [core/custom_strategy_loader.py](core/custom_strategy_loader.py) | Discovers and imports user-uploaded strategies; merges with built-ins via `get_merged_registry()`. |
| [core/report_generator.py](core/report_generator.py) | Renders backtest results into the HTML tearsheet template. |
| [core/strategies.py](core/strategies.py) | Legacy strategy adapters (kept for backwards compatibility). |
| [core/templates.py](core/templates.py) | Shared HTML / report template helpers. |
| [core/runtime_history.py](core/runtime_history.py) | Persists run history to `.runtime_history.json`. |
| [core/_pandas_utils.py](core/_pandas_utils.py) | Internal pandas helpers shared by the loader and aggregator. |

### Data — input → catalog → output

| Folder | What it stores |
|---|---|
| [csv/](csv/) | **Raw input data**. Subfolders `fx_csv/` (1-min ASK / BID CSV exports per pair) and `commodities_csv/` (commodity 1-min bars). This is what gets ingested into the catalog. |
| [catalog/](catalog/) | NautilusTrader `ParquetDataCatalog` — the single source of truth that `server.py` and the runner read from. Aggregated parquets live at `catalog/data/bar/<bar_type>/`; **every higher-timeframe bar (5m, 15m, 30m, 1h, 2h, 1d, 1w, 1mo) produced by the aggregator ends up here**. Instrument definitions live at `catalog/data/currency_pair/`; feature sidecars at `catalog/features/bar/`. |
| [data/](data/) | Debug snapshots from sniffer / engine-dispatch runs (raw 1-min CSVs, `order_events/`). Not used by production code paths. |
| [temp_csv/](temp_csv/) | Short-lived per-run output: `fills.csv`, `positions.csv`, `account.csv`, `summary.csv` written by smoke scripts such as [scripts/run_ema_cross_april2024.py](scripts/run_ema_cross_april2024.py). |

### Aggregation validation

| Folder / file | What it validates |
|---|---|
| [ipynb/](ipynb/) | Notebooks that **verify the aggregation pipeline** and demonstrate Nautilus's two-layer data model (`DataEngine` → strategy vs `SimulatedExchange` → matching). Start with [ipynb/README.md](ipynb/README.md) for the full methodology. |
| [ipynb/individual_timeframes_validation_scripts/](ipynb/individual_timeframes_validation_scripts/) | **Per-timeframe aggregation validation.** Each notebook compares three independent OHLCV paths within `1e-8` price / `1e-6` volume tolerance: (a) `core/aggregator.py` output, (b) an independent `pandas.resample()` reference, and (c) the stored catalog parquet. |
| [ipynb/individual_timeframes_validation_scripts/fx_validation/](ipynb/individual_timeframes_validation_scripts/fx_validation/) | FX universe (EURUSD / GBPUSD / USDJPY × BID / ASK). Validates 1-min → 15-min, 30-min, 1-hour, 2-hour, 1-week, 1-month. Plus [aggregate_1min_to_5min.ipynb](ipynb/individual_timeframes_validation_scripts/fx_validation/aggregate_1min_to_5min.ipynb) (5-min aggregation pass) and [check_missing_minutes_eurusd_ask.ipynb](ipynb/individual_timeframes_validation_scripts/fx_validation/check_missing_minutes_eurusd_ask.ipynb) (input-side gap audit). |
| [ipynb/individual_timeframes_validation_scripts/commodities_validation/](ipynb/individual_timeframes_validation_scripts/commodities_validation/) | Commodities universe — all 8 target timeframes: 5-min, 15-min, 30-min, 1-hour, 2-hour, 1-day, 1-week, 1-month. |
| [ipynb/catalog_aggregation_verification.ipynb](ipynb/catalog_aggregation_verification.ipynb) | Catalog-wide sweep: verifies all 48 `(instrument, side, timeframe)` parquets at once and writes per-bar window metadata to `reports/catalog_aggregation_windows.csv.gz`. |
| [ipynb/aggregator_strategy_demo.ipynb](ipynb/aggregator_strategy_demo.ipynb) | Compares a hand-written 5-min aggregator against Nautilus's internal `TimeBarAggregator` running the same EMA-cross strategy. |
| [ipynb/oms_granularity_simulation.ipynb](ipynb/oms_granularity_simulation.ipynb), [ipynb/engine_dispatch_verification.ipynb](ipynb/engine_dispatch_verification.ipynb) | Empirical proofs that the strategy can see 5-min bars while the matching engine fills against 1-min bars (two-layer dispatch). |
| [ipynb/run_ema_cross_april2024.ipynb](ipynb/run_ema_cross_april2024.ipynb) | Notebook companion to [scripts/run_ema_cross_april2024.py](scripts/run_ema_cross_april2024.py). |
| [tests/test_aggregator.py](tests/test_aggregator.py) | Unit-level counterpart to the validation notebooks — pytest invariants on bucketing math, volume clipping, and feature parity. |

### Frontend

| Folder / file | What it does |
|---|---|
| [static/](static/) | Vanilla HTML / CSS / JS SPA. |
| [static/index.html](static/index.html) | App shell with early-paint theme script. |
| [static/css/style.css](static/css/style.css) | Styles + `data-theme` light/dark variables. |
| [static/js/app.js](static/js/app.js) | SPA router and per-user gating. |
| [static/js/dashboard.js](static/js/dashboard.js), [static/js/load_data.js](static/js/load_data.js), [static/js/view_data.js](static/js/view_data.js), [static/js/backtest.js](static/js/backtest.js), [static/js/tearsheet.js](static/js/tearsheet.js), [static/js/orderbook.js](static/js/orderbook.js), [static/js/portfolio.js](static/js/portfolio.js), [static/js/portfolio_tearsheet.js](static/js/portfolio_tearsheet.js) | Page modules — each is a named export from `app.js`'s router. |

### Configuration & saved state

| Folder | What it holds |
|---|---|
| [config/](config/) | `users.json` (identity registry for the `X-User-Id` header) and `portfolio_tabs/` (declarative UI tab definitions for the portfolio editor). |
| [portfolios/](portfolios/) | Saved portfolio JSON, organised as `portfolios/<user_id>/<name>.json`. The reserved `_default/` bucket holds legacy pre-multi-user portfolios. |

### Scripts & CLIs

[scripts/](scripts/) — standalone command-line entry points.

| Script | Purpose |
|---|---|
| [scripts/aggregate_catalog.py](scripts/aggregate_catalog.py) | **CLI entry point to the aggregation pipeline.** Reads 1-min parquets from the catalog and writes higher timeframes back, using `core/aggregator.py`. |
| [scripts/load_commodities.py](scripts/load_commodities.py) | Bulk-ingests commodity CSVs into the catalog and auto-aggregates post-load. |
| [scripts/ingest_fx_bulk.py](scripts/ingest_fx_bulk.py) | Bulk-ingests FX CSVs into the catalog. |
| [scripts/run_ema_cross_april2024.py](scripts/run_ema_cross_april2024.py) | Smallest reproducible smoke run of the production stack (EMA Cross + `ManagedExitStrategy` over EURUSD April-2024). Writes `temp_csv/{fills,positions,account,summary}.csv`. |
| [scripts/run_yearly_eurusd_emaxma.py](scripts/run_yearly_eurusd_emaxma.py) | Year-long EMA crossover run for benchmarking. |
| [scripts/verify_grouping_parity.py](scripts/verify_grouping_parity.py) | Proves `_USE_GROUPING=1` produces identical P&L to per-slot for a given portfolio. |
| [scripts/benchmark_portfolio.py](scripts/benchmark_portfolio.py), [scripts/profile_slot.py](scripts/profile_slot.py), [scripts/aggregate_phase_profiles.py](scripts/aggregate_phase_profiles.py) | Performance benchmarking / phase profiling. |
| [scripts/capture_order_events.py](scripts/capture_order_events.py) | Captures engine order-event logs for debugging. |
| [scripts/render_report_html.py](scripts/render_report_html.py) | Renders a saved backtest result as a standalone HTML report. |

### Tests

| Folder / file | What it covers |
|---|---|
| [tests/test_aggregator.py](tests/test_aggregator.py) | Aggregator bucketing, volume clipping, feature parity (unit-level companion to the validation notebooks). |
| [tests/test_custom_strategy_loader.py](tests/test_custom_strategy_loader.py) | Custom-strategy discovery & registry merging. |
| [tests/test_perf_regression.py](tests/test_perf_regression.py) | Performance regression baselines. |
| [tests/smoke_tests/](tests/smoke_tests/) | Portfolio smoke + stress tests (`smoke_test_logics_audit.py`, `stress_test_portfolios.py`, `stress_test_stoploss.py` and matching `*_report.py` companions that render results as HTML). |
| [verify_session_changes.py](verify_session_changes.py) | Hand-rolled 100+-check wiring verifier (not pytest; keep green). |

### Reports & outputs

| Folder | What it holds |
|---|---|
| [reports/](reports/) | Long-lived analysis artefacts, including `catalog_aggregation_windows.csv.gz` (~64 MB; one row per aggregated bar — the receipt for [ipynb/catalog_aggregation_verification.ipynb](ipynb/catalog_aggregation_verification.ipynb)). |
| [reports_aggregator_demo/](reports_aggregator_demo/) | Per-run CSVs from [ipynb/aggregator_strategy_demo.ipynb](ipynb/aggregator_strategy_demo.ipynb) (internal vs custom aggregator comparison). |
| [html_reports/](html_reports/) | Rendered HTML user guides on aggregation, SL/TP logic, storage patterns. |
| [output/](output/) | Debug dumps (missing-minute scans, sniffer outputs, ad-hoc analysis CSVs). |

### Documentation

| Folder / file | Contents |
|---|---|
| [docs/](docs/) | HTML user guides and the BTSoftware FX-clone design target. |
| [5. Logics/](5.%20Logics/) | HTML business-logic specs (execution, exit-settings, re-execution, hedge, stop-loss/target, RBO). Subfolders archive stress-test / stoploss-test results. |
| [README.md](README.md) | Top-level project pointer (unchanged short overview). |
| [CLAUDE.md](CLAUDE.md), [PORTFOLIO_SETUP_GUIDE.md](PORTFOLIO_SETUP_GUIDE.md), [LOGICS_BACKEND_STATUS.md](LOGICS_BACKEND_STATUS.md), [how_to_use.txt](how_to_use.txt) | Authoritative specs and runtime-flag reference. |

### Build / misc

| File | Purpose |
|---|---|
| [requirements.txt](requirements.txt) | Pinned Python dependencies (`nautilus_trader==1.224.0`, `Flask==3.1.3`, `pandas==2.3.3`, `numpy==2.4.3`, `pyarrow==23.0.1`). |
| [Dockerfile](Dockerfile), [.dockerignore](.dockerignore) | Container build. |
| [start.bat](start.bat) | Windows one-shot setup + launch. |
| [how_to_use.txt](how_to_use.txt) | Runtime env-var flags (`_USE_GROUPING`, `_USE_BACKTEST_NODE`, `_PROFILE_PHASES`). |

---

## The Aggregation Pipeline — Where & How

**Aggregation happens in [core/aggregator.py](core/aggregator.py).** Public entry points: `aggregate_ohlcv(df_1min, timeframe)` for a single timeframe and `aggregate_to_timeframes(...)` for the full fan-out. Bucketing uses `closed='right', label='right'`: the bar stamped at `T` aggregates the 1-min bars in the half-open window `(T − N, T]`. Volume is summed and clipped to `QUANTITY_MAX`. RSI / ATR / EMA / VWAP feature sidecars are computed post-aggregation.

It is invoked from three places:
- The `/api/csv/load` endpoint in [server.py](server.py) when fresh CSVs are ingested via the UI.
- The CLI [scripts/aggregate_catalog.py](scripts/aggregate_catalog.py) — reads 1-min parquets from the catalog and writes higher timeframes back.
- The CLI [scripts/load_commodities.py](scripts/load_commodities.py) — bulk-loads commodity CSVs and auto-aggregates as a post-step.

Output is written to [catalog/](catalog/) as Parquet at `catalog/data/bar/<bar_type>/`, one folder per `(instrument, side, timeframe)` tuple.

**Validation of aggregation lives in [ipynb/individual_timeframes_validation_scripts/](ipynb/individual_timeframes_validation_scripts/).** Two parallel folders cover the two asset universes:
- [fx_validation/](ipynb/individual_timeframes_validation_scripts/fx_validation/) — one notebook per target timeframe for EURUSD / GBPUSD / USDJPY (BID + ASK).
- [commodities_validation/](ipynb/individual_timeframes_validation_scripts/commodities_validation/) — one notebook per target timeframe for the commodity universe.

Each notebook enforces a **three-way invariant**: `core.aggregator.aggregate_ohlcv(...)` ≡ independent `pandas.resample(...)` reference ≡ stored catalog parquet, within `1e-8` price / `1e-6` volume tolerance. The catalog-wide sweep over all 48 `(instrument, side, timeframe)` tuples is [ipynb/catalog_aggregation_verification.ipynb](ipynb/catalog_aggregation_verification.ipynb). The unit-level counterpart is [tests/test_aggregator.py](tests/test_aggregator.py). Full methodology: [ipynb/README.md](ipynb/README.md).

---

## How to Use the App

1. **Load Data.** Point the loader at your CSV folder. Use presets (BTC / ETH / SOL / EURUSD / …) or pick symbols manually. Click "Load Selected" — the file is parsed by [core/csv_loader.py](core/csv_loader.py), the 1-minute bars are written to the catalog, and [core/aggregator.py](core/aggregator.py) fans out the higher timeframes.
2. **View Data.** Pick a loaded symbol; the UI shows candlestick charts, OHLCV tables, daily return distribution, and cumulative returns.
3. **Run Backtest.** Pick a symbol, a strategy (EMA Cross / RSI Mean Reversion / Bollinger Bands / Four MA / Range Breakout, or a custom `.py` upload), tweak its parameters, click "Run Backtest".
4. **Build a Portfolio.** Open the portfolio editor; add slots (each is a strategy + instrument + bar type + exit config), set portfolio-level capital / squareoff / SL/TP, save, then run. Multi-slot runs dispatch through [core/backtest_runner.py](core/backtest_runner.py).
5. **Tearsheet.** Equity curve, drawdown, win / loss distribution, trade-by-trade P&L.

---

## Further Reading

- [CLAUDE.md](CLAUDE.md) — backend layer cheat-sheet (L0–L4), Path A / Path B / grouping flags, FX-specific concerns, repo conventions.
- [PORTFOLIO_SETUP_GUIDE.md](PORTFOLIO_SETUP_GUIDE.md) — definitive schema and end-to-end design of the portfolio system.
- [LOGICS_BACKEND_STATUS.md](LOGICS_BACKEND_STATUS.md) — UI tab → backend wiring status table.
- [docs/btsoftware_fx_clone_design.md](docs/btsoftware_fx_clone_design.md) — long-term design target.
- [ipynb/README.md](ipynb/README.md) — full tour of the verification notebooks.
- [how_to_use.txt](how_to_use.txt) — runtime env-var flags.
