# `ipynb/` — Aggregation & Engine-Dispatch Notebooks

This directory contains the notebooks and one companion script used to **verify**
the bar-aggregation pipeline (1-MIN → higher timeframes) and **demonstrate** how
NautilusTrader's two-layer data model (`DataEngine` → strategy, `SimulatedExchange`
→ matching) actually behaves under different subscription patterns.

Everything below operates on the same FX universe used elsewhere in the project:
`EURUSD / GBPUSD / USDJPY` on the `FOREX_MS` venue, BID and ASK price types,
1-MIN-EXTERNAL bars stored in the local `catalog/` plus the raw daily CSVs
under `Dataset/FX_yyyy/Fx/<PAIR>/<YYYY>/<MM>/`.

| File | What it answers |
|---|---|
| [aggregator_strategy_demo.ipynb](aggregator_strategy_demo.ipynb) | Does running an EMA-cross strategy with a **user-built** 5-min aggregator produce the same result as letting **Nautilus internally** aggregate the same 1-min stream? |
| [catalog_aggregation_verification.ipynb](catalog_aggregation_verification.ipynb) | Are all 48 aggregated parquet datasets in `catalog/data/bar/` correct OHLCV roll-ups of their 1-MIN-EXTERNAL source? |
| [oms_granularity_simulation.ipynb](oms_granularity_simulation.ipynb) | When the strategy subscribes to 5-min bars but the engine is fed 1-min bars, at what granularity does the matching engine actually fill orders? |
| [engine_dispatch_verification.ipynb](engine_dispatch_verification.ipynb) | Does `engine.data` (pre-run) exactly equal what the `DataEngine` dispatches at runtime? And does the `TimeBarAggregator` consume only those dispatched EXTERNAL bars? |
| [../scripts/run_ema_cross_april2024.py](../scripts/run_ema_cross_april2024.py) | Standalone CLI: run the production `ManagedExitStrategy` (EMA Cross + 20%/40% SL/TP) on EURUSD April-2024 and write `temp_csv/{fills,positions,account,summary}.csv`. |

---

## 1. `aggregator_strategy_demo.ipynb`

**Goal.** Run the *same* EMA-cross strategy with the *same* bracket SL/TP twice
over EURUSD January-2024 1-min bars and compare a custom aggregator against
Nautilus's internal `TimeBarAggregator`.

### Setup
- Loads the full month of `*_ASK_OHLCV.csv` **and** `*_BID_OHLCV.csv` from
  `Dataset/FX_yyyy/Fx/EUROUSD/2024/01/`. Both sides are required because the
  matching engine needs a complete L1 quote (ASK *and* BID) to fill against —
  loading only one side causes every market entry to be rejected with
  `'no market for EURUSD.FOREX_MS'`. This rule is the same one
  `_pair_bid_ask_bar_type` enforces in [core/backtest_runner.py](../core/backtest_runner.py).
- Wraps both sides into 1-MIN-EXTERNAL `Bar` lists and concatenates them as
  `ALL_BARS` — the engine sorts by timestamp internally.

### Two strategies, one shared bracket helper
- **`InternalEmaCrossStrategy`** — subscribes to
  `EURUSD.FOREX_MS-5-MINUTE-ASK-INTERNAL@1-MINUTE-EXTERNAL`. The 5-min bars are
  built by Nautilus's `DataEngine`. EMAs are registered with
  `register_indicator_for_bars` so the framework updates them automatically.
- **`CustomAggEmaCrossStrategy`** — subscribes to
  `1-MINUTE-ASK-EXTERNAL` and pushes every 1-min bar through `FiveMinAggregator`
  (a hand-written class that computes `Bar5MinFeatures`: OHLCV plus ATR/RSI/EMA/VWAP/etc.).
  When the aggregator emits a 5-min bar, the strategy manually feeds
  `b5.close` into the EMAs via `update_raw(...)`.
- Both call the shared `submit_bracket(...)` helper, which builds a
  `order_factory.bracket()` with a `STOP_MARKET` SL 20 pips away and a `LIMIT`
  TP 40 pips away (1:2 R:R), linked under an OUO contingency so when one child
  fills the other auto-cancels.

### Reports
`run_backtest(strategy, prefix)` builds a fresh `BacktestEngine` (margin USD
account, FX leverage 30x), adds all bars, runs the strategy, and writes
three reports per run to `reports_aggregator_demo/`:

```
reports_aggregator_demo/
  internal_order_fills.csv      internal_positions.csv      internal_account.csv
  custom_order_fills.csv        custom_positions.csv        custom_account.csv
```

The observed counts in the notebook (78/39/315 vs 80/40/323) show that the two
aggregation paths produce **essentially equivalent** results — small differences
come from the internal aggregator's clock-driven emission filling some boundary
5-min bars the data-driven custom aggregator skips.

---

## 2. `catalog_aggregation_verification.ipynb`

**Goal.** Prove that every aggregated bar stored in `catalog/data/bar/` is a
correct OHLCV roll-up of its `1-MINUTE-...-EXTERNAL` source.

### Scope — 48 tuples
3 instruments × 2 sides × 8 timeframes = **48 datasets**.

| Instrument | Side | Timeframe |
|---|---|---|
| EURUSD / GBPUSD / USDJPY | BID / ASK | 5-MINUTE, 15-MINUTE, 30-MINUTE, 1-HOUR, 2-HOUR, 1-DAY, 1-WEEK, 1-MONTH |

### Three-way invariant
For each `(instrument, side, timeframe)`, the OHLCV of every aggregated bar
must agree across all three of:

1. **Path A** — `core.aggregator.aggregate_ohlcv(df_1min, tf, drop_partial=True)` (production logic).
2. **Path B** — independent `pd.resample(rule, closed='right', label='right').agg(OHLCV_AGG)` (separate implementation).
3. **Cat** — the bars currently stored in the parquet catalog.

The aggregation rule (shared by all three): the bar stamped at `T` aggregates
the 1-MIN bars in the half-open window `(T − N, T]`. Volume is summed and clipped
to `QUANTITY_MAX` (16,000,000,000).

### Tolerances
- Price: `1e-08`
- Volume: `1e-06`

`compare_frames(...)` aligns two OHLCV frames on the union of their
timestamps and counts how many bars match within those tolerances (vectorised
with `np.abs` and `np.searchsorted`).

### Memory discipline
~24M 1-MIN bars span 6 `(instrument, side)` pairs. Each pair is loaded **once**
and reused across all 8 timeframes, then dropped via `gc.collect()` before the
next pair. The per-bar window index (7.7M rows) is streamed to
`reports/catalog_aggregation_windows.csv.gz` as each tuple is verified — never
held in memory all at once.

### Outputs
- `reports/catalog_aggregation_windows.csv.gz` (~64 MB gzipped, 7.7M rows) —
  one row per aggregated bar with `agg_ts`, `window_open_expected`,
  `first_1min_ts`, `last_1min_ts`, `n_1min_in_window`, and three `delta_*`
  columns measuring the gap between nominal window edges and the
  first/last actually-observed 1-MIN tick. Useful for spotting FX weekend gaps,
  holiday gaps, and the Sunday-evening session open.
- `reports/catalog_aggregation_windows_sample.csv` (~16 MB) — first 1k + middle 1k + last 1k rows per tuple, for quick inspection.
- A 48-row summary DataFrame with `PASS / FAIL / SKIP` cells for `A_vs_cat`,
  `B_vs_cat`, `A_vs_B`, and overall `status`.
- A rollup grouped by target timeframe (1-Minute → 5-MINUTE, → 15-MINUTE, …),
  with a worked example for `EURUSD ASK 5-MINUTE` (fixed-width bucket) and
  `USDJPY BID 1-WEEK` (variable-width bucket, anchored W-SUN) so the
  FX-weekend behaviour around the `(T−N, T]` rule is directly inspectable.

---

## 3. `oms_granularity_simulation.ipynb`

**Goal.** Empirically prove the two-layer dispatch model:

| Setup | Strategy `on_bar` sees | `SimulatedExchange` matches on |
|---|---|---|
| `add_data` = 5-min EXTERNAL only; subscribe 5-min EXTERNAL | 5-min | 5-min |
| `add_data` = 1-min EXTERNAL; subscribe `5-MIN-INTERNAL@1-MIN-EXTERNAL` | 5-min | **1-min** |

### The two scenarios
Same strategy ([strategies/ema_cross.py](../strategies/ema_cross.py),
`fast=10`, `slow=20`), same trade size, same price path (EURUSD April 2024).
Only two things change:

- **Scenario A** — pre-aggregate 1-min to 5-min via pandas
  (`resample("5min", closed='right', label='right')`) and feed only 5-min
  EXTERNAL bars to `engine.add_data(...)`. Strategy subscribes to
  `…-5-MINUTE-BID-EXTERNAL`.
- **Scenario B** — feed raw 1-min EXTERNAL bars and have the strategy subscribe
  to `…-5-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL`. The `DataEngine` spins up an
  internal `TimeBarAggregator` to synthesize 5-min bars from the 1-min stream.

### The proof
Fill timestamps land at different cadences:
- **A:** every `latency_sec` is a multiple of **300 s** — the next 5-min bar is
  the next event `SimulatedExchange` sees.
- **B:** every `latency_sec` is a multiple of **60 s** — the matching engine
  consumes every 1-min bar, even though the strategy still only receives 5-min
  bars (verified: identical crossover-signal counts).

### Scenario C — direct sniffer proof
Four passive `BarSnifferStrategy` instances run in one engine, each
subscribed to a different `BarType`. The strategies record every `Bar` they
receive into a class-level dict, then dump three CSVs to `csv/`:

```
csv/raw_engine_data_1min_2024_04.csv       # engine.data pre-run(), filtered to 1-MIN-EXTERNAL
csv/sniffer_engine_1min_2024_04.csv        # 1-min bars the DataEngine dispatched
csv/sniffer_strategy_5min_2024_04.csv      # 5-min bars the aggregator emitted to on_bar
```

The notebook prints `cross-check raw == sniffer (engine side): missing=0 extra=0`
— proving the engine **held** N 1-min bars in its merge-sorted buffer,
**dispatched** all N at runtime, and the aggregator **consumed** exactly those N.

> **Aggregator emits on a clock, not on data.** Nautilus's INTERNAL
> `TimeBarAggregator` ticks every 5-min wall-clock boundary in the run window
> regardless of whether an input 1-min bar arrived in that window. So a
> pandas resample with `dropna` yields fewer 5-min rows than the Nautilus
> aggregator. This does *not* affect logic — empty windows carry forward OHLC
> from the prior bar, so signals on dry windows are no-ops — but it does
> explain ~37% more rows from the aggregator vs. the pandas count.

### Scenario D — paced live visualizer
A small windowed run (1 hour, ~60 1-min bars) where each `on_bar` print is
spaced by `time.sleep(...)` so you can *watch* the dispatch stream unfold.
Because `BacktestEngine` uses a `TestClock` (event-driven, not wall-clock-driven —
see `_advance_time` in `nautilus_trader/backtest/engine.pyx:1684`), the sleep
parks only the OS thread; bar timestamps, aggregator emissions, fills, and
P&L are **identical bit-for-bit** to a zero-delay run.

---

## 4. `engine_dispatch_verification.ipynb`

**Goal.** A focused, two-variant follow-on to scenario C above — formalises
the equality `set(raw engine.data) == set(sniffer-dispatched)` for both 5-min
and 15-min aggregation patterns.

### What it does
- Loads 1-min BID + ASK from the catalog for EURUSD April 2024 (`+1d −1ns` end-of-day
  expansion to fully include April 30, mirroring `_cached_catalog_bars`).
- Builds two engines with the **identical** input `all_bars` (1-MIN-EXTERNAL × BID/ASK).
- **Variant 1** — strategy subscribes to 4 bar types: 1-MIN BID/ASK EXTERNAL +
  5-MIN BID/ASK INTERNAL@1-MINUTE-EXTERNAL.
- **Variant 2** — same, but with 15-MIN INTERNAL@1-MINUTE-EXTERNAL instead of 5-MIN.

### CSVs produced (6 total, all under `csv/`)

| Variant | CSV | Source | Represents |
|---|---|---|---|
| 5-min | `raw_engine_data_1min_2024_04.csv` | `engine.data` **before** `run()`, filtered to 1-MIN-EXTERNAL | What the engine **holds** pre-dispatch |
| 5-min | `sniffer_engine_1min_2024_04.csv` | runtime sniffer, `aggregation_source==EXTERNAL` | Bars `DataEngine` **dispatched** |
| 5-min | `sniffer_strategy_5min_2024_04.csv` | runtime sniffer, `aggregation_source==INTERNAL` | 5-min bars the **aggregator emitted** |
| 15-min | `raw_engine_data_15min_2024_04.csv` | engine pre-run | engine holds |
| 15-min | `sniffer_engine_15min_2024_04.csv` | runtime sniffer EXTERNAL | DataEngine dispatched |
| 15-min | `sniffer_strategy_15min_2024_04.csv` | runtime sniffer INTERNAL | 15-min aggregator output |

All six share the same 12 columns: `stream, price_type, bar_type, ts_event_ns,
ts_init_ns, open, high, low, close, volume, ts_event, ts_init`.

### Assertion
Per-variant, with `key = (bar_type, ts_init_ns)`:

```python
assert set(raw_engine) == set(sniffer_engine)   # missing=0, extra=0
```

The 15-min variant additionally reports `sniffer_strategy_15min` row count —
expected ≈ 30 days × 96 windows/day = **2,880 per side** (clock-driven
aggregator, one bar per wall-clock 15-min boundary regardless of input gaps).

---

## 5. `scripts/run_ema_cross_april2024.py`

**Goal.** A standalone CLI version of the production stack: a single command
that runs the `ManagedExitStrategy` (L2) wrapping EMA Cross (L1) over EURUSD
April-2024 catalog bars and writes a tidy summary to `temp_csv/`.

```powershell
python scripts\run_ema_cross_april2024.py
```

### What it exercises
- **Catalog read.** Loads 1-MIN-BID-EXTERNAL and 1-MIN-ASK-EXTERNAL via
  `ParquetDataCatalog.bars(...)` for `2024-04-01` → `2024-04-30` (with the
  same `+1d −1ns` end-of-day expansion used by the runner).
- **On-the-fly aggregation.** The strategy `bar_type` is
  `EURUSD.FOREX_MS-15-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL` — **no 15-min
  parquet is read**; the `DataEngine` aggregates on the fly from the 1-min
  EXTERNAL source. This is the same pattern used by the production portfolio
  runner.
- **Production exit engine.** Wraps the entry signal in `ManagedExitStrategy`
  via `config_from_exit(...)` with an `ExitConfig` of
  `stop_loss=20%`, `target=40%`, `on_sl_action="close"`, `on_target_action="close"`.
- **Reports.** Writes four CSVs to `temp_csv/`:
  - `fills.csv` — `trader.generate_order_fills_report()`
  - `positions.csv` — `trader.generate_positions_report()`
  - `account.csv` — `trader.generate_account_report(venue)`
  - `summary.csv` — top-line metrics (total trades, win rate, realized/unrealized PnL, return %)

### Why this script exists
It's the smallest reproducible setup that exercises:
- The catalog → engine bar load path,
- The `INTERNAL@…-EXTERNAL` on-the-fly aggregation pattern,
- The full `ManagedExitStrategy` state machine (signal → entry → SL/TP → close → flat).

Useful as a smoke test when changing the runner, the managed strategy, or the
catalog format.

---

## Cross-references

- Production aggregator: [core/aggregator.py](../core/aggregator.py)
- Catalog loader: [core/csv_loader.py](../core/csv_loader.py), [core/nautilus_loader.py](../core/nautilus_loader.py)
- Pairing BID/ASK at run time: `_pair_bid_ask_bar_type` in [core/backtest_runner.py](../core/backtest_runner.py)
- Exit-management engine: [core/managed_strategy.py](../core/managed_strategy.py)
- Per-timeframe validation notebooks: [individual_timeframes_validation_scripts/](individual_timeframes_validation_scripts/)
