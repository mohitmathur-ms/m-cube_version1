# Session Report — Multi‑Timeframe Data, Timezones, and Base‑Resolution Exits

A complete, in‑depth record of everything investigated and changed in this
session, ordered so you can jump straight to any topic. Five distinct pieces of
work, plus the deep‑dives that explain *why* each was needed.

**Headline change:** when a strategy signals on an aggregated timeframe (e.g.
5‑MINUTE built from 1‑SECOND base data), **the signal/entries stay on the
aggregated bar, but all exit management — leg SL/Target/Trailing/ATR/Square‑off
and portfolio SL/Target — now runs on the BASE bar**, so exits fire at the exact
base‑data second instead of being snapped to the 5‑minute grid.

---

## Table of contents

1. [Load Data: user‑selectable timeframe (incl. 1‑SECOND)](#1-load-data--user-selectable-timeframe)
2. [Index scanner fix: spot folder not detected](#2-index-scanner-fix)
3. [Timezones & the catalog: how timestamps are stored](#3-timezones--the-catalog)
4. [Square‑off: how it resolves and how to disable it](#4-square-off-resolution)
5. [Custom adapter correctness for second data](#5-custom-adapter-for-second-data)
6. [The exit engine: one evaluate→act pair](#6-the-exit-engine)
7. [★ Base‑resolution exits (the main feature)](#7--base-resolution-exits)
8. ["Market Exit" — what it means](#8-market-exit-explained)
9. [Verification & test portfolios](#9-verification--test-portfolios)
10. [Files changed / added](#10-files-changed--added)

---

## 1. Load Data — user‑selectable timeframe

**Problem.** Each asset class hard‑coded a single timeframe in
`adapter_admin/data_formats/<class>.json` (`"timeframe": "1-MINUTE"`), so the
catalog could only ever hold 1‑minute data per venue.

**Change.** The timeframe is now chosen on the **Load Data** page (same list as
the Portfolio "base TF" dropdown, **plus `1 sec`**) and flows through to the
catalog write. The per‑asset‑class `timeframe` field was removed.

| Area | File | What changed |
|---|---|---|
| Load Data UI | `static/js/load_data.js` | Added `TIMEFRAMES` list + a **Timeframe** dropdown (default `1-MINUTE`); `doLoad` sends `timeframe` in the `/api/csv/load` body |
| Portfolio UI | `static/js/portfolio.js` | Added `{ label:"1 sec", value:"1-SECOND" }` to `TIMEFRAMES`; `"1-SECOND": 1/60` in `TF_MINUTES` |
| Load endpoint | `server.py` | `/api/csv/load` reads `timeframe`, threads it through `load_csv_and_store` + the background job |
| Loader | `core/nautilus_loader.py` | `load_csv_and_store(..., timeframe=None)`; uses the UI value (falls back to `"1-MINUTE"`) |
| Data formats | `adapter_admin/data_formats/*.json` + admin UI | Removed the now‑unused `timeframe` field |

**Nautilus format verified:** `1-SECOND` is the correct tail —
`BarType.from_str("…-1-SECOND-…-EXTERNAL")` parses, and the composite a strategy
subscribes to (`…-5-MINUTE-…-INTERNAL@1-SECOND-EXTERNAL`) also parses. Unit is
uppercase: `SECOND/MINUTE/HOUR/DAY`.

You can now load, e.g., `nifty_futures_ms` at both 1‑minute and 1‑second; they
coexist as separate bar‑type folders in the catalog.

---

## 2. Index scanner fix

**Symptom.** Scanning `D:\MS\Dataset\Nifty_spot_yyyy` showed "no CSV files",
while `Nifty_futures_yyyy` worked.

**Root cause.** Both folders are structurally identical (3‑deep `YYYY/MM/<file>`,
same columns, same `DD.MM.YYYY` prefix). The index scanner's filename regex
hard‑coded the token `complete_df`:

```python
# core/csv_loader/constants/__init__.py  (BEFORE)
r"^(\d{2})\.(\d{2})\.(\d{4})_complete_df_OHLCV\.csv$"
```

Futures files are `01.01.2025_complete_df_OHLCV.csv` (match); spot files are
`02.03.2026_nifty_spot_OHLCV.csv` (**no** match) → 0 entries.

**Fix.** Generalised the infix so any `DD.MM.YYYY_<infix>_OHLCV.csv` is accepted:

```python
# AFTER
r"^(\d{2})\.(\d{2})\.(\d{4})_.+_OHLCV\.csv$"
```

Result: spot now returns 54 files, futures unchanged at 1376; both synthetic
display names still pass the Index `filename_pattern`. Docs updated in
`core/csv_loader/README.md` and `adapter_admin/data_formats/index.json`.

---

## 3. Timezones & the catalog

This explains the "exits at 11:30" puzzle and how data is physically stored.

### How pandas parses a CSV timestamp

`core/csv_loader/timestamp_parser/__init__.py` decides everything:

| Incoming value | Has offset? | What pandas does | Stored value |
|---|---|---|---|
| `2026-03-02 09:15:00` (naive, NIFTY) | ❌ | `pd.to_datetime(s, utc=True)` **relabels** as UTC — no shift | `09:15:00Z` (IST numbers, fake UTC) |
| `…23:00:00.000 GMT+0000` (FX/commodity) | ✅ +00:00 | offset applied (zero shift) | `23:00:00Z` — true UTC |
| `…09:15:00 GMT+0530` | ✅ +05:30 | **shifted −5:30** | `03:45:00Z` — true UTC |

> Your statement "pandas always converts to UTC" is only half right: **only if the
> source carries a timezone/offset**. A naive timestamp is *relabeled* UTC, not
> converted — the numbers don't move.

### What the catalog physically stores

NautilusTrader stores every bar's `ts_event`/`ts_init` as **int64 nanoseconds
since the Unix epoch** — UTC by definition. There is no per‑bar timezone tag.

For the NIFTY NSE‑open bar the catalog holds the integer `1772442900000000000`:

```
decoded as UTC : 2026-03-02 09:15:00+00:00
decoded as IST : 2026-03-02 14:45:00+05:30
TRUE UTC of 09:15 IST NSE open : 2026-03-02 03:45:00+00:00   ← what it SHOULD be
```

So **the unit is UTC, but the value is the IST wall‑clock** (09:15), because the
+5:30 conversion was never applied at load (the source CSV is naive). **Do not
`tz_convert` NIFTY bars to IST** — you'd get 14:45, which is wrong. Treat the
stored UTC value *as* your IST clock.

### Catalog audit (all 18 bar types)

| Group | Time‑of‑day (UTC) | Verdict |
|---|---|---|
| Crypto ×7 (COINBASE_MS) | `00:00 .. 23:59` | ✅ true UTC (source is UTC, 24/7) |
| FX ×6 (EURUSD/USDJPY FOREX_MS) | `00:00 .. 23:59` | ✅ true UTC (GMT offset applied; week opens Sun 22:00 UTC) |
| Commodity ×3 (LIGHTCMD COMMODITIES_MS) | `00:00 .. 23:59` | ✅ true UTC (`GMT+0000`) |
| **NIFTY spot 1‑sec** | `09:15 .. 15:29` | ❌ IST stored as UTC |
| **NIFTY futures 1‑min** | `09:15 .. 19:14` | ❌ IST stored as UTC (+ a tail past 15:30 worth checking) |

**16/18 are genuine UTC; the 2 NIFTY datasets are IST‑stored‑as‑UTC.**

### The 11:30 square‑off artifact

`squareoff_time:"17:00"` + `squareoff_tz:"Asia/Kolkata"` is the *only* setting
that applies a real tz conversion (`managed_strategy.py` converts the bar's UTC
ts → Asia/Kolkata before comparing). With IST‑as‑UTC bars, `17:00 IST → 11:30
UTC`, so the square‑off fired at 11:30 in the data clock — 5.5 h "early". The
entry window, by contrast, is compared **naively in UTC**, so 09:30 lined up
correctly by accident.

**Practical rule for NIFTY:** keep `squareoff_tz = UTC` and put **IST clock
numbers** in the time field (e.g. `15:25` = 15:25 IST in the data). A `17:00`
square‑off can never fire on NSE data anyway (the session ends 15:30).

---

## 4. Square‑off resolution

Precedence is **leg > slot > portfolio** (`core/models/squareoff/__init__.py`),
with a product‑type default: explicit `portfolio.squareoff_time` wins; else if
`product=="MIS"`, `mis_squareoff_time`; else none. `_parse_squareoff_minute`
returns **−1 (disabled)** when the time is `None`/empty.

**To disable square‑off:** clear the **SqOff Time** field on the Timing tab (it's
two‑way‑linked with the MIS field, so both clear) → `squareoff_time` becomes
`null`. Or set `"squareoff_time": null` in the JSON.

> ⚠️ With SL, Target *and* square‑off all off, the only thing that closes the
> position is the end‑of‑backtest flatten → a single **"Market Exit"** (see §8).

---

## 5. Custom adapter for second data

The custom venue config (`adapter_admin/adapters_config/nifty_futures_ms.json`,
`account_base_currency: INR`, session window, empty `fx_conversion`) is applied
**identically for 1‑second and 1‑minute** data:

- **Venue parsing is timeframe‑independent** — `venue_from_bar_type` splits on the
  instrument‑id part only (`core/venue_config.py`). `…-1-SECOND-…` and
  `…-1-MINUTE-…` resolve the same venue.
- **FX/PnL conversion** uses `pandas.asof()` — works at any resolution.
- **Session‑window derivation** uses `HH:MM:SS` — second‑precise.

Two caveats (neither blocks second data):
- **Entry‑window & square‑off time matching is minute‑granular** (`_NANOS_PER_MINUTE`
  / `_squareoff_min`). Whole‑minute config is exact; sub‑minute seconds in those
  *config fields* are dropped.
- **The engine account currency is hard‑coded `USD`** in `add_venue`, but PnL is
  read per‑position in the instrument's own currency (INR) and converted by the
  resolver (base INR, empty rules → identity) → reported in INR. This is the same
  for all timeframes, so it's not a second‑data issue.

---

## 6. The exit engine

There isn't one single function for *every* exit, but exits funnel through a
small, well‑defined set in `core/managed_strategy.py`:

```
_on_primary_bar()                       per‑bar orchestrator
   ├─ square‑off check ─→ _force_squareoff()      (time exit, plain close)
   └─ _check_exits()  ─→ _handle_exit()           (price exits + actions)
```

- **`_check_exits`** is the single evaluator for **all price‑based exits**: highest‑
  profit, move‑SL‑to‑cost, trailing‑SL, target‑lock, SL trigger, TP trigger.
- **`_handle_exit(exit_type, was_long, close)`** is the single dispatcher for what
  happens after SL/TP fires: parses `on_sl_action`/`on_target_action` and runs
  **close / re_execute / reverse / execute / re_entry**.
- **Square‑off** is deliberately separate (`_force_squareoff`) so it's always a
  plain close — it never accidentally re_executes or reverses.

---

## 7. ★ Base‑resolution exits

### The problem

In aggregating mode the old `on_bar` fed each base bar to the aggregator and only
acted on the **emitted** aggregated bar:

```python
# BEFORE — core/managed_strategy.py
if self._aggregating:
    agg = self._primary_agg.on_bar(bar)
    if agg is None:
        return                 # ← every mid‑window base bar bailed: no exit check
    self._feed_indicators(agg)
    self._on_primary_bar(agg)  # ← signal AND exits both on the 5‑min bar
```

So SL/Target/Trailing/Square‑off were only evaluated once per 5‑minute window →
all exit timestamps snapped to 5‑minute boundaries.

### The fix (one file: `core/managed_strategy.py`)

`_on_primary_bar` is **left untouched** (so the non‑aggregating path is byte‑for‑
byte identical). Two helpers split its responsibilities, used **only** by the
aggregating branch:

- **`_manage_open_position(bar, bid, ask)`** — runs on **every base bar**:
  mark‑price roll, square‑off, and SL/Target/Trailing via the **unchanged**
  `_check_exits` / `_force_squareoff`.
- **`_evaluate_entry(agg_bar)`** — runs only on **window close**: RBO, readiness
  gate, and the **unchanged** `_check_entries`.

```python
# AFTER — Format B/C (ohlcv/ltp/mark)
if self._aggregating:
    if self._exit_fmt != "bidask":
        self._manage_open_position(bar)          # exits on EVERY base bar
        agg = self._primary_agg.on_bar(bar)
        if agg is not None:
            self._feed_indicators(agg)
            self._evaluate_entry(agg)            # signal/entry on window close
    else:
        self._on_bar_aggregating_bidask(bar)     # Format A, reworked
    return
```

**Format A (bid/ask)** was reworked to buffer the **base** (primary, bid, ask)
trio for exits (reusing the no‑aggregation trio logic) and use the aggregated
primary for entries — so the 5‑minute bid/ask aggregators were removed and
`on_start` simplified.

### Why portfolio SL/TP needed no change

Portfolio SL/Target is a **post‑run equity‑curve clip** (`portfolio_clip`) keyed
off each leg's position‑close timestamps. Once leg exits are base‑granular, the
clip automatically becomes base‑granular too.

### Deliberate scoping / behavior notes

- **RBO stays on the aggregated (signal) bar** — it's entry‑side logic.
- **Mark‑price `_prev_close` now rolls per base bar** (the exit‑trigger reference).
- On the boundary base bar an exit and a fresh entry can both occur in one
  `on_bar`; existing re‑entry guards govern this.
- **Performance:** exit checks now run ~300× more often (per second vs per 5 min);
  cheap, indicators still feed only on emission.

### What is explicitly NOT changed

`_check_exits`, `_handle_exit`, `_force_squareoff`, `_check_entries`, every
SL/TP/trailing/target‑lock/move‑to‑cost/square‑off parameter, the portfolio‑clip
code, and the entire non‑aggregating path.

---

## 8. "Market Exit" explained

`core/report_generator.py::_determine_reason` derives the exit reason as
**tags > order type**. A close *with* a reason tag shows "Stop Loss" / "Take
Profit" / "Trailing SL" / "Squareoff" / "Reverse on SL". A close with **no tag**
falls back to the order type → a reduce‑only `MARKET` order becomes
**"Market Exit"**.

There are only four ways a position closes:

| Path | Tag |
|---|---|
| SL / Target / Trailing hit | "Stop Loss" / "Take Profit" / "Trailing SL" |
| Square‑off time | "Squareoff" |
| Reverse action on SL/TP | "Reverse on SL/TP" |
| **End of backtest** (`on_stop` flatten) | **(untagged → "Market Exit")** |

**Why Portfolio 30 still showed 5‑minute exits:** at the time it had **no SL,
no Target, no square‑off**, and the EMA Cross signal only *opens* when flat (it
does not flip on the opposite cross). So the only close is the end‑of‑run
flatten → a single untagged **"Market Exit"**. The base‑resolution change moves
**SL/Target/Trailing/Square‑off** to the base bar — Portfolio 30 had none of
those enabled, so there was nothing to relocate. **Add an SL/Target/square‑off
to see 1‑second exits.**

> Also: the running Flask server caches the imported module — **restart
> `python server.py`** to pick up the `managed_strategy.py` change.

---

## 9. Verification & test portfolios

### Regression checks (all green)
- `pytest tests/` → **253 passed** (aggregator, three‑format engine, strategy‑
  subscribe‑tf, leg‑target).
- `tests/run_agg_mis_nrml_tests.py` → **4/4 PASS** (aggregation coarseness, the
  reworked Format‑A bid/ask path, MIS/NRML square‑off).

### Direct proof (EURUSD 5‑min signal / 1‑min base, tight SL/TP)

| Fill type | Count | Off the 5‑min grid |
|---|---|---|
| EMA Cross BUY/SELL (entries) | 428 | **0** — on the signal grid ✓ |
| Stop Loss (exits) | 266 | **211** — base resolution ✓ |
| Take Profit (exits) | 162 | **120** — base resolution ✓ |

### Base‑resolution suite — `tests/base_exit_resolution_suite.py` → **11/11 PASS**

Generates these runnable portfolios into `portfolios/_default/` (NIFTY 1‑sec base
unless noted) and asserts entries‑on‑signal‑grid + exits‑at‑base‑resolution:

| Portfolio | Covers | Exits on‑grid (lower better) |
|---|---|---|
| `BXT_01_sl_tgt_5m` | SL 0.1% + Target 0.15% | 0.02 |
| `BXT_02_squareoff_5m` | Square‑off @ **11:02** (off the grid) | 0.00 |
| `BXT_03_trailing_5m` | Trailing SL | 0.00 |
| `BXT_04_sl_reexecute_5m` | `on_sl_action=re_execute` ×3 | 0.09 |
| `BXT_05_sl_reverse_5m` | `on_sl_action=reverse` | 0.00 |
| `BXT_06_atr_sl_5m` | ATR SL + ATR target | 0.11 |
| `BXT_07_ltp_5m` | LTP exit format (Format C) | 0.02 |
| `BXT_08_agg_1hour` | 1‑HOUR signal / 1‑sec base | 0.00 |
| `BXT_09_noagg_control` | No aggregation (base == signal) | n/a — 95% have seconds |
| `BXT_10_pf_sl_tgt_5m` | **Portfolio** SL + Target | 0.02 (pf‑clip fired) |
| `BXT_11_format_a_bidask_fx` | **Format‑A bid/ask** (EURUSD 1‑min base) | 0.25 |
| `Base Exit Test` | SL + Target (hand‑made) | — |

Sample exit timestamps (NIFTY 1‑sec base, 5‑min signal) — note the seconds:
```
Take Profit -> 2026-03-02 11:20:06
Take Profit -> 2026-03-02 11:59:22
Stop Loss   -> 2026-03-02 14:03:26
Stop Loss   -> 2026-03-02 14:05:14
```

**How to check in the UI:** restart the server, open any `BXT_` portfolio, run it,
and read the order‑book **EXIT TIME** column — SL/Target/Trailing/Square‑off/
Reverse rows show real seconds while ENTRY TIME stays on the signal grid.

---

## 10. Files changed / added

**Feature — base‑resolution exits**
- `core/managed_strategy.py` — split helpers (`_manage_open_position`,
  `_evaluate_entry`); rewired aggregating Format B/C branch; reworked
  `_on_bar_aggregating_bidask`; simplified `on_start` (dropped bid/ask
  aggregators; removed dead `_collect_agg`/`_drain_agg_pending`).

**Feature — Load Data timeframe**
- `static/js/load_data.js`, `static/js/portfolio.js`, `server.py`,
  `core/nautilus_loader.py`, `adapter_admin/data_formats/*.json`,
  `adapter_admin/static/js/data_formats.js`, `adapter_admin/admin_server.py`.

**Fix — index scanner**
- `core/csv_loader/constants/__init__.py`, `core/csv_loader/README.md`,
  `adapter_admin/data_formats/index.json`.

**Tests / portfolios added**
- `tests/base_exit_resolution_suite.py` (generator + verifier)
- `portfolios/_default/BXT_01..11_*.json`, `portfolios/_default/Base Exit Test.json`

**Known stale harnesses (pre‑existing, unrelated)**
- `verify_session_changes.py` crashes on a missing `core/backtest_runner.py`
  (the runner was long ago split into a package); it never reads
  `managed_strategy.py`.
