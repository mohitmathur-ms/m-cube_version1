# SL/Target Testing Process — Artifacts & Verification

End-to-end process for testing **our** SL/Target engine. The **artifact formats** (config reference, orderbook, logs, report) are modelled on a real production run so the engine emits proven, verifiable outputs. The **design and methodology stay ours**: futures + equity scope, our test portfolios, the **Time / Logic / Price** verification, and our locked decisions (limit orders, product modes MIS/NRML/CNC, step-based trailing, `curr_leg_pnl` anchor).

> **Inspiration vs ours.** *Borrowed format:* the column/layout shapes of the orderbook, logs, portfolio reference, and report. *Kept ours:* what we test (futures features), how we verify (time/logic/price + manual traces), and every spec decision we made.

## Document set — read in this order

**This is the entry point.** The testing work spans five docs:

1. **`sl_tgt_testing_process`** (this) — the process + artifact formats (config reference, orderbook, logs, report).
2. **`sl_tgt_test_portfolios`** — the test portfolios + per-feature **manual traces** (what to test, expected answers).
3. **`product_modes_limit_orders`** — the **order / fill model** + MIS / NRML / CNC behaviour.
4. **`manual_verification_process`** — the **Time / Logic / Price** check, step by step against the orderbook.
5. **`test_strategies_catalog`** — the **EMA-cross harness** to make the portfolios runnable on real data.

---

## 1. The pipeline

```
Define test portfolios  →  Run engine  →  Emit Orderbook + Logs  →  Verify (Time/Logic/Price)  →  Result/Report  →  Freeze regression baseline  →  Sign-off
        (you/oracle)          (dev)            (dev)                      (you)                     (dev)               (you)                  (you)
```

- **Dev produces:** the run, the orderbook, the logs, the summary report.
- **You verify:** recompute expected Time/Logic/Price from the config and check the artifacts. Evidence before assertions — never accept "it works."

---

## 2. Artifact A — Test-portfolio config reference *(the oracle)*

Lay our test portfolios out in a **quick-reference table + feature matrix + per-PF cards** (format borrowed from the real reference doc). These are our **futures/equity** portfolios (from `sl_tgt_test_portfolios` / `test_strategies_catalog`), each engineered to isolate features:

| PF | Level | Instrument | Legs | Target | Stoploss | Mode | Cross-PF | Feature focus |
|---|---|---|---|---|---|---|---|---|
| PF-1L | Leg | NIFTY-FUT | 1 long | Premium % | Premium % | NRML | — | leg SL/TGT trigger + fill |
| PF-1S | Leg | 6E | 1 short | — | Points | NRML | — | short-side SL |
| PF-ATR | Leg | BTC | 1 long | — | ATR (mult) | NRML | — | ATR SL sizing |
| PF-WAIT | Leg | NIFTY-FUT | 1 long | — | SL + SL-Wait | NRML | — | delay/wait timing |
| PF-2 | Leg | GC | 2 legs | — | Move-SL-to-Cost + Trail-After | NRML | — | move-to-cost + trail (`curr_leg_pnl`) |
| PF-3 | Portfolio | BANKNIFTY-FUT | 3 legs | Combined Profit + Trailing Target | Combined Loss + Trailing SL | NRML | — | portfolio SL/TGT, trailing, breach |
| PF-U | User | NIFTY-FUT + 6E | 2 portfolios | — | Max Loss / Max Profit | NRML | user cascade | user level |
| PF-MIS | Mode | NIFTY-FUT | 1 | — | SL | **MIS** | — | intraday cutoff square-off |
| PF-CNC | Mode | RELIANCE (equity) | 1 | — | SL | **CNC** | — | equity delivery carry |
| PF-XPF | Portfolio | NIFTY-FUT → 6E | 1+1 | SqOff/Execute/Start Other | — | NRML | → target PF | cross-portfolio actions |

Each PF also gets a **card** (full config: type, value, action, delay, trail params, product mode) and an entry in a **feature-coverage matrix** (feature × PF, ✓/—) — same layout as the real reference, so coverage gaps are visible at a glance.

### Run dimensions — every PF runs across timeframes (and assets)

A test portfolio is **not one run** — it's run across **timeframes** and, where relevant, **asset classes**. The SL/Target logic is identical, but these change per cell and must be re-verified:

**Timeframes: 1m · 3m · 5m · 2h · 1d · 1w · 1M.** Where exact intrabar ordering must be settled, drop to **tick / 1-sec** as the finest resolution (see "Intrabar ordering" below).

| Timeframe-specific check | What to verify |
|---|---|
| **ATR period (in bars)** | `ATR(N)` spans N **bars** → `ATR(14)` = 14 min on 1m, 70 min on 5m, 28 h on 2h, 14 days on 1d, 14 months on 1M. The **same `(period, mult)` gives a different SL distance** per timeframe. Recompute per cell — don't assume cross-timeframe equality. |
| **SL-Wait / delay units** | `wait` in **bars** scales with bar size (2 bars = 2 min on 1m, 10 min on 5m, 2 days on 1d); `sl_wait_sec` is **wall-clock** (timeframe-independent). |
| **Safety Seconds vs bar size** | Safety < bar size (e.g. 30 s on a 1m bar) → engine **refuses + warns**; ≥ bar size → applies at trigger-event + safety. |
| **Trigger granularity** | finer timeframes pierce the level on smaller bars → trigger **timestamp differs**; the level/price is identical. |
| **Intrabar ordering** | all seven are coarse bars → use the high-then-low convention when a bar both makes a new high and breaches; only **tick / 1-sec** data (the finest resolution) resolves the order exactly. |
| **Tick snapping** | timeframe-independent — depends only on the instrument. |

**Assets:** test each type across **different asset classes** — re-check the per-asset specifics (tick size, lot multiplier, currency) per cell.

**Sweep policy by level.** Sweep a dimension only where the logic *interacts* with it — a full level × asset × timeframe cross would be thousands of runs that re-prove nothing. The leg fixtures get the full sweep because tick-snap, lot multiplier, currency and ATR-in-bars all bite at the atomic SL; higher levels inherit that and add only their own interactions:

| Level | What to sweep | Asset coverage | Timeframe coverage | Why these dimensions bite |
|---|---|---|---|---|
| **Leg** | the SL types (%, points, ATR) + SL-Wait | **all 7 assets** | **all 7 (1m…1M)** + tick/1-sec for intrabar ordering | tick-snap, lot-mult, currency, ATR-in-bars all bite at the atomic SL; wait-units scale with bar size |
| **Portfolio** | dynamic — Trailing SL, Profit-Lock, Move-to-Cost, Delay; static — Combined Loss/Profit | **1 INR + 1 USD** asset (exercises currency inside Combined PnL) | dynamic: **all 7**; static: 1m + a coarse-TF spot-check (e.g. 1d) | step ratchet & profit-lock floor fire on bar boundaries → the step bar shifts per timeframe; Combined PnL aggregates across multiplier + currency |
| **User** | Max Loss/Profit + User Trailing | **cross-currency cascade** (INR PF + USD PF) + an all-INR baseline | 1m + a coarse-TF spot-check on the trailing cases | the user roll-up normalises both books to a base currency; same-bar User→Portfolio→Leg precedence depends on which bar |
| **Cross-PF** | the dispatch pair | as configured | 1m + a coarse TF for the cross-PF delay (2 s/3 s) | the cross-PF delay is wall-clock → re-check against bar size |
| **Product mode** | MIS, NRML, CNC | MIS on a future, CNC on equity | any one TF (e.g. 1m) | square-off cutoff & delivery carry are clock/calendar logic, not bar-size sensitive |

The runnable per-case version (with the exact TL/TP/TU/TX case IDs) is in `sl_tgt_test_portfolios` §7.

**Why not sweep everything.** A *cell* = one (case × asset × timeframe) run, and each cell costs a **manual Time/Logic/Price walk** — that hand-reconciliation, not CPU, is the budget. Sweep a dimension only where the *logic* changes with it: at the **leg** the SL number itself moves (tick size, lot multiplier, currency, ATR-in-bars) so the full 7×7 is real; **above the leg** the core arithmetic (Σ leg PnL vs a threshold) is scale- and timeframe-free, so re-running it across 49 cells just re-proves the same sum. Targeting the cells that bite (**≈ 240**) versus a blind full cross (**≈ 2,250**) is roughly a **10× difference in manual effort for zero extra bug coverage**. Widen surgically only when a specific interaction is suspected — characterising a bug across timeframes, possible cross-currency rounding, or a wall-clock timer used where bars were intended.

---

## 3. Artifact B — Orderbook *(the primary evidence)*

Column schema adapted from the real orderbook, **futures/equity-ised** and extended with the columns our verification needs:

| Column | Source | Use |
|---|---|---|
| `USERID` `SYMBOL` `EXCHANGE` `TRANSACTION` `QUANTITY` `LOTS` `MULTIPLIER` | real | identity + PnL math (lot × mult) |
| `OrderID` (with `_exec.reentry` suffix, e.g. `pf3_1.2`) · `_PARENT_ID` | real | trace re-executions / re-entries |
| `ENTRY TIME` · `ENTRY PRICE` · `ENTRY REASON` | real | when/why a position opened |
| `EXIT TIME` · `AVG EXIT PRICE` · `EXIT REASON` · `PNL` | real | when/why/at-what-price it closed |
| `PORTFOLIO NAME` · `STRATEGY` · `PRODUCT_MODE` | real (+mode) | which PF + MIS/NRML/CNC |
| **`LEVEL_PRICE` · `CURRENT_SL` · `CURRENT_TGT`** | **ours** | the SL/Target level at the event — for the **Price** check |
| **`MOVE_SL_ANCHOR` · `ANCHOR_PNL`** | **ours** | trailing/move-to-cost state — to re-walk the ratchet |
| **`COMBINED_PNL` · `UNDERLYING`** | **ours** | portfolio/underlying thresholds |
| **`BAR_O/H/L/C`** (or bid/ask/ltp) | **ours** | the bar that caused the row — for the **Time** + reach check |

**Reason vocabulary** (borrow + extend):
- *Entry reasons:* `Start time entry` · `Portfolio re-executed due to TARGET/SL` · `ReEntry at original price` · `Triggered by <PF> TARGET/STOPLOSS` · `Started by <PF>`.
- *Exit reasons:* `SL HIT` · `TARGET HIT` · `COMBINED LOSS/PROFIT HIT` · `TRAIL_SL` · `TARGET_TRAIL` · `MOVE_TO_COST` · `UNDERLYING MOVEMENT HIT` · `LOSS AND RANGE HIT` · `MIS_CUTOFF` · `SqOff Time`.

---

## 4. Artifact C — Logs *(per-event timing)*

Schema from the real logs — the **`Backtest_Timestamp`** is what makes precise timing verifiable:

| Column | Use |
|---|---|
| `Timestamp` (wall) · `Backtest_Timestamp` (sim) | the time axis for every event |
| `Log Type` (TRADING / MESSAGE) | filter trades vs system messages |
| `Message` | the event + reason in words (ENTRY/EXIT/trigger/delay/reexec/winter-time/global-limits) |
| `UserID` · `Strategy Tag` · `Portfolio` | which entity |

The logs carry things the orderbook can't: **delay countdowns, trail-step updates, "Triggered by" dispatch, Winter-Time adjustments, Global-limit status, DELAY_CLEARED**. They're how you settle the **Time** axis (e.g. did the 2s cross-PF delay actually elapse; did the SL fire on the *first* qualifying second).

---

## 5. Artifact D — Result / Report *(summary)*

Per-PF roll-up (format borrowed): trades, hits-by-feature (e.g. "trailing-target hits = N"), PnL, win/loss, and a **feature-fired tally** so you can confirm every targeted feature actually triggered at least once in the run.

---

## 6. Verification — Time / Logic / Price *(our methodology)*

Apply the **3-axis check** (full detail in `manual_verification_process`) to every event row, recomputing from the config:

- **TIME** — event `Backtest_Timestamp` == first bar the condition is true (±wait/±safety); same-bar precedence User→Portfolio→Leg.
- **LOGIC** — `EXIT REASON` matches the condition actually true; correct action (SqOff / ReExecute / ReEntry / cross-PF); guards respected (trailing only-tightens, lock only-rises, re-exec count).
- **PRICE** — recompute the level from `ENTRY PRICE` + config (tick-snapped); it must equal `CURRENT_SL`/`LEVEL_PRICE`; `PNL = (exit−entry) × MULTIPLIER × LOTS`; fill per the model (limit-level / VWAP — *pending the parked fill question*).

**Loop:** run → export orderbook+logs → sort by `Backtest_Timestamp` → 3-axis check each row → green/red (red carries the row). For dynamic features, walk the rows in sequence so the state (`CURRENT_SL`, anchors) evolves exactly as your manual trace says.

---

## 7. Sign-off + regression

- Every feature in the §2 matrix has a green Time/Logic/Price result.
- Cross-PF actions verified end-to-end (source trigger → target action, with delay).
- Once green and hand-reconciled, **freeze the orderbook as a regression baseline** (one per asset class / mode) — re-run on every code change; any drift = fail.

---

### What carried over from the real run (format only)
Orderbook columns · logs `Backtest_Timestamp` + Log Type/Message · reason strings · the reference table + feature matrix + cards layout · the report roll-up.

### What stayed ours (everything that matters)
Futures + equity scope · the test portfolios and feature cases · Time/Logic/Price verification + manual traces · limit-order / product-mode / step-trailing / `curr_leg_pnl` decisions · the parked fill-price + gap questions.
