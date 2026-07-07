# Leg-Level SL — Test Scenarios

Scope: **leg-level stop-loss only**, the four types in the backend report — `percentage` · `points` · `atr` · `trailing`. Oracle = the implementation's own formulas and fields (`stop_loss_type`, `stop_loss_value`, `sl_atr_period`, `sl_atr_multiplier`, `trailing_sl_step`, `trailing_sl_offset`). Values below are **tick-snapped** (the engine snaps every SL to the instrument grid).

Baseline for the formula fixtures: entry `100.00`, tick `0.01`. The asset/timeframe matrix (§5) re-runs them on real instruments.

---

## 1. SL-type golden fixtures (compute + trigger)

| ID | Type | Dir | Config | Computed SL | Trigger test | Fires on bar |
|---|---|---|---|---|---|---|
| LS-PCT-01 | percentage | LONG | value = 2 | `100×(1−0.02) = 98.00` | `bar_low ≤ 98.00` | low 97.96 → hit |
| LS-PCT-02 | percentage | SHORT | value = 2 | `100×(1+0.02) = 102.00` | `bar_high ≥ 102.00` | high 102.05 → hit |
| LS-PTS-01 | points | LONG | value = 2.50 | `100 − 2.50 = 97.50` | `bar_low ≤ 97.50` | low 97.40 → hit |
| LS-PTS-02 | points | SHORT | value = 2.50 | `100 + 2.50 = 102.50` | `bar_high ≥ 102.50` | high 102.60 → hit |
| LS-ATR-01 | atr | LONG | period 14, mult 1.5, ATR=0.80 | `100 − (1.5×0.80) = 98.80` | `bar_low ≤ 98.80` | low 98.75 → hit |
| LS-ATR-02 | atr | SHORT | period 14, mult 1.5, ATR=0.80 | `100 + 1.20 = 101.20` | `bar_high ≥ 101.20` | high 101.25 → hit |
| LS-ATR-03 | atr | LONG | ATR not initialised (warm-up) | `dist=0 → current_sl = 0` | **no SL armed** | never fires |
| LS-NONE | none | — | — | no SL | — | leg never stopped |

**Assert for each:** computed `current_sl` exactly equals the value shown (after tick snap), trigger uses the correct side (`low` for long, `high` for short), and `_handle_exit("sl")` fires on the first piercing bar — not on the close.

---

## 2. ATR multiplier sweep

ATR alone is just a volatility number — the **multiplier** sets the distance (`dist = mult × ATR`). Verify the SL scales linearly with the multiplier and with the ATR regime. LONG, entry 100:

| ATR at entry ↓ / multiplier → | 1.0 | 1.5 | 2.0 | 3.0 |
|---|---|---|---|---|
| 0.40 (calm) | 99.60 | 99.40 | 99.20 | 98.80 |
| 0.80 (normal) | 99.20 | 98.80 | 98.40 | 97.60 |
| 1.60 (volatile) | 98.40 | 97.60 | 96.80 | 95.20 |

**Assert:** each cell = `100 − (ATR × mult)`, tick-snapped. Higher multiplier and/or higher ATR → wider stop. (SHORT mirrors: `100 + ATR×mult`.) Also confirm: **mult = 0 or ATR = 0 ⇒ no SL armed** (`current_sl = 0`).

---

## 3. Trailing (leg) — step-based per the final spec

Leg-level trailing is **Move SL to Cost + Trail After Move SL** (PnL / step-based) — see `trailing_final_report` / `trailing_spec`. Concrete fixtures live in **§5**: **MC-01…MC-03** (Move SL to Cost, 3 variants) and **TM-01** (Trail After Move SL ratchet).

The implementation's **standalone `trailing` SL type** (percentage step off entry, `trailing_sl_step` / `trailing_sl_offset`) is **not in the spec → being removed** (it's also the buggy one, see `trailing_sl_bug_report.md`). **No standalone price-trailing ("Way 2") leg stop is adopted.**

---

## 4. Product type — MIS vs NRML (each SL type)

The SL **computation and intrabar trigger are product-agnostic** — identical under MIS and NRML. The difference is end-of-session handling.

| ID | Product | Scenario | Expected |
|---|---|---|---|
| PR-MIS-01 | MIS | SL **not** hit by the intraday cutoff (e.g. 15:20 IST NSE; session close for CME) | Leg **force-closed at the cutoff** at market, regardless of SL |
| PR-MIS-02 | MIS | SL hit at 13:00 (before cutoff) | Normal SL exit at 13:00; cutoff never reached |
| PR-NRML-01 | NRML | SL not hit | **No** intraday forced close; position carries to next session / expiry |
| PR-NRML-02 | NRML | SL hit | Normal SL exit; identical fill to PR-MIS-02 |

**Cross-check:** run PR-MIS-02 and PR-NRML-02 with the *same* SL config and bars → the SL trigger bar and fill must be **identical** (proves SL logic doesn't depend on product). Run all four for each of `percentage`/`points`/`atr`/`trailing`.

> **Dependency:** product type (MIS/NRML auto-square-off cutoff) is not yet in the spec — tracked as **TBD-7**. PR-MIS-01 can only pass once the engine has product-type + cutoff handling. PR-NRML / SL-trigger parts are testable now.

---

## 5. Asset-class × timeframe matrix

Run LS-PCT / LS-PTS / LS-ATR in **every cell** = every asset × every timeframe. **Timeframes: 1m · 3m · 5m · 2h · 1d · 1w · 1M** (drop to tick/1-sec where exact intrabar ordering must be settled). The SL logic is the same; what changes per cell is tick snapping, lot multiplier, currency, and (for ATR) the lookback window.

| Asset | tick | lot/mult | ccy |
|---|---|---|---|
| NIFTY-FUT | 0.05 | 65 | INR |
| BANKNIFTY-FUT | 0.05 | 25 | INR |
| US index futures (E-mini S&P) | 0.25 | $50 | USD |
| 6E (EUR/USD) | 0.00005 | 125,000 | USD |
| GC (Gold) | 0.10 | 100 | USD |
| BTC | 5 | 5 | USD |

**Per-asset tick-snap checks** (percentage 2%, LONG — shows the snap):

| Asset | raw `entry×0.98` | snapped SL |
|---|---|---|
| NIFTY-FUT, entry 22001 | 21560.98 | **21561.00** |
| 6E, entry 1.10003 | 1.0780294 | **1.07805** |
| GC, entry 1950.07 | 1911.0686 | **1911.10** |
| BTC, entry 60003 | 58802.94 | **58805** |

**Per-asset asserts:** PnL = `(exit − entry) × lot_mult × n_lots` (NIFTY 65, BANKNIFTY 25, 6E 125,000, GC 100, BTC 5); currency INR for NSE futures, USD for the rest.

**Per-timeframe asserts:**
- **ATR is in *bars*** — `ATR(14)` spans 14 bars → 14 min on 1m, 70 min on 5m, 28 h on 2h, 14 days on 1d, 14 months on 1M. So the **same `(period, mult)` gives different SL distances per timeframe** — verify the distance is recomputed from the active bar series, never assumed equal across timeframes.
- **SL-wait:** `sl_wait_sec` is wall-clock (timeframe-independent); `sl_wait_bars` scales with bar size (2 bars = 2 min on 1m, 10 min on 5m, 2 days on 1d).
- **Trigger granularity:** finer timeframes pierce the level on smaller bars → trigger bar/timestamp differs, but the SL price is identical.
- **Tick snap is timeframe-independent** — depends only on the instrument.

---

## 6. Common mechanics (all four types)

| Check | Expected |
|---|---|
| Tick snap | every computed SL = `round(price/tick) × tick`; off-grid raw values snap (e.g. 108.54 → 108.55 at tick 0.05) |
| Intrabar trigger | LONG fires on `bar_low ≤ SL`, SHORT on `bar_high ≥ SL` — the bar **low/high**, not the close |
| SL-wait gate | with `sl_wait_sec` (preferred) / `sl_wait_bars`, exit is suppressed until the level holds that long; if price recovers first, no exit |
| No-SL guard | `current_sl = 0` ⇒ leg is never stopped (covers ATR warm-up / none) |

---

## 7. How to run as a test strategy (EMA-cross harness)

Use a trivial EMA/MA crossover purely as the **entry trigger** so the strategy takes real trades; then check the SL against each realised fill. You no longer control the exact entry price, so verify **relative to the fill**:

```
expected_sl = formula(fill_price, sl_config)   then snap to instrument tick
assert  engine.current_sl == expected_sl
```
Per type: percentage `fill × (1 ∓ v/100)` · points `fill ∓ v` · atr `fill ∓ (mult × ATR_at_entry)`.

**Example.** EMA(9)/EMA(21) on NIFTY-FUT 1-min, long on bullish cross, `percentage` SL value 2. Trade fills at `22037.40` → expected SL `= 22037.40 × 0.98 = 21596.652` → snap 0.05 → **21596.65**. Assert engine SL = 21596.65; the first bar with `low ≤ 21596.65` must exit at that level. Repeat the check on **every** trade the strategy generates.

### A. Ask the developer team to — build + instrument the harness

1. **Provide a minimal deterministic test strategy:** EMA(fast)/EMA(slow) crossover — long on bullish cross, short on bearish — single leg, with the leg SL block (`stop_loss_type`, `stop_loss_value`, `sl_atr_period`, `sl_atr_multiplier`, `trailing_sl_step`, `trailing_sl_offset`) fully configurable.
2. **Parameterise it** for instrument, bar timeframe, product type (MIS / NRML), and SL config — so one harness runs every cell of the matrix.
3. **Log per entry:** fill price, timestamp, direction, and — for `atr` — the ATR value used at entry; for `trailing` — the per-bar `current_sl` updates.
4. **Log per exit:** exit reason (`sl` / `target` / `eod` / `cutoff`), exit price, timestamp, and `current_sl` at exit.
5. **Emit a machine-readable orderbook** (CSV / JSON), one row per fill/exit, so checks can be automated.
6. **State the tick** used for snapping per instrument, and the **lot multiplier + currency** used for PnL.
7. **Confirm whether product type / MIS cutoff is implemented** (TBD-7). If not, mark MIS scenarios blocked.

### B. Then verify yourself — only the orderbook needed, no engine internals

1. **One SL type at a time.** Run the harness with `percentage`, value 2, one instrument, one timeframe.
2. **Recompute every SL.** For each entry, apply the type formula to the logged fill, snap to tick → your expected SL. Compare to the engine's logged `current_sl`. Must match to the tick.
3. **Check the trigger** on `sl` exits: the exit bar's low (long) / high (short) pierced the SL, and exit price = the SL level.
4. **Repeat for `points` and `atr`.** For ATR, use the logged ATR-at-entry: expected = `fill ∓ (mult × ATR)`.
5. **Trailing — trace it bar by bar.** `current_sl` should ratchet up and lock profit; if it freezes after the first step, you've reproduced the trailing bug.
6. **Sweep the matrix.** Re-run across NIFTY-FUT / BANKNIFTY-FUT / US index fut / 6E / GC / BTC × 1m / 3m / 5m / 2h / 1d / 1w / 1M. Spot-check tick snap, `PnL = move × lot-mult × n_lots`, currency, and that ATR distance changes with timeframe.
7. **MIS vs NRML.** Same config, both products: the SL trades must match exactly; under MIS, any position open at the cutoff must show a `cutoff` exit (pending TBD-7).
8. **Record results** against the §1–§6 fixtures — green where expected SL = engine SL and triggers line up; red (with the offending trade) where they don't.

> The split: the dev team hands you a **parameterised EMA harness + a logged orderbook**; you verify purely from that orderbook — no need to read engine code.

---

### Notes
- **Tick snapping** here follows the *implementation* (it snaps). This contradicts TBD-2 (which said keep raw) — flagged separately; align before locking expected values that hinge on a snapped vs raw tick.
- All fixtures are leg-level. Move-SL-to-Cost, profit-lock, and portfolio/user levels are out of scope for this set.
