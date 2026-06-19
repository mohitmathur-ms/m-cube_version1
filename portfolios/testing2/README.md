# Test Portfolios — Manual-Verification Set v2 (VP-01 … VP-12, OT-01 … OT-06)

Runnable test portfolios that exercise each SL/Target feature **and** each
order-type concept **end-to-end on real data**, built for the
**Time · Logic · Price** verification loop in
[`SL_TGT_Testing/new_tests/manual_verification_v2.html`](../../SL_TGT_Testing/new_tests/manual_verification_v2.html).
EMA-cross is the deterministic entry trigger. Because you don't control the
exact fill, every check is verified **relative to the realised fill**.

Regenerate any time with:
```powershell
venv\Scripts\python.exe portfolios\testing2\_generate_portfolios.py
```

## Design rules baked into every file (the test brief)

1. **Indian-market / MIS framing.** Every file is `product = "MIS"` (intraday
   trade type) with a **hard square-off at the NSE close, 15:29 IST**
   (`squareoff_time = "15:29:00"`, `squareoff_tz = "Asia/Kolkata"`;
   `mis_squareoff_time` set to match). The NIFTY session is **09:15–15:30 IST**
   (= 03:45–10:00 UTC). The intra-day entry window is left **open**
   (`entry_start_time = entry_end_time = null`) — the session already bounds the
   data, so no bars are dropped and exits can fire any time within the session.
2. **Why 15:29 and not 15:30 — read this.** The strategy fires the square-off on
   the first bar whose **local (IST) time ≥ the cutoff minute**
   (`core/managed_strategy.py`). The NSE close is 15:30 IST, but this 1-second
   feed's last bar each day is **15:29:59 IST** (minute 29) — so a literal
   `15:30:00` cutoff (minute 30) would never be reached and would be **dormant**
   (the earlier 23:59 problem). `15:29:00` fires on the closing-minute bars
   (15:29:00–15:29:59), i.e. **at the session close**, which is the intent.
3. **The square-off FIRES every session.** Any position still open at 15:29 IST
   is force-closed (reason `squareoff`) on the first 15:29 bar, **re-entry blocked
   until the next session**. The square-off check runs **before** SL/TP, so on a
   bar where both would trigger the **square-off wins** (outer envelope). So a
   position the feature never exits is **squared off daily at the close** (not
   carried overnight); only a position opened on the final session and not yet at
   15:29 is flattened by `on_stop` at the backtest data end (m-cube's own close —
   Nautilus does not auto-flatten).
4. **One feature isolated per file** so a red result points at one cause.
5. **Verify relative to the realised fill** — never the entry you "expected".

## Data — the only instrument in this catalog

The catalog holds **NIFTY only**. All files use the proven-runnable
[`NIFTY_FUT_SL_TEST`](../_default/NIFTY_FUT_SL_TEST.json) shape:

| What | Value |
|---|---|
| Feed (`bar_type_str`) | `NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS-1-SECOND-LAST-EXTERNAL` |
| Strategy bar (`strategy_bar_types`) | `NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS-5-MINUTE-LAST-INTERNAL@1-SECOND-EXTERNAL` |
| Window | `2026-03-02` → `2026-03-20` |
| Tick (`price_increment`) | **0.01** (price_precision 2) |
| Lot / multiplier | 1 (`size_increment` 1, `multiplier` 1) |
| Product (trade type) | **MIS** (intraday) |
| Hard square-off | **15:29:00 IST** (`Asia/Kolkata`) — NSE close; fires every session (see rules 2–3) |
| Session | **09:15–15:30 IST** = 03:45–10:00 UTC (data's last bar 15:29:59 IST); real open 2026-03-02 ≈ **24741** |

The **1-second feed** is deliberate: it gives the finest intrabar granularity
so you can pin the **exact second** an SL/Target level was touched. The
**5-minute composite** drives the EMA so signals are clean and few.

> **Easier-to-hand-check alternative:** swap the feed to the **1-minute
> futures** `NIFTY_FUTURES_YYYYINR.NIFTY_FUTURES_MS-1-MINUTE-LAST-EXTERNAL`
> (data range `2020-01-01` → `2025-07-31`, set the dates inside that span) and
> set `strategy_bar_types` to `[]`. Fewer, coarser bars are simpler to walk by
> hand at the cost of intrabar precision.

## The engine facts every check rests on (verified against the code)

* **Entry fill price.** The EMA signal fires on a (composite) bar; the entry is
  a **MARKET / GTC** order (`core/managed_strategy.py:_submit_order`). With no
  `LatencyModel` configured (default Path A), it fills at **that signal bar's
  close** — *not* the next bar's open. Your "expected price" for an entry is the
  signal bar's close.
* **SL / Target exit fill price.** When `bar_low ≤ SL` (long) the engine does
  **not** fill at the SL level — it submits a reduce-only **MARKET** close, which
  fills at the **trigger bar's close** (`_close_with_reason` →
  `close_all_positions`). So the *trigger* is at the level; the *realised fill*
  is the trigger bar's close. **Check both, and expect them to differ.** (The
  optional `vwap_exit_fill` / `directional_close_fill` toggles repriced this —
  they are **off** in every file here so the base behaviour is what you test.)
* **MIS square-off timing.** The square-off fires on the first bar whose **local
  (`squareoff_tz`) time ≥ the cutoff minute** (`core/managed_strategy.py`, the
  square-off check runs *before* SL/TP, so it's the outer envelope). At **15:29
  IST** it fires on the closing-minute bar (15:29:00–15:29:59) **every session**:
  any open position is force-closed (reason `squareoff`), re-entry blocked until
  the next session. (A literal 15:30:00 would be dormant — this data's last bar
  is 15:29:59.) Only a position opened on the final session and not reaching
  15:29 is flattened by `on_stop` at data end — m-cube's own close; Nautilus does
  **not** auto-flatten, and fills in `on_stop` even bypass `on_order_filled`.

## Execution realism — the six caveats (diagnostic tier)

The Time / Logic / Price loop proves the SL/Target **level** is correct, trips on
the right second, and that the fill equals the trigger bar's close. It does **not**
prove the fill is *realistic*. The single most important caveat:

> **A stop is triggered using the bar's high/low, but the trade is filled at that
> same bar's CLOSE. This can give unrealistically good exits and inflate
> performance.**

The RESULTS report now carries a second, **diagnostic** tier (it quantifies these
per trade but does **not** flip a Time/Logic/Price verdict — every one is
documented base-engine behaviour, not a bug):

| Axis | Caveat | How it's quantified in the report |
|---|---|---|
| **D · Entry correctness** | The MARKET entry fills at the signal bar's **close** (no next-bar-open look-ahead, no latency). | `ENTRY PRICE` reconciled against the bar feed; entries are MARKET/GTC on the EMA signal bar. |
| **E · Realistic fills** | The reduce-only MARKET exit fills at the **trigger bar's close**, not at the level — *trigger ≠ fill*. | Per-trade `Δ fill−lvl` column (signed). |
| **F · Slippage** | Default `FillModel` applies **zero** slippage (`prob_slippage=0.0`). | An engine-fact note + a conservative **1-tick-per-exit** P&L haircut. |
| **G · Gap behaviour** | A bar that **opens** past the level (intraday gap / overnight hold) fills the close far from the level. m-cube's manual market exit **bypasses** Nautilus' gap-aware native-stop fill. | A `↑gap` marker + counts of gap-through and cross-session holds. |
| **H · Intrabar ordering** | Fixed **Open→High→Low→Close** path (`bar_adaptive_high_low_ordering` defaults off); SL checked before TP, so SL wins a same-bar collision. | Dual-level files (VP-03/04, OT-05): count of genuine same-bar SL+Target collisions. |
| **I · P&L realism** | The inflation/deflation from filling at the close instead of the level — favourable fills are the "unrealistically good exits". | A **fill-at-level counterfactual** (= a native move-through stop): net P&L inflation per case and overall. |

**Engine facts** (NautilusTrader 1.224.0, verified via the ntm3 docs +
`core/managed_strategy.py`): intrabar path is fixed `O→H→L→C`; a MARKET order
submitted in `on_bar` with no `LatencyModel` fills at the **current bar's close**
(not the next bar's open); the default FillModel applies **no slippage**; m-cube's
manual market-on-breach exits **bypass** the engine's gap-aware native-stop fill, so
they fill at the bar close regardless of where the bar opened. The docs note bar
data is lower-fidelity for fills — quote/trade/L2 data is more realistic.

> **Favourable Δ = inflation.** A `+Δ fill−lvl` (the close beat the stop within the
> bar) makes the exit look better than the stop and **inflates** reported P&L, even
> though Time/Logic/Price still PASS. A `−Δ` (often `↑gap`, often a cross-session
> hold) cuts the other way. The net inflation across all level exits is the number
> that says how much the bar-close fill model flattered the backtest.

## How to read a case

- **Goal** — the feature/order-type it isolates.
- **Setup** — feed · EMA · side · the one config delta.
- **Per-trade check** — applied to *every* trade the file generates.
- **Pass** — overall criterion.

Run a file → export the **orderbook** (fills, exits, reasons, per-bar
`current_sl`/levels) → walk the Time / Logic / Price check → record green/red.

---

## A. Verification process — SL / Target features (backtest)

### VP-01 · Leg fixed SL (percentage), long
- **Goal:** leg SL trigger + realised fill, end-to-end.
- **Setup:** 1 leg · EMA(9/21) · `stop_loss_type=percentage, stop_loss_value=0.5` · no target.
- **Per-trade:** for fill `F`, `expected_sl = round(F × (1 − 0.005), 0.01)`. SL **triggers** on the first second-bar with `low ≤ expected_sl`; the **realised exit fill** = that trigger bar's **close**.
- **Pass:** engine `current_sl` == `expected_sl` to the tick; trigger second = first `low ≤ SL`; exit fill = trigger-bar close; reason `SL`.

### VP-02 · Leg fixed Target (percentage), long
- **Goal:** leg Target trigger + fill.
- **Setup:** 1 leg · `target_type=percentage, target_value=0.5` · no SL.
- **Per-trade:** `expected_tgt = round(F × (1 + 0.005), 0.01)`; triggers on first `high ≥ expected_tgt`; exit fill = trigger-bar close; reason `TARGET`.
- **Pass:** `current_tp` == `expected_tgt`; correct trigger second; correct fill.

### VP-03 · SL **and** Target together (which condition fired?)
- **Goal:** multiple-condition resolution — *which* exit caused the stop and *when*.
- **Setup:** 1 leg · SL `percentage 0.3` + Target `percentage 0.5`.
- **Per-trade:** record `reason` (SL vs TARGET) and the second it fired. The engine checks **SL before TP** in `_check_exits`, so if both levels sit inside one bar the **SL wins** — confirm that ordering and that **never both** fire on the same trade.
- **Pass:** exactly one of SL/Target per trade; reason matches the level actually reached first; SL-precedence holds on a same-bar collision.

### VP-04 · Short-side SL + Target
- **Goal:** SL/Target math on the short side.
- **Setup:** 1 leg · SL `percentage 0.4` + Target `percentage 0.6`. EMA Cross goes short on bearish crosses — **pick the SHORT trades** from the orderbook.
- **Per-trade (short):** `SL = F × (1 + 0.004)` **above** entry (fires on `high ≥ SL`); `TGT = F × (1 − 0.006)` **below** entry (fires on `low ≤ TGT`).
- **Pass:** short SL above / target below; correct side fires; mirror of VP-03.

### VP-05 · Trailing SL (ratchet, only tightens)
- **Goal:** the trailing-SL step ratchet.
- **Setup:** 1 leg · SL `percentage 0.5`, `trailing_sl_step=0.2`, `trailing_sl_offset=0.1`.
- **Per-trade:** as profit rises the SL **only tightens** (long: `current_sl` never decreases). Walk per-bar `current_sl`; each ratchet must land on the bar profit crosses the next step, by the configured amount.
- **Pass:** SL ratchets monotonically up; never loosens; exit checked against the *current* (tightened) level.

### VP-06 · ATR SL
- **Goal:** volatility-adaptive SL sizing per trade.
- **Setup:** 1 leg · `stop_loss_type=atr, sl_atr_period=14, sl_atr_multiplier=2.0`.
- **Per-trade:** `expected_sl = F − 2.0 × ATR(14)` at the entry bar (long), snapped, using the **logged ATR-at-entry**. Warm-up trades (ATR not initialised) arm **no SL** → they must carry, not exit.
- **Pass:** `current_sl` matches the formula with the logged ATR; warm-up legs arm no SL.

### VP-07 · SL Wait (confirmation gate)
- **Goal:** the SL-Wait delay.
- **Setup:** 1 leg · SL `percentage 0.4`, `sl_wait_bars=3`.
- **Per-trade:** on the first bar SL is pierced, the exit is **delayed**; it fires only after the level holds **3 bars**. If price recovers above SL within the wait → **cleared**, no exit.
- **Pass:** delayed exits fire exactly trigger + 3 bars; recovered ones don't exit.

### VP-08 · On-SL action = Re-Execute (count 2)
- **Goal:** On-SL dispatch + count limit.
- **Setup:** 1 leg · SL `percentage 0.3`, `on_sl_action=re_execute, max_re_executions=2`.
- **Per-trade:** after the SL the leg goes IDLE → re-enters at market on a later signal; **max 3 entries** (1 + 2 re-exec); the 4th is suppressed (`LEG_REEXEC_LIMIT_REACHED`). Record each re-entry's second + price.
- **Pass:** re-entries at market; total entries capped at 3.

### VP-09 · On-SL action = Reverse
- **Goal:** reverse-on-SL dispatch.
- **Setup:** 1 leg · SL `percentage 0.3`, `on_sl_action=reverse`.
- **Per-trade:** SL hit → close + open the **opposite** side; the reversed position's side flips and its fresh SL recomputes off the new fill.
- **Pass:** side flips; new SL computed off the reverse fill; clean ACTIVE→reversed transition.

### VP-10 · Portfolio Combined Loss **+** leg SL (precedence)
- **Goal:** which level caused the stop when a leg SL **and** the portfolio SL both trip.
- **Setup:** **2 legs** (EMA 9/21 + 5/13), each leg SL `percentage 0.5`; portfolio `pf_sl_enabled, pf_sl_type="Combined Loss", pf_sl_value=250, pf_sl_action="SqOff"`.
- **Per-run:** on a drawdown that trips both, the **portfolio** sqoff fires first and flattens **both** legs on the same bar (reason `PORTFOLIO_LOSS`) before the individual leg SLs. Record which level caused each exit and at what second.
- **Pass:** portfolio exit precedes the leg exits; both legs close on the same bar; precedence Portfolio → Leg.

### VP-11 · Zero / disabled feature + MIS square-off
- **Goal:** confirm a disabled feature, and isolate the **daily MIS square-off** (no feature noise).
- **Setup:** 1 leg · `stop_loss_type="none"` **and** `target_type="none"` (both values 0); MIS + 15:29 IST square-off; no entry window.
- **Per-trade:** `current_sl`/`current_tp` stay `0` → the `>0` gate skips them → **no SL/Target exit**. The only exits are the **daily MIS square-off**: every position open at 15:29 IST is force-closed (reason `squareoff`), re-entry blocked until the next session; it re-enters next day on the EMA signal and is squared off again.
- **Pass:** zero SL/Target exits; **one square-off per session**, each firing on the first 15:29 bar of its day; re-entry only after the session rolls.

### VP-12 · Zero **value** with a type set — the flaw probe
- **Goal:** "what happens if I set SL and TP to 0?" — the answer depends on *type*.
- **Setup:** 1 leg · `stop_loss_type="percentage", stop_loss_value=0` **and** `target_type="percentage", target_value=0`.
- **Per-trade:** a 0% level is **computed**, not disabled: `SL = F × (1 − 0) = F` and `TGT = F × (1 + 0) = F` — both land **on the entry**. The position exits almost **immediately** at ~breakeven (SL checked first, fires on the first `low ≤ entry`).
- **Pass:** documents the difference vs VP-11 — `type="none"` disables; `type="percentage", value=0` places the level **at entry** → near-instant exit. **Flag this** (a 0 value arguably should disable, not arm-at-entry).

---

## B. Order types — live-market verification

> **Engine reality:** `core/managed_strategy.py` submits **only MARKET / GTC**
> orders. Limit / Stop-Limit are **not wired** for backtest — they are
> **blocked at save** (`server._validate_portfolio_sl` → HTTP 400). "Good till
> triggered" is not a Nautilus time-in-force at all; "good till cancelled" =
> `GTC`, the default on every order. See the report's order-type section for
> the full Nautilus mapping.

### OT-01 · Market order — expected vs **actual** fill
- **Goal:** the only wired path; verify the fill price/time you *expected* vs what executed.
- **Setup:** 1 leg · `exit_order_type="MARKET"` · SL `percentage 0.5`.
- **Per-trade:** the EMA entry is MARKET/GTC on the signal bar → fills at **that bar's close** (no latency). Record your expected price (signal-bar close) and compare to `OrderFilled.last_px` / `ts_event`. The SL exit is also a market close → trigger-bar close.
- **Pass:** `last_px` == signal-bar close for entries; trigger-bar close for SL exits; timestamps match the bars.

### OT-02 · Limit exit — **negative test (must be rejected)**
- **Goal:** confirm Limit exits are blocked.
- **Setup:** `exit_order_type="Limit"`.
- **Per-run:** saving this portfolio **must** return HTTP 400 — *"Exit Order Type 'Limit' is not implemented … only MARKET exits are supported (spec §8.1)."* The file is the fixture for the rejection; it is intentionally **not runnable** as-is.
- **Pass:** save rejected with that 400; backtest never runs a limit exit.

### OT-03 · Stop-Loss **Limit** — negative test (must be rejected)
- **Goal:** confirm SL-Limit is blocked.
- **Setup:** `exit_order_type="SL_Limit"`.
- **Per-run:** save **must** return HTTP 400 (only MARKET wired). A stop-limit rests a limit at `price` once `trigger_price` is touched and can **miss in a fast gap** — a live-only nuance the backtester doesn't model.
- **Pass:** save rejected with the 400.

### OT-04 · Stop-Loss **Market**
- **Goal:** map "stop-loss (market)" to the wired SL feature.
- **Setup:** 1 leg · SL `percentage 0.5` · `exit_order_type="MARKET"`.
- **Per-trade:** the managed strategy watches bar low/high; once the SL is pierced it submits a reduce-only **MARKET** close (StopMarket semantics) → fills at the trigger bar's close.
- **Pass:** trigger second = first `low ≤ SL`; fill = trigger-bar close; reason `SL`.

### OT-05 · Good-Till-Triggered (GTT)
- **Goal:** document + run the GTT concept.
- **Setup:** 1 leg · SL `percentage 0.4` + Target `percentage 0.6` as the two resting "triggers" · MARKET.
- **Per-trade:** GTT is **not** a Nautilus TIF — it's a conditional (Stop / If-Touched) order armed by a `trigger_price` + `GTC`. The backtester has no native conditional path; it **emulates** "rest until triggered" by monitoring each bar and firing a market exit when a level is touched. Each of SL/Target acts as one trigger.
- **Pass:** each trigger fires on the first bar its level is touched (and not before); no premature/again firing.

### OT-06 · Good-Till-Cancelled (GTC)
- **Goal:** document + run the GTC default.
- **Setup:** 1 leg · no SL/Target · MIS + 15:29 IST square-off · MARKET.
- **Per-trade:** `GTC` is the **default time-in-force** on every order the engine submits. With nothing to exit on, the GTC entry **holds** intraday until either the opposite EMA signal "cancels" it by flipping the position **or** the MIS 15:29 IST square-off force-closes it at the session close.
- **Pass:** the order never expires on its own (no GTD/DAY expiry) — it ends only by signal-flip or the daily MIS square-off; re-entry resumes the next session.

---

## Coverage map (paragraph → file)

| Brief requirement | Files |
|---|---|
| Order fires at open/normalisation; verify SL level vs asset price (long, SL 5% style) | VP-01, OT-01 |
| Target verification | VP-02 |
| Multiple conditions — *which* fired & *when* | VP-03, VP-10 |
| Time & price match the asset data | every VP/OT (Time·Price axes) |
| **MIS trade type + hard square-off 15:29 IST / NSE close (Indian market)** | baked into all 18 files; fires every session |
| **MIS daily square-off isolated (no feature)** | VP-11 |
| **Feature value = 0** (SL & TP 0 → what happens?) | VP-11 (`type=none` → disabled), VP-12 (`value=0` → arms at entry) |
| Short side | VP-04 |
| Trailing / ATR / Wait / Re-Execute / Reverse | VP-05, VP-06, VP-07, VP-08, VP-09 |
| Different order types: market / limit / GTT / stop-loss / SL(limit·market) / GTC | OT-01 … OT-06 |
| **Execution realism: entry correctness, realistic fills, slippage, gap behaviour, intrabar ordering, P&L realism** | diagnostic tier on every runnable VP/OT (see *Execution realism* above) |

## Notes & caveats

- **Screenshots:** the brief asks for a screenshot per check (the chart with the
  level + the orderbook row). Capture (a) the entry with your computed SL/Target
  drawn on, and (b) the exit bar showing `low ≤ SL` / `high ≥ Target`, alongside
  the orderbook `current_sl`/`reason`/`fill`. Store next to each case ID.
- **Trigger ≠ fill.** Expect the realised SL/Target fill to differ from the
  nominal level (market-close fill). That gap is *correct* base behaviour, not a
  bug — but quantify it per trade; a large gap on a quiet bar is worth a flag.
- **Few trades per file** is expected — EMA(9/21) on a 5-min composite signals
  sparingly. Widen the window (up to `2026-05-22`) or add the EMA(5/13) leg for
  more activity.
- **User-level (Max Loss/Profit) tests are not here** — they need
  `config/users.json` edits, not a portfolio file (see the v1 set in
  `portfolios/testing/`).

## MIS square-off note — the 15:29 cutoff

Every file is `product="MIS"` with `squareoff_time="15:29:00"` /
`squareoff_tz="Asia/Kolkata"` — the **NSE close**. It **fires every session**:
any position open at 15:29 IST is force-closed (reason `squareoff`) on the first
15:29 bar, re-entry blocked until the next session. Because the square-off runs
**before** SL/TP, on a bar where both the cutoff and an SL would trigger the
**square-off wins** (outer envelope).

**Why 15:29 and not 15:30:** the NSE close is 15:30, but this 1-second feed's
last bar each day is **15:29:59 IST** (minute 29). The square-off fires when a
bar's IST minute ≥ the cutoff minute, so a literal `15:30:00` (minute 30) would
never be reached → **dormant** (the same reason 23:59 never fired). `15:29:00`
lands on the closing-minute bars, firing at the session close as intended.

To change the cutoff (e.g. an earlier intraday auto-square-off like 15:20, or
back to a dormant end-of-day envelope), edit `squareoff_time` / `squareoff_tz`
in `base_portfolio` inside `_generate_portfolios.py` and re-run.

> If you reload the catalog with data whose last bar is exactly 15:30:00 IST,
> change the cutoff to `15:30:00` to match — on the current data, 15:30 is dormant
> and 15:29 is what actually fires at the close.
