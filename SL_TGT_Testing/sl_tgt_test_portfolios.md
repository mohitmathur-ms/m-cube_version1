# SL/Target Test Portfolios — Full Feature Coverage

Comprehensive test set for **every** SL/Target feature — leg, portfolio, and user levels — built as runnable **test portfolios** (EMA-cross entry). Each case uses **distinct parameters** and carries a **manual logic-execution trace**: the hand-computed expected behaviour you check the engine's orderbook against.

**How to use:** run the portfolio → export the orderbook → walk the manual trace → confirm the engine matches at each step. Verify *relative to the realised fill* (entry price isn't controlled).

---

## 1. Test portfolios (reusable structures)

| ID | Structure | Instrument | Entry | Used for |
|---|---|---|---|---|
| **PF-1L** | 1 long leg | NIFTY-FUT | EMA(9/21) bull cross | leg SL/Target/Wait/ATR/actions |
| **PF-1S** | 1 short leg | 6E | EMA bear cross | short-side SL/Target |
| **PF-2** | 2 legs (same instr.) | GC | both on cross | Move-SL-to-Cost + Trail-After |
| **PF-3** | 3 legs | BANKNIFTY-FUT | all on cross | portfolio SL types, filters, breach |
| **PF-U** | 2 portfolios, 1 user (NIFTY-FUT + 6E) | mixed | per-portfolio cross | user Max Loss/Profit, cascade |
| **PF-X** | 1 leg, run as A/B/C feeds + MIS/NRML | NIFTY-FUT | cross | cross-format, product type |

---

## 2. Coverage matrix

| Feature | Level | Case | Portfolio | Key params (distinct) |
|---|---|---|---|---|
| SL percentage | Leg | TL-1 | PF-1L | 2% |
| SL points (absolute) | Leg | TL-2 | PF-1S | 0.0050 |
| SL ATR | Leg | TL-3 | PF-1L (BTC) | period 14, mult 2.0 |
| Target percentage | Leg | TL-4 | PF-1L (GC) | 3% |
| Target points | Leg | TL-5 | PF-1S | 0.0080 |
| SL Wait / delay | Leg | TL-6 | PF-1L | SL 2%, wait 3 bars |
| Move SL to Cost — profitable-only | Leg | TL-7 | PF-2 | — |
| Move SL to Cost — all legs | Leg | TL-8 | PF-2 | — |
| Move SL to Cost — LTP+buffer (losers) | Leg | TL-9 | PF-3 | ±0.05% |
| Trail After Move SL | Leg | TL-10 | PF-2 | every 4, by 2 |
| Leg action — SqOff | Leg | TL-11 | PF-1L | — |
| Leg action — Re-Execute | Leg | TL-12 | PF-1L | count 2 |
| Leg action — ReEntry | Leg | TL-13 | PF-1L | count 1 |
| Leg action — KeepLegRunning | Leg | TL-14 | PF-2 | — |
| Leg action — Execute (other leg) | Leg | TL-15 | PF-2 | — |
| Action combination rules | Leg | TL-16 | PF-1L | valid + invalid |
| Combined Loss | Portfolio | TP-1 | PF-3 | 5000 |
| Underlying Movement (SL) | Portfolio | TP-2 | PF-3 | level 48000 |
| Loss and Underlying Range | Portfolio | TP-3 | PF-3 | 5000, [47800, 48200] |
| Combined Profit | Portfolio | TP-4 | PF-3 | 6000 |
| Underlying Movement (Target) | Portfolio | TP-5 | PF-3 | level 48500 |
| Trailing SL (step) | Portfolio | TP-6 | PF-3 | every 1000, by 500 |
| Trailing Target / Profit Lock | Portfolio | TP-7 | PF-3 | reach 1000, lock 600, every 500, by 300 |
| Move SL to Cost (portfolio-driven) | Portfolio | TP-8 | PF-3 | — |
| Portfolio Delay | Portfolio | TP-9 | PF-3 | 3 bars |
| SqOff filters (loss / profit only) | Portfolio | TP-10 | PF-3 | — |
| Cross-portfolio actions (7) | Portfolio | TP-11 | PF-U | — |
| Leg-breach validation | Portfolio | TP-12 | PF-3 | Σ leg SL > pf SL |
| Max Loss (absolute) | User | TU-1 | PF-U | 10000 |
| Max Profit (absolute) | User | TU-2 | PF-U | 15000 |
| Max Loss (% of allocation) | User | TU-3 | PF-U | 5% of 200000 |
| User Trailing SL | User | TU-4 | PF-U | every 2000, by 1000 |
| User Trailing Target | User | TU-5 | PF-U | reach 5000, lock 3000, every 1000, by 500 |
| Three-format equivalence (A/B/C) | X | TX-1 | PF-X | — |
| MIS cutoff *(TBD-7)* | X | TX-2 | PF-X | 15:20 |
| NRML carry | X | TX-3 | PF-X | — |
| Asset × timeframe | X | TX-4 | PF-1L | all assets × 7 TFs (1m…1M) |
| Tick snapping | X | TX-5 | PF-1L | per-instrument tick |
| Safety Seconds | X | TX-6 | PF-1L | 30s vs a 1m bar |

---

## 3. Leg-level cases — config + manual trace

**TL-1 · SL percentage (long), PF-1L NIFTY-FUT, value 2%.** Fill 22000 → `SL = 22000×0.98 = 21560.00` (tick 0.05 → 21560.00). Bar low 21558 ≤ 21560 → **exit 21560**. *Pass:* engine SL = 21560.00; exit on that bar.

**TL-2 · SL points (short), PF-1S 6E, value 0.0050.** Fill 1.10000 short → `SL = 1.10000 + 0.0050 = 1.10500`. Bar high 1.10520 ≥ 1.10500 → **exit 1.10500**. *Pass:* SL = 1.10500.

**TL-3 · SL ATR (long), BTC, period 14, mult 2.0; ATR@entry 300.** Fill 60000 → `dist = 2.0×300 = 600` → `SL = 59400`. Bar low 59380 → **exit 59400**. *Pass:* SL = fill − 600 using logged ATR; warm-up → no SL.

**TL-4 · Target percentage (long), GC, value 3%.** Fill 2000 → `tgt = 2000×1.03 = 2060`. Bar high 2061 ≥ 2060 → **exit 2060**. *Pass:* tgt = 2060.

**TL-5 · Target points (short), 6E, value 0.0080.** Fill 1.10000 short → `tgt = 1.10000 − 0.0080 = 1.09200`. Bar low ≤ 1.09200 → exit. *Pass:* tgt = 1.09200.

**TL-6 · SL Wait, PF-1L, SL 2%, wait 3 bars.** Fill 22000 → SL 21560.
| bar | low | ≤21560? | elapsed | action |
|---|---|---|---|---|
| t | 21558 | yes | 0 | trigger; delay start |
| t+1 | 21555 | yes | 1<3 | hold |
| t+2 | 21556 | yes | 2<3 | hold |
| t+3 | 21557 | yes | 3≥3 | **exit** |
*Variant:* if t+1 low = 21562 (>SL) → DELAY_CLEARED, no exit. *Pass:* exit only at t+3; cleared variant doesn't exit.

**TL-7 · Move SL to Cost — profitable-only, PF-2 GC (legs A, B), entry 2000.** Leg A hits its SL → triggers move on B. Leg B price 2012 (pnl +12 >0) → `sl_B = entry = 2000`. *Pass:* B's SL = 2000, flag set; a loss-making leg would be untouched.

**TL-8 · Move SL to Cost — all legs, PF-2.** Same trigger; variant = all. Leg B even at −5 pnl → `sl_B = 2000`. *Pass:* B's SL = 2000 regardless of profit (note: may immediately trigger if price already past 2000).

**TL-9 · Move SL to Cost — LTP+buffer for losers, PF-3, buffer ±0.05%.** On move: profitable leg → sl=entry; loss SELL → `sl = ltp×1.0005`; loss BUY → `sl = ltp×0.9995`. e.g., loss SELL ltp 48030 → `sl = 48054.015`. *Pass:* each variant computes per formula.

**TL-10 · Trail After Move SL, PF-2, every 4, by 2 (anchor = curr_leg_pnl).** Long, entry 100; move fired at price 102 → anchor = 2, sl = 100.
| price | pnl | gain = pnl−anchor | step | sl | anchor |
|---|---|---|---|---|---|
| 106 | 6 | 4 | +2 | **102** | 6 |
| 110 | 10 | 4 | +2 | **104** | 10 |
| 111 | 11 | 1 | no | 104 | 10 |
*Pass:* SL ratchets 100→102→104; anchor is **PnL** (ratchets the same on a 22000-priced leg).

**TL-11 · Leg action SqOff.** SL hit → leg CLOSED, PnL realised. *Pass:* single clean exit.

**TL-12 · Leg action Re-Execute (count 2).** SL hit → leg IDLE → re-enters at market next bar. Max 3 entries; 4th suppressed (`LEG_REEXEC_LIMIT_REACHED`). *Pass:* exactly 3 entries.

**TL-13 · Leg action ReEntry (count 1).** SL hit → REENTRY_WAITING → re-enters when price returns to original entry. *Pass:* re-entry only on price return; count-limited.

**TL-14 · Leg action KeepLegRunning, PF-2.** SL hit → `sl_pending_val` cleared, leg stays ACTIVE. *Pass:* leg not closed; SL disarmed.

**TL-15 · Leg action Execute (other leg), PF-2.** Leg A SL hit → sets `execute_trigger` on leg B (B enters); A closes. *Pass:* B enters, A closes.

**TL-16 · Action combination rules, PF-1L.** Valid: SqOff+Re-Execute(within 3). Invalid: KeepLegRunning+ReEntry, KeepLegRunning+SqOff, Re-Execute+ReEntry, >3 actions. *Pass:* valid combos run; invalid combos rejected on save.

---

## 4. Portfolio-level cases — config + manual trace

**TP-1 · Combined Loss, PF-3, sl 5000.** combined_pnl −4800 → no; −5100 ≤ −5000 → **force-sqoff all legs**. *Pass:* exit when combined ≤ −5000.

**TP-2 · Underlying Movement (SL), PF-3, level 48000 (underlying = self).** prev 47960, curr 48040 → crosses 48000 → **fires once**. *Pass:* fires on the bi-directional crossing bar, once.

**TP-3 · Loss and Underlying Range, PF-3, sl 5000, range [47800, 48200].** combined −5100 AND underlying 47750 (≤47800) → **fires**. combined −5100 AND underlying 48000 (in range) → no. *Pass:* both conditions required.

**TP-4 · Combined Profit, PF-3, tgt 6000.** combined 5900 → no; 6100 ≥ 6000 → **sqoff all**. *Pass:* exit at ≥6000.

**TP-5 · Underlying Movement (Target), PF-3, level 48500.** crossing 48500 → fires once. (No Profit-and-Range type on the target side.) *Pass:* crossing fires.

**TP-6 · Trailing SL (step), PF-3, sl 5000, every 1000, by 500.** `anchor_pnl=0`, `current_sl=5000`.
| curr_pnl | gain | steps | current_sl | anchor |
|---|---|---|---|---|
| 2000 | 2000 | 2 | **4000** | 2000 |
| 4500 | 2500 | 2 | **3000** | 4000 |
| 4200 | 200 | 0 | 3000 | 4000 |
*Pass:* loss-limit tightens 5000→4000→3000; only tightens; SL-hit checked vs current_sl.

**TP-7 · Trailing Target / Profit Lock, PF-3, reach 1000, lock 600, every 500, by 300.**
| curr_pnl | action | current_stop | anchor |
|---|---|---|---|
| 1000 | activate | 600 | 1000 |
| 1500 | gain 500 → step | 900 | 1500 |
| 2000 | gain 500 → step | 1200 | 2000 |
| 1100 | 1100 ≤ 1200 | **exit TARGET_TRAIL** | — |
*Pass:* floor ratchets up only; exits on floor breach; fixed target suppressed while active.

**TP-8 · Move SL to Cost (portfolio-driven), PF-3.** Triggered by aggregate portfolio profit; same 3 variants as leg §1.3. *Pass:* per-variant SL repositioning across the book.

**TP-9 · Portfolio Delay, PF-3, 3 bars.** Portfolio SL/Target triggers → wait 3 bars before dispatch; if condition reverses, cleared. *Pass:* dispatch delayed; reversal clears.

**TP-10 · SqOff filters, PF-3.** On portfolio hit with "SqOff Only Loss-Making Legs" → only loss legs closed, profit legs stay. "Only Profit-Making" → inverse. *Pass:* only the filtered subset exits.

**TP-11 · Cross-portfolio actions, PF-U.** Test each: SqOff Other Portfolio, Execute Other Portfolio, Start Other Portfolio, ReExecute, ReExecute at Entry Price, ReExecute Same Contract. *Pass:* each dispatches to the target portfolio correctly.

**TP-12 · Leg-breach validation, PF-3.** 3 legs SLs sum to 7000; portfolio Combined Loss = 5000. On save → **flag breach**; at run → portfolio SL fires at −5000 before the legs. *Pass:* save-time flag + portfolio precedence.

---

## 5. User-level cases — config + manual trace

**TU-1 · Max Loss (absolute), PF-U, 10000.** combined user pnl −9000 → no; −10200 ≤ −10000 → **force-sqoff ALL portfolios**, `gst["hit"]` sticky. *Pass:* both books close same bar; no further entries.

**TU-2 · Max Profit (absolute), PF-U, 15000.** combined 15300 ≥ 15000 → force-sqoff all. Order: Max Loss checked first, then Max Profit. *Pass:* only one fires/bar; cascade closes all.

**TU-3 · Max Loss (% of allocation), PF-U, 5% of 200000 = 10000.** Resolves to the same 10000 threshold (engine-defined allocation base, not live margin). *Pass:* same behaviour as TU-1; raw % rejected.

**TU-4 · User Trailing SL, PF-U, every 2000, by 1000.** combined profit 6000 → steps 3 → user SL tightens by 3000. *Pass:* user loss-limit tightens with combined profit; only tightens.

**TU-5 · User Trailing Target, PF-U, reach 5000, lock 3000, every 1000, by 500.** combined 5000 → activate, floor 3000; 6000 → floor 3500; falls to 3400 ≤ 3500 → exit all. *Pass:* user-wide profit-lock ratchets + cascades.

---

## 6. Cross-cutting cases

**TX-1 · Three-format equivalence, PF-X.** Same price path encoded as A (bid/ask), B (OHLC), C (LTP); run identical config. *Pass:* trigger bar, delay, move-to-cost, action dispatch **identical** across A/B/C; only the fill price may differ.

**TX-2 · MIS cutoff, PF-X, 15:20 *(TBD-7)*.** Position open at 15:20 → forced `cutoff` exit. *Pass:* flat at cutoff. **Blocked until product-type built.**

**TX-3 · NRML carry, PF-X.** SL not hit → position carries to next session. *Pass:* no intraday forced exit.

**TX-4 · Asset × timeframe, PF-1L.** Run TL-1/TL-3 across NIFTY-FUT · BANKNIFTY-FUT · US index fut · 6E · GC · BTC × 1m · 3m · 5m · 2h · 1d · 1w · 1M. *Pass:* tick-snap, PnL multiplier, currency, ATR-by-timeframe all correct per cell.

**TX-5 · Tick snapping, PF-1L.** Off-grid computed SL (e.g., 21560.98 → tick 0.05 → 21561.00). *Pass:* every SL on the instrument tick grid.

**TX-6 · Safety Seconds, PF-1L, 60s.** Move-SL fires at t → applies at t+60s. With a 30s safety on a 1m bar (sub-bar) → engine refuses + warns. *Pass:* anchored to trigger-event time; sub-bar refused.

### Combination & odd-type cases (the tricky ones)

Single-feature cases (TL/TP/TU) prove each rule in isolation; these deliberately **stack features and hit corners** to prove the rules still hold when they collide. They layer on the existing PFs, so they're not in the §2 one-feature matrix.

**TC-1 · Same-bar level race (precedence), PF-U.** One bar makes a leg SL, the portfolio Combined Loss, *and* the user Max Loss all true together. *Expected:* **User→Portfolio→Leg** — `USER_SQOFF` fires and force-flats both books; the leg/portfolio hits are subsumed under the user reason, not logged as three independent exits. *Pass:* one `USER_MAX_LOSS` exit per leg, same bar; no double-count; nothing re-enters.

**TC-2 · Trail-step + breach on the same bar, PF-3.** A bar lifts combined PnL enough to ratchet the Trailing SL up *and then* trades back through the **new** (tightened) stop in the same bar. *Expected:* tighten `current_sl` first, then test the SL-hit against the *new* level → exit same bar. *Pass:* exit reason `TRAIL_SL` at the ratcheted level, not the old one (guards "stale level after move").

**TC-3 · Move-to-Cost then gap-through, PF-2.** Move-SL-to-Cost sets leg SL = entry; the **next bar opens past entry** (gaps through the level). *Expected:* fill per the gap rule — **at the level** or at the worse open? *Pass:* behaviour is explicit and matches the agreed rule — this is the concrete trade that settles the **parked gap question (Q2)**.

**TC-4 · Re-Execute into an instant re-stop, PF-1L (SqOff+Re-Execute, count 2).** Leg SL hit → re-enters next bar → the fresh SL is hit on that **same re-entry bar**. *Expected:* the re-exec counter increments, the 2nd exit is attributed to the re-executed leg (`_2` suffix), and the limit still caps total entries. *Pass:* exactly the allowed entries; no infinite loop; counts correct.

**TC-5 · Trailing SL vs fixed Combined Loss — which governs, PF-3.** The portfolio carries both a fixed Combined Loss (5000) and a Trailing SL that has ratcheted the effective stop **tighter than 5000**. *Expected:* the **tighter** stop governs; the fixed value is the floor and never loosens the trailed stop. *Pass:* exit at the tighter level; the two SL mechanisms compose by min-distance, they don't double-fire.

**TC-6 · Cross-PF start racing a self-entry, PF-XPF.** PF-A fires "Start Other (PF-B)" with a 2 s delay; within that delay PF-B's *own* EMA entry also triggers. *Expected:* PF-B enters **once** — the external Start and the self-entry don't stack into a double position; the delay is still honoured. *Pass:* a single PF-B entry; one entry reason logged.

**TC-7 · Boundary — exactly at the threshold, PF-3.** A bar lands combined PnL at **exactly −5000.00** (after tick-snap and FX). *Expected:* the `≤`/`<` convention is explicit and consistent (we use `≤` → fires *at* the level). *Pass:* fires at exactly −5000; the equality case is not skipped.

---

## 7. Run matrix — assets × timeframes by level

§6's **TX-4** already sweeps the *leg* cases across the full asset × timeframe grid. The same discipline runs upward — but sweep a dimension only where the logic interacts with it (a full level × asset × timeframe cross is thousands of runs that re-prove nothing). Run each level's listed cells across these cells only:

| Level | Cells | Assets | Timeframes | Rationale |
|---|---|---|---|---|
| **Leg** | TL-1, TL-2, TL-3 (SL types) + TL-6 (wait) | all 7 | all 7 (1m…1M) + tick/1-sec for intrabar ordering | tick-snap, lot-mult, currency, ATR-in-bars bite at the SL itself; wait-units scale with bar size (extends **TX-4**) |
| **Portfolio — dynamic** | TP-6, TP-7, TP-8, TP-9 | 1 INR (BANKNIFTY-FUT) + 1 USD (GC) | all 7 (1m…1M) | step ratchet, profit-lock floor and move-to-cost fire on bar boundaries → the step/trigger bar shifts per timeframe; Combined PnL must aggregate the correct multiplier + currency |
| **Portfolio — static** | TP-1, TP-3, TP-4 | 1 INR + 1 USD | 1m + a coarse-TF spot-check (e.g. 1d) | only the *trigger bar* moves with timeframe; the threshold level itself doesn't |
| **User** | TU-1, TU-4, TU-5 | cross-currency (NIFTY-FUT INR + 6E USD) once + an all-INR pair | 1m + a coarse-TF spot-check on TU-4/TU-5 | the user roll-up normalises both books to a base currency; same-bar User→Portfolio→Leg precedence depends on which bar the breach lands |
| **Cross-PF** | TP-11 | the dispatch pair as configured | 1m + a coarse TF for the cross-PF delay (2 s/3 s) | the cross-PF delay is wall-clock → re-verify against bar size |
| **Product mode** | TX-2 (MIS), TX-3 (NRML) | PF-X (NIFTY-FUT); CNC equity → `product_modes_limit_orders` | any one TF (e.g. 1m) | the MIS cutoff and CNC carry are clock/calendar events, not bar-size sensitive |

**Per cell, re-verify (never assume cross-cell equality):** ATR distance (in bars → differs per timeframe), tick-snap (per instrument), `PnL = move × lot_mult × n_lots`, currency (INR vs USD), and that wall-clock timers (SL-Wait seconds, Safety Seconds, cross-PF delay) stay fixed while bar-count timers scale with bar size. Every non-leg cell still gets the full **Time / Logic / Price** check from §8.

---

## 8. How to check manually (the verify loop)

1. Run the portfolio → export the **orderbook** (fills, exits, reasons, per-bar `current_sl`/levels).
2. For each **entry**: take the logged fill, apply the case formula, tick-snap → compare to the engine's value. Match to the tick.
3. For each **dynamic feature** (move-to-cost, trailing, profit-lock, user cascade): walk the manual trace **row by row** against the engine's per-bar log — the level must change on the same bar, by the same amount.
4. For each **exit**: confirm the trigger condition (bar low/high / pnl vs level) and the fill.
5. Record green/red; red rows carry the offending trade/bar.
