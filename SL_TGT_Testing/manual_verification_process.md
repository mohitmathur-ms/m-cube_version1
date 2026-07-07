# Manual Verification Process — Time · Logic · Price (via the Order Book)

A repeatable process to manually confirm that every SL/Target feature fired **at the right time**, **with the correct logic**, and **at the correct price** — by checking the engine's **order book** against numbers you recompute yourself.

**Principle:** you re-derive the expected time/logic/price from the order book's **own logged inputs** (fill, bar data, config, PnL) — never from what the engine *claims* it did. The engine's claim and your recomputation must agree.

---

## 1. The order book — what the engine must emit

For verification to be possible, the order book needs one row per **event** (not just per fill), with these columns:

| Column | Why it's needed |
|---|---|
| `seq`, `timestamp`, `bar_index` | the **time** axis — when did it happen |
| `level` (leg / portfolio / user) · `leg_id` · `portfolio_id` · `user_id` | which entity acted |
| `instrument` · `direction` (BUY/SELL) · `product_mode` (MIS/NRML/CNC) | context for price + lifecycle |
| `event` | ENTRY · SL_TRIGGER · DELAY_START · DELAY_CLEARED · MOVE_TO_COST · TRAIL_UPDATE · SL_EXIT · TARGET_EXIT · PORTFOLIO_SQOFF · USER_SQOFF · MIS_CUTOFF · REEXEC · REENTRY … |
| `reason` | SL · TARGET · TRAIL_SL · TARGET_TRAIL · MOVE_TO_COST · PORTFOLIO_LOSS · USER_MAX_LOSS · MIS_CUTOFF … |
| `entry_price` · `fill_price` · `level_price` | the **price** axis |
| `current_sl` · `current_target` · `move_sl_trail_anchor` · `anchor_pnl` | the live state to verify trailing/move-to-cost |
| `leg_pnl` · `combined_pnl` · `underlying` | thresholds for PnL/underlying triggers |
| `bar_o/h/l/c` (or `bid_*`/`ask_*`/`ltp`) | the bar that caused it — needed to verify trigger + price |
| `config` (sl_type, value, every/by, wait, safety, reach/lock…) | to recompute expected levels/times |

> Without the **per-bar state** (`current_sl`, anchors, pnl) and the **bar data**, you can only check entries/exits — not *when* and *why* the dynamic features moved. Insist on these columns.

---

## 2. The 3-axis check (run on every event row)

### A. TIME — did it fire at the right moment?
1. Identify the **trigger condition** for the event (from `reason`).
2. From the logged **bar data**, find the **first bar** where that condition became true.
3. The event's `bar_index` must equal that bar — **not earlier** (look-ahead) and **not later** (lag) — *unless* a delay applies:
   - SL/Target Wait → expected exit bar = trigger bar **+ wait** (bars or seconds).
   - Safety Seconds → effect applies at trigger-event time **+ safety_seconds**.
4. If multiple levels fired the **same bar**, confirm **precedence: User → Portfolio → Leg** (higher-level row precedes lower).

### B. LOGIC — did the right thing fire, for the right reason?
1. The `reason` matches the condition that was **actually true** at that bar (e.g. `reason = SL` only if price reached the SL; `TARGET_TRAIL` only if the locked floor was breached).
2. The correct **action** dispatched (SqOff vs ReExecute vs ReEntry vs KeepLegRunning vs cross-portfolio).
3. **Exactly one** outcome where only one is valid (e.g. not both SL and Target on the same leg on the same bar; fixed Target suppressed while Trailing Target active).
4. **State transition** is valid (ACTIVE → CLOSED / IDLE / REENTRY_WAITING per the action).
5. **Guards/limits** respected (Re-Execute count; trailing only-tightens; lock only-rises).

### C. PRICE — right level, right fill?
1. **Recompute the level** from `entry_price` + `config`, tick-snapped:
   - SL %: `entry × (1 ∓ v/100)` · points: `entry ∓ v` · ATR: `entry ∓ mult×ATR_at_entry`.
   - Trailing/Move-to-Cost: the **ratcheted** value (re-walk the steps).
   - The logged `current_sl` / `level_price` must equal your recomputation.
2. **Fill price** matches the fill model (limit level, or VWAP-conservative — *pending the parked fill-price question*).
3. **PnL** = `(exit − entry) × lot_mult × n_lots` matches `leg_pnl` (and the combined/user roll-ups add up).

---

## 3. Per-feature timing — what "right time" means

| Feature | "Right time" | How to check from the order book |
|---|---|---|
| Leg SL / Target trigger | first bar the level is reached | event `bar_index` == first bar where `low ≤ SL` / `high ≥ Tgt` |
| SL / Target **Wait** | trigger + wait | exit bar == trigger bar + `wait` (bars) **or** exit time == trigger time + `wait` (sec) |
| **Safety Seconds** (Move-to-Cost) | trigger-event time + safety | MOVE_TO_COST applied `safety_seconds` after the triggering event's timestamp |
| **Trail update** | bar where PnL crosses the next step | `current_sl` changes on **that** bar; no change between steps |
| Move-to-Cost trigger | bar a sibling leg hit SL/Target | MOVE_TO_COST same `bar_index` as the sibling's exit |
| Portfolio SL / Target | bar combined PnL / underlying crosses | PORTFOLIO_SQOFF on that bar |
| User Max Loss / Profit | bar combined **user** PnL crosses | USER_SQOFF — **all** portfolios same bar |
| MIS cutoff | the cutoff bar (e.g. 15:20) | MIS_CUTOFF at the cutoff `bar_index`; nothing forced before it |
| Evaluation order | within one bar | User row → Portfolio row → Leg row in `seq` order |

---

## 4. The verification loop (step by step)

1. **Run** the test portfolio → **export the order book** (schema §1).
2. **Sort** by `timestamp` / `seq`.
3. For **each event row**, run the **A-Time / B-Logic / C-Price** checks (§2).
4. **Recompute** expected values from the row's *own* inputs — independent of the engine's claim.
5. Mark **green** (engine == your recomputation) or **red** (mismatch). Each red row records: the column, expected, actual.
6. For **dynamic features** (trail, move-to-cost, profit-lock, user cascade), walk the rows **in sequence** — the state (`current_sl`, anchor) must evolve exactly as your manual trace says, bar by bar.

---

## 5. Worked example — leg SL with Wait = 2 bars

Config: NIFTY-FUT, long, `percentage 2%`, `SL Wait = 2 bars`, MIS.

| seq | bar | event | reason | bar low | current_sl | fill | leg_pnl |
|---|---|---|---|---|---|---|---|
| 1 | 5 | ENTRY | — | — | 21560 | 22000 | 0 |
| 2 | 100 | SL_TRIGGER | SL | 21558 | 21560 | — | — |
| 3 | 101 | (hold) | SL_WAIT | 21557 | 21560 | — | — |
| 4 | 102 | SL_EXIT | SL | 21556 | 21560 | 21560 | −440 / unit |

**A-Time:** trigger at bar 100 (first `low ≤ 21560` ✓); exit at bar **102 = 100 + 2** (the wait) ✓ — not 100, not 101.
**B-Logic:** `reason = SL` is correct (level reached, held through the wait, not cleared); single exit; leg ACTIVE→CLOSED.
**C-Price:** SL level = `22000 × 0.98 = 21560` (tick 0.05 → 21560) ✓; fill = 21560; PnL = `(21560 − 22000) = −440 / unit` ✓.
→ all green.

---

## 6. Timing & logic pitfalls to watch (the subtle ones)

| Pitfall | What to look for |
|---|---|
| **Off-by-one bar** | exit fires one bar early/late vs the trigger + wait |
| **Look-ahead** | a decision uses a price it couldn't know yet (e.g. the bar **close** to justify an intrabar exit, or a future bar) |
| **Intrabar ordering** | on a bar that both makes a new high *and* breaches a stop — which the engine assumed happened first |
| **Same-bar precedence** | when user + portfolio + leg all trigger together, did they fire in User→Portfolio→Leg order |
| **Wait units** | `wait` applied in **bars** when it should be **seconds** (or vice-versa) on that timeframe |
| **Snapped vs raw** | `current_sl` compared at the raw value when the engine (correctly) snapped to tick |
| **Stale level after move** | after Move-to-Cost / a trail step, later bars still checked against the **old** level |

---

## 7. For the dev team — self-verify before handoff (the same lens)

Verification isn't only the tester's job. Before you hand a build off, run the **same Time / Logic / Price lens** on a small sample so the obvious breaks are caught at source.

**Before handoff, for at least one trade per feature you touched:**
1. **Emit, then read your own order book.** Every event row must carry the §1 columns — especially per-bar `current_sl`, anchors, `leg_pnl`/`combined_pnl`, and the `bar_o/h/l/c` that caused the row. If a row can't be re-derived from its own columns, it isn't verifiable — fix the emit first.
2. **TIME.** For each exit, find the first bar the condition is true in your own bar data; the event must land on *that* bar (+wait/+safety), not where you happened to evaluate it. Watch off-by-one, look-ahead (using the close to justify an intrabar exit), and wait-units (bars vs seconds).
3. **LOGIC.** The `reason` must be the condition actually true — not a default. Assert the guards **in code**: trailing only-tightens, profit-lock floor only-rises, re-exec count caps, fixed Target suppressed while Trailing Target active.
4. **PRICE.** Recompute the level from `entry_price` + config, tick-snap it; logged `current_sl` must equal it. `PnL = (exit − entry) × lot_mult × n_lots` must tie to `leg_pnl` and roll up.
5. **State machine.** Every position ends in a terminal state (CLOSED / IDLE / REENTRY_WAITING) consistent with the action; no orphaned ACTIVE legs after a force-sqoff.

**Invariants to assert in the engine itself** (cheap, catch whole classes of bugs):
- `current_sl` for a long never decreases after a trail/move step; for a short never increases.
- After `MOVE_TO_COST`, later bars are checked against the **new** level (no stale level).
- A user/portfolio force-sqoff flattens **every** child position on the **same bar**; nothing re-enters after a sticky user hit.
- Same-bar precedence is always **User → Portfolio → Leg**.

**What "done" means:** the build emits the §1 order book, your sample passes all three axes, and the engine-level invariants hold. Hand off the order book + logs so the tester can re-run the full **§4 loop** independently — recomputing from your logged inputs, not from your claims.
