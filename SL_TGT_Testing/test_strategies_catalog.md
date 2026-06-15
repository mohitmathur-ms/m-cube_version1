# Test Strategies — EMA-Cross Harness Catalog (TS-1 … TS-10)

Runnable strategies that exercise each SL/Target feature **end-to-end on real data**, using a trivial **EMA-cross** as a deterministic entry trigger. Each TS isolates one feature. Because you don't control the exact entry price, every check is verified **relative to the realised fill**.

> Full "ask the dev team / verify yourself" recipe: `leg_sl_test_scenarios` §7. This doc is the strategy list + per-trade assertions.

## How to read a TS
- **Goal** — the feature it isolates.
- **Setup** — instrument · timeframe · EMA params · entry side · feature config.
- **Per-trade check** — applied to *every* trade the strategy generates.
- **Pass** — overall criterion.

**Shared entry trigger:** EMA(fast)/EMA(slow) crossover — long on bullish cross, short on bearish — one entry per cross. SL/Target verified as `expected = formula(fill, config)`, tick-snapped, compared to the engine's logged value.

---

## Catalog

### TS-1 · Leg fixed SL (percentage) — single leg
- **Goal:** leg SL trigger + fill, end-to-end.
- **Setup:** NIFTY-FUT · 1-min · EMA(9/21) · long · 1 leg · `stop_loss_type=percentage, value=2` · no target.
- **Per-trade:** `expected_sl = fill × (1 − 2/100)`, snapped; SL exit fires on the first bar with `low ≤ expected_sl`, fills at the SL level.
- **Pass:** every trade's engine `current_sl` == expected_sl; every SL exit matches the trigger bar + fill.

### TS-2 · Leg Target + SL — single leg, short
- **Goal:** leg Target and SL on the short side.
- **Setup:** 6E · 1-min · EMA(9/21) · short · 1 leg · Target `Premium 1%`, SL `Premium 1%`.
- **Per-trade:** short → `tgt = fill × (1 − 1/100)` (below), `sl = fill × (1 + 1/100)` (above); exit on whichever the bar hits first (`high ≥ sl` or `low ≤ tgt`).
- **Pass:** computed tgt/sl match; correct side fires; never both on the same trade.

### TS-3 · Leg SL Wait (delay)
- **Goal:** the SL-Wait confirmation gate.
- **Setup:** NIFTY-FUT · 1-min · long · SL `percentage 2` · `SL Wait = 2 bars`.
- **Per-trade:** on SL trigger, exit only after the level holds 2 bars; if price recovers above the SL within the wait, **no exit** (DELAY_CLEARED).
- **Pass:** delayed exits fire exactly 2 bars after trigger; recovered ones don't exit.

### TS-4 · ATR SL
- **Goal:** ATR-based SL sizing per trade (volatility-adaptive).
- **Setup:** BTC · 1-min · long · `stop_loss_type=atr, sl_atr_period=14, sl_atr_multiplier=1.5`.
- **Per-trade:** read ATR(14) at the entry bar → `expected_sl = fill − 1.5×ATR`, snapped.
- **Pass:** engine `current_sl` == expected_sl using the logged ATR-at-entry; warm-up legs (ATR not ready) arm **no** SL.

### TS-5 · Move SL to Cost + Trail After — multi-leg
- **Goal:** Move-SL-to-Cost (triggered by a sibling leg) + Trail-After ratchet (**anchor = curr_leg_pnl**).
- **Setup:** GC · 1-min · EMA · 2 legs entered together · `move_sl_to_cost.enabled`, `trail_after_move_sl=True`, `sl_trail every=X, by=Y`.
- **Per-trade:** when leg A hits its SL/Target, leg B's `sl == leg-B entry_price` (breakeven); thereafter leg B ratchets — for every `every` of **leg-B PnL gain** (anchor = leg-B PnL at the move), `sl ± by`. **Confirm anchor is PnL** (it must ratchet identically on GC's ~2000 price, not freeze).
- **Pass:** move-to-cost fires on the sibling event; trail ratchets per spec; no freeze.

### TS-6 · Portfolio Combined Loss + leg-breach validation — multi-leg
- **Goal:** portfolio Combined Loss SL + the "Σ leg SLs ≤ portfolio SL" validation.
- **Setup:** BANKNIFTY-FUT · 1-min · 3 legs whose individual SLs **sum to more than** the portfolio Combined Loss SL.
- **Per-run:** on config save the engine **flags the breach**; at run, the portfolio force-sqoffs all legs when `combined_pnl ≤ −sl_val` (before the individual leg SLs would).
- **Pass:** breach flagged on save; portfolio exit precedes the leg exits; all legs close on the same bar.

### TS-7 · Portfolio Trailing SL + Profit-Lock Target
- **Goal:** the finalised portfolio trailing — step Trailing SL + Profit-Lock.
- **Setup:** NIFTY-FUT · 1-min · 1 portfolio · Trailing SL `every/by`, Trailing Target `when_profit_reach / lock_min_profit / every / by`.
- **Per-run:** Trailing SL tightens `current_sl -= steps×by` per `every` of combined profit (`anchor_pnl=0`); Profit-Lock activates at `when_profit_reach`, floor ratchets up, exits on `curr_pnl ≤ current_stop`.
- **Pass:** both ratchets match spec; SL only tightens; lock only rises; the fixed target is suppressed while the lock is active.

### TS-8 · User Max Loss / Max Profit — 2 portfolios
- **Goal:** user-level aggregation + cascade square-off.
- **Setup:** 1 user · 2 portfolios (NIFTY-FUT + 6E) · `max_loss`, `max_profit` (absolute or % of allocation).
- **Per-run:** when combined **user** PnL ≤ `max_loss` (or ≥ `max_profit`), **all** portfolios force-sqoff on the same bar; `gst["hit"]` sticky for the day; Max Loss checked before Max Profit.
- **Pass:** the cascade closes both books simultaneously; no further entries after the hit.

### TS-9 · Leg actions — Re-Execute / ReEntry
- **Goal:** On-SL-Action dispatch + count limits.
- **Setup:** NIFTY-FUT · 1-min · long · SL `percentage 2` · On SL Action = Re-Execute (count 2).
- **Per-trade:** after the SL, the leg → IDLE → re-enters at market next bar; max 3 entries total; the 4th attempt suppressed (`LEG_REEXEC_LIMIT_REACHED`).
- **Pass:** re-entries occur at market on the next bar; the count is respected.

### TS-10 · MIS intraday auto-square-off  *(pending TBD-7)*
- **Goal:** MIS cutoff forced exit vs NRML carry.
- **Setup:** NIFTY-FUT · 1-min · product = MIS · cutoff 15:20 (run the same config as NRML for contrast).
- **Per-run:** any position still open at 15:20 → forced `cutoff` exit; positions that hit SL earlier exit normally; under NRML the position carries.
- **Pass:** open positions flat at the cutoff under MIS; carry under NRML. **Blocked until product-type/cutoff (TBD-7) is built.**

---

## Coverage note
Run **TS-1** and **TS-4** across the asset × timeframe matrix (NIFTY-FUT · BANKNIFTY-FUT · US index futures · 6E · GC · BTC × 1m · 3m · 5m · 2h · 1d · 1w · 1M) to confirm tick-snapping, PnL multiplier, currency, and the ATR-by-timeframe effect. The rest are single-asset feature-isolation runs.

## Verify-yourself loop (every TS)
1. Run the strategy; export the **orderbook** (CSV/JSON) — fills, exits, reasons, the engine's SL/level values.
2. For each **entry**, recompute the SL/Target/level from the **logged fill** + config; compare to the engine — must match to the tick.
3. For each **exit**, confirm the trigger condition (bar low/high vs level) and the fill.
4. Record green / red; each red row carries the offending trade for the dev team.
