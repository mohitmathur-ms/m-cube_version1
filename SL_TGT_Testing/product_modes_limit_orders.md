# Product Modes (MIS / CNC / NRML) + Limit-Order Fill — Spec & Tests

The engine is a **backtester using limit orders** (no market orders). This doc defines the limit-order fill model, the three product modes, and the test coverage for each. Equity (cash) is now in scope (CNC).

---

## 1. Limit-order fill model (backtest)

SL/Target exits are **limit orders placed at the level** (tick-snapped — a limit must sit on a valid tick).

| Leg / side | Limit sits | Fills when | Fill price |
|---|---|---|---|
| Long — SL (below) | sell limit at SL | `bar_low ≤ SL` | SL |
| Long — Target (above) | sell limit at Target | `bar_high ≥ Target` | Target |
| Short — SL (above) | buy limit at SL | `bar_high ≥ SL` | SL |
| Short — Target (below) | buy limit at Target | `bar_low ≤ Target` | Target |

- **Reached → fill at the limit price** (no slippage in the base model).
- **Not reached → no exit that bar**; the position **carries** to the next bar, the limit still resting.
- **Per format:** the "reached" check uses the format's price — A: bid/ask, B: bar high/low, C: ltp.

> **Gap caveat (known limitation):** if a bar **opens beyond** the limit (gaps through it), the base backtest still assumes a fill **at the limit** — optimistic. A gap-aware variant fills at the **bar open** instead. Decide which the engine uses; test both (PM-6).

---

## 2. Product modes

The SL/Target **limit logic is identical** across modes. The mode governs the **position lifecycle / forced exit**, not the SL math.

| Mode | Lifecycle | Forced exit | Resting SL/Target limits | Instruments |
|---|---|---|---|---|
| **MIS** | intraday | **Yes — force-flat at the intraday cutoff** (e.g. NSE ~15:20) | cancelled at cutoff if unfilled | futures, equity intraday |
| **NRML** | carryforward | No | persist across sessions to expiry | futures (F&O) |
| **CNC** | delivery | No | persist to delivery | **equity (cash)** |

- **MIS cutoff** overrides any unfilled limit: at the cutoff the engine squares off the open position at the cutoff bar's price (reason `MIS_CUTOFF`).
- **NRML / CNC** never force intraday: an unfilled SL/Target limit simply carries.

---

## 3. Equity now in scope (CNC)

Add cash-equity instruments alongside futures:

| Asset | Example | Tick | Lot / unit | Currency | Mode |
|---|---|---|---|---|---|
| NSE equity (cash) | RELIANCE | 0.05 | 1 share | INR | CNC (delivery) / MIS (intraday) |
| US equity (cash) | AAPL | 0.01 | 1 share | USD | CNC / MIS |

(Futures stay NRML / MIS as before.)

---

## 4. Test cases — product modes

Base: long NIFTY-FUT, entry 22000, SL limit at **21560** (2%), unless noted.

**PM-1 · MIS — SL limit unfilled by cutoff.** Price never drops to 21560 by 15:20. At 15:20 → **force square-off** at the cutoff bar's price (e.g. 21900), reason `MIS_CUTOFF`; resting limit cancelled. *Pass:* exit at the cutoff, not at 21560.

**PM-2 · MIS — SL limit fills before cutoff.** At 13:00 `bar_low 21555 ≤ 21560` → SL limit **fills at 21560**, reason `SL`. Cutoff never reached. *Pass:* normal SL exit at 13:00.

**PM-3 · NRML — SL unfilled.** Price doesn't reach 21560 by EOD → **no forced exit**; position carries to the next session; SL limit still resting at 21560. *Pass:* no intraday square-off; limit persists.

**PM-4 · CNC (equity) — SL unfilled.** RELIANCE long, SL limit; unfilled by EOD → carries to next session (delivery); no auto-square-off. *Pass:* equity position carries; limit persists.

**PM-5 · Cross-mode equivalence.** Same SL config + same data, run **MIS / NRML / CNC**:
- Intraday: if the limit is reached, the **fill is identical** across all three (same price, same bar).
- EOD: **MIS squares off**; **NRML / CNC carry**.
*Pass:* SL fill identical intraday across modes; only the EOD handling differs.

**PM-6 · Gap / non-fill.** Bar gaps from 21600 → opens 21500 (through the 21560 limit). Base model fills **at 21560** (optimistic); gap-aware variant fills **at 21500** (the open). *Pass:* matches the chosen convention; flag the difference.

---

## 5. Spec reconciliation needed (limit orders + equity)

The limit-order reality and equity scope **contradict several finalised docs** — these need updating before the test fill-values are trusted:

| Doc / item | Was | Should be |
|---|---|---|
| `execution_logic` **§8.1 Exit Order Type** | MARKET active; Limit "not implemented" | **Limit is the live path**; MARKET unsupported (or N/A in backtest) |
| `sl_features` **TBD-2 (tick)** | "Don't snap — broker aligns at market fill" | **Snapping required** — a limit must sit on a valid tick *(this is why the implementation's `_snap_to_tick` was correct)* |
| `execution_logic` **§4.2 Fill model** | Conservative `MAX/MIN(vwap, hit)` | **Fill at the limit price** when the bar reaches it; carry if not; gap caveat |
| `sl_features` **Asset classes** | IF · FX · CF · XF (futures) | **Add equity (cash)** — NSE + US — for CNC |
| Test docs (`sl_target_test_plan`, `leg_sl_test_scenarios`, `sl_tgt_test_portfolios`) | fills assumed market/VWAP | **fills at the limit level**; add equity to the matrix; un-block MIS (PM-1) |
| `sl_features` **TBD-7 (product type)** | parked (CNC parked) | **Active** — MIS / NRML / CNC all in scope |

---

### What I'd do next (confirm before I touch the finalised docs)
1. **Update the order model** in `execution_logic` (§8.1 + §4.2) to limit-order fill, and **flip TBD-2** to "snap required."
2. **Add equity** to `sl_features` asset classes; **activate TBD-7** (MIS/NRML/CNC).
3. **Update the test docs'** fill expectations to limit fills, add equity to the coverage matrix, and un-block the MIS cutoff case (PM-1).

Say the word and I'll apply 1–3 across the docs. (Otherwise this doc stands alone as the product-mode + limit-order spec/tests.)
