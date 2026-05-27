# Test Portfolios — SL/Target Full Feature Coverage

Generated from `sl_tgt_test_portfolios.html`. One JSON per test case (TL / TP /
TX / TC). User-level (TU) cases are **not** included here — they require
`config/users.json` edits, not a portfolio file.

These files are loaded exactly like any saved portfolio
(`core/models.serialization.portfolio_from_dict`). To run one:
upload/select it in the Portfolio UI, or call `run_portfolio_backtest`.

## Instrument mapping (HTML → this catalog)

The catalog has bars only for the instruments below, so the HTML's
NIFTY/BANKNIFTY/US-index/6E/GC/BTC set is mapped to the closest runnable
instrument (BANKNIFTY, US-index futures and Gold bars are absent → substituted).

| HTML instrument | bar_type_str (use verbatim) | lots | ccy | notes |
|---|---|---|---|---|
| NIFTY-FUT | `COMPLETE_DF_YYYYINR.NIFTY_FUTURES_MS-1-MINUTE-LAST-EXTERNAL` | 1 | INR | LAST price |
| 6E (EUR fut) | `EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL` | 0.05 | USD | has ASK/BID → supports `bidask` format |
| GC (Gold) | `LIGHTCMDUSDUSD.COMMODITIES_MS-1-MINUTE-MID-EXTERNAL` | 1 | USD | commodity proxy (no Gold bars) |
| BTC | `USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL` | 0.05 | USD | remapped: BTC bars are orphaned (no matching instrument), so TL-03 uses USDJPY |
| BANKNIFTY-FUT | `COMPLETE_DF_YYYYINR.NIFTY_FUTURES_MS-1-MINUTE-LAST-EXTERNAL` | 1 | INR | substituted with NIFTY |
| US index fut | `USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL` | 0.05 | USD | substituted with USDJPY |

Common runnable window for every file: `start_date "2024-01-01"`,
`end_date "2024-01-31"` (all instruments have data here).

## Wiring caveats (record in each file's `description`)

- `product`/MIS cutoff (TX-2) is **TBD-7 — not wired** for backtest.
- Cross-portfolio actions (TP-11, TC-6) need cross-portfolio dispatch infra and
  may be downgraded to `SqOff` at run time.
- `move_sl_action` "LTP + Buffer" variant is options-flavoured (may warn/downgrade).
- Underlying levels (TP-2/3/5) are taken verbatim from the HTML
  (BANKNIFTY-scale, e.g. 48000); on the substituted NIFTY they may not be
  crossed — config is valid, trigger is illustrative.

## Feature → schema field mapping key

**Leg-level (per-slot `exit_config`):**
| HTML feature | fields |
|---|---|
| SL percentage | `stop_loss_type="percentage"`, `stop_loss_value=<pct>` |
| SL points | `stop_loss_type="points"`, `stop_loss_value=<abs>` |
| SL ATR | `stop_loss_type="atr"`, `sl_atr_period=<p>`, `sl_atr_multiplier=<m>` |
| Target percentage | `target_type="percentage"`, `target_value=<pct>` |
| Target points | `target_type="points"`, `target_value=<abs>` |
| SL Wait (bars) | `sl_wait_bars=<n>` |
| Leg action SqOff | `on_sl_action="close"` |
| Leg action Re-Execute | `on_sl_action="re_execute"`, `max_re_executions=<n>` |
| Leg action ReEntry | `on_sl_action="re_entry"`, `max_re_entries=<n>` |
| Leg action KeepLegRunning | `on_sl_action="keep_leg_running"` |
| Leg action Execute (sibling) | leg A `on_sl_action="execute"`, `execute_target_leg_id="<slot_id of B>"`; leg B `armed_at_start=false` |
| Action combination (valid) | `on_sl_action="re_execute,execute"` (comma-separated; ≤3) |

**Portfolio-level (top-level fields):**
| HTML feature | fields |
|---|---|
| Combined Loss | `pf_sl_enabled=true`, `pf_sl_type="Combined Loss"`, `pf_sl_value=<v>`, `pf_sl_action="SqOff"` |
| Underlying Movement (SL) | `pf_sl_enabled=true`, `pf_sl_type="Underlying Movement"`, `pf_sl_value=<level>` |
| Loss and Underlying Range | `pf_sl_enabled=true`, `pf_sl_type="Loss and Underlying Range"`, `pf_sl_value=<v>`, `pf_sl_underlying_below=<lo>`, `pf_sl_underlying_above=<hi>` |
| Combined Profit | `pf_tgt_enabled=true`, `pf_tgt_type="Combined Profit"`, `pf_tgt_value=<v>`, `pf_tgt_action="SqOff"` |
| Underlying Movement (Target) | `pf_tgt_enabled=true`, `pf_tgt_type="Underlying Movement"`, `pf_tgt_value=<level>` |
| Trailing SL (step) | `pf_sl_enabled=true`, `pf_sl_type="Combined Loss"`, `pf_sl_value=<v>`, `pf_sl_trail_enabled=true`, `pf_sl_trail_every=<e>`, `pf_sl_trail_by=<b>` |
| Trailing Target / Profit Lock | `pf_tgt_enabled=true`, `pf_tgt_trail_enabled=true`, `pf_tgt_trail_when_profit_reach=<r>`, `pf_tgt_trail_lock_min_profit=<lock>`, `pf_tgt_trail_every=<e>`, `pf_tgt_trail_by=<b>` |
| Move SL to Cost (profitable-only) | `move_sl_enabled=true`, `move_sl_action="Move Only for Profitable Legs"`, `move_sl_hit_on_leg_sl=true` |
| Move SL to Cost (all legs) | `move_sl_enabled=true`, `move_sl_action="Move SL for All Legs Despite Loss/Profit"` |
| Move SL to Cost (LTP+buffer) | `move_sl_enabled=true`, `move_sl_action="Move SL to LTP + Buffer for Loss Making Legs"`, `move_sl_ltp_buffer=<b>` |
| Move SL — Trail After | `move_sl_enabled=true`, `move_sl_trail_after=true` |
| Move SL — portfolio-driven (agg PnL) | `move_sl_enabled=true`, `move_sl_agg_pnl_enabled=true`, `move_sl_agg_pnl_threshold=<v>`, `move_sl_agg_pnl_direction="profit"` |
| Portfolio Delay (N bars) | `pf_sl_delay_sec=<N×60>` (1-min bars) |
| SqOff filters | `pf_sl_sqoff_only_loss_legs=true` (or `pf_sl_sqoff_only_profit_legs=true`) |
| Cross-portfolio Start | `pf_sl_action="Start Other Portfolio"`, `pf_sl_target_portfolio="<other file name>"` |
| Safety Seconds | `move_sl_enabled=true`, `move_sl_safety_sec=<sec>` |

**Cross-cutting:**
| HTML feature | fields |
|---|---|
| Three-format A/B/C | `exit_config.exit_price_format` = `"bidask"` / `"ohlcv"` / `"ltp"` (3 files; bidask → use EURUSD) |
| MIS cutoff | `product="MIS"`, `mis_squareoff_time="15:20"` |
| NRML carry | `product="NRML"` |
| Timeframe sweep | vary the timeframe in `bar_type_str` (e.g. `-5-MINUTE-`, `-1-HOUR-`, `-1-DAY-`) |

## Slot template (EMA Cross entry, one per leg)

```json
{
  "slot_id": "<unique 8-hex>",
  "strategy_name": "EMA Cross",
  "strategy_params": {"fast_ema_period": 9, "slow_ema_period": 21},
  "bar_type_str": "<from instrument map>",
  "strategy_bar_types": [],
  "lots": "<from instrument map>",
  "allocation_pct": 0.0,
  "exit_config": { /* ExitConfig defaults below, with per-case deltas */ },
  "enabled": true,
  "start_date": null, "end_date": null,
  "squareoff_time": null, "squareoff_tz": null
}
```

ExitConfig defaults (apply per-case deltas on top):
```json
{
  "exit_price_format": "ohlcv",
  "stop_loss_type": "none", "stop_loss_value": 0.0,
  "trailing_sl_step": 0.0, "trailing_sl_offset": 0.0,
  "sl_atr_period": 0, "sl_atr_multiplier": 0.0,
  "target_type": "none", "target_value": 0.0,
  "tgt_atr_period": 0, "tgt_atr_multiplier": 0.0,
  "target_lock_trigger": 0.0, "target_lock_minimum": 0.0,
  "tgt_trail_enabled": false, "tgt_trail_when_profit_reach": 0.0,
  "tgt_trail_lock_min_profit": 0.0, "tgt_trail_every": 0.0, "tgt_trail_by": 0.0,
  "sl_wait_sec": 0, "sl_wait_bars": 0, "tgt_wait_sec": 0, "tgt_wait_bars": 0,
  "on_sl_action": "close", "on_target_action": "close", "max_re_executions": 0,
  "execute_target_leg_id": "", "reentry_price": 0.0, "max_re_entries": 0,
  "armed_at_start": true, "squareoff_time": null, "squareoff_tz": null
}
```

## Case index (file name → structure → deltas)

Legs all use EMA Cross 9/21. "PF-1L/PF-1S" = 1 leg; "PF-2" = 2 legs same
instrument; "PF-3" = 3 legs same instrument; "PF-X" = 1 leg.

### Leg-level (exit_config)
- **TL-01_sl_pct.json** — 1 leg NIFTY-FUT — `stop_loss_type=percentage, stop_loss_value=2`
- **TL-02_sl_points.json** — 1 leg 6E — `stop_loss_type=points, stop_loss_value=0.0050`
- **TL-03_sl_atr.json** — 1 leg BTC — `stop_loss_type=atr, sl_atr_period=14, sl_atr_multiplier=2.0`
- **TL-04_tgt_pct.json** — 1 leg GC — `target_type=percentage, target_value=3`
- **TL-05_tgt_points.json** — 1 leg 6E — `target_type=points, target_value=0.0080`
- **TL-06_sl_wait.json** — 1 leg NIFTY-FUT — `stop_loss_type=percentage, stop_loss_value=2, sl_wait_bars=3`
- **TL-07_movecost_profitonly.json** — 2 legs GC — pf: `move_sl_enabled=true, move_sl_action="Move Only for Profitable Legs", move_sl_hit_on_leg_sl=true`; each leg `stop_loss_type=percentage, stop_loss_value=2`
- **TL-08_movecost_all.json** — 2 legs GC — pf: `move_sl_enabled=true, move_sl_action="Move SL for All Legs Despite Loss/Profit"`; legs SL pct 2
- **TL-09_movecost_ltpbuffer.json** — 3 legs NIFTY-FUT — pf: `move_sl_enabled=true, move_sl_action="Move SL to LTP + Buffer for Loss Making Legs", move_sl_ltp_buffer=0.0005`; legs SL pct 2
- **TL-10_trail_after_move.json** — 2 legs GC — pf: `move_sl_enabled=true, move_sl_trail_after=true`; legs SL pct 2  (note: per-step every/by has no schema field)
- **TL-11_action_sqoff.json** — 1 leg NIFTY-FUT — `stop_loss_type=percentage, stop_loss_value=2, on_sl_action="close"`
- **TL-12_action_reexecute.json** — 1 leg NIFTY-FUT — `stop_loss_type=percentage, stop_loss_value=2, on_sl_action="re_execute", max_re_executions=2`
- **TL-13_action_reentry.json** — 1 leg NIFTY-FUT — `stop_loss_type=percentage, stop_loss_value=2, on_sl_action="re_entry", max_re_entries=1`
- **TL-14_action_keeplegrunning.json** — 2 legs GC — `stop_loss_type=percentage, stop_loss_value=2, on_sl_action="keep_leg_running"`
- **TL-15_action_execute.json** — 2 legs GC — leg A `stop_loss_type=percentage, stop_loss_value=2, on_sl_action="execute", execute_target_leg_id="<B slot_id>"`; leg B `armed_at_start=false`
- **TL-16_action_combo.json** — 1 leg NIFTY-FUT — `stop_loss_type=percentage, stop_loss_value=2, on_sl_action="re_execute,execute", max_re_executions=2` (valid combo)

### Portfolio-level (PF-3 = 3 legs NIFTY-FUT unless noted)
- **TP-01_combined_loss.json** — `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000`
- **TP-02_underlying_sl.json** — `pf_sl_enabled=true, pf_sl_type="Underlying Movement", pf_sl_value=48000`
- **TP-03_loss_and_range.json** — `pf_sl_enabled=true, pf_sl_type="Loss and Underlying Range", pf_sl_value=5000, pf_sl_underlying_below=47800, pf_sl_underlying_above=48200`
- **TP-04_combined_profit.json** — `pf_tgt_enabled=true, pf_tgt_type="Combined Profit", pf_tgt_value=6000`
- **TP-05_underlying_tgt.json** — `pf_tgt_enabled=true, pf_tgt_type="Underlying Movement", pf_tgt_value=48500`
- **TP-06_trailing_sl.json** — `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000, pf_sl_trail_enabled=true, pf_sl_trail_every=1000, pf_sl_trail_by=500`
- **TP-07_trailing_target.json** — `pf_tgt_enabled=true, pf_tgt_trail_enabled=true, pf_tgt_trail_when_profit_reach=1000, pf_tgt_trail_lock_min_profit=600, pf_tgt_trail_every=500, pf_tgt_trail_by=300`
- **TP-08_movecost_portfolio.json** — `move_sl_enabled=true, move_sl_action="Move Only for Profitable Legs", move_sl_agg_pnl_enabled=true, move_sl_agg_pnl_threshold=300, move_sl_agg_pnl_direction="profit"`
- **TP-09_portfolio_delay.json** — `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000, pf_sl_delay_sec=180`
- **TP-10_sqoff_filter_loss.json** — `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000, pf_sl_sqoff_only_loss_legs=true`
- **TP-11_crossportfolio_A.json** + **TP-11_crossportfolio_B.json** — PF-U: file A 3 legs NIFTY-FUT with `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000, pf_sl_action="Start Other Portfolio", pf_sl_target_portfolio="TP-11_crossportfolio_B"`; file B 1 leg 6E baseline. (cross-PF may downgrade — note in description)
- **TP-12_leg_breach.json** — 3 legs NIFTY-FUT each `stop_loss_type=percentage, stop_loss_value=5`; pf `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000` (Σ leg SL intended > pf SL → breach flag)

### Cross-cutting
- **TX-01_format_A_bidask.json** / **TX-01_format_B_ohlcv.json** / **TX-01_format_C_ltp.json** — 1 leg 6E (EURUSD), identical `stop_loss_type=percentage, stop_loss_value=2`, differing only by `exit_price_format` = bidask / ohlcv / ltp
- **TX-02_mis_cutoff.json** — 1 leg NIFTY-FUT, `product="MIS", mis_squareoff_time="15:20"`, `stop_loss_type=percentage, stop_loss_value=2` (TBD-7 — unwired)
- **TX-03_nrml_carry.json** — 1 leg NIFTY-FUT, `product="NRML"`, SL pct 2
- **TX-04_tf_1m.json** / **TX-04_tf_5m.json** / **TX-04_tf_1h.json** / **TX-04_tf_1d.json** — 1 leg NIFTY-FUT, SL pct 2, varying timeframe in bar_type_str: `-1-MINUTE-` / `-5-MINUTE-` / `-1-HOUR-` / `-1-DAY-` (keep `LAST-EXTERNAL`)
- **TX-05_tick_snap.json** — 1 leg NIFTY-FUT, SL pct 2 (engine snaps SL to tick)
- **TX-06_safety_seconds.json** — 1 leg NIFTY-FUT, `move_sl_enabled=true, move_sl_safety_sec=60`, SL pct 2

### Combination (the tricky ones)
- **TC-01_samebar_precedence_A.json** + **TC-01_samebar_precedence_B.json** — PF-U; file A 3 legs NIFTY-FUT with tight legs `stop_loss_type=percentage, stop_loss_value=0.2` + `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=1000`; file B 1 leg 6E. (User Max-Loss part needs config/users.json — note it)
- **TC-02_trailstep_breach.json** — 3 legs NIFTY-FUT, `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000, pf_sl_trail_enabled=true, pf_sl_trail_every=1000, pf_sl_trail_by=500`
- **TC-03_movecost_gap.json** — 2 legs GC, `move_sl_enabled=true, move_sl_action="Move Only for Profitable Legs", move_sl_hit_on_leg_sl=true`
- **TC-04_reexec_instant_restop.json** — 1 leg NIFTY-FUT, tight `stop_loss_type=percentage, stop_loss_value=0.2, on_sl_action="re_execute", max_re_executions=2`
- **TC-05_trail_vs_fixed.json** — 3 legs NIFTY-FUT, `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000, pf_sl_trail_enabled=true, pf_sl_trail_every=500, pf_sl_trail_by=400`
- **TC-06_crosspf_race_A.json** + **TC-06_crosspf_race_B.json** — PF-U; A NIFTY-FUT `pf_sl_action="Start Other Portfolio", pf_sl_target_portfolio="TC-06_crosspf_race_B", pf_sl_delay_sec=2`; B 6E baseline
- **TC-07_boundary_threshold.json** — 3 legs NIFTY-FUT, `pf_sl_enabled=true, pf_sl_type="Combined Loss", pf_sl_value=5000`
