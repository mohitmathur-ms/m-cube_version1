"""Generate the `portfolios/testing2/` manual-verification test set.

These portfolios back the verification methodology in
`SL_TGT_Testing/new_tests/manual_verification_v2.html`. The design rules
(straight from the test brief) are encoded here so the fixtures are
reproducible and every deviation from the default is explicit:

  * **Indian-market / MIS framing.** `product = "MIS"` (intraday trade type),
    and a HARD square-off at the **NSE close** (`squareoff_time = "15:29:00"`,
    `squareoff_tz = "Asia/Kolkata"`; `mis_squareoff_time` set to match). The
    NIFTY trading session is 09:15-15:30 IST (= 03:45-10:00 UTC).
  * **Why 15:29 and not 15:30.** The strategy fires the square-off on the first
    bar whose LOCAL (IST) time >= the cutoff minute. The NSE close is 15:30 IST,
    but this 1-second feed's last bar each day is **15:29:59 IST** (minute 29) —
    so a literal `15:30:00` cutoff (minute 30) would never be reached and would
    be DORMANT (the old 23:59 problem). `15:29:00` fires on the closing-minute
    bars (15:29:00..15:29:59), i.e. at the session close, which is the intent.
  * **The square-off now FIRES every session.** Any position still open at
    15:29 IST is force-closed (reason `squareoff`) on the first 15:29 bar,
    re-entry blocked until the next session. The square-off check runs BEFORE
    SL/TP, so on a bar where both would trigger the square-off wins (outer
    envelope). A position the feature never exits is thus squared off daily at
    the close rather than carried; only a position opened on the final session
    and not yet at 15:29 is flattened by `on_stop` at the backtest data end.
  * **One feature isolated per file** so a red result points at one cause.
  * **Runnable on the only data in the catalog** — NIFTY. We use the proven
    `NIFTY_FUT_SL_TEST` shape: the 1-SECOND spot feed with a 5-MINUTE
    composite strategy bar type (fine intrabar granularity for verifying the
    exact trigger second + price), window 2026-03-02 .. 2026-03-20.

Run:  venv\\Scripts\\python.exe portfolios\\testing2\\_generate_portfolios.py
"""
from __future__ import annotations

import copy
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

# ── The only runnable feed in this catalog (verified) ─────────────────────────
FEED = "NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS-1-SECOND-LAST-EXTERNAL"
STRAT_BARS = ["NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS-5-MINUTE-LAST-INTERNAL@1-SECOND-EXTERNAL"]
START_DATE = "2026-03-02"
END_DATE = "2026-03-20"

EXIT_DEFAULTS = {
    "exit_price_format": "ohlcv",
    "stop_loss_type": "none", "stop_loss_value": 0,
    "sl_value_is_absolute": None,
    "trailing_sl_step": 0, "trailing_sl_offset": 0,
    "sl_atr_period": 0, "sl_atr_multiplier": 0,
    "target_type": "none", "target_value": 0,
    "target_value_is_absolute": None,
    "tgt_atr_period": 0, "tgt_atr_multiplier": 0,
    "target_lock_trigger": 0, "target_lock_minimum": 0,
    "tgt_trail_enabled": False, "tgt_trail_when_profit_reach": 0,
    "tgt_trail_lock_min_profit": 0, "tgt_trail_every": 0, "tgt_trail_by": 0,
    "sl_wait_sec": 0, "sl_wait_bars": 0, "tgt_wait_sec": 0, "tgt_wait_bars": 0,
    "on_sl_action": "close", "on_target_action": "close", "max_re_executions": 0,
    "execute_target_leg_id": "", "reentry_price": 0, "max_re_entries": 0,
    "armed_at_start": True, "squareoff_time": None, "squareoff_tz": None,
}


def slot(slot_id: str, fast: int, slow: int, exit_deltas: dict | None = None, lots: int = 1) -> dict:
    ec = copy.deepcopy(EXIT_DEFAULTS)
    if exit_deltas:
        ec.update(exit_deltas)
    return {
        "slot_id": slot_id,
        "strategy_name": "EMA Cross",
        "strategy_params": {"fast_ema_period": fast, "slow_ema_period": slow},
        "bar_type_str": FEED,
        "strategy_bar_types": list(STRAT_BARS),
        "lots": lots,
        "allocation_pct": 0,
        "exit_config": ec,
        "enabled": True,
        "start_date": None, "end_date": None,
        "squareoff_time": None, "squareoff_tz": None,
    }


def base_portfolio(name: str, description: str, slots: list[dict], pf_deltas: dict | None = None) -> dict:
    pf = {
        "name": name,
        "description": description,
        "portfolio_tag": None,
        "created_at": "2026-06-16T00:00:00+00:00",
        "updated_at": "2026-06-16T00:00:00+00:00",
        "starting_capital": 300000,
        "max_loss": None,
        "max_profit": None,
        "allocation_mode": "equal",
        "start_date": START_DATE,
        "end_date": END_DATE,
        # ── MIS trade type + HARD square-off at the NSE close, 15:29 IST.
        #    NSE closes 15:30 IST, but this 1-sec feed's last bar each day is
        #    15:29:59 IST (minute 29) — a literal 15:30:00 cutoff (minute 30)
        #    would never be reached (dormant), so 15:29:00 is used to fire on the
        #    closing-minute bars. Any position open at 15:29 IST is force-closed
        #    (reason squareoff), re-entry blocked until next session. Runs BEFORE
        #    SL/TP (outer envelope). ──
        "squareoff_time": "15:29:00",
        "squareoff_tz": "Asia/Kolkata",
        "product": "MIS",
        "mis_squareoff_time": "15:29:00",
        "mis_squareoff_tz": "Asia/Kolkata",
        "run_on_days": None,
        # ── ENTRY WINDOW LEFT OPEN (session 09:15-15:30 IST already bounds the data)
        #    so no bars are dropped and exits can fire any time within the session ──
        "entry_start_time": None,
        "entry_end_time": None,
        "winter_time_adjust": False,
        "winter_time_auto": False,
        "rbo_enabled": False,
        "range_monitoring_start": "04:00:00",
        "range_monitoring_end": "05:00:00",
        "rbo_entry_start": "05:00:00",
        "rbo_entry_end": "10:45:00",
        "rbo_range_buffer": 0,
        "rbo_entry_at": "Any",
        "rbo_monitoring": "Underlying",
        "rbo_cancel_other_side": False,
        "delay_between_legs_sec": 0,
        "on_sl_action_on": "OnSL_N_Trailing_Both",
        "on_target_action_on": "OnTarget_N_Trailing_Both",
        "straddle_width_multiplier": 0,
        "trail_wait_trade": False,
        "pf_sl_enabled": False,
        "pf_sl_type": "Combined Loss",
        "pf_sl_value": 0,
        "pf_sl_value_is_pct": False,
        "pf_sl_underlying_below": 0,
        "pf_sl_underlying_above": 0,
        "pf_sl_action": "SqOff",
        "pf_sl_delay_sec": 0,
        "pf_sl_reexecute_count": 0,
        "pf_sl_sqoff_only_loss_legs": False,
        "pf_sl_sqoff_only_profit_legs": False,
        "pf_sl_trail_enabled": False,
        "pf_sl_trail_every": 0,
        "pf_sl_trail_by": 0,
        "move_sl_enabled": False,
        "move_sl_safety_sec": 0,
        "move_sl_action": "Move Only for Profitable Legs",
        "move_sl_trail_after": False,
        "move_sl_no_buy_legs": False,
        "move_sl_hit_on_leg_sl": False,
        "move_sl_hit_on_leg_target": False,
        "move_sl_ltp_buffer": 0,
        "move_sl_agg_pnl_enabled": False,
        "move_sl_agg_pnl_threshold": 0,
        "move_sl_agg_pnl_direction": "loss",
        "pf_sl_target_portfolio": "",
        "pf_tgt_target_portfolio": "",
        "pf_tgt_enabled": False,
        "pf_tgt_type": "Combined Profit",
        "pf_tgt_value": 0,
        "pf_tgt_value_is_pct": False,
        "pf_tgt_action": "SqOff",
        "pf_tgt_delay_sec": 0,
        "pf_tgt_reexecute_count": 0,
        "pf_tgt_sqoff_only_loss_legs": False,
        "pf_tgt_sqoff_only_profit_legs": False,
        "pf_tgt_trail_enabled": False,
        "pf_tgt_trail_lock_min_profit": 0,
        "pf_tgt_trail_when_profit_reach": 0,
        "pf_tgt_trail_every": 0,
        "pf_tgt_trail_by": 0,
        "no_reexec_sl_cost": False,
        "no_wait_trade_reexec": False,
        "no_strike_change_reexec": False,
        "no_reentry_after_end": False,
        "no_reentry_sl_cost": True,
        "exit_order_type": "MARKET",
        "exit_sell_first": True,
        "on_portfolio_complete": "None",
        "vwap_exit_fill": False,
        "directional_close_fill": False,
        "leg_target_monitoring": "Realtime",
        "leg_trailing_monitoring": "Realtime",
        "leg_sl_monitoring": "Realtime",
        "leg_sl_trailing_monitoring": "Realtime",
        "combined_target_monitoring": "Realtime",
        "combined_sl_monitoring": "Realtime",
        "slots": slots,
    }
    if pf_deltas:
        pf.update(pf_deltas)
    return pf


# ── Case definitions ──────────────────────────────────────────────────────────
CASES: list[tuple[str, dict]] = []


def add(filename: str, pf: dict):
    CASES.append((filename, pf))


# ===== A. VERIFICATION PROCESS (backtest features) =====

add("VP-01_long_sl_pct.json", base_portfolio(
    "VP-01_long_sl_pct",
    "VERIFY: leg fixed SL %. SL=0.5%. For a LONG fill F, expected SL = F*(1-0.005) "
    "snapped to tick 0.01. Trigger = first 1-sec bar with low<=SL; realised exit = that "
    "trigger bar's CLOSE (market reduce-only close, NOT the SL level). No squareoff/window.",
    [slot("vp01_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.5})]))

add("VP-02_long_tgt_pct.json", base_portfolio(
    "VP-02_long_tgt_pct",
    "VERIFY: leg fixed Target %. TGT=0.5%. For a LONG fill F, expected TGT = F*(1+0.005) "
    "snapped. Trigger = first bar with high>=TGT; realised exit = trigger bar CLOSE. SL off.",
    [slot("vp02_leg1", 9, 21, {"target_type": "percentage", "target_value": 0.5})]))

add("VP-03_sl_and_tgt.json", base_portfolio(
    "VP-03_sl_and_tgt",
    "VERIFY MULTIPLE CONDITIONS: SL 0.3% AND Target 0.5% on one leg. For each trade record "
    "WHICH side fired (reason SL vs TARGET) and at WHAT second. SL is checked before TP in the "
    "engine, so if both levels fall inside one bar the SL wins — confirm that ordering.",
    [slot("vp03_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.3,
                               "target_type": "percentage", "target_value": 0.5})]))

add("VP-04_short_sl_tgt.json", base_portfolio(
    "VP-04_short_sl_tgt",
    "VERIFY SHORT SIDE: SL 0.4% / Target 0.6%. EMA Cross also goes short on bearish crosses — "
    "pick the SHORT trades. For a SHORT fill F: SL = F*(1+0.004) ABOVE entry (fires on high>=SL), "
    "TGT = F*(1-0.006) BELOW entry (fires on low<=TGT). Mirror of the long-side math.",
    [slot("vp04_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.4,
                               "target_type": "percentage", "target_value": 0.6})]))

add("VP-05_trailing_sl.json", base_portfolio(
    "VP-05_trailing_sl",
    "VERIFY: trailing SL ratchet. Base SL 0.5%, trailing_sl_step=0.2, trailing_sl_offset=0.1. "
    "As profit rises the SL only TIGHTENS (long: current_sl never decreases). Walk per-bar "
    "current_sl and confirm each ratchet lands on the bar profit crosses the next step.",
    [slot("vp05_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.5,
                               "trailing_sl_step": 0.2, "trailing_sl_offset": 0.1})]))

add("VP-06_atr_sl.json", base_portfolio(
    "VP-06_atr_sl",
    "VERIFY: ATR SL. sl_atr_period=14, sl_atr_multiplier=2.0. expected SL = fill - 2.0*ATR(14) "
    "at the entry bar (long), snapped. Warm-up trades (ATR not initialised) arm NO SL — confirm "
    "those carry instead of exiting. Read the logged ATR-at-entry from the orderbook/log.",
    [slot("vp06_leg1", 9, 21, {"stop_loss_type": "atr", "sl_atr_period": 14, "sl_atr_multiplier": 2.0})]))

add("VP-07_sl_wait.json", base_portfolio(
    "VP-07_sl_wait",
    "VERIFY: SL Wait confirmation gate. SL 0.4%, sl_wait_bars=3. On the bar SL is first pierced "
    "the exit is DELAYED; it fires only after the level holds 3 bars. If price recovers above SL "
    "within the wait it CLEARS (no exit). Confirm exit second = trigger + 3 bars, and clears.",
    [slot("vp07_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.4, "sl_wait_bars": 3})]))

add("VP-08_action_reexecute.json", base_portfolio(
    "VP-08_action_reexecute",
    "VERIFY: On-SL action = Re-Execute, count 2. After the SL the leg goes IDLE then re-enters at "
    "market on a later signal; max 3 entries total (1 original + 2 re-exec), the 4th suppressed "
    "(LEG_REEXEC_LIMIT_REACHED). Record entry second + price of each re-entry. Tight SL 0.3%.",
    [slot("vp08_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.3,
                               "on_sl_action": "re_execute", "max_re_executions": 2})]))

add("VP-09_action_reverse.json", base_portfolio(
    "VP-09_action_reverse",
    "VERIFY: On-SL action = Reverse. SL hit -> close + open the OPPOSITE side at market on the "
    "same/next bar. Confirm the reversed position's side flips (LONG->SHORT) and its fresh SL is "
    "recomputed off the new fill. SL 0.3%.",
    [slot("vp09_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.3,
                               "on_sl_action": "reverse"})]))

add("VP-10_pf_combined_plus_leg.json", base_portfolio(
    "VP-10_pf_combined_plus_leg",
    "VERIFY MULTIPLE CONDITIONS / PRECEDENCE: 2 legs, each leg SL 0.5%, PLUS portfolio Combined "
    "Loss SL = 250 (currency). On a drawdown that trips both, the PORTFOLIO sqoff should fire "
    "first and flatten BOTH legs on the same bar (reason PORTFOLIO_LOSS) before the individual "
    "leg SLs. Record which level caused each exit and at what second.",
    [slot("vp10_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.5}),
     slot("vp10_leg2", 5, 13, {"stop_loss_type": "percentage", "stop_loss_value": 0.5})],
    {"pf_sl_enabled": True, "pf_sl_type": "Combined Loss", "pf_sl_value": 250, "pf_sl_action": "SqOff"}))

add("VP-11_zero_disabled_no_exit.json", base_portfolio(
    "VP-11_zero_disabled_no_exit",
    "ZERO-VALUE / DISABLED-FEATURE + MIS SQUARE-OFF: stop_loss_type='none' AND target_type='none' "
    "(both values 0). MIS + 15:29 IST hard square-off (NSE close). The features are DISABLED "
    "(current_sl/current_tp stay 0 -> the '>0' gate skips them), so there is NO SL/Target exit. "
    "EXPECTED: the only exits are the daily MIS SQUARE-OFF — every position still open at 15:29 IST "
    "is force-closed (reason squareoff), re-entry blocked until the next session; the position "
    "re-enters next day on the EMA signal and is squared off again. This isolates the MIS square-off "
    "(no feature noise). VERIFY each square-off fires on the first 15:29 bar of its session.",
    [slot("vp11_leg1", 9, 21, {"stop_loss_type": "none", "stop_loss_value": 0,
                               "target_type": "none", "target_value": 0})]))

add("VP-12_zero_value_pct_flaw.json", base_portfolio(
    "VP-12_zero_value_pct_flaw",
    "ZERO-VALUE FLAW PROBE: stop_loss_type='percentage' value=0 AND target_type='percentage' "
    "value=0. Unlike type='none', a 0% level is COMPUTED: SL = F*(1-0)=F and TGT = F*(1+0)=F — "
    "both land exactly ON the entry. EXPECTED: the position exits almost immediately at ~breakeven "
    "(SL checked first, fires on the first bar low<=entry). Confirm: does a 0% value disable the "
    "feature or place it at entry? (It places it at entry — a flaw to flag.)",
    [slot("vp12_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0,
                               "target_type": "percentage", "target_value": 0})]))

# ===== B. ORDER TYPES (live-market verification) =====

add("OT-01_market_exit.json", base_portfolio(
    "OT-01_market_exit",
    "ORDER TYPE = MARKET (the only wired backtest path). VERIFY EXPECTED vs ACTUAL FILL: the EMA "
    "entry is a MARKET/GTC order submitted on the signal bar; with no LatencyModel it fills at "
    "THAT bar's CLOSE. Record your expected price (the signal bar close) and compare to "
    "OrderFilled.last_px / ts_event. SL exit (0.5%) is also a market close -> trigger bar close.",
    [slot("ot01_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.5})]))

add("OT-02_limit_exit_blocked.json", base_portfolio(
    "OT-02_limit_exit_blocked",
    "ORDER TYPE = Limit. NEGATIVE TEST: saving this portfolio MUST be rejected with HTTP 400 "
    "(server._validate_portfolio_sl: \"Exit Order Type 'Limit' is not implemented ... only MARKET "
    "exits are supported (spec section 8.1)\"). Limit exits are a LIVE-only path. The file is the "
    "fixture for the rejection — it is intentionally not runnable as-is.",
    [slot("ot02_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.5})],
    {"exit_order_type": "Limit"}))

add("OT-03_sl_limit_blocked.json", base_portfolio(
    "OT-03_sl_limit_blocked",
    "ORDER TYPE = SL_Limit (stop-loss LIMIT). NEGATIVE TEST: save MUST be rejected with HTTP 400 "
    "(only MARKET wired). A stop-limit places a resting limit at `price` once `trigger_price` is "
    "touched and can MISS in a fast gap — a live-only nuance the backtester does not model.",
    [slot("ot03_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.5})],
    {"exit_order_type": "SL_Limit"}))

add("OT-04_stop_loss_market.json", base_portfolio(
    "OT-04_stop_loss_market",
    "ORDER TYPE = Stop-Loss (MARKET). In m-cube this maps to the SL feature: the managed strategy "
    "watches the bar low/high and, once the SL is pierced, submits a reduce-only MARKET close "
    "(StopMarket semantics) -> fills at the trigger bar close. VERIFY trigger second = first "
    "low<=SL and fill = that bar's close. SL 0.5%, exit_order_type MARKET.",
    [slot("ot04_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.5})]))

add("OT-05_good_till_triggered.json", base_portfolio(
    "OT-05_good_till_triggered",
    "ORDER TYPE = Good-Till-Triggered (GTT). DOC/RUN: GTT is NOT a Nautilus time-in-force — it is "
    "a conditional (Stop/IfTouched) order armed by a trigger_price + GTC. The backtester has no "
    "native conditional path; it EMULATES 'rest until triggered' by monitoring each bar and firing "
    "a market exit when the level is touched. Here SL 0.4% + Target 0.6% act as the two resting "
    "triggers. VERIFY each fires on the first bar its level is touched (and not before).",
    [slot("ot05_leg1", 9, 21, {"stop_loss_type": "percentage", "stop_loss_value": 0.4,
                               "target_type": "percentage", "target_value": 0.6})]))

add("OT-06_good_till_cancelled.json", base_portfolio(
    "OT-06_good_till_cancelled",
    "ORDER TYPE = Good-Till-Cancelled (GTC). DOC/RUN: GTC is the DEFAULT time-in-force on every "
    "order the engine submits (entries and exits, core/managed_strategy.py _submit_order). With no "
    "SL/Target, the GTC entry holds intraday until either the opposite EMA signal 'cancels' it by "
    "flipping the position OR the MIS 15:29 IST square-off force-closes it at the session close. "
    "VERIFY the order itself never expires on its own (no GTD/DAY expiry) - it ends only by signal "
    "flip or by the daily MIS square-off; re-entry resumes the next session.",
    [slot("ot06_leg1", 9, 21, {"stop_loss_type": "none", "stop_loss_value": 0,
                               "target_type": "none", "target_value": 0})]))


def main():
    written = []
    for filename, pf in CASES:
        path = os.path.join(HERE, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(pf, f, indent=2)
        written.append(filename)
    print(f"Wrote {len(written)} portfolios to {HERE}:")
    for w in written:
        print("  -", w)


if __name__ == "__main__":
    main()
