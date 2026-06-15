"""Base-resolution exit suite.

Verifies the feature: when a leg subscribes to an aggregated signal timeframe
(e.g. 5-MINUTE built from 1-SECOND base), the strategy SIGNAL/entries run on the
aggregated bar, but ALL exit management (SL / target / trailing / ATR / square-off /
re_execute / reverse) is evaluated on the BASE bar — so exits land at base-data
resolution, not snapped to the signal-timeframe grid.

Each case is saved to portfolios/_default/BXT_*.json so it can be opened/run from
the m-cube UI, then run here and asserted:

  * ENTRY fills cluster on the signal-timeframe grid (e.g. minute % 5 == 0).
  * EXIT fills (Stop Loss / Take Profit / Trailing SL / Squareoff / Reverse...)
    are mostly OFF that grid — i.e. at the exact base bar the trigger fired.

Headline data: NIFTY 1-SECOND base (so base resolution == 1 second). A FX case
(EURUSD 1-MINUTE base) covers Format-A bid/ask, and a no-aggregation control
proves the non-aggregating path is unchanged.

Run:  venv\\Scripts\\python.exe tests\\base_exit_resolution_suite.py
"""
from __future__ import annotations
import collections, json, sys, time, traceback
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
CATALOG = str(ROOT / "catalog")
PF_DIR = ROOT / "portfolios" / "_default"

# ── instruments ──────────────────────────────────────────────────────────────
NIFTY = "NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS-1-SECOND-LAST-EXTERNAL"
EUR_MID = "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL"

# Short windows keep each run quick while still spanning many days / triggers.
NIFTY_START, NIFTY_END = "2026-03-02", "2026-03-20"
FX_START, FX_END = "2021-01-04", "2021-01-29"

EXIT_PREFIXES = {
    "Stop Loss", "Take Profit", "Trailing SL", "Trailing Target",
    "Squareoff", "Reverse on SL", "Reverse on TP", "OCO Exit",
}


def agg(base_bt: str, tf: str) -> str:
    """Composite carrier the UI emits for a strategy subscribe-timeframe, for ANY
    base step/unit. NIFTY 1-SECOND base + '5-MINUTE' ->
    '...-5-MINUTE-LAST-INTERNAL@1-SECOND-EXTERNAL'."""
    p = base_bt.split("-")          # INSTR.VENUE, step, unit, price, EXTERNAL
    inst, step, unit, price = p[0], p[1], p[2], p[3]
    return f"{inst}-{tf}-{price}-INTERNAL@{step}-{unit}-EXTERNAL"


def tf_minutes(tf: str) -> float:
    n, u = tf.split("-")
    return int(n) * {"SECOND": 1 / 60, "MINUTE": 1, "HOUR": 60, "DAY": 1440}[u]


def ec(**kw):
    base = dict(
        exit_price_format="ohlcv", stop_loss_type="none", stop_loss_value=0.0,
        trailing_sl_step=0.0, trailing_sl_offset=0.0, sl_atr_period=0,
        sl_atr_multiplier=0.0, target_type="none", target_value=0.0,
        tgt_atr_period=0, tgt_atr_multiplier=0.0, target_lock_trigger=0.0,
        target_lock_minimum=0.0, tgt_trail_enabled=False,
        tgt_trail_when_profit_reach=0.0, tgt_trail_lock_min_profit=0.0,
        tgt_trail_every=0.0, tgt_trail_by=0.0, sl_wait_sec=0, sl_wait_bars=0,
        tgt_wait_sec=0, tgt_wait_bars=0, on_sl_action="close",
        on_target_action="close", max_re_executions=0, execute_target_leg_id="",
        reentry_price=0.0, max_re_entries=0, armed_at_start=True)
    base.update(kw)
    return base


def leg(strategy, bt, lots, exit_cfg, agg_tf=None, **kw):
    s = {"strategy_name": strategy, "bar_type_str": bt, "lots": lots,
         "strategy_params": kw.pop("params", {}), "exit_config": exit_cfg,
         "enabled": True}
    if agg_tf:
        s["strategy_bar_types"] = [agg(bt, agg_tf)]
    s.update(kw)
    return s


def pf(name, start, end, slots, **kw):
    base = dict(name=name, starting_capital=100000.0, start_date=start,
                end_date=end, allocation_mode="equal", slots=slots)
    base.update(kw)
    return base


def emap(f, s):
    return {"fast_ema_period": f, "slow_ema_period": s}


# ── cases: (id, signal_tf or None for no-agg, builder) ───────────────────────
# signal_tf is the aggregated timeframe the entries should snap to; None means
# base == signal (no aggregation control).
NW = dict(entry_start_time="09:30:00", entry_end_time="16:15:00")
CASES = []


def C(cid, title, signal_tf, builder):
    CASES.append((cid, title, signal_tf, builder))


C("BXT_01_sl_tgt_5m", "SL 0.1% + Target 0.15%, 5-min signal / 1-sec base", "5-MINUTE",
  lambda: pf("BXT_01_sl_tgt_5m", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(stop_loss_type="percentage", stop_loss_value=0.1,
                  target_type="percentage", target_value=0.15), agg_tf="5-MINUTE", params=emap(10, 20))], **NW))

C("BXT_02_squareoff_5m", "Square-off 11:02 (off the 5-min grid), 5-min signal", "5-MINUTE",
  lambda: pf("BXT_02_squareoff_5m", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(), agg_tf="5-MINUTE", params=emap(10, 20))],
             squareoff_time="11:02", squareoff_tz="UTC", **NW))

C("BXT_03_trailing_5m", "Trailing SL (step/offset), 5-min signal", "5-MINUTE",
  lambda: pf("BXT_03_trailing_5m", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(stop_loss_type="trailing", stop_loss_value=0.20,
                  trailing_sl_step=0.05, trailing_sl_offset=0.03), agg_tf="5-MINUTE", params=emap(10, 20))], **NW))

C("BXT_04_sl_reexecute_5m", "SL 0.1% with on_sl_action=re_execute (x3), 5-min signal", "5-MINUTE",
  lambda: pf("BXT_04_sl_reexecute_5m", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(stop_loss_type="percentage", stop_loss_value=0.1,
                  on_sl_action="re_execute", max_re_executions=3), agg_tf="5-MINUTE", params=emap(10, 20))], **NW))

C("BXT_05_sl_reverse_5m", "SL 0.1% with on_sl_action=reverse, 5-min signal", "5-MINUTE",
  lambda: pf("BXT_05_sl_reverse_5m", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(stop_loss_type="percentage", stop_loss_value=0.1,
                  on_sl_action="reverse"), agg_tf="5-MINUTE", params=emap(10, 20))], **NW))

C("BXT_06_atr_sl_5m", "ATR SL + ATR target, 5-min signal", "5-MINUTE",
  lambda: pf("BXT_06_atr_sl_5m", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(stop_loss_type="atr", sl_atr_period=14, sl_atr_multiplier=2.0,
                  target_type="atr", tgt_atr_period=14, tgt_atr_multiplier=3.0), agg_tf="5-MINUTE", params=emap(10, 20))], **NW))

C("BXT_07_ltp_5m", "LTP exit format (Format C) SL/Target, 5-min signal", "5-MINUTE",
  lambda: pf("BXT_07_ltp_5m", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(exit_price_format="ltp", stop_loss_type="percentage",
                  stop_loss_value=0.1, target_type="percentage", target_value=0.15), agg_tf="5-MINUTE", params=emap(10, 20))], **NW))

C("BXT_08_agg_1hour", "SL/Target, 1-HOUR signal / 1-sec base", "1-HOUR",
  lambda: pf("BXT_08_agg_1hour", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(stop_loss_type="percentage", stop_loss_value=0.1,
                  target_type="percentage", target_value=0.15), agg_tf="1-HOUR", params=emap(10, 20))], **NW))

C("BXT_09_noagg_control", "Control: NO aggregation (base 1-sec == signal)", None,
  lambda: pf("BXT_09_noagg_control", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(stop_loss_type="percentage", stop_loss_value=0.1,
                  target_type="percentage", target_value=0.15), params=emap(10, 20))], **NW))

C("BXT_10_pf_sl_tgt_5m", "Leg SL/Target + PORTFOLIO SL/Target, 5-min signal", "5-MINUTE",
  lambda: pf("BXT_10_pf_sl_tgt_5m", NIFTY_START, NIFTY_END,
             [leg("EMA Cross", NIFTY, 1, ec(stop_loss_type="percentage", stop_loss_value=0.1,
                  target_type="percentage", target_value=0.15), agg_tf="5-MINUTE", params=emap(10, 20))],
             pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=1500, pf_sl_action="SqOff",
             pf_tgt_enabled=True, pf_tgt_type="Combined Profit", pf_tgt_value=2500, pf_tgt_action="SqOff", **NW))

C("BXT_11_format_a_bidask_fx", "Format-A bid/ask SL/Target, 5-min signal / 1-min FX base", "5-MINUTE",
  lambda: pf("BXT_11_format_a_bidask_fx", FX_START, FX_END,
             [leg("EMA Cross", EUR_MID, 0.05, ec(exit_price_format="bidask", stop_loss_type="percentage",
                  stop_loss_value=0.05, target_type="percentage", target_value=0.08), agg_tf="5-MINUTE", params=emap(9, 21))]))


def classify(fr):
    """Return (entries_df, exits_df) using is_reduce_only + exit-reason tags."""
    def pfx(t):
        if isinstance(t, (list, tuple)) and t:
            return str(t[0]).split(":")[0].strip()
        return ""
    fr = fr.copy()
    fr["_pfx"] = fr["tags"].map(pfx)
    fr["_ts"] = pd.to_datetime(fr["ts_last"], utc=True)
    is_exit = fr["_pfx"].isin(EXIT_PREFIXES) | (fr["is_reduce_only"] == True)  # noqa: E712
    # Market Exit (end-of-run / untagged reduce) is excluded from the
    # base-resolution assertion (it's the final flatten, not a trigger).
    exits = fr[fr["_pfx"].isin(EXIT_PREFIXES)]
    entries = fr[~is_exit]
    return entries, exits


def on_grid(ts_series, signal_min):
    """Fraction of timestamps sitting exactly on the signal grid (minute multiple
    of signal_min AND second == 0). signal_min < 1 (sub-minute) => grid is every
    second => everything counts as on-grid."""
    if len(ts_series) == 0:
        return None
    if signal_min < 1:
        return 1.0
    on = ((ts_series.dt.minute % int(signal_min) == 0) & (ts_series.dt.second == 0)).mean()
    return float(on)


def run():
    from core.models import portfolio_from_dict, portfolio_to_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    from core.users import reset_user_pnl

    PF_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for cid, title, signal_tf, builder in CASES:
        t0 = time.time()
        rec = dict(id=cid, title=title)
        try:
            d = builder()
            pfobj = portfolio_from_dict(d)
            canon = portfolio_to_dict(pfobj)
            (PF_DIR / f"{d['name']}.json").write_text(
                json.dumps(canon, indent=2, default=str), encoding="utf-8")
            reset_user_pnl()
            clear_cross_portfolio_bus()
            r = run_portfolio_backtest(CATALOG, pfobj, user_id="_default")
            fr = r.get("fills_report")
            entries, exits = classify(fr)
            sig_min = tf_minutes(signal_tf) if signal_tf else 0.5
            ent_on = on_grid(entries["_ts"], sig_min)
            exit_on = on_grid(exits["_ts"], sig_min)
            exit_sec = float((exits["_ts"].dt.second != 0).mean()) if len(exits) else None

            reasons = collections.Counter(exits["_pfx"])
            if signal_tf is None:
                # Control: everything at 1-sec. PASS if exits exist with sub-minute spread.
                ok = len(exits) > 0 and (exit_sec or 0) > 0.2
                verdict = "PASS" if ok else "FAIL"
                note = f"no-agg control: exits={len(exits)} sec!=0 frac={exit_sec}"
            else:
                # Aggregating: entries should be on grid; exits should be mostly off it.
                # Skip the entry-grid check when fresh entries are too few to be
                # meaningful (e.g. on_sl_action=reverse: the flip fill is tagged as
                # the exit, leaving few standalone signal entries to sample).
                ent_ok = ent_on is None or len(entries) < 20 or ent_on >= 0.85
                exit_ok = len(exits) > 0 and (exit_on is not None and exit_on <= 0.50)
                ok = ent_ok and exit_ok
                verdict = "PASS" if ok else "FAIL"
                note = (f"entries_on_grid={ent_on:.2f} (>=0.85), "
                        f"exits_on_grid={exit_on if exit_on is None else round(exit_on,2)} (<=0.50), "
                        f"exits={len(exits)} {dict(reasons)}")
            rec.update(status="COMPLETED", verdict=verdict, note=note,
                       total_trades=r.get("total_trades"),
                       entries=len(entries), exits=len(exits),
                       saved=f"portfolios/_default/{d['name']}.json")
        except Exception as e:
            rec.update(status="ERROR", verdict="FAIL",
                       note=f"{type(e).__name__}: {e}",
                       trace=traceback.format_exc()[-800:])
        rec["seconds"] = round(time.time() - t0, 1)
        rows.append(rec)
        print(f"{rec['verdict']:<5} {cid:<26} ({rec['seconds']:>5}s)  {rec['note']}", flush=True)

    npass = sum(1 for r in rows if r["verdict"] == "PASS")
    print(f"\nBASE-EXIT SUITE — {npass}/{len(rows)} PASS")
    for r in rows:
        if r["verdict"] != "PASS":
            print(f"  !! {r['id']}: {r.get('note')}")
            if r.get("trace"):
                print(r["trace"])
    return rows


if __name__ == "__main__":
    run()
