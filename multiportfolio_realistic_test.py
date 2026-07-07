"""Realistic multi-portfolio integration test — the ACTUAL use case:
SEVERAL portfolios, EACH with MULTIPLE legs, EACH leg different settings, EACH
portfolio different settings, all in ONE session — plus cross-portfolio actions.

Part A (parity): run each portfolio STANDALONE (production run_portfolio_backtest),
then ALL together in a SESSION (no cross-pf). Each portfolio's total (trades, pnl)
must match its standalone — proving multi-leg + mixed-settings + co-residence is exact.

Part B (cross-pf on top): P1's pf-SL fires "SqOff Other Portfolio" -> P3. In the
session P3 must be AFFECTED (differs from its standalone) while P2 (not targeted)
still matches its standalone — proving cross-pf works amid rich real portfolios.

All legs are NIFTY 5-min (same instrument -> shared-instrument session path), same range.
Run under unified OR _USE_BACKTEST_NODE=1.
"""
import os, sys, json, copy
os.environ.setdefault("_USE_UNIFIED_ENGINE", "1")
from pathlib import Path
from core.models import portfolio_from_dict
from core.backtest_runner import run_portfolio_backtest
from core.backtest_runner.session import run_session_backtest

_BASE = json.loads(Path("portfolios/_default/BXT_01_sl_tgt_5m.json").read_text(encoding="utf-8"))
_SLOT = _BASE["slots"][0]


def _leg(slot_id, **ec):
    s = copy.deepcopy(_SLOT)
    s["slot_id"] = slot_id
    s["exit_config"].update({"stop_loss_type": "percentage", "stop_loss_value": 0.1,
                             "target_type": "none", "target_value": 0.0,
                             "on_sl_action": "close", "on_target_action": "close"})
    s["exit_config"].update(ec)
    return s


def _pf(name, legs, **pf):
    d = copy.deepcopy(_BASE)
    d["name"] = name
    d["slots"] = legs
    for k in ("pf_sl_enabled", "pf_tgt_enabled", "rbo_enabled", "move_sl_enabled",
              "squareoff_time", "pf_sl_action", "pf_sl_target_portfolio"):
        d.pop(k, None)
    d.update(pf)
    return portfolio_from_dict(d)


def _p1(xpf=False):
    legs = [
        _leg("p1_legA", stop_loss_type="percentage", stop_loss_value=0.1,
             target_type="percentage", target_value=0.15),
        _leg("p1_legB", stop_loss_type="trailing", stop_loss_value=0.2,
             trailing_sl_step=0.05, trailing_sl_offset=0.03),
        _leg("p1_legC", stop_loss_type="atr", sl_atr_period=14, sl_atr_multiplier=2.0),
    ]
    return _pf("REAL_P1", legs, pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=3000.0,
               pf_sl_action="SqOff Other Portfolio" if xpf else "SqOff",
               pf_sl_target_portfolio="REAL_P3" if xpf else "")


def _p2():
    legs = [
        _leg("p2_legA", stop_loss_type="percentage", stop_loss_value=0.1, on_sl_action="reverse"),
        _leg("p2_legB", stop_loss_type="percentage", stop_loss_value=0.1, sl_wait_bars=3,
             target_type="percentage", target_value=0.2,
             target_lock_trigger=0.1, target_lock_minimum=0.05),
    ]
    return _pf("REAL_P2", legs, pf_tgt_enabled=True, pf_tgt_type="Combined Profit",
               pf_tgt_value=2500.0, squareoff_time="09:55:00")


def _p3():
    legs = [
        _leg("p3_legA", stop_loss_type="percentage", stop_loss_value=0.1,
             on_sl_action="re_execute", max_re_executions=2),
        _leg("p3_legB", stop_loss_type="percentage", stop_loss_value=0.1,
             on_sl_action="re_entry", reentry_price=0.0),
    ]
    return _pf("REAL_P3", legs, move_sl_enabled=True, move_sl_safety_sec=0,
               move_sl_agg_pnl_enabled=True, move_sl_agg_pnl_threshold=400,
               move_sl_agg_pnl_direction="loss")


def _tot(r):
    return (r.get("total_trades"), round(float(r.get("total_pnl", 0)), 2))


def main():
    cat, cdir, uid = "catalog", "custom_strategies", "_default"
    std = {}
    for mk in (_p1, _p2, _p3):
        std[mk().name] = _tot(run_portfolio_backtest(cat, mk(), cdir, uid))
    sessA = run_session_backtest(cat, [_p1(), _p2(), _p3()], cdir, uid)
    print("-- Part A: multi-leg / mixed-settings parity (no cross-pf) --")
    okA = True
    for nm in ("REAL_P1", "REAL_P2", "REAL_P3"):
        s = _tot(sessA[nm]); m = (s == std[nm]); okA &= m
        print(f"  [{'OK' if m else 'DIFF'}] {nm}: standalone={std[nm]} session={s}")

    sessB = run_session_backtest(cat, [_p1(xpf=True), _p2(), _p3()], cdir, uid)
    print("-- Part B: cross-pf (P1 -> SqOff P3) amid rich portfolios --")
    p3_std, p3_x = std["REAL_P3"], _tot(sessB["REAL_P3"])
    p2_std, p2_x = std["REAL_P2"], _tot(sessB["REAL_P2"])
    xfired = (p3_x != p3_std); p2_clean = (p2_x == p2_std)
    print(f"  [{'OK' if xfired else 'FAIL'}] cross-pf reached P3: standalone={p3_std} session(xpf)={p3_x}")
    print(f"  [{'OK' if p2_clean else 'FAIL'}] P2 (untargeted) unaffected: standalone={p2_std} session(xpf)={p2_x}")

    ok = okA and xfired and p2_clean
    print("REALISTIC_OK" if ok else "REALISTIC_FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
