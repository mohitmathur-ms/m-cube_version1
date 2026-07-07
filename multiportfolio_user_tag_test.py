"""Committed session test for TAG- and USER-level enforcement (spec sl_features
s3 / execution_logic s6, s11) in the multi-portfolio session engine. Covers the
items the compliance report flagged for coverage:

  A. Tag  Max-Loss  -> whole tag group squared off live on the combined breach
  B. User Max-Loss  -> whole user book squared off live on the combined breach
  C. No double-clip -> the live monitor result is NOT re-clipped post-run
  D. Trailing SL ratchet (deterministic) -> cap tightens with profit, fires on pullback
  E. Trailing Target / Profit-Lock (deterministic) -> floor locks/ratchets, fires on pullback
  F. No-base-cap guard -> a pure trailing SL does NOT fire before any profit

Live cases use two ETHUSD portfolios (both losing -> combined breaches); trailing
cases use a synthetic P&L curve so they are data-independent. tags.json is restored;
users.json is NOT touched (the user accessors are monkeypatched).

Run: venv\\Scripts\\python.exe multiportfolio_user_tag_test.py
"""
import os, sys, json, copy
os.environ.setdefault("_USE_UNIFIED_ENGINE", "1")
from pathlib import Path
from core.models import portfolio_from_dict
from core.backtest_runner.session import run_session_backtest
from core.portfolio_monitor import PortfolioMonitorStrategy, PortfolioMonitorConfig
from core import tags
import core.users as users

_BASE = json.loads(Path("portfolios/_default/CRYPTO_REEXEC_ENTRY.json").read_text(encoding="utf-8"))


def _mk(name, coin, tag=None):
    d = copy.deepcopy(_BASE); d["name"] = name
    d["slots"] = [s for s in d["slots"] if coin in s["slot_id"].lower()]
    d.update(pf_sl_enabled=False, pf_tgt_enabled=False)
    d.pop("portfolio_tag", None)
    if tag:
        d["portfolio_tag"] = tag
    return portfolio_from_dict(d)


def _tot(r):
    return (r.get("total_trades"), round(float(r.get("total_pnl", 0)), 2))


def _trail_fires(cfg, curve):
    m = PortfolioMonitorStrategy(cfg); fires = []
    m._fire = lambda ts, a, mk, r: fires.append(ts)
    for i, pnl in enumerate(curve):
        m._eval_trailing(i, float(pnl))
    return fires


def main():
    cat, cdir, uid = "catalog", "custom_strategies", "_default"
    results = {}

    # ── A. Tag Max-Loss: whole group squared ──
    tags.upsert_tag({"tag": "UTTEST", "max_loss": 150.0})
    try:
        os.environ["_USE_TAG_ENFORCE"] = "0"
        a_no = run_session_backtest(cat, [_mk("UT_A", "eth", "UTTEST"), _mk("UT_B", "eth", "UTTEST")], cdir, uid)
        os.environ["_USE_TAG_ENFORCE"] = "1"
        a_yes = run_session_backtest(cat, [_mk("UT_A", "eth", "UTTEST"), _mk("UT_B", "eth", "UTTEST")], cdir, uid)
    finally:
        tags.delete_tag("UTTEST")
    results["A_tag_maxloss"] = (_tot(a_yes["UT_A"]) != _tot(a_no["UT_A"])
                                and _tot(a_yes["UT_B"]) != _tot(a_no["UT_B"]))

    # ── B. User Max-Loss: whole book squared (monkeypatch the accessors) ──
    _save = {k: getattr(users, k) for k in
             ("get_user_max_loss", "get_user_max_profit", "get_user_trailing_sl", "get_user_trailing_target")}
    users.get_user_max_loss = lambda u: 150.0
    users.get_user_max_profit = users.get_user_trailing_sl = users.get_user_trailing_target = lambda u: None
    try:
        os.environ["_USE_USER_ENFORCE"] = "0"
        b_no = run_session_backtest(cat, [_mk("UU_A", "eth"), _mk("UU_B", "eth")], cdir, uid)
        os.environ["_USE_USER_ENFORCE"] = "1"
        b_yes = run_session_backtest(cat, [_mk("UU_A", "eth"), _mk("UU_B", "eth")], cdir, uid)
        # C. No double-clip: a HUGE cap must NOT clip → result identical to no-enforce.
        users.get_user_max_loss = lambda u: 10_000_000.0
        c_yes = run_session_backtest(cat, [_mk("UU_A", "eth"), _mk("UU_B", "eth")], cdir, uid)
    finally:
        for k, v in _save.items():
            setattr(users, k, v)
    results["B_user_maxloss"] = (_tot(b_yes["UU_A"]) != _tot(b_no["UU_A"]))
    results["C_no_double_clip"] = (_tot(c_yes["UU_A"]) == _tot(b_no["UU_A"]))

    # ── D/E/F. Trailing logic (deterministic) ──
    sl_cfg = PortfolioMonitorConfig(pf_sl_enabled=True, pf_sl_value=100.0, pf_sl_trail_enabled=True,
                                    pf_sl_trail_every=50.0, pf_sl_trail_by=60.0, portfolio_id="X")
    results["D_trail_sl"] = bool(_trail_fires(sl_cfg, [0, 50, 100, 150, 200, 120, 40, 0, -5]))
    tg_cfg = PortfolioMonitorConfig(pf_tgt_enabled=True, pf_tgt_trail_enabled=True,
                                    pf_tgt_trail_when_reach=100.0, pf_tgt_trail_lock=50.0,
                                    pf_tgt_trail_every=50.0, pf_tgt_trail_by=40.0, portfolio_id="Y")
    results["E_trail_tgt"] = bool(_trail_fires(tg_cfg, [0, 80, 100, 150, 200, 250, 200, 150, 120, 100]))
    nob_cfg = PortfolioMonitorConfig(pf_sl_enabled=True, pf_sl_value=0.0, pf_sl_trail_enabled=True,
                                     pf_sl_trail_every=50.0, pf_sl_trail_by=60.0, portfolio_id="Z")
    results["F_no_base_guard"] = (not _trail_fires(nob_cfg, [0, -10, -30, -50])  # loss-first: no fire
                                  and bool(_trail_fires(nob_cfg, [0, 60, 120, 60, 0])))  # profit+pullback: fire

    for k, v in results.items():
        print(f"  [{'OK' if v else 'FAIL'}] {k}")
    ok = all(results.values())
    print("USER_TAG_OK" if ok else "USER_TAG_FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
