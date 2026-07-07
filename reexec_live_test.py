"""Phase 3 live ReExecute test: unified one-pass live re-execute (monitor signals
legs to close + re-arm a re-entry at their entry price) vs the per-slot two-pass
replay. Live engine P&L is the ground truth; the replay is a reconstruction, so
they should be in the same ballpark and move together, not byte-identical.
"""
import sys, os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import enforce_parity_test as E


def build(pf_value, action, count):
    d = E.build(pf_value)
    d["pf_sl_action"] = action
    d["pf_sl_reexecute_count"] = count
    return d


def run(pf_value, action, count, env):
    for k in ("_USE_PER_SLOT", "_USE_PF_MONITOR", "_USE_PF_ENFORCE", "_USE_UNIFIED_ENGINE"):
        os.environ.pop(k, None)
    os.environ.update(env)
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    clear_cross_portfolio_bus()
    r = run_portfolio_backtest(str(ROOT / "catalog"),
                               portfolio_from_dict(build(pf_value, action, count)),
                               user_id="_default")
    # count live "Portfolio Stoploss ... ReExecute" closes in the fills tags
    pf_tag = 0
    fr = r.get("fills_report")
    try:
        if fr is not None and hasattr(fr, "iterrows"):
            for _, row in fr.iterrows():
                if "Portfolio Stoploss" in str(row.get("tags")):
                    pf_tag += 1
    except Exception:
        pass
    return round(r.get("total_pnl") or 0, 4), r.get("total_trades"), pf_tag


if __name__ == "__main__":
    PF = float(os.environ.get("PFV", "200"))
    CNT = int(os.environ.get("CNT", "3"))
    ACT = os.environ.get("ACT", "ReExecute at Entry Price")
    print(f"=== {ACT}, pf_sl_value={PF}, count={CNT} ===")
    base = run(PF, ACT, CNT, {"_USE_PER_SLOT": "1"})
    live = run(PF, ACT, CNT, {"_USE_PF_MONITOR": "1", "_USE_PF_ENFORCE": "1"})
    print(f"per-slot two-pass replay : pnl={base[0]} trades={base[1]} pf_tagged={base[2]}")
    print(f"unified LIVE re-execute   : pnl={live[0]} trades={live[1]} pf_tagged={live[2]}")
