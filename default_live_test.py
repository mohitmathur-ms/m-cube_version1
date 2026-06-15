"""Verify the one-pass live engine is the DEFAULT (no flags) for portfolio-level
SL and Target. Compares default (live) vs _USE_POST_RUN_PF=1 (old reconstruction)."""
import sys, os
from pathlib import Path
ROOT = Path(__file__).resolve().parent; sys.path.insert(0,str(ROOT))
import enforce_parity_test as E

def build_sl():
    d = E.build(200); d["pf_sl_action"]="SqOff"; return d
def build_tgt():
    d = E.build(0); d["pf_sl_enabled"]=False
    d["pf_tgt_enabled"]=True; d["pf_tgt_type"]="Combined Profit"
    d["pf_tgt_value"]=300; d["pf_tgt_action"]="SqOff"; return d

def run(dct, env):
    for k in ("_USE_PER_SLOT","_USE_PF_MONITOR","_USE_PF_ENFORCE","_USE_POST_RUN_PF","_USE_UNIFIED_STREAMING"):
        os.environ.pop(k,None)
    os.environ.update(env)
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    clear_cross_portfolio_bus()
    r = run_portfolio_backtest(str(ROOT/"catalog"), portfolio_from_dict(dct), user_id="_default")
    return round(r.get("total_pnl") or 0,4), r.get("total_trades")

if __name__=="__main__":
    print("=== pf_sl SqOff (combined loss 200) ===")
    print("  DEFAULT (no flags, live):", run(build_sl(), {}))
    print("  escape _USE_POST_RUN_PF :", run(build_sl(), {"_USE_POST_RUN_PF":"1"}))
    print("=== pf_tgt SqOff (combined profit 300) -- NEW live target ===")
    print("  DEFAULT (no flags, live):", run(build_tgt(), {}))
    print("  escape _USE_POST_RUN_PF :", run(build_tgt(), {"_USE_POST_RUN_PF":"1"}))
