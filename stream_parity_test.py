import sys, os
from pathlib import Path
ROOT = Path(__file__).resolve().parent; sys.path.insert(0,str(ROOT))
import enforce_parity_test as E
def run(env):
    for k in ("_USE_PER_SLOT","_USE_UNIFIED_STREAMING","_UNIFIED_CHUNK_SIZE","_USE_PF_MONITOR","_USE_PF_ENFORCE"):
        os.environ.pop(k,None)
    os.environ.update(env)
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    clear_cross_portfolio_bus()
    d = E.build(0); d["pf_sl_enabled"]=False
    r = run_portfolio_backtest(str(ROOT/"catalog"), portfolio_from_dict(d), user_id="_default")
    return round(r.get("total_pnl") or 0,6), r.get("total_trades")
if __name__=="__main__":
    one = run({})
    s_big = run({"_USE_UNIFIED_STREAMING":"1","_UNIFIED_CHUNK_SIZE":"1000000"})
    s_sm  = run({"_USE_UNIFIED_STREAMING":"1","_UNIFIED_CHUNK_SIZE":"5000"})
    print(f"oneshot          : {one}")
    print(f"streaming chunk1M: {s_big}")
    print(f"streaming chunk5k: {s_sm}")
    print("PARITY" if one==s_big==s_sm else "MISMATCH")
