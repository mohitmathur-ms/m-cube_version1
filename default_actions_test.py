import sys, os
from pathlib import Path
ROOT = Path(__file__).resolve().parent; sys.path.insert(0,str(ROOT))
import enforce_parity_test as E

def run(dct):
    for k in ("_USE_PER_SLOT","_USE_PF_MONITOR","_USE_PF_ENFORCE","_USE_POST_RUN_PF"):
        os.environ.pop(k,None)  # pure default, NO flags
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    clear_cross_portfolio_bus()
    r = run_portfolio_backtest(str(ROOT/"catalog"), portfolio_from_dict(dct), user_id="_default")
    return round(r.get("total_pnl") or 0,4), r.get("total_trades")

def base(**kw):
    d=E.build(0); d["pf_sl_enabled"]=False
    d.update(kw); return d

if __name__=="__main__":
    # plain portfolio, NO pf features -> monitor must NOT attach
    print("PLAIN (no pf):", run(base()))
    # pf_sl ReExecute at entry
    print("pf_sl ReExec :", run(base(pf_sl_enabled=True, pf_sl_type="Combined Loss",
          pf_sl_value=200, pf_sl_action="ReExecute at Entry Price", pf_sl_reexecute_count=3)))
    # pf_tgt ReExecute at entry
    print("pf_tgt ReExec:", run(base(pf_tgt_enabled=True, pf_tgt_type="Combined Profit",
          pf_tgt_value=300, pf_tgt_action="ReExecute at Entry Price", pf_tgt_reexecute_count=3)))
    # aggregated legs (5-min signal from 1-min base) + pf_sl
    agg=base(pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=200, pf_sl_action="SqOff")
    for s in agg["slots"]:
        bt=s["bar_type_str"]; sym=bt.split("-")[0]
        s["strategy_bar_types"]=[f"{sym}-5-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL"]
    print("aggregation :", run(agg))
