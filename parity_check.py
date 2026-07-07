import sys, os
from pathlib import Path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

def run_case(cid, grouping):
    os.environ["_USE_GROUPING"] = "1" if grouping else "0"
    import tests.run_stress_suite as S
    S.write_users({"user_id":"u_stress","multiplier":1.5})
    from core.users import reset_user_pnl
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    reset_user_pnl(); clear_cross_portfolio_bus()
    c = [x for x in S.CASES if x["id"] == cid][0]
    r = run_portfolio_backtest(str(ROOT/"catalog"), portfolio_from_dict(c["builder"]()), user_id="u_stress")
    return r.get("total_trades"), round(r.get("total_pnl") or 0, 4)

if __name__ == "__main__":
    cid = "T06"
    on = run_case(cid, True)
    off = run_case(cid, False)
    import tests.run_stress_suite as S
    S.USERS.write_text(__import__("json").dumps({"users":[
        {"user_id":"_default","alias":"Default","multiplier":6,"allowed_instruments":None},
        {"user_id":"u_mownziwo","alias":"New User","multiplier":1,"allowed_instruments":None}]}, indent=2), encoding="utf-8")
    print(f"{cid} grouped ON : trades={on[0]} pnl={on[1]}")
    print(f"{cid} grouped OFF: trades={off[0]} pnl={off[1]}")
    print("PARITY:", "OK (grouping matches per-slot)" if on == off else "MISMATCH")
