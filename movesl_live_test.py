"""Phase 4 live Move-SL test: aggregate-P&L + Hit-On-Leg Move-SL one-pass in the
unified engine (monitor computes the agg trigger; legs share the in-process bus
for Hit-On-Leg) vs the per-slot two-pass discovery. Live engine P&L is the ground
truth; same-ballpark expected, not byte-identical.
"""
import sys, os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import enforce_parity_test as E


def build(mode):
    d = E.build(0)
    d["pf_sl_enabled"] = False  # isolate Move-SL
    # SL/TP via env so we can probe churn sensitivity (default moderate).
    _sl = float(os.environ.get("SL", "5"))
    _tp = float(os.environ.get("TP", "10"))
    for s in d["slots"]:
        s["exit_config"]["stop_loss_value"] = _sl
        s["exit_config"]["target_value"] = _tp
    if mode == "agg":
        d["move_sl_agg_pnl_enabled"] = True
        d["move_sl_agg_pnl_threshold"] = 150
        d["move_sl_agg_pnl_direction"] = "loss"
    elif mode == "hit":
        d["move_sl_enabled"] = True
        d["move_sl_safety_sec"] = 0
        d["move_sl_action"] = "Move Only for Profitable Legs"
        d["move_sl_hit_on_leg_sl"] = True
        d["move_sl_hit_on_leg_target"] = True
    return d


def run(mode, env):
    for k in ("_USE_PER_SLOT", "_USE_PF_MONITOR", "_USE_PF_ENFORCE", "_USE_UNIFIED_ENGINE"):
        os.environ.pop(k, None)
    os.environ.update(env)
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    clear_cross_portfolio_bus()
    r = run_portfolio_backtest(str(ROOT / "catalog"), portfolio_from_dict(build(mode)),
                               user_id="_default")
    return round(r.get("total_pnl") or 0, 4), r.get("total_trades")


if __name__ == "__main__":
    MODE = os.environ.get("MODE", "agg")
    print(f"=== Move-SL mode={MODE} ===")
    base = run(MODE, {"_USE_PER_SLOT": "1"})
    live = run(MODE, {"_USE_PF_MONITOR": "1", "_USE_PF_ENFORCE": "1"})
    print(f"per-slot two-pass : pnl={base[0]} trades={base[1]}")
    print(f"unified LIVE      : pnl={live[0]} trades={live[1]}")
