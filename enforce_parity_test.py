"""Phase 3 ENFORCEMENT parity test (live SqOff via the portfolio monitor).

Runs a clean 2-leg crypto portfolio (no aggregation / entry-window → unified
eligible) with a Combined-Loss SqOff portfolio stoploss, three ways:

  1. per-slot Path A      (_USE_PER_SLOT=1)            -> post-run clip (baseline)
  2. unified detect-only  (_USE_PF_MONITOR=1)          -> post-run clip + monitor observes
  3. unified ENFORCE      (_USE_PF_MONITOR=1 +_USE_PF_ENFORCE=1) -> live SqOff in-engine

Live enforcement P&L is the TRUE engine value; the post-run clip is a ppp
reconstruction, so (3) is expected CLOSE to (1) but not byte-identical. The key
checks: enforce differs from the unclipped run, lands near the baseline, and the
orderbook shows live "Portfolio Stoploss" closes.
"""
import sys, os, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def _leg(slot_id, strat, params, btype, lots=1):
    return {
        "slot_id": slot_id, "strategy_name": strat, "strategy_params": params,
        "bar_type_str": btype, "strategy_bar_types": [], "lots": lots,
        "allocation_pct": 0, "enabled": True,
        "start_date": None, "end_date": None,
        "squareoff_time": None, "squareoff_tz": None,
        "exit_config": {
            "exit_price_format": "ohlcv",
            "stop_loss_type": "percentage", "stop_loss_value": 10,
            "target_type": "percentage", "target_value": 20,
            "on_sl_action": "close", "on_target_action": "close",
            "max_re_executions": 0, "max_re_entries": 0,
            "armed_at_start": True, "squareoff_time": None, "squareoff_tz": None,
        },
    }


def build(pf_value):
    return {
        "name": "ENFORCE_PARITY", "starting_capital": 100000,
        "allocation_mode": "equal",
        "start_date": "2026-02-01", "end_date": "2026-02-28",
        "pf_sl_enabled": True, "pf_sl_type": "Combined Loss",
        "pf_sl_value": pf_value, "pf_sl_action": "SqOff",
        "slots": [
            _leg("eth_ema", "EMA Cross", {"fast_ema_period": 9, "slow_ema_period": 21},
                 "ETHUSD.COINBASE_MS-1-MINUTE-LAST-EXTERNAL", lots=1),
            _leg("sol_ema", "EMA Cross", {"fast_ema_period": 5, "slow_ema_period": 13},
                 "SOLUSD.COINBASE_MS-1-MINUTE-LAST-EXTERNAL", lots=20),
        ],
    }


def run(pf_value, env):
    for k in ("_USE_PER_SLOT", "_USE_PF_MONITOR", "_USE_PF_ENFORCE", "_USE_UNIFIED_ENGINE"):
        os.environ.pop(k, None)
    os.environ.update(env)
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    clear_cross_portfolio_bus()
    r = run_portfolio_backtest(str(ROOT / "catalog"), portfolio_from_dict(build(pf_value)),
                               user_id="_default")
    # Count live "Portfolio Stoploss" exit reasons across legs' orderbooks.
    live_closes = 0
    for sr in (r.get("slot_results") or {}).values():
        for t in (sr.get("orderbook") or []):
            if "Portfolio Stoploss" in str(t.get("EXIT DETAILED REASON") or t.get("EXIT REASON") or ""):
                live_closes += 1
    return round(r.get("total_pnl") or 0, 4), r.get("total_trades"), live_closes


if __name__ == "__main__":
    PF = float(os.environ.get("PFV", "200"))
    print(f"=== Combined-Loss SqOff, pf_sl_value={PF} ===")
    base = run(PF, {"_USE_PER_SLOT": "1"})
    detect = run(PF, {"_USE_PF_MONITOR": "1"})
    enforce = run(PF, {"_USE_PF_MONITOR": "1", "_USE_PF_ENFORCE": "1"})
    print(f"per-slot (post-run clip) : pnl={base[0]} trades={base[1]} live_closes={base[2]}")
    print(f"unified detect-only      : pnl={detect[0]} trades={detect[1]} live_closes={detect[2]}")
    print(f"unified ENFORCE (live)   : pnl={enforce[0]} trades={enforce[1]} live_closes={enforce[2]}")
