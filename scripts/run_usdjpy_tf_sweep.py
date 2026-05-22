"""Run USD/JPY EMA-Cross backtests across target timeframes (1/5/15/30 MIN).

One single-slot portfolio per target timeframe, all sharing 1-MINUTE base data
(coarser targets are internally aggregated). Honours the user's spec:

  Instrument     USD/JPY  (USDJPY.FOREX_MS, MID bars)
  Base TF        1-MINUTE ; targets 1 / 5 / 15 / 30-MINUTE
  SL / TGT       15% / 30% (percentage)
  Order type     MIS, square-off 23:50 (Asia/Kolkata)
  Entry window   06:00 - 23:50 (Asia/Kolkata)
  Days           Mon-Fri
  Strategy       EMA Cross (fast=10, slow=20)
  Range          2024-01-01 .. 2024-12-31

Run from repo root:  venv\\Scripts\\python.exe scripts\\run_usdjpy_tf_sweep.py
"""
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT))

from core.models import build_composite_bar_type, portfolio_from_dict
from core.backtest_runner import run_portfolio_backtest

CATALOG = str(PROJECT / "catalog")
BASE_BAR = "USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL"
TARGETS = ["1-MINUTE", "5-MINUTE", "15-MINUTE", "30-MINUTE"]
START, END = "2024-01-01", "2024-12-31"


def make_portfolio_dict(target: str) -> dict:
    composite = build_composite_bar_type(BASE_BAR, target)
    # 1-MINUTE target == base TF -> strategy runs on the base bars directly.
    strategy_bar_types = [] if target == "1-MINUTE" else [composite]
    exit_config = {
        "stop_loss_type": "percentage",
        "stop_loss_value": 15.0,
        "target_type": "percentage",
        "target_value": 30.0,
        "on_sl_action": "close",
        "on_target_action": "close",
    }
    slot = {
        "slot_id": f"jpy_ema_{target.lower()}",
        "strategy_name": "EMA Cross",
        "strategy_params": {"fast_ema_period": 10, "slow_ema_period": 20},
        "bar_type_str": BASE_BAR,
        "strategy_bar_types": strategy_bar_types,
        "lots": 1,
        "exit_config": exit_config,
        "enabled": True,
    }
    return {
        "name": f"USDJPY_EMA_{target}_2024",
        "description": f"USD/JPY EMA Cross 10/20, {target} target, SL15%/TGT30%, MIS",
        "starting_capital": 100000.0,
        "allocation_mode": "equal",
        "start_date": START,
        "end_date": END,
        "product": "MIS",
        # 23:30 lands on the 1/5/15/30-min bar grid, so every timeframe
        # flattens daily (23:50 is absent from the 15m/30m grids).
        "mis_squareoff_time": "23:30",
        "mis_squareoff_tz": "Asia/Kolkata",
        "run_on_days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
        "entry_start_time": "06:00:00",
        "entry_end_time": "23:50:00",
        "entry_window_tz": "Asia/Kolkata",
        "slots": [slot],
    }


def main():
    rows = []
    for target in TARGETS:
        print(f"\n{'='*70}\n  Running USD/JPY EMA Cross — target {target}\n{'='*70}")
        portfolio = portfolio_from_dict(make_portfolio_dict(target))
        result = run_portfolio_backtest(
            catalog_path=CATALOG,
            portfolio=portfolio,
            user_id="_default",
        )
        rows.append((target, result))
        print(
            f"  -> trades={result.get('total_trades')}  "
            f"pnl={result.get('total_pnl'):.2f}  "
            f"final={result.get('final_balance'):.2f}  "
            f"win_rate={result.get('win_rate')}"
        )

    print(f"\n\n{'='*78}\n  USD/JPY EMA Cross (10/20) — 2024 — SL 15% / TGT 30% / MIS 23:50\n{'='*78}")
    hdr = f"{'Target TF':<12}{'Trades':>8}{'Wins':>7}{'Losses':>8}{'Win%':>8}{'Net P&L':>14}{'Final Bal':>14}{'Ret%':>9}"
    print(hdr)
    print("-" * len(hdr))
    for target, r in rows:
        print(
            f"{target:<12}{r.get('total_trades', 0):>8}{r.get('wins', 0):>7}"
            f"{r.get('losses', 0):>8}{(r.get('win_rate') or 0):>8.1f}"
            f"{r.get('total_pnl', 0):>14,.2f}{r.get('final_balance', 0):>14,.2f}"
            f"{(r.get('total_return_pct') or 0):>9.2f}"
        )


if __name__ == "__main__":
    main()
