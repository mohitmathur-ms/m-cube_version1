"""
Ad-hoc multi-timeframe BTC/USD backtest.

Runs EMA Cross on BTCUSD.COINBASE (base 1-MINUTE feed) once per target
timeframe (1 / 5 / 15 / 30 MINUTE), each with:
  - SL 15% / TGT 30% (percentage)
  - MIS order type, square-off 23:50 IST
  - entry window 06:00-23:50 IST, Mon-Fri only
  - 1.0 BTC per trade, 2024 full year

Each timeframe is its own single-slot PortfolioConfig run (exit management +
entry window + run_on_days route through the portfolio path, not the simple
single-strategy endpoint).
"""

import os
import sys

# Run from the project root so "./catalog" resolves.
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_DIR)
sys.path.insert(0, PROJECT_DIR)

from core.models import (
    PortfolioConfig,
    StrategySlotConfig,
    ExitConfig,
    build_composite_bar_type,
)
from core.backtest_runner import run_portfolio_backtest

BASE_BAR_TYPE = "BTCUSD.COINBASE-1-MINUTE-LAST-EXTERNAL"
CATALOG_PATH = "./catalog"
TARGET_TFS = ["1-MINUTE", "5-MINUTE", "15-MINUTE", "30-MINUTE"]

START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
TZ = "Asia/Kolkata"


def build_portfolio(target_tf: str) -> PortfolioConfig:
    composite = build_composite_bar_type(BASE_BAR_TYPE, target_tf)
    exit_cfg = ExitConfig(
        stop_loss_type="percentage",
        stop_loss_value=15.0,
        target_type="percentage",
        target_value=30.0,
        # leg squareoff left to portfolio MIS squareoff (23:50 IST)
    )
    slot = StrategySlotConfig(
        slot_id=f"btc-{target_tf.lower()}",
        strategy_name="EMA Cross",
        strategy_params={},  # defaults: fast=10, slow=20
        bar_type_str=BASE_BAR_TYPE,         # base feed (fills resolution)
        strategy_bar_types=[composite],      # signal/exit resolution
        lots=1.0,
        exit_config=exit_cfg,
        start_date=START_DATE,
        end_date=END_DATE,
    )
    return PortfolioConfig(
        name=f"BTC_EMA_{target_tf.replace('-', '')}_MIS",
        starting_capital=100_000.0,
        start_date=START_DATE,
        end_date=END_DATE,
        # ORDER_TYPE = MIS, square-off 23:50 IST
        product="MIS",
        mis_squareoff_time="23:50",
        mis_squareoff_tz=TZ,
        # Opening 06:00 / closing 23:50, Mon-Fri
        entry_start_time="06:00",
        entry_end_time="23:50",
        entry_window_tz=TZ,
        run_on_days=["MON", "TUE", "WED", "THU", "FRI"],
        slots=[slot],
    )


def main() -> None:
    summary = []
    for tf in TARGET_TFS:
        composite = build_composite_bar_type(BASE_BAR_TYPE, tf)
        print("\n" + "=" * 70)
        print(f"RUNNING  target={tf}  subscribe={composite}")
        print("=" * 70)
        pf = build_portfolio(tf)
        result = run_portfolio_backtest(CATALOG_PATH, pf)

        if result.get("errors"):
            print(f"  ERRORS: {result['errors']}")
        row = {
            "tf": tf,
            "trades": result.get("total_trades", 0),
            "wins": result.get("wins", 0),
            "losses": result.get("losses", 0),
            "flat": result.get("flat_trades", 0),
            "win_rate": result.get("win_rate", 0.0),
            "decisive_win_rate": result.get("decisive_win_rate", 0.0),
            "total_pnl": result.get("total_pnl", 0.0),
            "return_pct": result.get("total_return_pct", 0.0),
            "max_dd": result.get("max_drawdown", 0.0),
            "final_balance": result.get("final_balance", 0.0),
        }
        summary.append(row)
        print(
            f"  trades={row['trades']}  W/L/flat={row['wins']}/{row['losses']}/{row['flat']}"
            f"  pnl={row['total_pnl']:,.2f}  ret={row['return_pct']:.2f}%"
            f"  maxDD={row['max_dd']:,.2f}"
        )

    print("\n\n" + "#" * 78)
    print("SUMMARY  BTC/USD EMA Cross | SL 15% TGT 30% | MIS 23:50 IST | 06:00-23:50 | Mon-Fri | 2024")
    print("#" * 78)
    hdr = f"{'TF':<10}{'trades':>8}{'W':>6}{'L':>6}{'flat':>7}{'win%':>8}{'decW%':>8}{'PnL($)':>15}{'ret%':>9}{'maxDD($)':>14}"
    print(hdr)
    print("-" * len(hdr))
    for r in summary:
        print(
            f"{r['tf']:<10}{r['trades']:>8}{r['wins']:>6}{r['losses']:>6}{r['flat']:>7}"
            f"{r['win_rate']:>8.2f}{r['decisive_win_rate']:>8.2f}{r['total_pnl']:>15,.2f}"
            f"{r['return_pct']:>9.2f}{r['max_dd']:>14,.2f}"
        )


if __name__ == "__main__":
    main()
