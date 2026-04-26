"""Single-slot profiler. Runs ONE backtest slot in the main process and
reports where wall-time goes.

Usage:
    # Plain run with phase timers only (fast):
    python scripts/profile_slot.py --span 10y --strategy ema --mode run

    # Add cProfile on top (2-5x slowdown):
    python scripts/profile_slot.py --span 1y --strategy ema --mode cprofile

    # Generate flame graph via py-spy (wraps this script externally):
    py-spy record -o scratch/profiles/10y_ema.svg --subprocesses -- \
        python scripts/profile_slot.py --span 10y --strategy ema --mode run

Bypasses ProcessPoolExecutor so the profiler sees the full stack. Sets
_PROFILE_PHASES=1 before importing core.backtest_runner so the inline phase
timers populate results["phase_times"].
"""
from __future__ import annotations

import argparse
import cProfile
import json
import os
import pstats
import sys
import time
from pathlib import Path

# MUST be set before the backtest_runner import so the module-level _phase
# context managers see the flag.
os.environ["_PROFILE_PHASES"] = "1"

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT))

from core.backtest_runner import _run_single_slot  # noqa: E402
from core.models import StrategySlotConfig, ExitConfig  # noqa: E402


SPANS = {
    "30d": ("2024-12-01", "2024-12-31"),
    "1y":  ("2024-01-01", "2024-12-31"),
    "5y":  ("2020-01-01", "2024-12-31"),
    "10y": ("2015-01-01", "2024-12-31"),
}

STRATEGIES = {
    "ema": {
        "strategy_name": "EMA Cross",
        "strategy_params": {"fast_ema_period": 10, "slow_ema_period": 30},
    },
    "bollinger": {
        "strategy_name": "Bollinger Bands",
        "strategy_params": {"bb_period": 20, "bb_std": 2},
    },
}


def _build_slot(args) -> StrategySlotConfig:
    start, end = SPANS[args.span]
    strat = STRATEGIES[args.strategy]
    bar_type = f"{args.pair}.FOREX_MS-1-MINUTE-{args.side}-EXTERNAL"
    return StrategySlotConfig(
        slot_id=f"profile_{args.pair}_{args.strategy}_{args.span}",
        strategy_name=strat["strategy_name"],
        strategy_params=strat["strategy_params"],
        bar_type_str=bar_type,
        trade_size=1000.0,
        allocation_pct=0.0,
        exit_config=ExitConfig(
            stop_loss_type="percentage", stop_loss_value=0.5,
            target_type="percentage", target_value=1.0,
        ),
        enabled=True,
        start_date=start,
        end_date=end,
    )


def _run(args) -> dict:
    slot = _build_slot(args)
    t0 = time.time()
    results = _run_single_slot(
        catalog_path=args.catalog,
        slot=slot,
        capital=args.capital,
        custom_strategies_dir=None,
        slot_index=0,
        default_start_date=None,
        default_end_date=None,
    )
    results["_wall_seconds_outer"] = round(time.time() - t0, 3)
    return results


def _print_summary(args, results: dict) -> None:
    print("=" * 74)
    print(f"  pair:        {args.pair}")
    print(f"  side:        {args.side}")
    print(f"  span:        {args.span}  [{SPANS[args.span][0]} .. {SPANS[args.span][1]}]")
    print(f"  strategy:    {STRATEGIES[args.strategy]['strategy_name']}")
    print(f"  params:      {STRATEGIES[args.strategy]['strategy_params']}")
    print(f"  bar_type:    {args.pair}.FOREX_MS-1-MINUTE-{args.side}-EXTERNAL")
    print(f"  capital:     ${args.capital:,.0f}")
    print("=" * 74)
    print(f"  total_pnl:          ${results.get('total_pnl', 0):,.2f}")
    print(f"  trades:             {results.get('total_trades', 0):,}")
    print(f"  wall_seconds:       {results.get('elapsed_seconds', '-')}")
    print(f"  wall_outer_seconds: {results.get('_wall_seconds_outer', '-')}")
    print(f"  worker_rss_mb:      {results.get('worker_rss_mb', '-')}")
    print(f"  cache_hits/misses:  {results.get('cache_hits', 0)} / {results.get('cache_misses', 0)}")
    print("-" * 74)
    phases = results.get("phase_times") or {}
    if phases:
        total = sum(phases.values())
        print("  PHASE BREAKDOWN")
        print(f"  {'phase':<22} {'seconds':>10} {'pct':>7}")
        for label, secs in sorted(phases.items(), key=lambda kv: -kv[1]):
            pct = 100.0 * secs / total if total else 0.0
            print(f"  {label:<22} {secs:>10.3f} {pct:>6.1f}%")
        print(f"  {'SUM':<22} {total:>10.3f} {'100.0%':>7}")
        accounted = results.get("elapsed_seconds") or 0
        unaccounted = accounted - total
        print(f"  {'(unaccounted)':<22} {unaccounted:>10.3f} "
              f"{100.0 * unaccounted / accounted if accounted else 0:>6.1f}%")
    else:
        print("  No phase_times in results — is _PROFILE_PHASES=1 set?")
    print("=" * 74)


def _dump_json(args, results: dict) -> Path:
    out_dir = PROJECT / "scratch" / "profiles"
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"phase_{args.pair}_{args.strategy}_{args.side}_{args.span}.json"
    path = out_dir / fname
    # Trim huge lists before dumping (equity curve, trades) — we only care about
    # phase times and summary stats here. Keep result untouched for extended debugging.
    dumpable = {
        "pair": args.pair, "side": args.side, "span": args.span,
        "strategy": args.strategy, "capital": args.capital,
        "start_date": SPANS[args.span][0], "end_date": SPANS[args.span][1],
        "phase_times": results.get("phase_times"),
        "elapsed_seconds": results.get("elapsed_seconds"),
        "wall_outer_seconds": results.get("_wall_seconds_outer"),
        "total_pnl": results.get("total_pnl"),
        "total_trades": results.get("total_trades"),
        "worker_rss_mb": results.get("worker_rss_mb"),
        "cache_hits": results.get("cache_hits"),
        "cache_misses": results.get("cache_misses"),
        "cache_currsize": results.get("cache_currsize"),
    }
    path.write_text(json.dumps(dumpable, indent=2))
    return path


def _run_with_cprofile(args) -> dict:
    out_dir = PROJECT / "scratch" / "profiles"
    out_dir.mkdir(parents=True, exist_ok=True)
    prof_path = out_dir / f"cprofile_{args.pair}_{args.strategy}_{args.side}_{args.span}.prof"

    profiler = cProfile.Profile()
    profiler.enable()
    try:
        results = _run(args)
    finally:
        profiler.disable()
    profiler.dump_stats(str(prof_path))

    print("\n" + "=" * 74)
    print(f"cProfile dumped to: {prof_path}")
    print(f"View: snakeviz {prof_path}")
    print("=" * 74)
    print("\nTOP 30 by cumulative time:")
    stats = pstats.Stats(profiler).strip_dirs().sort_stats("cumulative")
    stats.print_stats(30)
    print("\nTOP 30 by tottime:")
    stats.sort_stats("tottime").print_stats(30)

    return results


def main():
    ap = argparse.ArgumentParser(description="Profile a single backtest slot.")
    ap.add_argument("--span", choices=list(SPANS.keys()), required=True)
    ap.add_argument("--strategy", choices=list(STRATEGIES.keys()), required=True)
    ap.add_argument("--mode", choices=["run", "cprofile"], default="run")
    ap.add_argument("--pair", default="EURUSD", help="Currency pair symbol (EURUSD, GBPUSD, USDJPY).")
    ap.add_argument("--side", choices=["BID", "ASK", "MID"], default="BID")
    ap.add_argument("--catalog", default="./catalog")
    ap.add_argument("--capital", type=float, default=100_000.0)
    args = ap.parse_args()

    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    if args.mode == "cprofile":
        results = _run_with_cprofile(args)
    else:
        results = _run(args)

    _print_summary(args, results)
    path = _dump_json(args, results)
    print(f"\nJSON summary: {path}")


if __name__ == "__main__":
    main()
