"""Benchmark a portfolio backtest with resource metrics.

Loads a portfolio JSON, runs via run_portfolio_backtest, samples
parent+children RSS and CPU at 0.5s intervals, and prints a full report.

Usage:
    python scripts/benchmark_portfolio.py --portfolio portfolios/FX_9_Slot_2024.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.backtest_runner import run_portfolio_backtest
from core.models import portfolio_from_dict


class ResourceMonitor(threading.Thread):
    """Sample RSS and CPU of the parent process and all children every `interval` seconds."""

    def __init__(self, interval: float = 0.5):
        super().__init__(daemon=True)
        self.interval = interval
        self.parent = psutil.Process(os.getpid())
        self._stop_event = threading.Event()
        self.samples: list[dict] = []
        self._t_start = None

    def run(self):
        # Prime cpu_percent (first call always returns 0)
        try:
            self.parent.cpu_percent(None)
        except psutil.Error:
            pass
        for c in self.parent.children(recursive=True):
            try:
                c.cpu_percent(None)
            except psutil.Error:
                pass
        self._t_start = time.time()
        while not self._stop_event.is_set():
            try:
                self._sample()
            except psutil.Error:
                pass
            self._stop_event.wait(self.interval)

    def _sample(self):
        elapsed = time.time() - self._t_start
        children = [c for c in self.parent.children(recursive=True) if c.is_running()]
        try:
            parent_rss = self.parent.memory_info().rss
            parent_cpu = self.parent.cpu_percent(None)
        except psutil.Error:
            parent_rss, parent_cpu = 0, 0.0

        child_rss = 0
        child_cpu = 0.0
        n_children = 0
        for c in children:
            try:
                child_rss += c.memory_info().rss
                child_cpu += c.cpu_percent(None)
                n_children += 1
            except psutil.Error:
                continue

        self.samples.append({
            "t": round(elapsed, 2),
            "rss_total_mb": round((parent_rss + child_rss) / (1024 * 1024), 1),
            "rss_parent_mb": round(parent_rss / (1024 * 1024), 1),
            "rss_children_mb": round(child_rss / (1024 * 1024), 1),
            "cpu_total_pct": round(parent_cpu + child_cpu, 1),
            "cpu_parent_pct": round(parent_cpu, 1),
            "cpu_children_pct": round(child_cpu, 1),
            "n_workers": n_children,
        })

    def stop(self):
        self._stop_event.set()

    def summary(self) -> dict:
        if not self.samples:
            return {}
        rss = [s["rss_total_mb"] for s in self.samples]
        rss_children = [s["rss_children_mb"] for s in self.samples]
        cpu = [s["cpu_total_pct"] for s in self.samples]
        n_workers_max = max((s["n_workers"] for s in self.samples), default=0)
        return {
            "duration_seconds": round(self.samples[-1]["t"], 2),
            "sample_count": len(self.samples),
            "rss_peak_mb": max(rss),
            "rss_avg_mb": round(sum(rss) / len(rss), 1),
            "rss_children_peak_mb": max(rss_children),
            "cpu_peak_pct": max(cpu),
            "cpu_avg_pct": round(sum(cpu) / len(cpu), 1),
            "max_concurrent_workers": n_workers_max,
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portfolio", required=True, help="Path to portfolio JSON file")
    ap.add_argument("--catalog", default="./catalog")
    ap.add_argument("--custom-strategies-dir", default="./custom_strategies")
    ap.add_argument("--monitor-interval", type=float, default=0.5)
    ap.add_argument("--save-samples", default=None,
                    help="Optional path to write per-sample resource data as JSON")
    args = ap.parse_args()

    portfolio_path = Path(args.portfolio)
    if not portfolio_path.exists():
        print(f"ERROR: portfolio file not found: {portfolio_path}")
        sys.exit(1)

    config = portfolio_from_dict(json.loads(portfolio_path.read_text()))
    n_slots = len(config.enabled_slots)

    import sys as _sys
    try:
        _sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    print("=" * 74, flush=True)
    print(f"Portfolio benchmark: {config.name}", flush=True)
    print(f"  slots (enabled): {n_slots}", flush=True)
    print(f"  starting capital: ${config.starting_capital:,.2f}", flush=True)
    print(f"  allocation mode: {config.allocation_mode}", flush=True)
    print(f"  catalog: {args.catalog}", flush=True)
    print(f"  cpu_count: {os.cpu_count()}  pid: {os.getpid()}", flush=True)
    print(f"  date range: per-slot (see JSON)", flush=True)
    print("=" * 74, flush=True)

    slot_completion_times: list[tuple[str, float]] = []
    t0 = time.time()

    def on_slot_complete(slot_id: str):
        elapsed = round(time.time() - t0, 2)
        slot_completion_times.append((slot_id, elapsed))
        print(f"  [+{elapsed:7.2f}s] slot done: {slot_id}  "
              f"({len(slot_completion_times)}/{n_slots})", flush=True)

    monitor = ResourceMonitor(interval=args.monitor_interval)
    monitor.start()

    try:
        results = run_portfolio_backtest(
            catalog_path=args.catalog,
            portfolio=config,
            custom_strategies_dir=args.custom_strategies_dir,
            on_slot_complete=on_slot_complete,
        )
        total_elapsed = time.time() - t0
    finally:
        monitor.stop()
        monitor.join(timeout=2)

    res_summary = monitor.summary()

    print()
    print("=" * 74)
    print("PORTFOLIO RESULTS")
    print("=" * 74)
    print(f"  starting capital: ${results['starting_capital']:,.2f}")
    print(f"  final balance:    ${results['final_balance']:,.2f}")
    print(f"  total pnl:        ${results['total_pnl']:,.2f}  "
          f"({results['total_return_pct']:.2f}%)")
    print(f"  total trades:     {results['total_trades']}  "
          f"(W: {results['wins']}  L: {results['losses']}  win rate: {results['win_rate']:.1f}%)")
    print(f"  max drawdown:     {results['max_drawdown']:.2f}%")
    if results.get("errors"):
        print(f"  errors:           {len(results['errors'])}")
        for e in results["errors"]:
            print(f"    - {e}")

    print()
    print("=" * 74)
    print("PER-SLOT TIMING")
    print("=" * 74)
    print(f"{'slot_id':<14} {'strategy':<20} {'instrument':<10} "
          f"{'trades':>7} {'elapsed_s':>10} {'pid':>7} {'pnl':>12}")
    per = results.get("per_strategy", {})
    slot_elapsed_list = []
    for slot in config.enabled_slots:
        r = per.get(slot.slot_id, {})
        pnl = r.get("pnl", 0.0)
        inst = slot.bar_type_str.split("-")[0]
        trades = r.get("trades", 0)
        elapsed_s = r.get("elapsed_seconds")
        worker_pid = r.get("worker_pid")
        elapsed_str = f"{elapsed_s:.2f}" if elapsed_s is not None else "   n/a"
        pid_str = str(worker_pid) if worker_pid is not None else "   -"
        if elapsed_s is not None:
            slot_elapsed_list.append(elapsed_s)
        print(f"{slot.slot_id:<14} {slot.strategy_name:<20} {inst:<10} "
              f"{trades:>7} {elapsed_str:>10} {pid_str:>7} ${pnl:>11,.2f}")

    if slot_elapsed_list:
        sum_slot_time = sum(slot_elapsed_list)
        max_slot_time = max(slot_elapsed_list)
        print(f"\n  sum of per-slot engine time: {sum_slot_time:.2f}s  "
              f"(sequential-equivalent estimate)")
        print(f"  slowest single slot:         {max_slot_time:.2f}s")
        if total_elapsed > 0:
            speedup = sum_slot_time / total_elapsed
            print(f"  observed speedup:            {speedup:.2f}x  "
                  f"(sum_slot_time / wall_time)")

    # Print completion-order timings (main-process order)
    print()
    print("Completion order (main process view):")
    for i, (sid, t) in enumerate(slot_completion_times, 1):
        print(f"  {i:>2}. +{t:>7.2f}s  {sid}")

    print()
    print("=" * 74)
    print("RESOURCE USAGE")
    print("=" * 74)
    print(f"  total wall time:        {total_elapsed:.2f}s")
    print(f"  monitor samples:        {res_summary.get('sample_count', 0)}")
    print(f"  RSS peak (total):       {res_summary.get('rss_peak_mb', 0):,.1f} MB")
    print(f"  RSS avg (total):        {res_summary.get('rss_avg_mb', 0):,.1f} MB")
    print(f"  RSS peak (children):    {res_summary.get('rss_children_peak_mb', 0):,.1f} MB")
    print(f"  CPU peak:               {res_summary.get('cpu_peak_pct', 0):.1f}%")
    print(f"  CPU avg:                {res_summary.get('cpu_avg_pct', 0):.1f}%")
    print(f"  max concurrent workers: {res_summary.get('max_concurrent_workers', 0)}")

    # Parallelism efficiency: if workers were fully utilized, CPU ~= 100% * min(workers, cores)
    cores = os.cpu_count() or 1
    theoretical_max_cpu = 100.0 * min(res_summary.get("max_concurrent_workers", 0) or 1, cores)
    if theoretical_max_cpu > 0:
        util = 100.0 * res_summary.get("cpu_avg_pct", 0) / theoretical_max_cpu
        print(f"  parallel utilization:   {util:.1f}% "
              f"(avg_cpu / {theoretical_max_cpu:.0f}% theoretical max)")

    if args.save_samples:
        Path(args.save_samples).write_text(json.dumps({
            "portfolio": config.name,
            "total_elapsed_seconds": round(total_elapsed, 2),
            "summary": res_summary,
            "samples": monitor.samples,
            "slot_completion_times": slot_completion_times,
        }, indent=2))
        print(f"\n  resource samples saved: {args.save_samples}")


if __name__ == "__main__":
    main()
