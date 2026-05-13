"""Aggregate 1-minute catalog bars to higher timeframes.

Reads ``<symbol>.<venue>-1-MINUTE-<side>-EXTERNAL`` from a Nautilus
``ParquetDataCatalog`` and writes new BarTypes for each requested target
timeframe (5-MINUTE, 30-MINUTE, 1-HOUR, 2-HOUR, 1-DAY, 1-WEEK, 1-MONTH).
Optionally also emits a sidecar parquet of derived features per
``(instrument, side, timeframe)`` tuple at
``<catalog>/features/bar/<bartype>.parquet``.

Bucketing convention is left-open ``(T-N, T]`` — see ``core.aggregator`` for the
math and the notebook ``ipynb/aggregate_1min_to_5min.ipynb`` for the parity
proof against Nautilus' built-in aggregator.

Usage:
    python scripts/aggregate_catalog.py \\
        --catalog ./catalog \\
        --instruments EURUSD GBPUSD USDJPY \\
        --sides ASK BID \\
        --timeframes 5-MINUTE 30-MINUTE 1-HOUR 2-HOUR 1-DAY 1-WEEK 1-MONTH \\
        --overwrite
"""

from __future__ import annotations

import argparse
import gc
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nautilus_trader.model.data import Bar

from core.aggregator import (
    TIMEFRAME_TO_RULE,
    aggregate_ohlcv,
    aggregate_with_features,
)
from core.instrument_factory import create_instrument
from core.nautilus_loader import (
    invalidate_catalog_cache,
    load_catalog,
    make_bar_type_str,
    save_to_catalog,
    wrangle_bars,
)


def _bars_to_df(bars: list[Bar]) -> pd.DataFrame:
    """Convert a list of Nautilus ``Bar`` objects to a UTC-indexed DataFrame."""
    n = len(bars)
    opens = [0.0] * n
    highs = [0.0] * n
    lows = [0.0] * n
    closes = [0.0] * n
    volumes = [0.0] * n
    ts = [0] * n
    for i, b in enumerate(bars):
        opens[i] = float(b.open)
        highs[i] = float(b.high)
        lows[i] = float(b.low)
        closes[i] = float(b.close)
        volumes[i] = float(b.volume)
        ts[i] = int(b.ts_event)
    idx = pd.DatetimeIndex(pd.to_datetime(ts, unit="ns", utc=True), name="timestamp")
    return pd.DataFrame(
        {"open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes},
        index=idx,
    )


def _split_symbol(symbol: str) -> tuple[str, str]:
    """Split a 6-char FX pair like ``EURUSD`` into ``("EUR", "USD")``."""
    if len(symbol) != 6:
        raise ValueError(f"expected 6-char FX symbol, got {symbol!r}")
    return symbol[:3], symbol[3:]


def _bar_dir(catalog_path: str, bar_type_str: str) -> Path:
    return Path(catalog_path) / "data" / "bar" / bar_type_str


def _features_path(catalog_path: str, bar_type_str: str) -> Path:
    return Path(catalog_path) / "features" / "bar" / f"{bar_type_str}.parquet"


def aggregate_one(
    symbol: str,
    side: str,
    timeframe: str,
    catalog_path: str,
    *,
    venue: str = "FOREX_MS",
    price_precision: int | None = None,
    with_features: bool = True,
    overwrite: bool = True,
    vwap_session: str | None = "1D",
    week_anchor: str = "SUN",
) -> dict:
    """Aggregate one (symbol, side, timeframe) tuple. Returns a metrics dict."""
    metrics = {"symbol": symbol, "side": side, "timeframe": timeframe}
    t0 = time.time()

    base, quote = _split_symbol(symbol)
    instrument = create_instrument(base, quote, venue=venue, price_precision=price_precision)

    src_bar_type_str = make_bar_type_str(instrument, timeframe="1-MINUTE", price_type=side)
    dst_bar_type_str = make_bar_type_str(instrument, timeframe=timeframe, price_type=side)
    metrics["src"] = src_bar_type_str
    metrics["dst"] = dst_bar_type_str

    catalog = load_catalog(catalog_path)
    src_bars = catalog.bars(bar_types=[src_bar_type_str])
    metrics["src_bar_count"] = len(src_bars)
    if not src_bars:
        print(f"  [{symbol} {side} {timeframe}] no source bars at {src_bar_type_str}")
        return metrics

    t_load = time.time()
    df_1min = _bars_to_df(src_bars)
    del src_bars
    gc.collect()
    metrics["load_seconds"] = round(time.time() - t_load, 2)

    t_agg = time.time()
    if with_features:
        agg = aggregate_with_features(
            df_1min,
            timeframe,
            vwap_session=vwap_session,
            week_anchor=week_anchor,
        )
        ohlcv_df = agg[["open", "high", "low", "close", "volume"]]
    else:
        ohlcv_df = aggregate_ohlcv(df_1min, timeframe, week_anchor=week_anchor)
        agg = None
    metrics["agg_seconds"] = round(time.time() - t_agg, 2)
    metrics["dst_bar_count"] = len(ohlcv_df)

    if ohlcv_df.empty:
        print(f"  [{symbol} {side} {timeframe}] aggregation produced 0 bars")
        return metrics

    if overwrite:
        dst_dir = _bar_dir(catalog_path, dst_bar_type_str)
        if dst_dir.exists():
            shutil.rmtree(dst_dir)

    t_wrangle = time.time()
    bars = wrangle_bars(ohlcv_df, instrument, timeframe=timeframe, price_type=side)
    metrics["wrangle_seconds"] = round(time.time() - t_wrangle, 2)

    t_write = time.time()
    save_to_catalog(bars, instrument, catalog_path)
    metrics["write_seconds"] = round(time.time() - t_write, 2)

    if with_features and agg is not None:
        t_feat = time.time()
        feat_path = _features_path(catalog_path, dst_bar_type_str)
        feat_path.parent.mkdir(parents=True, exist_ok=True)
        agg.to_parquet(feat_path, index=True)
        metrics["features_path"] = str(feat_path)
        metrics["features_seconds"] = round(time.time() - t_feat, 2)

    metrics["total_seconds"] = round(time.time() - t0, 2)
    del df_1min, ohlcv_df, bars, agg
    gc.collect()
    return metrics


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", default="./catalog")
    ap.add_argument("--instruments", nargs="+", required=True,
                    help="6-char FX symbols, e.g. EURUSD GBPUSD USDJPY")
    ap.add_argument("--sides", nargs="+", default=["ASK", "BID"])
    ap.add_argument("--timeframes", nargs="+", default=list(TIMEFRAME_TO_RULE),
                    help=f"Subset of: {' '.join(TIMEFRAME_TO_RULE)}")
    ap.add_argument("--venue", default="FOREX_MS")
    ap.add_argument("--price-precision", type=int, default=None,
                    help="Override price precision (FX typical: 5; USDJPY: 3)")
    ap.add_argument("--features", action=argparse.BooleanOptionalAction, default=True,
                    help="Also write a per-tuple feature sidecar parquet (default true)")
    ap.add_argument("--overwrite", action=argparse.BooleanOptionalAction, default=True,
                    help="Delete the destination BarType directory before writing (default true)")
    ap.add_argument("--vwap-session", default="1D",
                    help='VWAP reset cadence (pandas freq, e.g. "1D"); pass "none" for cumulative')
    ap.add_argument("--week-anchor", default="SUN", choices=["SUN", "MON", "FRI"])
    ap.add_argument("--pair-workers", type=int, default=0,
                    help="Parallel workers across (symbol, side, timeframe). 0 = auto.")
    args = ap.parse_args()

    unknown = [tf for tf in args.timeframes if tf not in TIMEFRAME_TO_RULE]
    if unknown:
        raise SystemExit(f"unknown timeframes: {unknown}. valid: {list(TIMEFRAME_TO_RULE)}")

    vwap_session = None if args.vwap_session.lower() == "none" else args.vwap_session

    tasks = [
        (symbol, side, tf)
        for symbol in args.instruments
        for side in args.sides
        for tf in args.timeframes
    ]
    if not tasks:
        print("No tasks.")
        return

    if args.pair_workers <= 0:
        args.pair_workers = min(len(tasks), os.cpu_count() or 2, 8)

    print("=" * 70)
    print(f"Aggregate Catalog  |  catalog: {args.catalog}  |  workers: {args.pair_workers}")
    print(f"Tasks: {len(tasks)}  ({len(args.instruments)} instruments × "
          f"{len(args.sides)} sides × {len(args.timeframes)} timeframes)")
    print("=" * 70)

    t_run = time.time()
    all_metrics: list[dict] = []

    if args.pair_workers <= 1:
        for symbol, side, tf in tasks:
            print(f"\n>>> {symbol} {side} {tf}")
            m = aggregate_one(
                symbol, side, tf, args.catalog,
                venue=args.venue,
                price_precision=args.price_precision,
                with_features=args.features,
                overwrite=args.overwrite,
                vwap_session=vwap_session,
                week_anchor=args.week_anchor,
            )
            all_metrics.append(m)
            _print_row(m)
    else:
        from concurrent.futures import ProcessPoolExecutor, as_completed
        max_workers = min(len(tasks), args.pair_workers)
        print(f"Running {len(tasks)} tasks across {max_workers} workers...")
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(
                    aggregate_one,
                    symbol, side, tf, args.catalog,
                    venue=args.venue,
                    price_precision=args.price_precision,
                    with_features=args.features,
                    overwrite=args.overwrite,
                    vwap_session=vwap_session,
                    week_anchor=args.week_anchor,
                ): (symbol, side, tf)
                for symbol, side, tf in tasks
            }
            for fut in as_completed(futures):
                label = futures[fut]
                try:
                    m = fut.result()
                    all_metrics.append(m)
                    print(f"  [done] {label[0]} {label[1]} {label[2]}")
                    _print_row(m)
                except Exception as e:
                    print(f"  [ERROR] {label}: {e}")

    # Drop cached catalog handles so any running server picks up the new BarTypes.
    invalidate_catalog_cache(args.catalog)

    total = time.time() - t_run
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"{'symbol':<8} {'side':<5} {'timeframe':<10} "
          f"{'src_bars':>10} {'dst_bars':>10} {'agg':>6} {'write':>6} {'total':>7}")
    for m in sorted(all_metrics, key=lambda x: (x.get("symbol", ""), x.get("side", ""), x.get("timeframe", ""))):
        print(f"{m.get('symbol', ''):<8} {m.get('side', ''):<5} {m.get('timeframe', ''):<10} "
              f"{m.get('src_bar_count', 0):>10,} {m.get('dst_bar_count', 0):>10,} "
              f"{m.get('agg_seconds', 0):>6} {m.get('write_seconds', 0):>6} "
              f"{m.get('total_seconds', 0):>7}")
    print(f"\nTotal wall time: {total:.2f}s")


def _print_row(m: dict) -> None:
    if not m.get("dst_bar_count"):
        return
    print(f"    src={m.get('src_bar_count', 0):,}  dst={m.get('dst_bar_count', 0):,}  "
          f"load={m.get('load_seconds', 0)}s  agg={m.get('agg_seconds', 0)}s  "
          f"wrangle={m.get('wrangle_seconds', 0)}s  write={m.get('write_seconds', 0)}s  "
          f"total={m.get('total_seconds', 0)}s")


if __name__ == "__main__":
    main()
