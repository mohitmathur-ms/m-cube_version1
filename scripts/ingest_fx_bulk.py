"""Bulk-ingest FX 1-minute CSVs from nested year/month/day folders into the catalog.

Folder layout expected:
    <root>/<INSTRUMENT>/<YYYY>/<MM>/<DD>/<DD.MM.YYYY>_{BID,ASK}_OHLCV.csv

Usage:
    python scripts/ingest_fx_bulk.py \\
        --root D:/Data_all/Fx \\
        --instruments GPBUSD:GBPUSD USDJPY:USDJPY \\
        --year-from 2024 --year-to 2024 \\
        --catalog ./catalog
"""

from __future__ import annotations

import argparse
import gc
import os
import sys
import time
from pathlib import Path

import pandas as pd
import psutil

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.csv_loader import QUANTITY_MAX, _parse_timestamps
from core.instrument_factory import create_instrument
from core.nautilus_loader import save_to_catalog, wrangle_bars


def _read_ohlcv_csv(path: str) -> pd.DataFrame:
    """Read an OHLCV CSV with case-insensitive column handling."""
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    required = ["timestamp", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}. Found {list(df.columns)}")
    df = df[required].copy()
    df["timestamp"] = _parse_timestamps(df["timestamp"])
    df = df.set_index("timestamp")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna()
    df["volume"] = df["volume"].clip(upper=QUANTITY_MAX)
    return df


def rss_mb(pid: int | None = None) -> float:
    return psutil.Process(pid or os.getpid()).memory_info().rss / (1024 * 1024)


def ingest_one(
    src_folder: Path,
    out_symbol: str,
    side: str,
    year_from: int,
    year_to: int,
    catalog_path: str,
    venue: str = "FOREX_MS",
    price_precision: int | None = None,
    month_from: int | None = None,
    month_to: int | None = None,
) -> dict:
    """Ingest one (instrument, side) pair from nested year/month/day CSVs."""
    metrics = {"instrument": out_symbol, "side": side}
    t0 = time.time()

    def _in_range(f: Path) -> bool:
        try:
            y = int(f.parent.parent.parent.name)
            m = int(f.parent.parent.name)
        except ValueError:
            return False
        if not (year_from <= y <= year_to):
            return False
        if month_from is not None and (y == year_from) and m < month_from:
            return False
        if month_to is not None and (y == year_to) and m > month_to:
            return False
        return True

    files = [f for f in src_folder.rglob(f"*_{side}_OHLCV.csv") if _in_range(f)]
    files.sort()
    metrics["file_count"] = len(files)
    metrics["scan_seconds"] = round(time.time() - t0, 2)

    if not files:
        print(f"  [{out_symbol} {side}] no files in year range {year_from}-{year_to}")
        return metrics

    rss_before = rss_mb()

    t_read = time.time()
    dfs = []
    for f in files:
        dfs.append(_read_ohlcv_csv(str(f)))
    metrics["read_seconds"] = round(time.time() - t_read, 2)
    metrics["rss_after_read_mb"] = round(rss_mb(), 1)

    t_concat = time.time()
    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    metrics["concat_seconds"] = round(time.time() - t_concat, 2)
    metrics["bar_count"] = len(df)
    del dfs
    gc.collect()

    base, quote = out_symbol[:3], out_symbol[3:]
    instrument = create_instrument(base, quote, venue=venue, price_precision=price_precision)

    t_wrangle = time.time()
    bars = wrangle_bars(df, instrument, timeframe="1-MINUTE", price_type=side)
    metrics["wrangle_seconds"] = round(time.time() - t_wrangle, 2)

    t_write = time.time()
    save_to_catalog(bars, instrument, catalog_path)
    metrics["write_seconds"] = round(time.time() - t_write, 2)

    metrics["rss_peak_mb"] = round(rss_mb(), 1)
    metrics["rss_delta_mb"] = round(rss_mb() - rss_before, 1)
    metrics["total_seconds"] = round(time.time() - t0, 2)

    del df, bars
    gc.collect()
    return metrics


def parse_instruments(arg_list: list[str]) -> list[tuple[str, str]]:
    """Parse 'SRCFOLDER:OUTSYMBOL' pairs (e.g. 'GPBUSD:GBPUSD')."""
    pairs = []
    for item in arg_list:
        if ":" in item:
            src, out = item.split(":", 1)
        else:
            src = out = item
        pairs.append((src, out))
    return pairs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Root folder e.g. D:/Data_all/Fx")
    ap.add_argument("--instruments", nargs="+", required=True,
                    help="SRC_FOLDER:OUT_SYMBOL pairs, e.g. GPBUSD:GBPUSD USDJPY:USDJPY")
    ap.add_argument("--year-from", type=int, required=True)
    ap.add_argument("--year-to", type=int, required=True)
    ap.add_argument("--month-from", type=int, default=None, help="1-12, applied only in year-from")
    ap.add_argument("--month-to", type=int, default=None, help="1-12, applied only in year-to")
    ap.add_argument("--catalog", default="./catalog")
    ap.add_argument("--venue", default="FOREX_MS")
    ap.add_argument("--sides", nargs="+", default=["BID", "ASK"])
    ap.add_argument("--price-precision", type=int, default=None,
                    help="Override price precision (FX typical: 5; USDJPY: 3)")
    args = ap.parse_args()

    print("=" * 70)
    print(f"FX Bulk Ingest  |  years {args.year_from}-{args.year_to}  |  catalog: {args.catalog}")
    print(f"RSS at start: {rss_mb():.1f} MB")
    print("=" * 70)

    t_run = time.time()
    all_metrics = []
    for src_folder_name, out_symbol in parse_instruments(args.instruments):
        src = Path(args.root) / src_folder_name
        if not src.exists():
            print(f"[skip] {src} does not exist")
            continue
        for side in args.sides:
            print(f"\n>>> {out_symbol} {side}")
            m = ingest_one(
                src_folder=src,
                out_symbol=out_symbol,
                side=side,
                year_from=args.year_from,
                year_to=args.year_to,
                catalog_path=args.catalog,
                venue=args.venue,
                price_precision=args.price_precision,
                month_from=args.month_from,
                month_to=args.month_to,
            )
            all_metrics.append(m)
            print(f"    files={m.get('file_count', 0)}  bars={m.get('bar_count', 0):,}  "
                  f"read={m.get('read_seconds', 0)}s  concat={m.get('concat_seconds', 0)}s  "
                  f"wrangle={m.get('wrangle_seconds', 0)}s  write={m.get('write_seconds', 0)}s  "
                  f"total={m.get('total_seconds', 0)}s  peak_rss={m.get('rss_peak_mb', 0)}MB")

    total = time.time() - t_run
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"{'instrument':<10} {'side':<5} {'files':>6} {'bars':>10} "
          f"{'read':>7} {'concat':>7} {'wrangle':>8} {'write':>7} {'total':>7} {'rss_MB':>8}")
    for m in all_metrics:
        print(f"{m.get('instrument', ''):<10} {m.get('side', ''):<5} "
              f"{m.get('file_count', 0):>6} {m.get('bar_count', 0):>10,} "
              f"{m.get('read_seconds', 0):>7} {m.get('concat_seconds', 0):>7} "
              f"{m.get('wrangle_seconds', 0):>8} {m.get('write_seconds', 0):>7} "
              f"{m.get('total_seconds', 0):>7} {m.get('rss_peak_mb', 0):>8}")
    total_bars = sum(m.get("bar_count", 0) for m in all_metrics)
    print(f"\nTotal wall time: {total:.2f}s  |  total bars ingested: {total_bars:,}")


if __name__ == "__main__":
    main()
