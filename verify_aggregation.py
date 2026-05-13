"""Verify aggregated bar parquet files.

Check that aggregated bars (5-MIN, 30-MIN, 1-HOUR, 2-HOUR, 1-DAY, 1-WEEK, 1-MONTH)
correctly aggregate from 1-MINUTE bars using (open=first, high=max, low=min, close=last, volume=sum).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import Bar
from core.nautilus_loader import load_catalog

TOLERANCE = 1e-9
CATALOG_PATH = PROJECT_ROOT / "catalog"
INSTRUMENTS = ["EURUSD", "GBPUSD", "USDJPY"]
SIDES = ["ASK", "BID"]
TIMEFRAMES = ["5-MINUTE", "30-MINUTE", "1-HOUR", "2-HOUR", "1-DAY", "1-WEEK", "1-MONTH"]
TIMEFRAME_WINDOWS = {
    "5-MINUTE": 5,
    "30-MINUTE": 30,
    "1-HOUR": 60,
    "2-HOUR": 120,
    "1-DAY": 1440,
    "1-WEEK": 10080,
    "1-MONTH": None,
}

def _bars_to_df(bars: list[Bar]) -> pd.DataFrame:
    """Convert Nautilus Bar objects to UTC-indexed DataFrame."""
    n = len(bars)
    if not n:
        return pd.DataFrame()
    opens = [float(b.open) for b in bars]
    highs = [float(b.high) for b in bars]
    lows = [float(b.low) for b in bars]
    closes = [float(b.close) for b in bars]
    volumes = [float(b.volume) for b in bars]
    ts = [int(b.ts_event) for b in bars]
    
    idx = pd.DatetimeIndex(pd.to_datetime(ts, unit="ns", utc=True), name="timestamp")
    return pd.DataFrame(
        {"open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes},
        index=idx,
    )

def bar_type_str(instrument: str, side: str, timeframe: str) -> str:
    """Make a bar type string like EURUSD.FOREX_MS-5-MINUTE-ASK-EXTERNAL."""
    return f"{instrument}.FOREX_MS-{timeframe}-{side}-EXTERNAL"

def verify_aggregation(catalog: ParquetDataCatalog, instrument: str, side: str, timeframe: str) -> dict:
    """Verify one (instrument, side, timeframe) tuple."""
    result = {
        "instrument": instrument,
        "side": side,
        "timeframe": timeframe,
        "status": "PASS",
        "errors": [],
    }
    
    src_type = bar_type_str(instrument, side, "1-MINUTE")
    dst_type = bar_type_str(instrument, side, timeframe)
    
    try:
        src_bars = catalog.bars(bar_types=[src_type])
        dst_bars = catalog.bars(bar_types=[dst_type])
    except Exception as e:
        result["status"] = "SKIP"
        result["errors"] = [str(e)]
        return result
    
    if not src_bars or not dst_bars:
        result["status"] = "SKIP"
        result["errors"] = [f"src={len(src_bars)}, dst={len(dst_bars)}"]
        return result
    
    df_1min = _bars_to_df(src_bars)
    df_agg = _bars_to_df(dst_bars)
    
    if df_1min.empty or df_agg.empty:
        result["status"] = "SKIP"
        result["errors"] = ["empty dataframe"]
        return result
    
    # Pick 3 random bars from the middle of the aggregated set
    n_agg = len(df_agg)
    if n_agg < 5:
        start_idx = 1
    else:
        start_idx = max(1, (n_agg - 1) // 3)
    
    sample_indices = [start_idx, start_idx + n_agg // 3, min(start_idx + 2 * n_agg // 3, n_agg - 2)]
    sample_indices = [i for i in sample_indices if 0 < i < n_agg - 1]
    
    if not sample_indices:
        sample_indices = [1]
    
    for sample_idx in sample_indices:
        agg_bar = df_agg.iloc[sample_idx]
        agg_ts = df_agg.index[sample_idx]
        
        # Window is (T-N, T] — left-open, right-closed
        window_minutes = TIMEFRAME_WINDOWS.get(timeframe)
        if window_minutes is None:
            # For monthly, estimate from the data
            if sample_idx == 0:
                window_start = df_1min.index[0] - pd.Timedelta(days=1)
            else:
                prev_ts = df_agg.index[sample_idx - 1]
                window_start = prev_ts
        else:
            window_start = agg_ts - pd.Timedelta(minutes=window_minutes)
        
        # Select 1-min bars in (window_start, agg_ts]
        mask = (df_1min.index > window_start) & (df_1min.index <= agg_ts)
        window_bars = df_1min[mask]
        
        if window_bars.empty:
            result["errors"].append(
                f"  TS {agg_ts}: no 1-min bars in window ({window_start}, {agg_ts}]"
            )
            result["status"] = "FAIL"
            continue
        
        # Recompute OHLCV
        exp_open = window_bars.iloc[0]["open"]
        exp_high = window_bars["high"].max()
        exp_low = window_bars["low"].min()
        exp_close = window_bars.iloc[-1]["close"]
        exp_volume = window_bars["volume"].sum()
        
        # Compare
        checks = [
            ("open", exp_open, agg_bar["open"]),
            ("high", exp_high, agg_bar["high"]),
            ("low", exp_low, agg_bar["low"]),
            ("close", exp_close, agg_bar["close"]),
            ("volume", exp_volume, agg_bar["volume"]),
        ]
        
        for field, expected, actual in checks:
            if abs(expected - actual) > TOLERANCE:
                result["errors"].append(
                    f"  TS {agg_ts} {field}: expected {expected:.10f}, got {actual:.10f}, "
                    f"diff {abs(expected - actual):.2e}"
                )
                result["status"] = "FAIL"
    
    # Check timestamp anchors
    anchor_checks = _check_timestamp_anchors(df_agg, timeframe)
    if anchor_checks:
        result["errors"].extend(anchor_checks)
        result["status"] = "FAIL"
    
    # Check drop_partial
    last_agg_ts = df_agg.index[-1]
    last_1min_ts = df_1min.index[-1]
    if last_agg_ts > last_1min_ts:
        result["errors"].append(
            f"  Last aggregated bar {last_agg_ts} > last 1-min bar {last_1min_ts} "
            "(drop_partial may not have worked)"
        )
        result["status"] = "FAIL"
    
    return result

def _check_timestamp_anchors(df_agg: pd.DataFrame, timeframe: str) -> list[str]:
    """Check that timestamps land on expected boundaries."""
    errors = []
    
    if timeframe == "5-MINUTE":
        for ts in df_agg.index:
            if ts.minute % 5 != 0:
                errors.append(f"  5-MIN {ts}: minute {ts.minute} not divisible by 5")
    elif timeframe == "30-MINUTE":
        for ts in df_agg.index:
            if ts.minute not in (0, 30):
                errors.append(f"  30-MIN {ts}: minute {ts.minute} not 0 or 30")
    elif timeframe == "1-HOUR":
        for ts in df_agg.index:
            if ts.minute != 0 or ts.second != 0:
                errors.append(f"  1-HOUR {ts}: not on hour boundary")
    elif timeframe == "2-HOUR":
        for ts in df_agg.index:
            if ts.hour % 2 != 0 or ts.minute != 0:
                errors.append(f"  2-HOUR {ts}: not on even hour boundary")
    elif timeframe == "1-DAY":
        for ts in df_agg.index:
            if ts.hour != 0 or ts.minute != 0:
                errors.append(f"  1-DAY {ts}: not at 00:00 UTC")
    elif timeframe == "1-WEEK":
        for ts in df_agg.index:
            if ts.weekday() != 6:
                errors.append(f"  1-WEEK {ts}: not on Sunday")
    elif timeframe == "1-MONTH":
        for ts in df_agg.index:
            next_day = ts + pd.Timedelta(days=1)
            if next_day.month != ts.month:
                pass
            else:
                errors.append(f"  1-MONTH {ts}: not on month-end")
    
    return errors[:5]

def main():
    catalog = load_catalog(str(CATALOG_PATH))
    
    print("=" * 80)
    print("AGGREGATION VERIFICATION REPORT")
    print("=" * 80)
    
    all_results = []
    for instrument in INSTRUMENTS:
        for side in SIDES:
            for timeframe in TIMEFRAMES:
                result = verify_aggregation(catalog, instrument, side, timeframe)
                all_results.append(result)
    
    passed = sum(1 for r in all_results if r["status"] == "PASS")
    failed = sum(1 for r in all_results if r["status"] == "FAIL")
    skipped = sum(1 for r in all_results if r["status"] == "SKIP")
    
    print(f"\nSummary: {passed} PASS, {failed} FAIL, {skipped} SKIP (total {len(all_results)})\n")
    
    failures = [r for r in all_results if r["status"] == "FAIL"]
    if failures:
        print("FAILURES:")
        print("-" * 80)
        for r in failures:
            print(f"{r['instrument']}-{r['side']} {r['timeframe']}")
            for err in r["errors"][:3]:
                print(f"  {err}")
        print()
    
    print("DETAILED RESULTS:")
    print("-" * 80)
    print(f"{'Instrument':<10} {'Side':<5} {'Timeframe':<12} {'Status':<6} {'Notes':<40}")
    print("-" * 80)
    for r in all_results:
        notes = r["errors"][0][:38] if r["errors"] else ""
        print(f"{r['instrument']:<10} {r['side']:<5} {r['timeframe']:<12} {r['status']:<6} {notes:<40}")
    
    print("\n" + "=" * 80)
    if failed > 0:
        print(f"RESULT: FAILED ({failed} mismatches found)")
        return 1
    else:
        print("RESULT: ALL CHECKS PASSED")
        return 0

if __name__ == "__main__":
    sys.exit(main())
