"""Verify aggregated bar parquet files - with Nautilus binary decoding."""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent
CATALOG_PATH = PROJECT_ROOT / "catalog"
DATA_DIR = CATALOG_PATH / "data" / "bar"

TOLERANCE = 1e-8
INSTRUMENTS = ["EURUSD", "GBPUSD", "USDJPY"]
SIDES = ["ASK", "BID"]
TIMEFRAMES = ["5-MINUTE", "30-MINUTE", "1-HOUR", "2-HOUR", "1-DAY", "1-WEEK", "1-MONTH"]

def get_parquet_files(bar_type: str) -> list:
    bar_dir = DATA_DIR / bar_type
    if not bar_dir.exists():
        return []
    return sorted(bar_dir.glob("*.parquet"))

def decode_nautilus_price_series(col: pd.Series) -> np.ndarray:
    """Decode Nautilus fixed-precision price (object/bytes) to float."""
    scale = 1e9
    if col.dtype == object:
        nonnull = col.dropna()
        if nonnull.empty:
            return np.array([])
        sample = nonnull.iloc[0]
        if isinstance(sample, bytes) and len(sample) == 8:
            buf = b"".join(col.dropna().tolist())
            arr = np.frombuffer(buf, dtype="<i8")
            return arr.astype(np.float64) / scale
    return col.astype(np.float64)

def load_bars(bar_type: str) -> pd.DataFrame:
    """Load all bars for a bar type."""
    files = get_parquet_files(bar_type)
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=False)
    
    # Decode OHLCV
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = decode_nautilus_price_series(df[col])
    
    # Use ts_event as index
    if "ts_event" in df.columns:
        df["_ts"] = pd.to_datetime(df["ts_event"], unit="ns", utc=True)
        df = df.set_index("_ts")
    
    df = df.sort_index()
    return df

def verify_one(instrument: str, side: str, timeframe: str) -> dict:
    """Verify one (instrument, side, timeframe) tuple."""
    result = {
        "instrument": instrument,
        "side": side,
        "timeframe": timeframe,
        "status": "PASS",
        "errors": [],
    }
    
    src_type = f"{instrument}.FOREX_MS-1-MINUTE-{side}-EXTERNAL"
    dst_type = f"{instrument}.FOREX_MS-{timeframe}-{side}-EXTERNAL"
    
    df_1min = load_bars(src_type)
    df_agg = load_bars(dst_type)
    
    if df_1min.empty or df_agg.empty:
        result["status"] = "SKIP"
        result["errors"] = [f"1min={len(df_1min)}, agg={len(df_agg)}"]
        return result
    
    # Pick 3 samples from middle of agg set
    n_agg = len(df_agg)
    indices = [n_agg // 3, n_agg // 2, 2 * n_agg // 3]
    indices = [i for i in indices if 0 <= i < n_agg]
    if not indices:
        indices = [0]
    
    timeframe_mins = {
        "5-MINUTE": 5, "30-MINUTE": 30, "1-HOUR": 60,
        "2-HOUR": 120, "1-DAY": 1440, "1-WEEK": 10080
    }
    
    for idx in indices:
        agg_ts = df_agg.index[idx]
        agg_bar = df_agg.iloc[idx]
        
        # Window (T-N, T]
        if timeframe in timeframe_mins:
            window_start = agg_ts - pd.Timedelta(minutes=timeframe_mins[timeframe])
        else:  # 1-MONTH
            if idx == 0:
                window_start = df_1min.index[0] - pd.Timedelta(days=1)
            else:
                window_start = df_agg.index[idx - 1]
        
        mask = (df_1min.index > window_start) & (df_1min.index <= agg_ts)
        window_bars = df_1min[mask]
        
        if window_bars.empty:
            result["errors"].append(f"[{agg_ts.strftime('%Y-%m-%d %H:%M')}] no bars in window")
            result["status"] = "FAIL"
            continue
        
        # Expected OHLCV
        exp_o = window_bars.iloc[0]["open"]
        exp_h = window_bars["high"].max()
        exp_l = window_bars["low"].min()
        exp_c = window_bars.iloc[-1]["close"]
        exp_v = window_bars["volume"].sum()
        
        # Actual
        act_o = float(agg_bar["open"])
        act_h = float(agg_bar["high"])
        act_l = float(agg_bar["low"])
        act_c = float(agg_bar["close"])
        act_v = float(agg_bar["volume"])
        
        # Check
        for field, exp, act in [("O", exp_o, act_o), ("H", exp_h, act_h),
                                ("L", exp_l, act_l), ("C", exp_c, act_c),
                                ("V", exp_v, act_v)]:
            diff = abs(float(exp) - float(act))
            if diff > TOLERANCE:
                result["errors"].append(f"  {agg_ts.strftime('%Y-%m-%d %H:%M')} {field}: "
                    f"exp {exp:.8f} got {act:.8f} diff {diff:.2e}")
                result["status"] = "FAIL"
    
    # Timestamp anchor checks
    errs = 0
    for ts in df_agg.index:
        ok = False
        if timeframe == "5-MINUTE" and ts.minute % 5 == 0:
            ok = True
        elif timeframe == "30-MINUTE" and ts.minute in (0, 30):
            ok = True
        elif timeframe == "1-HOUR" and ts.minute == 0:
            ok = True
        elif timeframe == "2-HOUR" and ts.hour % 2 == 0 and ts.minute == 0:
            ok = True
        elif timeframe == "1-DAY" and ts.hour == 0:
            ok = True
        elif timeframe == "1-WEEK" and ts.weekday() == 6:
            ok = True
        elif timeframe == "1-MONTH":
            next_day = ts + pd.Timedelta(days=1)
            ok = next_day.month != ts.month
        
        if not ok and errs < 2:
            result["errors"].append(f"  timestamp anchor: {ts}")
            result["status"] = "FAIL"
            errs += 1
    
    return result

def main():
    print("=" * 80)
    print("AGGREGATION VERIFICATION REPORT")
    print("=" * 80)
    
    results = []
    for instr in INSTRUMENTS:
        for side in SIDES:
            for tf in TIMEFRAMES:
                print(f"  {instr}-{side} {tf}...", end=" ", flush=True)
                r = verify_one(instr, side, tf)
                results.append(r)
                print(r["status"])
    
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    skipped = sum(1 for r in results if r["status"] == "SKIP")
    
    print(f"\n{'=' * 80}")
    print(f"RESULTS: {passed} PASS, {failed} FAIL, {skipped} SKIP (total {len(results)})\n")
    
    if failed > 0:
        print("FAILURES:")
        print("-" * 80)
        for r in results:
            if r["status"] == "FAIL":
                print(f"{r['instrument']}-{r['side']} {r['timeframe']}")
                for e in r["errors"][:2]:
                    print(f"  {e}")
        print()
    
    print("SUMMARY:")
    print(f"{'Instrument':<12} {'Side':<5} {'Timeframe':<12} {'Status':<7}")
    print("-" * 40)
    for r in results:
        if r["status"] != "SKIP":
            print(f"{r['instrument']:<12} {r['side']:<5} {r['timeframe']:<12} {r['status']:<7}")
    
    print(f"\n{'=' * 80}")
    if failed > 0:
        print(f"FAILED: {failed} mismatches detected")
        return 1
    else:
        print("SUCCESS: All aggregations verified")
        return 0

if __name__ == "__main__":
    sys.exit(main())
