"""Verify aggregated bar parquet files - direct parquet read version."""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent
CATALOG_PATH = PROJECT_ROOT / "catalog"
DATA_DIR = CATALOG_PATH / "data" / "bar"

TOLERANCE = 1e-9
INSTRUMENTS = ["EURUSD", "GBPUSD", "USDJPY"]
SIDES = ["ASK", "BID"]
TIMEFRAMES = ["5-MINUTE", "30-MINUTE", "1-HOUR", "2-HOUR", "1-DAY", "1-WEEK", "1-MONTH"]

def get_parquet_files(bar_type: str) -> list[Path]:
    """Get all parquet files for a bar type."""
    bar_dir = DATA_DIR / f"{bar_type}"
    if not bar_dir.exists():
        return []
    return list(bar_dir.glob("*.parquet"))

def load_bars(bar_type: str) -> pd.DataFrame:
    """Load all bars for a bar type."""
    files = get_parquet_files(bar_type)
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}", file=sys.stderr)
    if not dfs:
        return pd.DataFrame()
    result = pd.concat(dfs, ignore_index=False)
    result = result.sort_index()
    return result

def decode_nautilus_price(obj_val) -> float:
    """Decode Nautilus binary-encoded price (object dtype)."""
    if isinstance(obj_val, float):
        return obj_val
    try:
        return float(obj_val)
    except:
        return np.nan

def df_to_float_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Convert object-dtype OHLCV to float."""
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = df[col].apply(decode_nautilus_price)
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
    
    # Decode OHLCV from object dtype
    df_1min = df_to_float_ohlcv(df_1min)
    df_agg = df_to_float_ohlcv(df_agg)
    
    # Use ts_event as index if available
    if "ts_event" in df_1min.columns:
        df_1min["ts"] = pd.to_datetime(df_1min["ts_event"], unit="ns", utc=True)
        df_1min = df_1min.set_index("ts")
    
    if "ts_event" in df_agg.columns:
        df_agg["ts"] = pd.to_datetime(df_agg["ts_event"], unit="ns", utc=True)
        df_agg = df_agg.set_index("ts")
    
    if df_1min.empty or df_agg.empty:
        result["status"] = "SKIP"
        result["errors"] = ["after decode"]
        return result
    
    # Pick 3 samples
    n_agg = len(df_agg)
    if n_agg < 5:
        indices = [0]
    else:
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
            result["errors"].append(f"TS {agg_ts}: no bars in window")
            result["status"] = "FAIL"
            continue
        
        exp_open = window_bars.iloc[0]["open"]
        exp_high = window_bars["high"].max()
        exp_low = window_bars["low"].min()
        exp_close = window_bars.iloc[-1]["close"]
        exp_vol = window_bars["volume"].sum()
        
        checks = [
            ("open", exp_open, agg_bar["open"]),
            ("high", exp_high, agg_bar["high"]),
            ("low", exp_low, agg_bar["low"]),
            ("close", exp_close, agg_bar["close"]),
            ("volume", exp_vol, agg_bar["volume"]),
        ]
        
        for field, exp, act in checks:
            diff = abs(float(exp) - float(act))
            if diff > TOLERANCE:
                result["errors"].append(
                    f"TS {agg_ts} {field}: exp {exp:.8f} got {act:.8f} diff {diff:.2e}"
                )
                result["status"] = "FAIL"
    
    # Timestamp anchor checks
    for idx, ts in enumerate(df_agg.index):
        if timeframe == "5-MINUTE":
            if ts.minute % 5 != 0:
                result["errors"].append(f"5-MIN {ts}: minute not /5")
                result["status"] = "FAIL"
        elif timeframe == "30-MINUTE":
            if ts.minute not in (0, 30):
                result["errors"].append(f"30-MIN {ts}: minute not 0/30")
                result["status"] = "FAIL"
        elif timeframe == "1-HOUR":
            if ts.minute != 0:
                result["errors"].append(f"1-HOUR {ts}: minute != 0")
                result["status"] = "FAIL"
        elif timeframe == "2-HOUR":
            if ts.hour % 2 != 0:
                result["errors"].append(f"2-HOUR {ts}: hour not even")
                result["status"] = "FAIL"
        elif timeframe == "1-DAY":
            if ts.hour != 0:
                result["errors"].append(f"1-DAY {ts}: hour != 0")
                result["status"] = "FAIL"
        elif timeframe == "1-WEEK":
            if ts.weekday() != 6:
                result["errors"].append(f"1-WEEK {ts}: not Sunday")
                result["status"] = "FAIL"
    
    return result

def main():
    print("=" * 80)
    print("AGGREGATION VERIFICATION")
    print("=" * 80)
    
    results = []
    for instr in INSTRUMENTS:
        for side in SIDES:
            for tf in TIMEFRAMES:
                r = verify_one(instr, side, tf)
                results.append(r)
    
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    skipped = sum(1 for r in results if r["status"] == "SKIP")
    
    print(f"\nRESULTS: {passed} PASS, {failed} FAIL, {skipped} SKIP / {len(results)} total\n")
    
    if failed > 0:
        print("FAILURES:")
        for r in results:
            if r["status"] == "FAIL":
                print(f"{r['instrument']}-{r['side']} {r['timeframe']}")
                for e in r["errors"][:2]:
                    print(f"  {e}")
        print()
    
    print("SUMMARY TABLE:")
    print(f"{'Instrument':<10} {'Side':<5} {'Timeframe':<12} {'Status':<7}")
    print("-" * 40)
    for r in results:
        print(f"{r['instrument']:<10} {r['side']:<5} {r['timeframe']:<12} {r['status']:<7}")
    
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
