"""Quick check: why are 1-WEEK and 1-MONTH failing?"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "catalog" / "data" / "bar"

def decode_nautilus_price_series(col: pd.Series) -> np.ndarray:
    scale = 1e9
    if col.dtype == object:
        nonnull = col.dropna()
        if not nonnull.empty:
            sample = nonnull.iloc[0]
            if isinstance(sample, bytes) and len(sample) == 8:
                buf = b"".join(col.dropna().tolist())
                arr = np.frombuffer(buf, dtype="<i8")
                return arr.astype(np.float64) / scale
    return col.astype(np.float64)

def load_bars(bar_type: str) -> pd.DataFrame:
    bar_dir = DATA_DIR / bar_type
    if not bar_dir.exists():
        return pd.DataFrame()
    files = sorted(bar_dir.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=False)
    
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = decode_nautilus_price_series(df[col])
    
    if "ts_event" in df.columns:
        df["_ts"] = pd.to_datetime(df["ts_event"], unit="ns", utc=True)
        df = df.set_index("_ts")
    
    df = df.sort_index()
    return df

print("Loading 1-WEEK bars...", file=sys.stderr)
df_week = load_bars("EURUSD.FOREX_MS-1-WEEK-ASK-EXTERNAL")
print(f"Loaded {len(df_week)} bars", file=sys.stderr)

print("\nChecking timestamps (should all be Sunday 00:00 UTC):")
for i, ts in enumerate(df_week.index[:10]):
    is_sunday = ts.weekday() == 6
    is_midnight = ts.hour == 0 and ts.minute == 0
    status = "OK" if (is_sunday and is_midnight) else "FAIL"
    print(f"  {i}: {ts} (weekday={ts.weekday()}, hour={ts.hour}) - {status}")

print("\nLoading 1-MONTH bars...", file=sys.stderr)
df_month = load_bars("EURUSD.FOREX_MS-1-MONTH-ASK-EXTERNAL")
print(f"Loaded {len(df_month)} bars", file=sys.stderr)

print("\nChecking timestamps (should all be month-end 00:00 UTC):")
for i, ts in enumerate(df_month.index[:10]):
    next_day = ts + pd.Timedelta(days=1)
    is_month_end = next_day.month != ts.month
    is_midnight = ts.hour == 0 and ts.minute == 0
    status = "OK" if (is_month_end and is_midnight) else "FAIL"
    print(f"  {i}: {ts} (month={ts.month}, next_month={next_day.month}, hour={ts.hour}) - {status}")
