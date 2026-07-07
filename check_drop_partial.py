"""Check if drop_partial is working correctly."""

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

df_1min = load_bars("EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL")
df_week = load_bars("EURUSD.FOREX_MS-1-WEEK-ASK-EXTERNAL")

last_1min = df_1min.index[-1]
last_week = df_week.index[-1]

print(f"Last 1-min bar:  {last_1min}", file=sys.stderr)
print(f"Last week bar:   {last_week}", file=sys.stderr)
print(f"Difference:      {last_week - last_1min}", file=sys.stderr)

if last_week > last_1min:
    print("\nWARNING: Last aggregated bar timestamp > last 1-min bar timestamp", file=sys.stderr)
    print("This indicates drop_partial=True did NOT work correctly.", file=sys.stderr)
    print("The last aggregated bar is PARTIAL (incomplete window).", file=sys.stderr)
else:
    print("\nOK: Last aggregated bar is within 1-min data range.", file=sys.stderr)
