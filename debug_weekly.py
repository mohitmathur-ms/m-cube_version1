"""Detailed check: what's failing in 1-WEEK?"""

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

print("Loading bars...", file=sys.stderr)
df_1min = load_bars("EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL")
df_week = load_bars("EURUSD.FOREX_MS-1-WEEK-ASK-EXTERNAL")

# Pick a middle week bar (say, bar 100)
idx = 100
agg_ts = df_week.index[idx]
agg_bar = df_week.iloc[idx]

print(f"Checking week bar {idx}: {agg_ts}", file=sys.stderr)
print(f"OHLCV: O={agg_bar['open']:.8f} H={agg_bar['high']:.8f} L={agg_bar['low']:.8f} C={agg_bar['close']:.8f} V={agg_bar['volume']:.2f}", file=sys.stderr)

# Find 1-min bars in window
prev_ts = df_week.index[idx - 1] if idx > 0 else df_1min.index[0] - pd.Timedelta(days=1)
window_start = prev_ts
window_end = agg_ts

print(f"\nWindow: ({window_start}, {window_end}]", file=sys.stderr)

mask = (df_1min.index > window_start) & (df_1min.index <= window_end)
window_bars = df_1min[mask]

print(f"Found {len(window_bars)} 1-min bars", file=sys.stderr)
if len(window_bars) > 0:
    print(f"  First: {window_bars.index[0]}", file=sys.stderr)
    print(f"  Last:  {window_bars.index[-1]}", file=sys.stderr)
    
    exp_o = window_bars.iloc[0]["open"]
    exp_h = window_bars["high"].max()
    exp_l = window_bars["low"].min()
    exp_c = window_bars.iloc[-1]["close"]
    exp_v = window_bars["volume"].sum()
    
    print(f"\nExpected: O={exp_o:.8f} H={exp_h:.8f} L={exp_l:.8f} C={exp_c:.8f} V={exp_v:.2f}", file=sys.stderr)
    
    checks = [("open", exp_o, agg_bar["open"]),
              ("high", exp_h, agg_bar["high"]),
              ("low", exp_l, agg_bar["low"]),
              ("close", exp_c, agg_bar["close"]),
              ("volume", exp_v, agg_bar["volume"])]
    
    for field, exp, act in checks:
        diff = abs(float(exp) - float(act))
        status = "OK" if diff < 1e-8 else "FAIL"
        print(f"{field:8s}: {status}  diff {diff:.2e}", file=sys.stderr)
