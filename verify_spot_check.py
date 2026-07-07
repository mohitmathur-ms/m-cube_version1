"""Quick aggregation spot-check: EURUSD ASK 5-MINUTE."""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "catalog" / "data" / "bar"

def decode_nautilus_price_series(col: pd.Series) -> np.ndarray:
    """Decode Nautilus fixed-precision price."""
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
    """Load all bars for a bar type."""
    bar_dir = DATA_DIR / bar_type
    if not bar_dir.exists():
        print(f"Not found: {bar_dir}")
        return pd.DataFrame()
    files = sorted(bar_dir.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    print(f"Loading {len(files)} parquet file(s)...", file=sys.stderr)
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

print("Loading 1-MINUTE bars...", file=sys.stderr, flush=True)
df_1min = load_bars("EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL")
print(f"Loaded {len(df_1min)} 1-min bars, range {df_1min.index[0]} to {df_1min.index[-1]}", file=sys.stderr)

print("Loading 5-MINUTE bars...", file=sys.stderr, flush=True)
df_5min = load_bars("EURUSD.FOREX_MS-5-MINUTE-ASK-EXTERNAL")
print(f"Loaded {len(df_5min)} 5-min bars, range {df_5min.index[0]} to {df_5min.index[-1]}", file=sys.stderr)

# Check the last 5-MIN bar
print("\nVerifying last 5-MINUTE bar:", file=sys.stderr)
agg_ts = df_5min.index[-1]
agg_bar = df_5min.iloc[-1]
print(f"5-min timestamp: {agg_ts}", file=sys.stderr)
print(f"5-min OHLCV: O={agg_bar['open']:.8f} H={agg_bar['high']:.8f} L={agg_bar['low']:.8f} C={agg_bar['close']:.8f} V={agg_bar['volume']:.2f}", file=sys.stderr)

# Find 1-min bars in window (T-5min, T]
window_start = agg_ts - pd.Timedelta(minutes=5)
mask = (df_1min.index > window_start) & (df_1min.index <= agg_ts)
window_bars = df_1min[mask]

print(f"Found {len(window_bars)} 1-min bars in window ({window_start}, {agg_ts}]", file=sys.stderr)
print(f"Window start ts: {window_bars.index[0] if len(window_bars) > 0 else 'N/A'}", file=sys.stderr)
print(f"Window end ts  : {window_bars.index[-1] if len(window_bars) > 0 else 'N/A'}", file=sys.stderr)

if not window_bars.empty:
    exp_o = window_bars.iloc[0]["open"]
    exp_h = window_bars["high"].max()
    exp_l = window_bars["low"].min()
    exp_c = window_bars.iloc[-1]["close"]
    exp_v = window_bars["volume"].sum()
    
    print(f"\nExpected OHLCV: O={exp_o:.8f} H={exp_h:.8f} L={exp_l:.8f} C={exp_c:.8f} V={exp_v:.2f}", file=sys.stderr)
    
    checks = [("open", exp_o, agg_bar["open"]),
              ("high", exp_h, agg_bar["high"]),
              ("low", exp_l, agg_bar["low"]),
              ("close", exp_c, agg_bar["close"]),
              ("volume", exp_v, agg_bar["volume"])]
    
    all_pass = True
    for field, exp, act in checks:
        diff = abs(float(exp) - float(act))
        status = "OK" if diff < 1e-8 else "FAIL"
        print(f"{field:8s}: {status}  (exp {exp:.10f} vs act {act:.10f}, diff {diff:.2e})", file=sys.stderr)
        if diff >= 1e-8:
            all_pass = False
    
    print(f"\n{'PASS' if all_pass else 'FAIL'}", file=sys.stderr)
    sys.exit(0 if all_pass else 1)
else:
    print("ERROR: No bars in window", file=sys.stderr)
    sys.exit(1)
