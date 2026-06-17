"""Verification helpers for the testing2 manual-verification set.

Reusable building blocks shared by the VP/OT verification driver:
  * load_bars()      -> decode the NIFTY 1-second feed (Nautilus raw int64 price
                        bytes -> float; ts_event UTC ns -> tz-aware IST index).
  * first_touch()    -> the Time axis: first bar in (entry, exit] whose low<=SL
                        (long) / high>=SL (short) — i.e. the trigger second.
  * expected_pct_sl()/expected_pct_tgt() -> the Price axis level math.

All times in the exported order book are IST ("Asia/Kolkata"); bars are decoded
to an IST DatetimeIndex so the two line up directly.
"""
from __future__ import annotations
import glob
import functools
import pandas as pd

FEED = "NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS-1-SECOND-LAST-EXTERNAL"
TICK = 0.01
IST = "Asia/Kolkata"
_PRICE_SCALE = 1e9  # Nautilus FIXED_PRECISION=9: raw int64 = price * 1e9


def _decode_price_col(s: pd.Series) -> pd.Series:
    """Nautilus serialises Price as little-endian int64 raw (value*1e9) packed
    into 8 bytes. Decode bytes -> int -> float price."""
    return s.map(lambda b: int.from_bytes(b, "little", signed=True) / _PRICE_SCALE)


@functools.lru_cache(maxsize=1)
def load_bars(catalog_root: str = "catalog") -> pd.DataFrame:
    """Return the full 1-second feed as a DataFrame indexed by IST timestamp
    with float open/high/low/close columns. Cached (the parquet is ~1.2M rows)."""
    files = sorted(glob.glob(f"{catalog_root}/data/bar/{FEED}/*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet for {FEED} under {catalog_root}")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    out = pd.DataFrame({
        "open": _decode_price_col(df["open"]),
        "high": _decode_price_col(df["high"]),
        "low": _decode_price_col(df["low"]),
        "close": _decode_price_col(df["close"]),
    })
    ts = pd.to_datetime(df["ts_event"].astype("int64"), utc=True).dt.tz_convert(IST)
    out.index = ts
    out = out.sort_index()
    return out


def snap(price: float, tick: float = TICK) -> float:
    return round(round(price / tick) * tick, 2)


def expected_pct_sl(fill: float, pct: float, is_long: bool) -> float:
    """Leg % SL level: long = F*(1-pct/100) below; short = F*(1+pct/100) above."""
    factor = (1 - pct / 100) if is_long else (1 + pct / 100)
    return snap(fill * factor)


def expected_pct_tgt(fill: float, pct: float, is_long: bool) -> float:
    factor = (1 + pct / 100) if is_long else (1 - pct / 100)
    return snap(fill * factor)


def first_touch(bars: pd.DataFrame, entry_ist: str, exit_ist: str,
                level: float, is_long: bool):
    """Time axis: first bar AFTER entry up to & incl. exit where the trigger is
    true (long: low<=level; short: high>=level). Returns (ts, row) or (None,None)."""
    lo = pd.Timestamp(entry_ist, tz=IST)
    hi = pd.Timestamp(exit_ist, tz=IST)
    window = bars[(bars.index > lo) & (bars.index <= hi)]
    if is_long:
        hit = window[window["low"] <= level + 1e-9]
    else:
        hit = window[window["high"] >= level - 1e-9]
    if hit.empty:
        return None, None
    ts = hit.index[0]
    return ts, bars.loc[ts] if ts in bars.index else hit.iloc[0]


def bar_at(bars: pd.DataFrame, ist: str):
    """The bar whose timestamp == this IST second (exit-fill cross-check)."""
    t = pd.Timestamp(ist, tz=IST)
    if t in bars.index:
        r = bars.loc[t]
        return r.iloc[0] if isinstance(r, pd.DataFrame) else r
    return None


if __name__ == "__main__":
    bars = load_bars()
    print(f"bars: {len(bars):,}  range {bars.index[0]} .. {bars.index[-1]}")
    print("first 2 rows:\n", bars.head(2).to_string())

    # Sanity: README says 2026-03-02 session open ~24741.
    open_bar = bars.loc["2026-03-02 09:15:00"]
    print("\n2026-03-02 09:15:00 open:", float(open_bar["open"]),
          "(README expects ~24741)")

    print("\n--- VP-01 OID2 (SHORT) ---")
    F, sl_ob, exit_ob = 24610.00, 24733.05, 24734.2
    exp = expected_pct_sl(F, 0.5, is_long=False)
    print(f"expected_sl={exp}  orderbook_sl={sl_ob}  match={abs(exp-sl_ob)<1e-9}")
    ts, row = first_touch(bars, "2026-03-05 09:15:00", "2026-03-05 14:41:02",
                          sl_ob, is_long=False)
    print(f"first high>=SL at {ts}  (orderbook exit 14:41:02)")
    fb = bar_at(bars, "2026-03-05 14:41:02")
    print(f"trigger-bar close={float(fb['close']) if fb is not None else None}  "
          f"orderbook fill={exit_ob}")

    print("\n--- VP-01 OID4 (LONG) ---")
    F, sl_ob, exit_ob = 24642.90, 24519.69, 24520.7
    exp = expected_pct_sl(F, 0.5, is_long=True)
    print(f"expected_sl={exp}  orderbook_sl={sl_ob}  match={abs(exp-sl_ob)<1e-9}")
    ts, row = first_touch(bars, "2026-03-06 09:15:00", "2026-03-06 14:48:03",
                          sl_ob, is_long=True)
    print(f"first low<=SL at {ts}  (orderbook exit 14:48:03)")
    fb = bar_at(bars, "2026-03-06 14:48:03")
    print(f"trigger-bar close={float(fb['close']) if fb is not None else None}  "
          f"orderbook fill={exit_ob}")