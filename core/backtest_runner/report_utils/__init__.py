"""Low-level report helpers shared by the results and exit-fill layers:
first-available-column pick and UTC timestamp coercion."""

from __future__ import annotations

import pandas as pd


# ─── FX-aware PnL helpers ───────────────────────────────────────────────────

def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column in `candidates` that exists in `df`."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_utc_ts(raw) -> pd.Timestamp | None:
    """Best-effort conversion of a report timestamp cell to a UTC Timestamp."""
    if raw is None:
        return None
    try:
        ts = pd.Timestamp(raw)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts
