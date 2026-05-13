"""Aggregate 1-minute OHLCV bars to higher timeframes and compute derived features.

Pure pandas — no Nautilus types, no catalog I/O. The CLI driver in
`scripts/aggregate_catalog.py` handles loading from / writing to the catalog.

Bucketing uses ``closed='right', label='right'`` so the window ``(T-N, T]`` is
stamped at ``T``. This matches Nautilus' ``TimeBarAggregator`` default — see
the sanity-check cell in ``ipynb/aggregate_1min_to_5min.ipynb`` which confirms
both paths produce bit-identical OHLCV on overlapping timestamps.

Feature math (RSI / ATR / EMA / VWAP / TR / returns / geometry) mirrors the
notebook's ``FiveMinAggregator._close_window`` exactly. Wilder smoothing uses
``ewm(alpha=1/period, adjust=False)`` which is mathematically equivalent to the
recursive ``avg += alpha*(x - avg)`` form used in the notebook, with the same
initialisation (first input value).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from core.csv_loader import QUANTITY_MAX


OHLCV_AGG: dict[str, str] = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
}

# Pandas resample rule per Nautilus timeframe string. Week defaults to Sunday
# anchor (``W-SUN`` = bucket ends Sunday 00:00, which fits the FX week ending
# Sun 21:00 UTC closely enough for daily-aligned aggregation). Override via
# the ``week_anchor`` kwarg on ``aggregate_ohlcv``.
TIMEFRAME_TO_RULE: dict[str, str] = {
    "5-MINUTE": "5min",
    "30-MINUTE": "30min",
    "1-HOUR": "1h",
    "2-HOUR": "2h",
    "1-DAY": "1D",
    "1-WEEK": "W-SUN",
    "1-MONTH": "ME",
}

FEATURE_COLUMNS: tuple[str, ...] = (
    "n_ticks",
    "range",
    "body",
    "upper_wick",
    "lower_wick",
    "log_return",
    "pct_return",
    "typical_price",
    "vwap",
    "tr",
    "atr_14",
    "ema_9",
    "ema_21",
    "rsi_14",
)


def _rule_for(timeframe: str, week_anchor: str = "SUN") -> str:
    if timeframe == "1-WEEK":
        anchor = week_anchor.upper()
        if anchor not in {"SUN", "MON", "FRI"}:
            raise ValueError(f"week_anchor must be SUN, MON, or FRI (got {anchor!r})")
        return f"W-{anchor}"
    try:
        return TIMEFRAME_TO_RULE[timeframe]
    except KeyError as exc:
        raise ValueError(
            f"unsupported timeframe {timeframe!r}; expected one of {list(TIMEFRAME_TO_RULE)}"
        ) from exc


def aggregate_ohlcv(
    df: pd.DataFrame,
    timeframe: str,
    *,
    drop_partial: bool = True,
    include_n_ticks: bool = False,
    week_anchor: str = "SUN",
) -> pd.DataFrame:
    """Resample a UTC-indexed 1-min OHLCV DataFrame to ``timeframe``.

    Uses ``closed='right', label='right'`` so the bucket ``(T-N, T]`` is stamped
    at ``T``, matching Nautilus' ``TimeBarAggregator`` default.

    ``drop_partial=True`` discards a trailing window where no 1-min bar landed
    *on* the right boundary — this matches Nautilus' behaviour of not emitting
    the in-progress window when source data ends mid-window.

    Output volume is clipped to ``QUANTITY_MAX`` so subsequent ``wrangle_bars``
    doesn't silently rewrite values.
    """
    if df.empty:
        return df.iloc[0:0].copy()

    rule = _rule_for(timeframe, week_anchor=week_anchor)
    grouped = df[list(OHLCV_AGG)].resample(rule, closed="right", label="right")
    out = grouped.agg(OHLCV_AGG)
    out = out.dropna(subset=["open", "high", "low", "close"])

    if drop_partial:
        out = _drop_trailing_partial(out, df.index, rule)

    if include_n_ticks:
        sizes = df.resample(rule, closed="right", label="right").size()
        out["n_ticks"] = sizes.reindex(out.index).astype("int64")

    out["volume"] = out["volume"].clip(upper=QUANTITY_MAX)
    return out


def _drop_trailing_partial(
    out: pd.DataFrame, source_index: pd.DatetimeIndex, rule: str
) -> pd.DataFrame:
    """Drop the last bucket if no source bar lands on its right boundary.

    Nautilus' ``TimeBarAggregator`` only emits a bar at the moment the window
    closes (i.e. a bar arrives at or after ``T``). With ``label='right'`` pandas
    happily labels a still-open window at ``T`` even though we never observed
    ``T`` in the source. Drop it.
    """
    if out.empty:
        return out
    last_label = out.index[-1]
    # The right edge of a (T-N, T] bucket is T itself. If any source bar's
    # timestamp equals T, the window observably closed — keep it.
    if (source_index == last_label).any():
        return out
    # Otherwise the last source bar fell strictly inside (T-N, T) — partial.
    return out.iloc[:-1]


def compute_features(
    df: pd.DataFrame,
    *,
    ema_short: int = 9,
    ema_long: int = 21,
    rsi_period: int = 14,
    atr_period: int = 14,
    vwap_session: str | None = "1D",
) -> pd.DataFrame:
    """Append the notebook's ``Bar5MinFeatures`` columns to an OHLCV frame.

    Wilder smoothing for ATR and RSI uses ``ewm(alpha=1/period, adjust=False)``
    which produces the same series as the recursive ``avg += alpha*(x - avg)``
    form in the notebook with the same initial condition.

    ``vwap_session`` controls VWAP reset cadence: a pandas freq string like
    ``"1D"`` resets per UTC day; ``None`` means cumulative across the entire
    series (matches the notebook's behaviour, which never resets).
    """
    out = df.copy()
    o, h, l, c, v = out["open"], out["high"], out["low"], out["close"], out["volume"]

    if "n_ticks" not in out.columns:
        out["n_ticks"] = pd.Series(np.nan, index=out.index, dtype="float64")

    out["range"] = h - l
    out["body"] = (c - o).abs()
    out["upper_wick"] = h - np.maximum(o, c)
    out["lower_wick"] = np.minimum(o, c) - l

    prev_c = c.shift(1)
    out["log_return"] = np.log(c / prev_c)
    out["pct_return"] = c.pct_change()

    typical = (h + l + c) / 3.0
    out["typical_price"] = typical
    out["vwap"] = _vwap(typical, v, vwap_session)

    tr_hl = h - l
    tr_hp = (h - prev_c).abs()
    tr_lp = (l - prev_c).abs()
    tr = pd.concat([tr_hl, tr_hp, tr_lp], axis=1).max(axis=1)
    out["tr"] = tr
    out["atr_14"] = tr.ewm(alpha=1.0 / atr_period, adjust=False).mean()

    out["ema_9"] = c.ewm(alpha=2.0 / (ema_short + 1), adjust=False).mean()
    out["ema_21"] = c.ewm(alpha=2.0 / (ema_long + 1), adjust=False).mean()

    out["rsi_14"] = _wilder_rsi(c, rsi_period)
    return out


def _vwap(typical: pd.Series, volume: pd.Series, session: str | None) -> pd.Series:
    pv = typical * volume
    if session is None:
        cum_pv = pv.cumsum()
        cum_v = volume.cumsum()
    else:
        # Per-session cumulative — group by the session floor of the index.
        # Use a fresh series so the original index is preserved on the result.
        bucket = typical.index.floor(session)
        cum_pv = pv.groupby(bucket).cumsum()
        cum_v = volume.groupby(bucket).cumsum()
    # Where cum_v == 0 (no volume traded yet), fall back to the typical price
    # so we never propagate a divide-by-zero — matches the notebook's behaviour
    # of treating an empty window as having VWAP == typical.
    vwap = cum_pv / cum_v.where(cum_v > 0, np.nan)
    return vwap.where(cum_v > 0, typical)


def _wilder_rsi(close: pd.Series, period: int) -> pd.Series:
    change = close.diff()
    # NaN-fill the first row so ewm starts from zero (matches the notebook's
    # avg_gain = avg_loss = 0.0 initialisation rather than seeding from the
    # first observed change).
    gain = change.clip(lower=0).fillna(0.0)
    loss = (-change).clip(lower=0).fillna(0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()

    rs = avg_gain / avg_loss.where(avg_loss > 0, np.nan)
    rsi = 100.0 - 100.0 / (1.0 + rs)
    # Pure-gain windows (avg_loss == 0, avg_gain > 0) ⇒ RSI = 100.
    pure_gain = (avg_loss == 0) & (avg_gain > 0)
    rsi = rsi.where(~pure_gain, 100.0)
    # Flat-or-undefined windows (both zero) ⇒ NaN, which the notebook returns as None.
    return rsi


def aggregate_with_features(
    df_1min: pd.DataFrame,
    timeframe: str,
    *,
    drop_partial: bool = True,
    ema_short: int = 9,
    ema_long: int = 21,
    rsi_period: int = 14,
    atr_period: int = 14,
    vwap_session: str | None = "1D",
    week_anchor: str = "SUN",
) -> pd.DataFrame:
    """Convenience: resample to ``timeframe`` and append derived features."""
    ohlcv = aggregate_ohlcv(
        df_1min,
        timeframe,
        drop_partial=drop_partial,
        include_n_ticks=True,
        week_anchor=week_anchor,
    )
    return compute_features(
        ohlcv,
        ema_short=ema_short,
        ema_long=ema_long,
        rsi_period=rsi_period,
        atr_period=atr_period,
        vwap_session=vwap_session,
    )
