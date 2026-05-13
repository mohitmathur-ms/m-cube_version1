"""Tests for ``core.aggregator``: OHLCV bucketing math, calendar anchors,
volume clipping, and feature-math parity with the notebook's recursive
``FiveMinAggregator`` reference implementation.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.aggregator import (
    OHLCV_AGG,
    TIMEFRAME_TO_RULE,
    aggregate_ohlcv,
    aggregate_with_features,
    compute_features,
)
from core.csv_loader import QUANTITY_MAX


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_1min(n: int, start: str = "2024-01-01 00:01:00", seed: int = 0) -> pd.DataFrame:
    """Synthetic 1-min OHLCV with a sensible H/L/O/C relationship per row."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start=start, periods=n, freq="1min", tz="UTC", name="timestamp")
    # Random walk for close, then derive plausible OHLC around it.
    close = 100.0 + rng.standard_normal(n).cumsum() * 0.01
    open_ = np.concatenate([[close[0]], close[:-1]])
    high = np.maximum(open_, close) + rng.uniform(0.0, 0.02, n)
    low = np.minimum(open_, close) - rng.uniform(0.0, 0.02, n)
    volume = rng.uniform(1.0, 100.0, n)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": volume},
        index=idx,
    )


# ---------------------------------------------------------------------------
# 1. OHLCV math
# ---------------------------------------------------------------------------


def test_resample_5min_ohlcv():
    """10 hand-built 1-min bars at the right bucket boundaries → 2 5-min bars."""
    # Bars stamped at 00:01..00:10. With closed='right', label='right' on a 5min
    # rule, the buckets are (00:00, 00:05] (bars 00:01..00:05) and
    # (00:05, 00:10] (bars 00:06..00:10), labelled at 00:05 and 00:10.
    idx = pd.date_range("2024-01-01 00:01:00", periods=10, freq="1min", tz="UTC", name="timestamp")
    df = pd.DataFrame(
        {
            "open":   [1.0, 1.1, 1.2, 1.3, 1.4, 2.0, 2.1, 2.2, 2.3, 2.4],
            "high":   [1.5, 1.6, 1.7, 1.8, 1.9, 2.5, 2.6, 2.7, 2.8, 2.9],
            "low":    [0.5, 0.6, 0.7, 0.8, 0.9, 1.5, 1.6, 1.7, 1.8, 1.9],
            "close":  [1.2, 1.3, 1.4, 1.5, 1.6, 2.2, 2.3, 2.4, 2.5, 2.6],
            "volume": [10.0] * 10,
        },
        index=idx,
    )

    out = aggregate_ohlcv(df, "5-MINUTE", drop_partial=False)

    assert len(out) == 2
    assert list(out.index) == [
        pd.Timestamp("2024-01-01 00:05:00", tz="UTC"),
        pd.Timestamp("2024-01-01 00:10:00", tz="UTC"),
    ]
    # First bucket: bars 0..4
    assert out.iloc[0]["open"] == 1.0
    assert out.iloc[0]["high"] == 1.9
    assert out.iloc[0]["low"] == 0.5
    assert out.iloc[0]["close"] == 1.6
    assert out.iloc[0]["volume"] == 50.0
    # Second bucket: bars 5..9
    assert out.iloc[1]["open"] == 2.0
    assert out.iloc[1]["high"] == 2.9
    assert out.iloc[1]["low"] == 1.5
    assert out.iloc[1]["close"] == 2.6
    assert out.iloc[1]["volume"] == 50.0


def test_bucketing_right_closed_at_boundary():
    """A 1-min bar timestamped exactly on a 5-min boundary closes the prior bucket.

    This is the left-open ``(T-5, T]`` convention that Nautilus' default
    TimeBarAggregator also uses (proven by the notebook's sanity cell).
    """
    # Bars at 00:01, 00:02, 00:03, 00:04, 00:05 — 00:05 is the right edge.
    idx = pd.DatetimeIndex(
        [pd.Timestamp(f"2024-01-01 00:0{i}:00", tz="UTC") for i in range(1, 6)],
        name="timestamp",
    )
    df = pd.DataFrame(
        {"open": [1.0] * 5, "high": [1.0] * 5, "low": [1.0] * 5,
         "close": [1.0] * 5, "volume": [1.0] * 5},
        index=idx,
    )
    out = aggregate_ohlcv(df, "5-MINUTE", drop_partial=True, include_n_ticks=True)
    assert len(out) == 1
    assert out.index[0] == pd.Timestamp("2024-01-01 00:05:00", tz="UTC")
    assert int(out.iloc[0]["n_ticks"]) == 5


def test_drop_partial_trailing_window():
    """When source bars end mid-window, the trailing partial bucket is dropped."""
    # 7 bars: 5 fill the first bucket (closed at 00:05), 2 partial in the second.
    df = _make_1min(7)
    full = aggregate_ohlcv(df, "5-MINUTE", drop_partial=False)
    trimmed = aggregate_ohlcv(df, "5-MINUTE", drop_partial=True)
    assert len(full) == 2
    assert len(trimmed) == 1
    assert trimmed.index[-1] == pd.Timestamp("2024-01-01 00:05:00", tz="UTC")


# ---------------------------------------------------------------------------
# 2. Cross-timeframe coherence
# ---------------------------------------------------------------------------


def test_chained_aggregation_5min_to_30min_matches_direct():
    """``aggregate(aggregate(df, 5-MIN), 30-MIN)`` must equal ``aggregate(df, 30-MIN)``.

    Validates that bucket alignment is consistent across timeframes — a 30-min
    bucket boundary always coincides with a 5-min bucket boundary.
    """
    df = _make_1min(60 * 24)  # one full day
    via_5min = aggregate_ohlcv(df, "5-MINUTE")
    chained = aggregate_ohlcv(via_5min, "30-MINUTE")
    direct = aggregate_ohlcv(df, "30-MINUTE")
    # Same bucket count and labels
    assert list(chained.index) == list(direct.index)
    # Same OHLC; volume can differ by a single ULP due to summing twice
    pd.testing.assert_series_equal(chained["open"], direct["open"], check_names=False)
    pd.testing.assert_series_equal(chained["high"], direct["high"], check_names=False)
    pd.testing.assert_series_equal(chained["low"], direct["low"], check_names=False)
    pd.testing.assert_series_equal(chained["close"], direct["close"], check_names=False)
    np.testing.assert_allclose(chained["volume"].values, direct["volume"].values, rtol=1e-12)


# ---------------------------------------------------------------------------
# 3. Calendar-aware timeframes
# ---------------------------------------------------------------------------


def test_week_anchor_variants_align_to_named_day():
    """Each ``week_anchor`` produces bucket labels on the chosen weekday."""
    # 60 days of 1-min data is overkill — use 60 daily bars instead, then resample.
    daily_idx = pd.date_range("2024-01-01", periods=60, freq="1D", tz="UTC", name="timestamp")
    daily = pd.DataFrame(
        {"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0},
        index=daily_idx,
    )
    # pandas weekday: Mon=0..Sun=6
    expected_weekday = {"SUN": 6, "MON": 0, "FRI": 4}
    for anchor, wd in expected_weekday.items():
        out = aggregate_ohlcv(daily, "1-WEEK", week_anchor=anchor, drop_partial=False)
        # Every emitted bucket label must fall on the configured weekday.
        weekdays = {ts.weekday() for ts in out.index}
        assert weekdays == {wd}, f"{anchor}: got weekdays {weekdays}, expected {{{wd}}}"


def test_month_end_aggregation_labels_at_month_end():
    """``1-MONTH`` uses pandas' ``ME`` (month-end) rule — labels fall on the last day of each month."""
    daily_idx = pd.date_range("2024-01-01", periods=120, freq="1D", tz="UTC", name="timestamp")
    daily = pd.DataFrame(
        {"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0},
        index=daily_idx,
    )
    out = aggregate_ohlcv(daily, "1-MONTH", drop_partial=False)
    # Each label should be the last day of some month — i.e. (label + 1 day) is the 1st.
    for ts in out.index:
        next_day = (ts + pd.Timedelta(days=1))
        assert next_day.day == 1, f"month label {ts} is not a month-end"


# ---------------------------------------------------------------------------
# 4. Volume cap
# ---------------------------------------------------------------------------


def test_volume_sum_clipped_to_quantity_max():
    """Aggregated volume above ``QUANTITY_MAX`` is clipped so ``wrangle_bars`` won't silently rewrite it."""
    n = 5
    idx = pd.date_range("2024-01-01 00:01:00", periods=n, freq="1min", tz="UTC", name="timestamp")
    # Each bar carries half of QUANTITY_MAX so the sum overflows.
    big = QUANTITY_MAX * 0.6
    df = pd.DataFrame(
        {"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": big},
        index=idx,
    )
    out = aggregate_ohlcv(df, "5-MINUTE", drop_partial=False)
    assert len(out) == 1
    assert out.iloc[0]["volume"] == QUANTITY_MAX
    assert out.iloc[0]["volume"] <= QUANTITY_MAX  # paranoia


# ---------------------------------------------------------------------------
# 5. Feature parity vs. the notebook's recursive reference
# ---------------------------------------------------------------------------


def _reference_features(ohlcv: pd.DataFrame, *, ema_short=9, ema_long=21,
                        rsi_period=14, atr_period=14) -> pd.DataFrame:
    """Row-by-row recursive implementation mirroring ``FiveMinAggregator._close_window``.

    Uses ``vwap_session=None`` semantics — cumulative VWAP across the whole series,
    matching the notebook (which never resets).
    """
    alpha_s = 2.0 / (ema_short + 1)
    alpha_l = 2.0 / (ema_long + 1)
    alpha_rsi = 1.0 / rsi_period
    alpha_atr = 1.0 / atr_period

    prev_close = None
    avg_gain = 0.0
    avg_loss = 0.0
    atr = None
    ema_s = None
    ema_l = None
    cum_pv = 0.0
    cum_v = 0.0

    rows = []
    for ts, row in ohlcv.iterrows():
        o, h, l, c, v = row["open"], row["high"], row["low"], row["close"], row["volume"]

        rng = h - l
        body = abs(c - o)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l

        if prev_close is None or prev_close == 0:
            log_ret = np.nan
            pct_ret = np.nan
        else:
            log_ret = float(np.log(c / prev_close))
            pct_ret = c / prev_close - 1.0

        typical = (h + l + c) / 3.0
        cum_pv += typical * v
        cum_v += v
        vwap = cum_pv / cum_v if cum_v > 0 else typical

        if prev_close is None:
            tr = rng
        else:
            tr = max(rng, abs(h - prev_close), abs(l - prev_close))
        atr = tr if atr is None else atr + alpha_atr * (tr - atr)

        ema_s = c if ema_s is None else ema_s + alpha_s * (c - ema_s)
        ema_l = c if ema_l is None else ema_l + alpha_l * (c - ema_l)

        rsi = np.nan
        if prev_close is not None:
            change = c - prev_close
            gain = max(change, 0.0)
            loss = max(-change, 0.0)
            avg_gain += alpha_rsi * (gain - avg_gain)
            avg_loss += alpha_rsi * (loss - avg_loss)
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100.0 - 100.0 / (1.0 + rs)
            elif avg_gain > 0:
                rsi = 100.0

        rows.append({
            "timestamp": ts,
            "range": rng, "body": body,
            "upper_wick": upper_wick, "lower_wick": lower_wick,
            "log_return": log_ret, "pct_return": pct_ret,
            "typical_price": typical, "vwap": vwap,
            "tr": tr, "atr_14": atr,
            "ema_9": ema_s, "ema_21": ema_l,
            "rsi_14": rsi,
        })
        prev_close = c

    return pd.DataFrame(rows).set_index("timestamp")


def test_features_match_reference_recursive_aggregator():
    """``compute_features`` (vectorised) must match the row-by-row reference."""
    # Use 5-min synthetic OHLCV; the feature math is timeframe-agnostic so this
    # also covers the same math the notebook applies on its 5-min output.
    df = _make_1min(60 * 6)  # 6 hours of 1-min bars
    ohlcv = aggregate_ohlcv(df, "5-MINUTE", include_n_ticks=True)

    # vwap_session=None to match the notebook's never-reset behaviour.
    got = compute_features(ohlcv, vwap_session=None)
    ref = _reference_features(ohlcv)

    for col in ["range", "body", "upper_wick", "lower_wick", "log_return",
                "pct_return", "typical_price", "vwap", "tr", "atr_14",
                "ema_9", "ema_21", "rsi_14"]:
        np.testing.assert_allclose(
            got[col].values, ref[col].values,
            rtol=1e-10, atol=1e-12, equal_nan=True,
            err_msg=f"{col} drift between vectorised and reference",
        )


def test_aggregate_with_features_round_trip_smoke():
    """End-to-end: 1-min input → 5-min OHLCV + all feature columns present, finite where expected."""
    df = _make_1min(120)
    out = aggregate_with_features(df, "5-MINUTE", vwap_session=None)
    expected_cols = set(OHLCV_AGG) | {
        "n_ticks", "range", "body", "upper_wick", "lower_wick",
        "log_return", "pct_return", "typical_price", "vwap",
        "tr", "atr_14", "ema_9", "ema_21", "rsi_14",
    }
    assert expected_cols.issubset(out.columns)
    assert (out["n_ticks"] > 0).all()
    # First row's pct_return/log_return/rsi_14 are NaN by construction;
    # everything else should be finite from row 1 onward.
    assert np.isfinite(out["range"]).all()
    assert np.isfinite(out["typical_price"]).all()
    assert np.isfinite(out["vwap"]).all()


# ---------------------------------------------------------------------------
# 6. Defensive: invalid inputs surface clearly
# ---------------------------------------------------------------------------


def test_unknown_timeframe_raises():
    df = _make_1min(5)
    with pytest.raises(ValueError, match="unsupported timeframe"):
        aggregate_ohlcv(df, "3-MINUTE")


def test_empty_input_returns_empty():
    empty = pd.DataFrame(
        {"open": [], "high": [], "low": [], "close": [], "volume": []},
        index=pd.DatetimeIndex([], tz="UTC", name="timestamp"),
    )
    out = aggregate_ohlcv(empty, "5-MINUTE")
    assert out.empty
    assert set(out.columns) >= set(OHLCV_AGG)


def test_timeframe_rule_table_covers_all_advertised_timeframes():
    """The CLI advertises seven timeframes — make sure the rule table has them all."""
    expected = {"5-MINUTE", "30-MINUTE", "1-HOUR", "2-HOUR", "1-DAY", "1-WEEK", "1-MONTH"}
    assert set(TIMEFRAME_TO_RULE) == expected