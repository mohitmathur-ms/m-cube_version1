"""Unit tests for the streaming BarAggregator and its helpers (core.aggregator).

The streaming aggregator replaces Nautilus' internal TimeBarAggregator: it
floor-buckets base bars and emits an EXTERNAL aggregated Bar at each window
close. Floor grouping means bars 10:00..10:04 form one 5-min window, emitted
when the 10:05 bar arrives (stamped at the 10:05 close).
"""

import pandas as pd
import pytest
from nautilus_trader.model.data import Bar, BarType

from core.aggregator import (
    BarAggregator,
    external_from_composite,
    interval_for_timeframe,
    timeframe_of_bar_type,
)
from core.instrument_factory import create_instrument


INSTRUMENT = create_instrument("EUR", "USD", venue="FOREX_MS")
BASE_BT = BarType.from_str(f"{INSTRUMENT.id}-1-MINUTE-ASK-EXTERNAL")
TARGET_5M = BarType.from_str(f"{INSTRUMENT.id}-5-MINUTE-ASK-EXTERNAL")


def _ns(hh_mm: str) -> int:
    return pd.Timestamp(f"2024-01-01 {hh_mm}:00", tz="UTC").value


def _bar(hh_mm: str, o, h, l, c, v=1.0, bar_type=BASE_BT) -> Bar:
    ts = _ns(hh_mm)
    return Bar(
        bar_type=bar_type,
        open=INSTRUMENT.make_price(o),
        high=INSTRUMENT.make_price(h),
        low=INSTRUMENT.make_price(l),
        close=INSTRUMENT.make_price(c),
        volume=INSTRUMENT.make_qty(v),
        ts_event=ts,
        ts_init=ts,
    )


# ── helpers ─────────────────────────────────────────────────────────────────

def test_external_from_composite_strips_internal_suffix():
    assert external_from_composite(
        "EURUSD.FOREX_MS-5-MINUTE-ASK-INTERNAL@1-MINUTE-EXTERNAL"
    ) == "EURUSD.FOREX_MS-5-MINUTE-ASK-EXTERNAL"


def test_external_from_composite_passthrough_external():
    s = "EURUSD.FOREX_MS-5-MINUTE-ASK-EXTERNAL"
    assert external_from_composite(s) == s


def test_external_from_composite_malformed():
    assert external_from_composite("") == ""
    assert external_from_composite("garbage") == ""


def test_timeframe_of_bar_type():
    assert timeframe_of_bar_type("EURUSD.FOREX_MS-5-MINUTE-ASK-EXTERNAL") == "5-MINUTE"
    assert timeframe_of_bar_type("EURUSD.FOREX_MS-1-HOUR-MID-EXTERNAL") == "1-HOUR"
    assert timeframe_of_bar_type("garbage") == ""


def test_interval_for_timeframe():
    assert interval_for_timeframe("5-MINUTE") == pd.Timedelta(minutes=5)
    assert interval_for_timeframe("1-HOUR") == pd.Timedelta(hours=1)
    assert interval_for_timeframe("1-DAY") == pd.Timedelta(days=1)
    with pytest.raises(ValueError):
        interval_for_timeframe("1-MONTH")   # not a fixed width
    with pytest.raises(ValueError):
        interval_for_timeframe("garbage")


# ── BarAggregator: floor bucketing ──────────────────────────────────────────

def test_floor_window_emits_at_next_boundary():
    agg = BarAggregator(INSTRUMENT, TARGET_5M, "5-MINUTE")
    # Bars 10:00..10:04 belong to one 5-min window; none emit yet.
    assert agg.on_bar(_bar("10:00", 1.0, 1.5, 0.9, 1.1)) is None
    assert agg.on_bar(_bar("10:01", 1.1, 1.6, 1.0, 1.2)) is None
    assert agg.on_bar(_bar("10:02", 1.2, 1.4, 1.1, 1.3)) is None
    assert agg.on_bar(_bar("10:03", 1.3, 1.7, 0.8, 1.0)) is None
    assert agg.on_bar(_bar("10:04", 1.0, 1.2, 0.95, 1.15)) is None
    # The 10:05 bar starts the next window → the 10:00 window closes & emits.
    out = agg.on_bar(_bar("10:05", 1.15, 1.15, 1.15, 1.15))
    assert out is not None
    # Stamped at the window CLOSE (10:05), right-edge label.
    assert out.ts_event == _ns("10:05")
    assert str(out.bar_type) == str(TARGET_5M)
    # OHLCV over 10:00..10:04 only (10:05 excluded — it opens the next window).
    assert float(out.open) == 1.0      # open of 10:00
    assert float(out.high) == 1.7      # max high (10:03)
    assert float(out.low) == 0.8       # min low (10:03)
    assert float(out.close) == 1.15    # close of 10:04


def test_no_lone_first_bar_at_aligned_open():
    # The opening boundary bar groups WITH the next four (floor), unlike the
    # ceil prototype which isolated 10:00 into a 1-bar window.
    agg = BarAggregator(INSTRUMENT, TARGET_5M, "5-MINUTE")
    for m in range(5):  # 10:00..10:04
        assert agg.on_bar(_bar(f"10:0{m}", 1.0, 1.0, 1.0, 1.0)) is None
    out = agg.on_bar(_bar("10:05", 2.0, 2.0, 2.0, 2.0))
    assert out is not None and out.ts_event == _ns("10:05")


def test_volume_summed():
    agg = BarAggregator(INSTRUMENT, TARGET_5M, "5-MINUTE")
    for m in range(5):
        agg.on_bar(_bar(f"10:0{m}", 1.0, 1.0, 1.0, 1.0, v=2.0))
    out = agg.on_bar(_bar("10:05", 1.0, 1.0, 1.0, 1.0, v=2.0))
    assert float(out.volume) == pytest.approx(10.0)  # 5 bars * 2.0


def test_flush_drains_trailing_window():
    agg = BarAggregator(INSTRUMENT, TARGET_5M, "5-MINUTE")
    for m in range(3):  # only 10:00..10:02 — window never closes via on_bar
        assert agg.on_bar(_bar(f"10:0{m}", 1.0, 2.0, 0.5, 1.5)) is None
    out = agg.flush()
    assert out is not None
    assert float(out.high) == 2.0 and float(out.low) == 0.5
    # Second flush has nothing left.
    assert agg.flush() is None


def test_second_window_independent():
    agg = BarAggregator(INSTRUMENT, TARGET_5M, "5-MINUTE")
    for m in range(5):
        agg.on_bar(_bar(f"10:0{m}", 1.0, 1.0, 1.0, 1.0))
    first = agg.on_bar(_bar("10:05", 5.0, 9.0, 4.0, 6.0))   # emits 10:00 window
    assert first is not None and float(first.high) == 1.0
    # Feed the rest of the 10:05 window, then the 10:10 boundary bar.
    for m in range(6, 10):
        agg.on_bar(_bar(f"10:0{m}", 5.0, 7.0, 4.5, 6.5))
    second = agg.on_bar(_bar("10:10", 1.0, 1.0, 1.0, 1.0))
    assert second is not None
    assert second.ts_event == _ns("10:10")
    assert float(second.open) == 5.0   # open of 10:05
    assert float(second.high) == 9.0   # max over 10:05..10:09
