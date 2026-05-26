"""Behavioural tests for multi-timeframe exits (``exit_check_on_base_bar``).

Scenario: a strategy whose entry SIGNAL runs on the 5-minute aggregated bar
(built on the fly from 1-minute data) but whose SL/TP/trailing/square-off are
re-checked on EVERY 1-minute base bar.

These drive ``ManagedExitStrategy.on_bar`` directly with synthetic 1-minute bars
and record which bar timestamps reach the exit / entry phases. The engine-bound
methods that actually submit orders (``_check_exits`` / ``_check_entries``) are
stubbed to recorders, so no live ``BacktestEngine`` is needed — we are asserting
the *dispatch cadence*, which is the whole of the feature. End-to-end fills are
covered by the existing smoke tests.

The on_start engine wiring (subscribe_bars, cache lookups, indicator
registration) is replicated by hand here: we flip ``_aggregating`` on, attach a
real ``BarAggregator``, and leave ``_agg_indicators`` empty so the readiness gate
is open. The config itself is built through the real ``config_from_exit`` path so
the flag threading is exercised too.
"""

from datetime import timezone

import pandas as pd
from nautilus_trader.model.data import Bar, BarType

from core.aggregator import BarAggregator
from core.instrument_factory import create_instrument
from core.managed_strategy import ManagedExitStrategy, config_from_exit
from core.models import ExitConfig


INSTRUMENT = create_instrument("EUR", "USD", venue="FOREX_MS")
BASE_1M = BarType.from_str(f"{INSTRUMENT.id}-1-MINUTE-MID-EXTERNAL")
TARGET_5M = BarType.from_str(f"{INSTRUMENT.id}-5-MINUTE-MID-EXTERNAL")
COMPOSITE_5M = f"{INSTRUMENT.id}-5-MINUTE-MID-INTERNAL@1-MINUTE-EXTERNAL"


def _ns(hh_mm: str) -> int:
    return pd.Timestamp(f"2024-01-01 {hh_mm}:00", tz="UTC").value


def _bar(hh_mm: str, o=1.10, h=1.12, l=1.08, c=1.10) -> Bar:
    ts = _ns(hh_mm)
    return Bar(
        bar_type=BASE_1M,
        open=INSTRUMENT.make_price(o),
        high=INSTRUMENT.make_price(h),
        low=INSTRUMENT.make_price(l),
        close=INSTRUMENT.make_price(c),
        volume=INSTRUMENT.make_qty(1.0),
        ts_event=ts,
        ts_init=ts,
    )


def _make_strategy(*, exit_check_on_base_bar: bool):
    """Build a ManagedExitStrategy in the post-on_start aggregating state, with
    the engine-bound exit/entry hooks replaced by timestamp recorders."""
    cfg = config_from_exit(
        exit_config=ExitConfig(
            stop_loss_type="points",
            stop_loss_value=50.0,
            exit_check_on_base_bar=exit_check_on_base_bar,
        ),
        signal_name="EMA Cross",
        signal_params={},
        instrument_id=INSTRUMENT.id,
        bar_type=BASE_1M,
        trade_size=1000,
        subscribe_bar_types=[COMPOSITE_5M],
    )
    assert cfg.aggregate_to_bar_type == str(TARGET_5M)
    assert cfg.exit_check_on_base_bar is exit_check_on_base_bar

    strat = ManagedExitStrategy(cfg)
    # Replicate the on_start aggregating wiring (engine binding not needed here).
    strat._aggregating = True
    strat._exit_check_on_base_bar = exit_check_on_base_bar
    strat._primary_agg = BarAggregator(INSTRUMENT, TARGET_5M, "5-MINUTE")
    strat._agg_indicators = []          # empty ⇒ readiness gate is open
    strat._exit_fmt = "ohlcv"

    exit_ts: list[int] = []
    entry_ts: list[int] = []
    strat._check_exits = lambda *a, **k: exit_ts.append(strat._current_bar_ts_ns)
    strat._check_entries = lambda *a, **k: entry_ts.append(strat._current_bar_ts_ns)
    return strat, exit_ts, entry_ts


def _feed(strat, mins):
    for m in mins:
        strat.on_bar(_bar(f"10:{m:02d}"))


# Two full 5-min windows: 10:00..10:04 and 10:05..10:09, plus the 10:10 boundary
# bar that closes the second window.
_MINUTES = list(range(0, 11))


# ── exits ───────────────────────────────────────────────────────────────────

def test_flag_on_checks_exits_every_base_bar():
    """In-position: with the flag ON, SL/TP are checked on EVERY 1-min bar."""
    strat, exit_ts, entry_ts = _make_strategy(exit_check_on_base_bar=True)
    strat.position_side = "LONG"          # held throughout (stubs don't flatten)
    _feed(strat, _MINUTES)

    # One exit check per base bar — at each 1-minute timestamp.
    assert exit_ts == [_ns(f"10:{m:02d}") for m in _MINUTES]
    # No fresh entries while in a position.
    assert entry_ts == []


def test_flag_off_checks_exits_only_on_aggregated_bar():
    """In-position: with the flag OFF (legacy), exits only fire at the 5-min
    window close — proving the new behaviour is strictly opt-in (parity)."""
    strat, exit_ts, entry_ts = _make_strategy(exit_check_on_base_bar=False)
    strat.position_side = "LONG"
    _feed(strat, _MINUTES)

    # Aggregated windows close (emit) at 10:05 and 10:10 only.
    assert exit_ts == [_ns("10:05"), _ns("10:10")]
    assert entry_ts == []


# ── entries ───────────────────────────────────────────────────────────────────

def test_flag_on_entries_only_on_aggregated_bar():
    """Flat: entries/signal still run only on the 5-min aggregated bar, never on
    an intermediate 1-min bar — the signal cadence stays coarse."""
    strat, exit_ts, entry_ts = _make_strategy(exit_check_on_base_bar=True)
    strat.position_side = None            # flat throughout (stubs don't enter)
    _feed(strat, _MINUTES)

    # Entries only at the aggregated window closes (10:05, 10:10).
    assert entry_ts == [_ns("10:05"), _ns("10:10")]
    # Flat ⇒ no exit checks.
    assert exit_ts == []


# ── legacy (non-aggregating) _on_primary_bar dispatch — refactor parity ──────
#
# _on_primary_bar was split into _run_exit_phase / _run_entry_phase. These guard
# that the single-timeframe composition is unchanged: square-off envelope first,
# then exits XOR entries on the same bar.

def _make_plain_strategy():
    cfg = config_from_exit(
        exit_config=ExitConfig(stop_loss_type="points", stop_loss_value=50.0),
        signal_name="EMA Cross", signal_params={},
        instrument_id=INSTRUMENT.id, bar_type=BASE_1M, trade_size=1000,
    )
    strat = ManagedExitStrategy(cfg)
    strat._aggregating = False
    strat._exit_fmt = "ohlcv"
    strat._agg_indicators = []
    strat._indicators_ready = lambda: True   # gate open: we test dispatch, not warm-up
    rec = {"exit": [], "entry": [], "sqoff": 0}
    strat._check_exits = lambda *a, **k: rec["exit"].append(strat._current_bar_ts_ns)
    strat._check_entries = lambda *a, **k: rec["entry"].append(strat._current_bar_ts_ns)
    strat._force_squareoff = lambda *a, **k: rec.__setitem__("sqoff", rec["sqoff"] + 1)
    return strat, rec


def test_legacy_flat_runs_entry_only():
    strat, rec = _make_plain_strategy()
    strat.position_side = None
    strat.on_bar(_bar("10:00"))
    assert rec["entry"] == [_ns("10:00")] and rec["exit"] == []


def test_legacy_in_position_runs_exit_only():
    strat, rec = _make_plain_strategy()
    strat.position_side = "LONG"
    strat.on_bar(_bar("10:00"))
    assert rec["exit"] == [_ns("10:00")] and rec["entry"] == []


def test_legacy_squareoff_envelope_closes_and_blocks_entry():
    strat, rec = _make_plain_strategy()
    # Square off at 10:00 local (UTC). The bar at/after that force-closes and
    # blocks any fresh entry on the same bar.
    strat._squareoff_min = 10 * 60
    strat._squareoff_tz = timezone.utc
    strat.position_side = "LONG"
    strat.on_bar(_bar("10:00"))
    assert rec["sqoff"] == 1           # force-closed
    assert rec["entry"] == []          # no entry on the squareoff bar
    assert strat._squareoff_done_date is not None
    # A later bar the same day is already squared off → neither exit nor entry,
    # even after going flat.
    strat.position_side = None
    strat.on_bar(_bar("10:01"))
    assert rec["entry"] == [] and rec["exit"] == []
