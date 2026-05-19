"""Unit tests for the portfolio ReExecute replay (spec §2.4):

  * _filter_bars_after_ns   — the replay-segment bar cutoff
  * _positions_pnl_series   — numeric realized-PnL extraction
  * _splice_merged_results  — splicing a pass-1 result with a replay segment

The recursive replay loop itself is exercised end-to-end by a live backtest
(see the manual EURUSD verification); these cover the pure helpers.
"""

from types import SimpleNamespace

import pandas as pd
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from core.models import ExitConfig
from core.backtest_runner import (
    _MoveSLConfig,
    _filter_bars_after_ns,
    _per_strategy_breakdown,
    _positions_pnl_series,
    _splice_merged_results,
    _ts_iso_to_ns,
)
from core.managed_strategy import config_from_exit


# ── _filter_bars_after_ns ───────────────────────────────────────────────────

def _bars(*ts):
    return [SimpleNamespace(ts_event=t) for t in ts]


def test_filter_bars_after_ns_noop_when_cutoff_zero():
    bars = _bars(10, 20, 30)
    kept, dropped = _filter_bars_after_ns(bars, 0)
    assert kept is bars and dropped == 0


def test_filter_bars_after_ns_drops_pre_cutoff():
    kept, dropped = _filter_bars_after_ns(_bars(10, 20, 30, 40), 25)
    assert [b.ts_event for b in kept] == [30, 40]
    assert dropped == 2


def test_filter_bars_after_ns_keeps_exact_cutoff():
    kept, _ = _filter_bars_after_ns(_bars(10, 25, 40), 25)
    assert [b.ts_event for b in kept] == [25, 40]


# ── _positions_pnl_series ───────────────────────────────────────────────────

def test_positions_pnl_series_prefers_base_column():
    df = pd.DataFrame({"realized_pnl": ["1 USD", "2 USD"],
                       "realized_pnl_USD": [10.0, -4.0]})
    s = _positions_pnl_series(df)
    assert list(s) == [10.0, -4.0]


def test_positions_pnl_series_money_string_fallback():
    df = pd.DataFrame({"realized_pnl": ["12.50 USD", "-3.25 USD"]})
    s = _positions_pnl_series(df)
    assert list(s) == [12.50, -3.25]


def test_positions_pnl_series_empty():
    assert _positions_pnl_series(pd.DataFrame()).empty


# ── _splice_merged_results ──────────────────────────────────────────────────

_CLIP = "2020-01-05T00:00:00+00:00"
_CLIP_NS = _ts_iso_to_ns(_CLIP)


def _merged(positions_ts_pnl, eq_points, fills_ts):
    """Build a minimal merged-result dict for splice tests."""
    return {
        "positions_report": pd.DataFrame({
            "ts_opened": [pd.Timestamp(t) for t, _ in positions_ts_pnl],
            "realized_pnl_USD": [p for _, p in positions_ts_pnl],
        }),
        "fills_report": pd.DataFrame({
            "ts_init": [pd.Timestamp(t) for t in fills_ts],
        }),
        "equity_curve_ts": eq_points,
        "per_strategy": {},
        "portfolio_name": "t",
    }


def test_splice_keeps_head_before_clip_and_all_tail():
    head = _merged(
        positions_ts_pnl=[("2020-01-03", 5.0), ("2020-01-04", -2.0),
                          ("2020-01-07", 99.0)],  # post-clip head trade — dropped
        eq_points=[{"timestamp": None, "balance": 1000.0},
                   {"timestamp": "2020-01-04T00:00:00+00:00", "balance": 1003.0}],
        fills_ts=["2020-01-03", "2020-01-07"],
    )
    tail = _merged(
        positions_ts_pnl=[("2020-01-06", 8.0), ("2020-01-09", -1.0)],
        eq_points=[{"timestamp": None, "balance": 1000.0},
                   {"timestamp": "2020-01-09T00:00:00+00:00", "balance": 1007.0}],
        fills_ts=["2020-01-06", "2020-01-09"],
    )
    out = _splice_merged_results(head, tail, _CLIP_NS, starting_capital=1000.0)
    # Head keeps the 2 pre-clip trades; the post-clip head trade is dropped;
    # the 2 tail trades are appended → 4 total.
    assert out["total_trades"] == 4
    # PnL = 5 - 2 (head pre-clip) + 8 - 1 (tail) = 10.
    assert abs(out["total_pnl"] - 10.0) < 1e-9
    assert out["wins"] == 2 and out["losses"] == 2
    assert abs(out["final_balance"] - 1010.0) < 1e-9


def test_splice_shifts_tail_equity_curve():
    head = _merged(
        positions_ts_pnl=[("2020-01-03", 3.0)],
        eq_points=[{"timestamp": None, "balance": 1000.0},
                   {"timestamp": "2020-01-04T00:00:00+00:00", "balance": 1003.0}],
        fills_ts=["2020-01-03"],
    )
    tail = _merged(
        positions_ts_pnl=[("2020-01-06", 4.0)],
        eq_points=[{"timestamp": None, "balance": 1000.0},
                   {"timestamp": "2020-01-06T00:00:00+00:00", "balance": 1004.0}],
        fills_ts=["2020-01-06"],
    )
    out = _splice_merged_results(head, tail, _CLIP_NS, starting_capital=1000.0)
    # Head balance at clip = 1003; tail's 1004 point shifts by (1003-1000)=+3.
    last = out["equity_curve_ts"][-1]
    assert last["timestamp"] == "2020-01-06T00:00:00+00:00"
    assert abs(last["balance"] - 1007.0) < 1e-9


def test_splice_carries_tail_clip_events_for_recursion():
    # The spliced result must expose the SEGMENT's clip events so the replay
    # loop can find the next ReExecute point.
    head = _merged([("2020-01-03", 1.0)],
                   [{"timestamp": None, "balance": 1000.0}], ["2020-01-03"])
    tail = _merged([("2020-01-06", 1.0)],
                   [{"timestamp": None, "balance": 1000.0}], ["2020-01-06"])
    tail["pf_clip_events"] = [("2020-01-08T00:00:00+00:00", "STOPLOSS", "ReExecute")]
    out = _splice_merged_results(head, tail, _CLIP_NS, starting_capital=1000.0)
    assert out["pf_clip_events"] == tail["pf_clip_events"]


# ── _per_strategy_breakdown — per-slot splice (partial #3) ──────────────────

def test_per_strategy_breakdown_groups_by_slot():
    positions = pd.DataFrame({
        "strategy_id": ["S-1", "S-1", "S-2"],
        "realized_pnl_USD": [5.0, -2.0, 7.0],
    })
    out = _per_strategy_breakdown(positions, {"slotA": "S-1", "slotB": "S-2"})
    assert out["slotA"]["pnl"] == 3.0
    assert out["slotA"]["trades"] == 2
    assert out["slotA"]["wins"] == 1 and out["slotA"]["losses"] == 1
    assert out["slotB"]["pnl"] == 7.0 and out["slotB"]["trades"] == 1


def test_per_strategy_breakdown_empty():
    assert _per_strategy_breakdown(pd.DataFrame(), {"x": "S-1"}) == {}


def test_splice_recomputes_per_strategy_from_both_segments():
    # Head positions carry a strategy_id; head.slot_to_strategy_id maps it to a
    # slot. The splice combines head's PRE-CLIP slot stats with the tail's.
    head = {
        "positions_report": pd.DataFrame({
            "ts_opened": [pd.Timestamp("2020-01-03"), pd.Timestamp("2020-01-07")],
            "strategy_id": ["S-1", "S-1"],
            "realized_pnl_USD": [4.0, 99.0],  # 2nd row is post-clip → dropped
        }),
        "fills_report": pd.DataFrame({"ts_init": [pd.Timestamp("2020-01-03")]}),
        "equity_curve_ts": [{"timestamp": None, "balance": 1000.0}],
        "slot_to_strategy_id": {"slotA": "S-1"},
        "per_strategy": {"slotA": {"display_name": "EMA on EURUSD",
                                   "strategy_name": "EMA Cross", "bar_type": "X"}},
    }
    tail = {
        "positions_report": pd.DataFrame({
            "ts_opened": [pd.Timestamp("2020-01-06")],
            "strategy_id": ["S-9"],
            "realized_pnl_USD": [10.0],
        }),
        "fills_report": pd.DataFrame({"ts_init": [pd.Timestamp("2020-01-06")]}),
        "equity_curve_ts": [{"timestamp": None, "balance": 1000.0}],
        "slot_to_strategy_id": {"slotA": "S-9"},
        "per_strategy": {"slotA": {"display_name": "EMA on EURUSD",
                                   "strategy_name": "EMA Cross", "bar_type": "X",
                                   "pnl": 10.0, "trades": 1, "wins": 1,
                                   "losses": 0, "trade_pnls": [10.0]}},
    }
    out = _splice_merged_results(head, tail, _CLIP_NS, starting_capital=1000.0)
    ps = out["per_strategy"]["slotA"]
    # Head pre-clip trade (+4) + tail trade (+10) = 14 over 2 trades.
    assert ps["pnl"] == 14.0
    assert ps["trades"] == 2
    assert ps["wins"] == 2
    assert ps["strategy_name"] == "EMA Cross"


# ── config_from_exit — entry window threading (partial #4 / #6) ─────────────

def _mcfg(exit_config, **kw):
    return config_from_exit(
        exit_config=exit_config, signal_name="EMA Cross", signal_params={},
        instrument_id=InstrumentId.from_str("EURUSD.SIM"),
        bar_type=BarType.from_str("EURUSD.SIM-1-MINUTE-MID-EXTERNAL"),
        trade_size=1000, **kw,
    )


def test_config_threads_entry_window_minutes():
    cfg = _mcfg(ExitConfig(), entry_start_time="08:30", entry_end_time="16:00")
    assert cfg.entry_start_minute == 8 * 60 + 30
    assert cfg.entry_end_minute == 16 * 60


def test_config_entry_window_disabled_by_default():
    cfg = _mcfg(ExitConfig())
    assert cfg.entry_start_minute == -1
    assert cfg.entry_end_minute == -1


def test_config_threads_no_reentry_after_end():
    cfg = _mcfg(ExitConfig(),
                move_sl_settings=_MoveSLConfig(no_reentry_after_end=True))
    assert cfg.no_reentry_after_end is True
    cfg2 = _mcfg(ExitConfig(), move_sl_settings=_MoveSLConfig())
    assert cfg2.no_reentry_after_end is False
