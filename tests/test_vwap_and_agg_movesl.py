"""Unit tests for the VWAP proxy-fill (spec §4.2) and the portfolio-aggregate
Move SL trigger (spec §2.3).

These exercise the pure helpers — no catalog / engine needed, so they run fast
and deterministically. End-to-end behaviour is covered by the smoke tests.
"""

from types import SimpleNamespace

import pandas as pd

from core.models import portfolio_from_dict, portfolio_to_dict
from core.backtest_runner import (
    _apply_vwap_fill,
    _build_vwap_lookup,
    _compute_agg_coordination,
    _resolve_move_sl_to_cost,
    _ts_iso_to_ns,
)


# ── VWAP proxy-fill ─────────────────────────────────────────────────────────

def _bar(bt, ts, h, l, c):
    return SimpleNamespace(bar_type=bt, ts_event=ts, high=h, low=l, close=c)


def _eur_bars():
    return [
        _bar("EURUSD.SIM-1-MINUTE-ASK-EXTERNAL", 1000, 1.10, 1.08, 1.09),
        _bar("EURUSD.SIM-1-MINUTE-BID-EXTERNAL", 1000, 1.099, 1.079, 1.089),
        _bar("EURUSD.SIM-1-MINUTE-MID-EXTERNAL", 1000, 1.0995, 1.0795, 1.0895),
    ]


def test_build_vwap_lookup_indexes_ask_bid():
    lk = _build_vwap_lookup(_eur_bars())
    assert lk is not None
    assert 1000 in lk["ask"] and 1000 in lk["bid"]
    # typical price proxy = (H + L + C) / 3
    assert abs(lk["ask"][1000][0] - (1.10 + 1.08 + 1.09) / 3) < 1e-12


def test_build_vwap_lookup_none_without_ask_bid():
    assert _build_vwap_lookup(
        [_bar("BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL", 1, 1, 1, 1)]
    ) is None


def test_apply_vwap_fill_short_sl_repriced():
    lk = _build_vwap_lookup(_eur_bars())
    pos = pd.DataFrame([{
        "realized_pnl": "50.0 USD", "ts_closed": 1000, "avg_px_close": 1.095,
        "signed_qty": -10.0, "peak_qty": 10.0,
    }])
    fills = pd.DataFrame([
        {"ts_init": 500, "tags": ["EMA Cross SELL: signal"]},
        {"ts_init": 1000, "tags": ["Stop Loss: price=... SL=..."]},
    ])
    n = _apply_vwap_fill(pos, fills, lk)
    assert n == 1
    # short + SL: vwap = ask typical (1.09), hit = bid_high (1.099) -> exit 1.099
    # delta = (actual - exit) * qty = (1.095 - 1.099) * 10 = -0.04
    assert abs(float(pos.at[0, "realized_pnl"].split()[0]) - 49.96) < 1e-6


def test_apply_vwap_fill_excludes_squareoff():
    lk = _build_vwap_lookup(_eur_bars())
    pos = pd.DataFrame([{
        "realized_pnl": "50.0 USD", "ts_closed": 1000, "avg_px_close": 1.095,
        "signed_qty": -10.0, "peak_qty": 10.0,
    }])
    fills = pd.DataFrame([{"ts_init": 1000, "tags": ["Squareoff: daily close @ 15:30 UTC"]}])
    assert _apply_vwap_fill(pos, fills, lk) == 0
    assert pos.at[0, "realized_pnl"] == "50.0 USD"


def test_apply_vwap_fill_noop_when_vwap_below_hit():
    lk = _build_vwap_lookup(_eur_bars())
    # long + Target: vwap = bid typical (~1.089), hit = ask_high (1.10);
    # exit = max -> 1.10 == actual -> zero delta -> not counted.
    pos = pd.DataFrame([{
        "realized_pnl": "0 USD", "ts_closed": 1000, "avg_px_close": 1.10,
        "signed_qty": 12.0, "peak_qty": 12.0,
    }])
    fills = pd.DataFrame([{"ts_init": 1000, "tags": ["Take Profit: price=..."]}])
    assert _apply_vwap_fill(pos, fills, lk) == 0


def test_apply_vwap_fill_noop_without_lookup():
    pos = pd.DataFrame([{"realized_pnl": "1 USD", "ts_closed": 1, "avg_px_close": 1.0,
                         "signed_qty": 1.0, "peak_qty": 1.0}])
    fills = pd.DataFrame([{"ts_init": 1, "tags": ["Stop Loss: x"]}])
    assert _apply_vwap_fill(pos, fills, None) == 0


# ── Portfolio-aggregate Move SL trigger ─────────────────────────────────────

def test_portfolioconfig_agg_fields_roundtrip():
    pf = portfolio_from_dict({
        "name": "t", "slots": [],
        "move_sl_agg_pnl_enabled": True,
        "move_sl_agg_pnl_threshold": 500.0,
        "move_sl_agg_pnl_direction": "profit",
    })
    assert pf.move_sl_agg_pnl_enabled is True
    assert pf.move_sl_agg_pnl_threshold == 500.0
    assert pf.move_sl_agg_pnl_direction == "profit"
    rt = portfolio_to_dict(pf)
    assert rt["move_sl_agg_pnl_enabled"] is True
    assert rt["move_sl_agg_pnl_threshold"] == 500.0
    # Old JSON missing the fields loads with defaults.
    old = portfolio_from_dict({"name": "t2", "slots": []})
    assert old.move_sl_agg_pnl_enabled is False
    assert old.move_sl_agg_pnl_direction == "loss"


def test_resolve_move_sl_reads_agg_fields_when_move_sl_disabled():
    pf = portfolio_from_dict({
        "name": "t", "slots": [],
        "move_sl_enabled": False,
        "move_sl_agg_pnl_enabled": True,
        "move_sl_agg_pnl_threshold": 300.0,
        "move_sl_agg_pnl_direction": "profit",
    })
    cfg, _warns = _resolve_move_sl_to_cost(pf)
    assert cfg.enabled is False  # per-leg Move SL stays off
    assert cfg.agg_pnl_enabled is True  # aggregate trigger is independent
    assert cfg.agg_pnl_threshold == 300.0
    assert cfg.agg_pnl_direction == "profit"


def test_resolve_move_sl_validates_agg_inputs():
    pf = portfolio_from_dict({
        "name": "t", "slots": [],
        "move_sl_agg_pnl_enabled": True,
        "move_sl_agg_pnl_threshold": 0.0,           # invalid -> disabled
        "move_sl_agg_pnl_direction": "sideways",    # invalid -> 'loss'
    })
    cfg, warns = _resolve_move_sl_to_cost(pf)
    assert cfg.agg_pnl_enabled is False
    assert cfg.agg_pnl_direction == "loss"
    assert len(warns) >= 2


class _MS:
    """Minimal _MoveSLConfig stand-in for _compute_agg_coordination."""
    def __init__(self, enabled=True, threshold=700.0, direction="profit"):
        self.agg_pnl_enabled = enabled
        self.agg_pnl_threshold = threshold
        self.agg_pnl_direction = direction


def _pass1():
    return {
        "s1": {"slot_id": "s1", "leg_exit_events": {"sl_ns": 111},
               "equity_curve_ts": [
                   {"timestamp": None, "balance": 10000},
                   {"timestamp": "2024-01-01T00:01:00+00:00", "balance": 10200},
                   {"timestamp": "2024-01-01T00:02:00+00:00", "balance": 10600}]},
        "s2": {"slot_id": "s2", "leg_exit_events": {"tgt_ns": 222},
               "equity_curve_ts": [
                   {"timestamp": None, "balance": 10000},
                   {"timestamp": "2024-01-01T00:01:00+00:00", "balance": 10100},
                   {"timestamp": "2024-01-01T00:02:00+00:00", "balance": 10300}]},
    }


def test_compute_agg_coordination_finds_first_crossing():
    pf = SimpleNamespace(starting_capital=20000.0)
    # Combined PnL: t1 = 20300 -> +300 ; t2 = 20900 -> +900 (crosses +700).
    coord = _compute_agg_coordination(pf, _pass1(), _MS(threshold=700.0))
    assert coord.agg_trigger_ts == "2024-01-01T00:02:00+00:00"
    assert coord.agg_trigger_ns == _ts_iso_to_ns("2024-01-01T00:02:00+00:00")
    # Event bus is the union of every leg's SL/target hit timestamps.
    assert coord.event_bus == {"s1": {"sl_ns": 111}, "s2": {"tgt_ns": 222}}


def test_compute_agg_coordination_no_crossing():
    pf = SimpleNamespace(starting_capital=20000.0)
    coord = _compute_agg_coordination(pf, _pass1(), _MS(threshold=99999.0, direction="loss"))
    assert coord.agg_trigger_ns == 0
    assert coord.agg_trigger_ts is None
    # The event bus is still built even when the PnL threshold never trips.
    assert coord.event_bus == {"s1": {"sl_ns": 111}, "s2": {"tgt_ns": 222}}
