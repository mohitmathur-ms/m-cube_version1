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
    _apply_directional_close_fill,
    _build_close_lookup,
    _compute_agg_coordination,
    _resolve_move_sl_to_cost,
    _ts_iso_to_ns,
    _entry_at_clip,
    _is_entry_price_reexec,
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


def test_build_vwap_lookup_single_series_for_last_only():
    # A crypto LAST-only slot now builds a "single" series for the Format B fill
    # (no bid/ask). Returns a dict with empty ask/bid and a populated single.
    lk = _build_vwap_lookup([_bar("BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL", 1, 3, 1, 2)])
    assert lk is not None
    assert lk["ask"] == {} and lk["bid"] == {}
    assert 1 in lk["single"]
    # single bar, no volume → vwap = typical = (3+1+2)/3 = 2.0
    assert abs(lk["single"][1][0] - 2.0) < 1e-9


def test_build_vwap_lookup_none_when_no_bars():
    assert _build_vwap_lookup([]) is None


def _bar_v(bt, ts, h, l, c, v):
    return SimpleNamespace(bar_type=bt, ts_event=ts, high=h, low=l, close=c, volume=v)


_MIN = 60_000_000_000          # ns per minute
_DAY = 1440 * _MIN             # ns per day


def test_build_vwap_lookup_session_cumulative_volume_weighted():
    # Two ASK bars in the same UTC session → running Σ(typical·vol)/Σ(vol).
    bars = [
        _bar_v("EURUSD.SIM-1-MINUTE-ASK-EXTERNAL", 1 * _MIN, 1.20, 1.00, 1.10, 10),  # typical 1.10
        _bar_v("EURUSD.SIM-1-MINUTE-ASK-EXTERNAL", 2 * _MIN, 2.40, 2.00, 2.20, 30),  # typical 2.20
    ]
    lk = _build_vwap_lookup(bars, session_start_min=0)
    assert abs(lk["ask"][1 * _MIN][0] - 1.10) < 1e-9            # first bar = its typical
    # second bar = (1.10*10 + 2.20*30) / (10+30) = 77/40 = 1.925
    assert abs(lk["ask"][2 * _MIN][0] - 1.925) < 1e-9


def test_build_vwap_lookup_resets_each_session():
    bars = [
        _bar_v("X.SIM-1-MINUTE-BID-EXTERNAL", 1 * _MIN, 1.20, 1.00, 1.10, 10),          # day 0
        _bar_v("X.SIM-1-MINUTE-BID-EXTERNAL", _DAY + 1 * _MIN, 3.30, 3.30, 3.30, 5),    # day 1
    ]
    lk = _build_vwap_lookup(bars, session_start_min=0)
    # The day-1 bar must not carry day-0 volume → VWAP == its own typical (3.30).
    assert abs(lk["bid"][_DAY + 1 * _MIN][0] - 3.30) < 1e-9


def test_build_vwap_lookup_zero_volume_falls_back_to_typical():
    bars = [_bar_v("X.SIM-1-MINUTE-ASK-EXTERNAL", 1 * _MIN, 1.20, 1.00, 1.10, 0.0)]
    lk = _build_vwap_lookup(bars, session_start_min=0)
    assert abs(lk["ask"][1 * _MIN][0] - 1.10) < 1e-9  # (1.20+1.00+1.10)/3


def test_vwap_session_bucket_rolls_at_session_start():
    from core.backtest_runner import _vwap_session_bucket
    # session starts 22:00 UTC = 1320 min. A bar at 21:00 day0 and 23:00 day0
    # straddle the 22:00 boundary → different buckets.
    before = 21 * 60 * _MIN          # 21:00 UTC, epoch day 0
    after = 23 * 60 * _MIN           # 23:00 UTC, epoch day 0
    assert _vwap_session_bucket(before, 1320) != _vwap_session_bucket(after, 1320)
    # With a midnight session both are the same UTC day → same bucket.
    assert _vwap_session_bucket(before, 0) == _vwap_session_bucket(after, 0)


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


def test_apply_vwap_fill_long_uses_min_conservative():
    # Spec §4.2: a BUY leg (long) closes by selling → exit = MIN(vwap, hit),
    # conservative (receive less). long + Target: vwap = bid typical (1.089),
    # hit = ask_high (1.10) → exit = min = 1.089 (< actual 1.10) → repriced down.
    lk = _build_vwap_lookup(_eur_bars())
    pos = pd.DataFrame([{
        "realized_pnl": "0 USD", "ts_closed": 1000, "avg_px_close": 1.10,
        "signed_qty": 12.0, "peak_qty": 12.0,
    }])
    fills = pd.DataFrame([{"ts_init": 1000, "tags": ["Take Profit: price=..."]}])
    n = _apply_vwap_fill(pos, fills, lk)
    assert n == 1
    # delta = (exit - actual) * qty = (1.089 - 1.10) * 12 = -0.132
    assert abs(float(pos.at[0, "realized_pnl"].split()[0]) - (-0.132)) < 1e-6


def test_apply_vwap_fill_long_noop_when_vwap_above_hit():
    # When bid_vwap >= hit, MIN picks hit; if hit == actual close → zero delta.
    # Construct a long SL where ask_low (hit) == actual and bid_vwap > ask_low.
    lk = _build_vwap_lookup([
        _bar("EURUSD.SIM-1-MINUTE-ASK-EXTERNAL", 1000, 1.10, 1.05, 1.08),  # ask_low 1.05
        _bar("EURUSD.SIM-1-MINUTE-BID-EXTERNAL", 1000, 1.20, 1.18, 1.19),  # bid typical 1.19
    ])
    pos = pd.DataFrame([{
        "realized_pnl": "0 USD", "ts_closed": 1000, "avg_px_close": 1.05,
        "signed_qty": 5.0, "peak_qty": 5.0,
    }])
    fills = pd.DataFrame([{"ts_init": 1000, "tags": ["Stop Loss: price=..."]}])
    # long SL: vwap = bid typical 1.19, hit = ask_low 1.05 → min = 1.05 == actual → noop.
    assert _apply_vwap_fill(pos, fills, lk) == 0


def test_apply_vwap_fill_format_b_short_sl_uses_max_high():
    # Crypto LAST-only → Format B. SELL leg SL: MAX(vwap, high), conservative.
    lk = _build_vwap_lookup([
        _bar_v("BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL", 1000, 110, 90, 100, 0),  # vwap=100, high=110
    ])
    pos = pd.DataFrame([{
        "realized_pnl": "0 USD", "ts_closed": 1000, "avg_px_close": 100.0,
        "signed_qty": -2.0, "peak_qty": 2.0,
    }])
    fills = pd.DataFrame([{"ts_init": 1000, "tags": ["Stop Loss: price=..."]}])
    n = _apply_vwap_fill(pos, fills, lk)
    assert n == 1
    # short SL: max(vwap 100, high 110) = 110; delta = (actual - exit)*qty = (100-110)*2 = -20
    assert abs(float(pos.at[0, "realized_pnl"].split()[0]) - (-20.0)) < 1e-6


def test_apply_vwap_fill_format_b_long_sl_uses_min_low():
    # Crypto LAST-only → Format B. BUY leg SL: MIN(vwap, low), conservative.
    lk = _build_vwap_lookup([
        _bar_v("BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL", 1000, 110, 90, 100, 0),  # vwap=100, low=90
    ])
    pos = pd.DataFrame([{
        "realized_pnl": "0 USD", "ts_closed": 1000, "avg_px_close": 100.0,
        "signed_qty": 2.0, "peak_qty": 2.0,
    }])
    fills = pd.DataFrame([{"ts_init": 1000, "tags": ["Stop Loss: price=..."]}])
    n = _apply_vwap_fill(pos, fills, lk)
    assert n == 1
    # long SL: min(vwap 100, low 90) = 90; delta = (exit - actual)*qty = (90-100)*2 = -20
    assert abs(float(pos.at[0, "realized_pnl"].split()[0]) - (-20.0)) < 1e-6


def test_apply_vwap_fill_noop_without_lookup():
    pos = pd.DataFrame([{"realized_pnl": "1 USD", "ts_closed": 1, "avg_px_close": 1.0,
                         "signed_qty": 1.0, "peak_qty": 1.0}])
    fills = pd.DataFrame([{"ts_init": 1, "tags": ["Stop Loss: x"]}])
    assert _apply_vwap_fill(pos, fills, None) == 0


# ── Directional-close exit fill (spec §8.1) ─────────────────────────────────

def test_build_close_lookup_indexes_ask_bid():
    lk = _build_close_lookup(_eur_bars())
    assert lk is not None
    # close per quote side: ask_close = 1.09, bid_close = 1.089
    assert abs(lk["ask"][1000] - 1.09) < 1e-12
    assert abs(lk["bid"][1000] - 1.089) < 1e-12


def test_build_close_lookup_none_without_ask_bid():
    assert _build_close_lookup(
        [_bar("BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL", 1, 1, 1, 1)]
    ) is None


def test_apply_directional_close_short_repriced_to_ask():
    lk = _build_close_lookup(_eur_bars())
    # Short closes by BUYING at the ASK close (1.09); engine filled at MID (1.0895).
    pos = pd.DataFrame([{
        "realized_pnl": "50.0 USD", "ts_closed": 1000, "avg_px_close": 1.0895,
        "signed_qty": -10.0, "peak_qty": 10.0,
    }])
    n = _apply_directional_close_fill(pos, lk)
    assert n == 1
    # short pnl delta = (actual - dir_close) * qty = (1.0895 - 1.09) * 10 = -0.005
    assert abs(float(pos.at[0, "realized_pnl"].split()[0]) - 49.995) < 1e-6


def test_apply_directional_close_long_repriced_to_bid():
    lk = _build_close_lookup(_eur_bars())
    # Long closes by SELLING at the BID close (1.089); engine filled at MID (1.0895).
    pos = pd.DataFrame([{
        "realized_pnl": "50.0 USD", "ts_closed": 1000, "avg_px_close": 1.0895,
        "signed_qty": 10.0, "peak_qty": 10.0,
    }])
    n = _apply_directional_close_fill(pos, lk)
    assert n == 1
    # long pnl delta = (dir_close - actual) * qty = (1.089 - 1.0895) * 10 = -0.005
    assert abs(float(pos.at[0, "realized_pnl"].split()[0]) - 49.995) < 1e-6


def test_apply_directional_close_applies_regardless_of_exit_reason():
    # Unlike the VWAP fill, the directional-close base applies to every MARKET
    # exit (SL, Target, squareoff) — it doesn't consult the fills' reason tag.
    lk = _build_close_lookup(_eur_bars())
    pos = pd.DataFrame([{
        "realized_pnl": "50.0 USD", "ts_closed": 1000, "avg_px_close": 1.0895,
        "signed_qty": -10.0, "peak_qty": 10.0,
    }])
    assert _apply_directional_close_fill(pos, lk) == 1


# ── Portfolio "ReExecute at Entry Price" (spec §5.2) ────────────────────────

def test_is_entry_price_reexec():
    assert _is_entry_price_reexec("ReExecute at Entry Price")
    assert _is_entry_price_reexec("ReExecute Same Contract at EntryPrice")
    assert not _is_entry_price_reexec("ReExecute")
    assert not _is_entry_price_reexec("SqOff")


def test_entry_at_clip_finds_position_open_at_clip():
    pos = pd.DataFrame([
        {"ts_opened": 100, "ts_closed": 200, "avg_px_open": 1.10, "signed_qty": 5.0},
        {"ts_opened": 300, "ts_closed": 500, "avg_px_open": 1.20, "signed_qty": -5.0},
    ])
    # clip at 400 → 2nd position is open (short, entry 1.20)
    assert _entry_at_clip(pos, 400) == (1.20, False)
    # clip at 150 → 1st position is open (long, entry 1.10)
    assert _entry_at_clip(pos, 150) == (1.10, True)


def test_entry_at_clip_none_when_flat_at_clip():
    pos = pd.DataFrame([
        {"ts_opened": 100, "ts_closed": 200, "avg_px_open": 1.10, "signed_qty": 5.0},
    ])
    assert _entry_at_clip(pos, 250) is None  # all positions closed before the clip


def test_entry_at_clip_none_on_empty_or_missing():
    assert _entry_at_clip(pd.DataFrame(), 100) is None
    assert _entry_at_clip(None, 100) is None


def test_apply_directional_close_noop_without_lookup():
    pos = pd.DataFrame([{"realized_pnl": "1 USD", "ts_closed": 1, "avg_px_close": 1.0,
                         "signed_qty": 1.0, "peak_qty": 1.0}])
    assert _apply_directional_close_fill(pos, None) == 0


def test_apply_directional_close_noop_when_ts_missing():
    lk = _build_close_lookup(_eur_bars())
    pos = pd.DataFrame([{"realized_pnl": "1 USD", "ts_closed": 9999, "avg_px_close": 1.0,
                         "signed_qty": 1.0, "peak_qty": 1.0}])
    assert _apply_directional_close_fill(pos, lk) == 0


def test_directional_skips_sl_tp_when_vwap_active():
    # Composition (spec §8.1 + §4.2): when VWAP is active it owns the leg
    # SL/Target exits, so the directional close must skip them.
    lk = _build_close_lookup(_eur_bars())
    pos = pd.DataFrame([{
        "realized_pnl": "50.0 USD", "ts_closed": 1000, "avg_px_close": 1.0895,
        "signed_qty": -10.0, "peak_qty": 10.0,
    }])
    fills = pd.DataFrame([{"ts_init": 1000, "tags": ["Stop Loss: price=..."]}])
    n = _apply_directional_close_fill(pos, lk, fills, vwap_active=True)
    assert n == 0
    assert pos.at[0, "realized_pnl"] == "50.0 USD"


def test_directional_reprices_squareoff_when_vwap_active():
    # Squareoff/time exits are never owned by VWAP — §8.1 fills them at the
    # directional close even when VWAP is active.
    lk = _build_close_lookup(_eur_bars())
    pos = pd.DataFrame([{
        "realized_pnl": "50.0 USD", "ts_closed": 1000, "avg_px_close": 1.0895,
        "signed_qty": -10.0, "peak_qty": 10.0,
    }])
    fills = pd.DataFrame([{"ts_init": 1000, "tags": ["Squareoff: daily close @ 15:30 UTC"]}])
    n = _apply_directional_close_fill(pos, lk, fills, vwap_active=True)
    assert n == 1
    # short → ask close 1.09; delta = (1.0895 - 1.09) * 10 = -0.005
    assert abs(float(pos.at[0, "realized_pnl"].split()[0]) - 49.995) < 1e-6


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
