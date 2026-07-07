"""Unit tests for the Visual Verification trailing-SL reconstruction.

Regression guard for the bug where the TIME axis tested the single FINAL logged
SL level against the whole hold and so false-failed trailing exits whose stop
ratcheted onto the profit side. The fix replays the per-bar SL state machine
(``_reconstruct_sl_hit``); these tests pin that replay against the static
``first_touch`` it replaces.
"""
import pandas as pd
import pytest

from core.visual_verification import (
    _build_recipe,
    _initial_sl,
    _initial_tp,
    _reconstruct_sl_hit,
    _reconstructable,
    analyze,
    first_touch,
    parse_reason_price,
)

IST = "Asia/Kolkata"


def _bars(rows, start="2026-03-02 09:00:00"):
    """rows = [(open, high, low, close), ...] at 1-second cadence from ``start``."""
    idx = pd.date_range(start, periods=len(rows), freq="1s", tz=IST)
    return pd.DataFrame(rows, index=idx, columns=["open", "high", "low", "close"])


def test_trailing_long_times_at_trailed_stop_not_first_bar():
    # entry=100 (long), trailing step=0.05% offset=0.03%, initial SL=entry.
    # Price climbs (lows stay above the ratcheting SL), then pulls back and the
    # *trailed* SL (≈101.2) is pierced on the 4th bar — the true exit.
    bars = _bars([
        (100.0, 100.0, 100.0, 100.0),   # 09:00:00  entry bar (excluded by window)
        (101.0, 101.2, 100.8, 101.0),   # 09:00:01  profit 1% -> SL trails to 100.6
        (102.0, 102.2, 101.5, 102.0),   # 09:00:02  profit 2% -> SL trails to 101.2
        (101.3, 101.4, 101.0, 101.1),   # 09:00:03  low 101.0 <= 101.2 -> HIT
        (101.1, 101.2, 100.9, 101.0),   # 09:00:04
    ])
    entry = bars.index[0]
    exit_ = bars.index[3]
    ec = {"stop_loss_type": "trailing", "stop_loss_value": 0,
          "trailing_sl_step": 0.05, "trailing_sl_offset": 0.03}

    ft_dyn = _reconstruct_sl_hit(bars, entry, exit_, 100.0, True, ec, 0.01)
    assert ft_dyn == exit_                     # reconstruction times it correctly

    # The old static check (final level 101.2 vs whole hold) mis-fires on bar 1.
    ft_static = first_touch(bars, entry, exit_, 101.2, use_high=False)
    assert ft_static == bars.index[1]
    assert ft_static != exit_                  # ...which is exactly the false FAIL


def test_trailing_short_mirror():
    # entry=100 (short); price falls (highs stay below the ratcheting SL), then
    # rebounds into the trailed SL on the 3rd bar.
    bars = _bars([
        (100.0, 100.0, 100.0, 100.0),   # entry
        (99.0, 99.2, 98.8, 99.0),       # profit 1% -> SL trails to 99.4
        (98.6, 99.45, 98.4, 98.7),      # high 99.45 >= 99.4 -> HIT
        (98.7, 98.9, 98.5, 98.6),
    ])
    entry, exit_ = bars.index[0], bars.index[2]
    ec = {"stop_loss_type": "trailing", "stop_loss_value": 0,
          "trailing_sl_step": 0.05, "trailing_sl_offset": 0.03}
    assert _reconstruct_sl_hit(bars, entry, exit_, 100.0, False, ec, 0.01) == exit_


def test_fixed_percentage_reduces_to_static_level():
    # A non-trailing leg: reconstruction = constant SL = the logged level, so the
    # first hit equals the static first_touch (no behaviour change for fixed SL).
    bars = _bars([
        (100.0, 100.0, 100.0, 100.0),   # entry
        (99.9, 100.0, 99.85, 99.9),     # SL = 100*(1-0.03/100) = 99.97 -> low 99.85 hits
        (99.8, 99.9, 99.7, 99.8),
    ])
    entry, exit_ = bars.index[0], bars.index[1]
    ec = {"stop_loss_type": "percentage", "stop_loss_value": 0.03}
    ft_dyn = _reconstruct_sl_hit(bars, entry, exit_, 100.0, True, ec, 0.01)
    ft_static = first_touch(bars, entry, exit_, 99.97, use_high=False)
    assert ft_dyn == ft_static == bars.index[1]


def test_sl_wait_bars_delays_confirmed_hit():
    # Fixed SL pierced on bar 1, but sl_wait_bars=2 holds the exit to bar 2.
    bars = _bars([
        (100.0, 100.0, 100.0, 100.0),   # entry
        (99.95, 100.0, 99.90, 99.95),   # SL 99.97 pierced (1st)
        (99.94, 99.98, 99.90, 99.94),   # still pierced (2nd) -> confirmed exit
        (99.93, 99.97, 99.88, 99.93),
    ])
    entry, exit_ = bars.index[0], bars.index[3]
    ec = {"stop_loss_type": "percentage", "stop_loss_value": 0.03, "sl_wait_bars": 2}
    assert _reconstruct_sl_hit(bars, entry, exit_, 100.0, True, ec, 0.01) == bars.index[2]


@pytest.mark.parametrize("ec,ok", [
    ({"stop_loss_type": "trailing", "trailing_sl_step": 0.05}, True),
    ({"stop_loss_type": "percentage"}, True),
    ({"stop_loss_type": "points"}, True),
    ({"stop_loss_type": "none"}, False),                 # ATR/none -> not modelled
    ({"stop_loss_type": "trailing", "move_sl_enabled": True}, False),
    ({"stop_loss_type": "trailing", "target_lock_trigger": 1, "target_lock_minimum": 0.5}, False),
    (None, False),
])
def test_reconstructable_gate(ec, ok):
    assert _reconstructable(ec) is ok


# ── Check 1: config -> level recomputation (the recipe's formula table) ───────

def test_initial_tp_percentage_and_points():
    # Long TP is ABOVE entry; short TP is BELOW (mirror of SL).
    ec_pct = {"target_type": "percentage", "target_value": 0.5}
    assert _initial_tp(100.0, True, ec_pct, 0.01) == pytest.approx(100.5)
    assert _initial_tp(100.0, False, ec_pct, 0.01) == pytest.approx(99.5)
    ec_pts = {"target_type": "points", "target_value": 8}
    assert _initial_tp(100.0, True, ec_pts, 0.01) == pytest.approx(108.0)
    assert _initial_tp(100.0, False, ec_pts, 0.01) == pytest.approx(92.0)
    assert _initial_tp(100.0, True, {"target_type": "none"}, 0.01) == 0.0


def test_recipe_check1_matches_screenshot_examples():
    # Example 1 — percentage SL, short, entry 24856.30, value 0.03%, tick 0.01:
    #   24856.30 × (1 + 0.03/100) = 24863.7569 -> snapped 24863.76 = logged SL -> PASS.
    ec = {"stop_loss_type": "percentage", "stop_loss_value": 0.03}
    lvl = _initial_sl(24856.30, False, ec, 0.01)
    assert lvl == pytest.approx(24863.76)
    rec = _build_recipe("SL", False, 24856.30, lvl, ec, 0.01, 0.005,
                        trig=None, fill=float("nan"), reason_price=None)
    assert rec["check1"]["ok"] is True
    assert "×" in rec["check1"]["formula"]

    # Example 2 — points SL, short, entry 24856.30, 8 pts -> 24864.30.
    ec_pts = {"stop_loss_type": "points", "stop_loss_value": 8}
    lvl_pts = _initial_sl(24856.30, False, ec_pts, 0.01)
    assert lvl_pts == pytest.approx(24864.30)
    rec_pts = _build_recipe("SL", False, 24856.30, lvl_pts, ec_pts, 0.01, 0.005,
                            trig=None, fill=float("nan"), reason_price=None)
    assert rec_pts["check1"]["ok"] is True


def test_recipe_check1_none_without_config_or_for_trailing():
    # No config -> cannot recompute -> ok None (does not fail the trade).
    rec = _build_recipe("SL", True, 100.0, 99.97, None, 0.01, 0.005,
                        trig=None, fill=float("nan"), reason_price=None)
    assert rec["check1"]["ok"] is None and "no portfolio config" in rec["check1"]["note"]
    # Trailing leg -> level moves per bar -> ok None with a replay note.
    rec_tr = _build_recipe("SL", True, 100.0, 99.5,
                           {"stop_loss_type": "trailing", "stop_loss_value": 0}, 0.01, 0.005,
                           trig=None, fill=float("nan"), reason_price=None)
    assert rec_tr["check1"]["ok"] is None and "trailing" in rec_tr["check1"]["note"]


def test_recipe_check1_flags_a_real_mismatch():
    # Engine logged a level inconsistent with the config formula -> Check 1 FAILs.
    ec = {"stop_loss_type": "points", "stop_loss_value": 8}
    rec = _build_recipe("SL", False, 24856.30, 24999.99, ec, 0.05, 0.025,
                        trig=None, fill=float("nan"), reason_price=None)
    assert rec["check1"]["ok"] is False


def test_recipe_check2_bar_breach_and_fill():
    # Short SL: detection on bar HIGH; fill == bar close; reason price == high.
    trig = pd.Series({"open": 24864.25, "high": 24864.65, "low": 24863.9, "close": 24864.40})
    rec = _build_recipe("SL", False, 24856.30, 24864.30,
                        {"stop_loss_type": "points", "stop_loss_value": 8}, 0.05, 0.025,
                        trig=trig, fill=24864.40, reason_price=24864.65)
    c2 = rec["check2"]
    assert c2["breach"] is True
    assert c2["extreme_label"] == "high" and c2["extreme_val"] == pytest.approx(24864.65)
    assert c2["fill_ok"] is True
    assert c2["reason_price_ok"] is True


def test_parse_reason_price():
    assert parse_reason_price("Stop Loss: price=24864.65 ≥ SL=24863.76 (entry ...)") == 24864.65
    assert parse_reason_price("Square off") is None
    assert parse_reason_price(None) is None


def test_config_ok_folds_into_verdict_without_bars():
    # No catalog feed (have_bars False): Time/Price are N/A, but Check 1 still runs
    # off entry+config and drives the verdict. A consistent level -> PASS.
    rows = [{
        "OrderID": "1", "SYMBOL": "X", "TRANSACTION": "SELL",
        "ENTRY TIME": "02-03-2026 11:00:00", "EXIT TIME": "02-03-2026 11:02:21",
        "ENTRY PRICE": 24856.30, "AVG EXIT PRICE": 24864.40,
        "EXIT REASON": "Stop Loss",
        "EXIT DETAILED REASON": "Stop Loss: price=24864.65 ≥ SL=24864.30 (entry 24856.30) -> SqOff",
        "PNL": -8.0, "QUANTITY": 1, "MULTIPLIER": 1, "SLOT_ID": "leg1",
    }]
    import hashlib
    token = hashlib.md5(b"leg1").hexdigest()[:8]
    config = {"slots": [{"slot_id": "leg1",
                         "exit_config": {"stop_loss_type": "points", "stop_loss_value": 8}}]}
    # SLOT_ID in the row must be the hashed token the report stamps.
    rows[0]["SLOT_ID"] = token
    trades, have_bars = analyze(rows, catalog_path="catalog", bar_type="", config=config)
    t = trades[0]
    assert t["config_ok"] is True
    assert t["recipe"]["check1"]["ok"] is True
    assert t["verdict"] == "PASS"        # only Check 1 was decidable; it passed
