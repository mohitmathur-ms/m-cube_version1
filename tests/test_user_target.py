"""Unit tests for the user-level Target enforcement added per the spec:

  * User Max Profit ceiling          — execution_logic_target.html §6
  * User Trailing Target / Profit-Lock — execution_logic_target.html §6.1

Covers the pure equity-curve clip (_user_tgt_clip), the users.py resolver
(get_user_trailing_target) and the registry validation.
"""

import core.users as users
from core.users import get_user_trailing_target, validate_registry_payload
from core.backtest_runner import _user_tgt_clip


def _curve(*balances, seed=10_000.0):
    """Build an equity_curve_ts: a seed point + one tick per balance."""
    out = [{"timestamp": None, "balance": seed}]
    for i, b in enumerate(balances, start=1):
        out.append({"timestamp": f"2024-01-01T00:{i:02d}:00+00:00", "balance": b})
    return out


# ── _user_tgt_clip — fixed Max Profit ceiling ───────────────────────────────

def test_user_tgt_clip_fixed_max_profit_fires():
    # Seed 10_000; balances → PnL 200, 600, 1200. Cap 1000 → fires at tick 3.
    clip = _user_tgt_clip(
        _curve(10_200, 10_600, 11_200), 10_000.0, 0.0,
        eff_max_profit=1000.0, user_trail_tgt=None, all_slot_ids=["s1", "s2"],
    )
    assert clip.clip_ts == "2024-01-01T00:03:00+00:00"
    assert clip.clip_reason == "USER_TARGET"
    assert clip.clip_action == "SqOff"
    assert set(clip.clipped_slots) == {"s1", "s2"}


def test_user_tgt_clip_no_fire_below_cap():
    clip = _user_tgt_clip(
        _curve(10_200, 10_400), 10_000.0, 0.0,
        eff_max_profit=1000.0, user_trail_tgt=None, all_slot_ids=["s1"],
    )
    assert clip.clip_ts is None


def test_user_tgt_clip_cumulative_pnl_counts():
    # cum_user_pnl 900 from prior portfolios; +200 here crosses the 1000 cap.
    clip = _user_tgt_clip(
        _curve(10_200), 10_000.0, 900.0,
        eff_max_profit=1000.0, user_trail_tgt=None, all_slot_ids=["s1"],
    )
    assert clip.clip_reason == "USER_TARGET"


def test_user_tgt_clip_disabled_returns_empty():
    clip = _user_tgt_clip(
        _curve(10_500), 10_000.0, 0.0,
        eff_max_profit=None, user_trail_tgt=None, all_slot_ids=["s1"],
    )
    assert clip.clip_ts is None


# ── _user_tgt_clip — Trailing Target / Profit-Lock ──────────────────────────

def test_user_tgt_clip_trailing_target_activates_and_holds():
    # Profit climbs 400 → 900 → 1300; trailing arms at 500, locks floor 200,
    # ratchets +100 per +200. Profit never falls back to the floor → no clip.
    trail = {"when_reach": 500.0, "lock": 200.0, "every": 200.0, "by": 100.0}
    clip = _user_tgt_clip(
        _curve(10_400, 10_900, 11_300), 10_000.0, 0.0,
        eff_max_profit=None, user_trail_tgt=trail, all_slot_ids=["s1"],
    )
    assert clip.clip_ts is None


def test_user_tgt_clip_trailing_target_hits_on_pullback():
    # Profit 600 (arms, floor=200) → 1400 (ratchets floor up) → 250 pullback.
    # 1400 vs anchor 500: gain 900 → 4 steps → floor 200 + 4×100 = 600.
    # Next tick PnL 250 ≤ 600 → trailing-target hit.
    trail = {"when_reach": 500.0, "lock": 200.0, "every": 200.0, "by": 100.0}
    clip = _user_tgt_clip(
        _curve(10_600, 11_400, 10_250), 10_000.0, 0.0,
        eff_max_profit=None, user_trail_tgt=trail, all_slot_ids=["s1", "s2"],
    )
    assert clip.clip_ts == "2024-01-01T00:03:00+00:00"
    assert clip.clip_reason == "USER_TRAIL_TARGET"
    assert set(clip.clipped_slots) == {"s1", "s2"}


def test_user_tgt_clip_fixed_cap_wins_tie_over_trailing():
    # Both could fire on the same tick — fixed Max Profit is checked first.
    trail = {"when_reach": 100.0, "lock": 50.0, "every": 100.0, "by": 50.0}
    clip = _user_tgt_clip(
        _curve(10_050, 11_000), 10_000.0, 0.0,
        eff_max_profit=900.0, user_trail_tgt=trail, all_slot_ids=["s1"],
    )
    assert clip.clip_reason == "USER_TARGET"


# ── get_user_trailing_target — users.py resolver ────────────────────────────

def test_get_user_trailing_target_enabled(monkeypatch):
    monkeypatch.setattr(users, "get_user", lambda uid: {
        "user_id": uid, "trailing_tgt_enabled": True,
        "trailing_tgt_when_reach": 1000.0, "trailing_tgt_lock": 400.0,
        "trailing_tgt_every": 200.0, "trailing_tgt_by": 100.0,
    })
    cfg = get_user_trailing_target("u_x")
    assert cfg == {"when_reach": 1000.0, "lock": 400.0, "every": 200.0, "by": 100.0}


def test_get_user_trailing_target_disabled(monkeypatch):
    monkeypatch.setattr(users, "get_user", lambda uid: {
        "user_id": uid, "trailing_tgt_enabled": False,
        "trailing_tgt_when_reach": 1000.0,
    })
    assert get_user_trailing_target("u_x") is None


def test_get_user_trailing_target_none_when_no_threshold(monkeypatch):
    monkeypatch.setattr(users, "get_user", lambda uid: {
        "user_id": uid, "trailing_tgt_enabled": True,
        "trailing_tgt_when_reach": 0.0,
    })
    assert get_user_trailing_target("u_x") is None


# ── registry validation ─────────────────────────────────────────────────────

def test_validate_registry_accepts_trailing_tgt_fields():
    ok, err = validate_registry_payload({"users": [{
        "user_id": "u_x", "multiplier": 1.0,
        "trailing_tgt_enabled": True, "trailing_tgt_when_reach": 1000.0,
        "trailing_tgt_lock": 300.0, "trailing_tgt_every": 100.0, "trailing_tgt_by": 50.0,
    }]})
    assert ok is True, err


def test_validate_registry_rejects_bad_trailing_tgt():
    ok, err = validate_registry_payload({"users": [{
        "user_id": "u_x", "multiplier": 1.0,
        "trailing_tgt_when_reach": "not-a-number",
    }]})
    assert ok is False
    assert "trailing_tgt_when_reach" in err
