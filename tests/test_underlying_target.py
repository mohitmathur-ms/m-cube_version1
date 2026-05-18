"""Unit tests for the Portfolio-level Underlying-Movement Target
(spec execution_logic_target.html §5.1) — _underlying_tgt_clip and the
_resolve_pf_target validation that now accepts the type.
"""

from core.models import portfolio_from_dict
from core.backtest_runner import (
    _PfStoplossSettings,
    _PfTargetSettings,
    _underlying_tgt_clip,
    _resolve_pf_target,
    _ts_iso_to_ns,
)


def _ucurve(*closes):
    return [
        {"timestamp": f"2024-01-01T00:{i:02d}:00+00:00", "close": c}
        for i, c in enumerate(closes, start=1)
    ]


_EQ = [{"timestamp": None, "balance": 10_000.0}]
_SL_OFF = _PfStoplossSettings(enabled=False)


# ── _underlying_tgt_clip ────────────────────────────────────────────────────

def test_underlying_tgt_fires_on_upward_cross():
    tgt = _PfTargetSettings(enabled=True, tgt_type="Underlying Movement", value=1.10)
    clip = _underlying_tgt_clip(
        _ucurve(1.08, 1.09, 1.11), _EQ, _SL_OFF, tgt,
        slot_pnl_at_clip={"s1": 0.0}, starting_capital=10_000.0,
    )
    assert clip.clip_ts == "2024-01-01T00:03:00+00:00"
    assert clip.clip_reason == "TARGET"


def test_underlying_tgt_fires_on_downward_cross():
    tgt = _PfTargetSettings(enabled=True, tgt_type="Underlying Movement", value=1.09)
    clip = _underlying_tgt_clip(
        _ucurve(1.12, 1.11, 1.08), _EQ, _SL_OFF, tgt,
        slot_pnl_at_clip={"s1": 0.0}, starting_capital=10_000.0,
    )
    assert clip.clip_ts == "2024-01-01T00:03:00+00:00"


def test_underlying_tgt_no_fire_when_never_crosses():
    tgt = _PfTargetSettings(enabled=True, tgt_type="Underlying Movement", value=2.00)
    clip = _underlying_tgt_clip(
        _ucurve(1.08, 1.09, 1.10), _EQ, _SL_OFF, tgt,
        slot_pnl_at_clip={"s1": 0.0}, starting_capital=10_000.0,
    )
    assert clip.clip_ts is None


def test_underlying_tgt_skipped_without_curve():
    tgt = _PfTargetSettings(enabled=True, tgt_type="Underlying Movement", value=1.10)
    clip = _underlying_tgt_clip(
        None, _EQ, _SL_OFF, tgt,
        slot_pnl_at_clip={"s1": 0.0}, starting_capital=10_000.0,
    )
    assert clip.clip_ts is None
    assert any("UNDERLYING_TGT_SKIPPED" in line for line in clip.logs)


def test_underlying_tgt_delay_shifts_clip_ts():
    tgt = _PfTargetSettings(
        enabled=True, tgt_type="Underlying Movement", value=1.10, delay_sec=120,
    )
    clip = _underlying_tgt_clip(
        _ucurve(1.08, 1.11), _EQ, _SL_OFF, tgt,
        slot_pnl_at_clip={"s1": 0.0}, starting_capital=10_000.0,
    )
    # Cross at 00:02:00 + 120 s delay → 00:04:00.
    assert _ts_iso_to_ns(clip.clip_ts) == _ts_iso_to_ns("2024-01-01T00:04:00+00:00")


# ── _resolve_pf_target — type validation ────────────────────────────────────

def test_resolve_pf_target_accepts_underlying_movement():
    pf = portfolio_from_dict({
        "name": "t", "slots": [],
        "pf_tgt_enabled": True,
        "pf_tgt_type": "Underlying Movement",
        "pf_tgt_value": 1.2345,
    })
    settings, warns = _resolve_pf_target(pf)
    assert settings.enabled is True
    assert settings.tgt_type == "Underlying Movement"
    assert settings.value == 1.2345


def test_resolve_pf_target_underlying_movement_needs_value():
    pf = portfolio_from_dict({
        "name": "t", "slots": [],
        "pf_tgt_enabled": True,
        "pf_tgt_type": "Underlying Movement",
        "pf_tgt_value": 0.0,
    })
    settings, warns = _resolve_pf_target(pf)
    assert settings.enabled is False
    assert any("Underlying Movement" in w for w in warns)


def test_resolve_pf_target_premium_still_downgraded():
    pf = portfolio_from_dict({
        "name": "t", "slots": [],
        "pf_tgt_enabled": True,
        "pf_tgt_type": "Combined Premium",
        "pf_tgt_value": 500.0,
    })
    settings, warns = _resolve_pf_target(pf)
    assert settings.enabled is True
    assert settings.tgt_type == "Combined Profit"  # downgraded
    assert any("options-only" in w for w in warns)
