"""Unit tests for the leg-level Target features added per the SL/Target spec:

  * Target Wait / Delay        — execution_logic_target.html §4.3
  * ATR-based Target           — sl_features.html §1.1 fn.4
  * Leg-level Trailing Target  — execution_logic_target.html §4.7

These exercise the pure ratchet helper and the config plumbing
(ExitConfig ↔ ManagedExitConfig) — no Nautilus engine required.
End-to-end behaviour is covered by the smoke tests.
"""

from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from core.models import ExitConfig, portfolio_from_dict, portfolio_to_dict
from core.managed_strategy import (
    _SL_TYPE_CANON,
    _TGT_TYPE_CANON,
    _canon_exit_type,
    advance_trailing_target,
    config_from_exit,
)


# ── advance_trailing_target — pure ratchet (spec §4.7) ──────────────────────

def test_trailing_target_not_active_below_threshold():
    # Profit 1.5% has not reached the 3% activation threshold.
    active, stop, anchor, hit = advance_trailing_target(
        False, 0.0, 0.0, profit_pct=1.5,
        when_reach=3.0, lock_min=1.0, every=1.0, by=0.5,
    )
    assert active is False and hit is False


def test_trailing_target_activates_at_threshold():
    active, stop, anchor, hit = advance_trailing_target(
        False, 0.0, 0.0, profit_pct=3.0,
        when_reach=3.0, lock_min=1.0, every=1.0, by=0.5,
    )
    assert active is True
    assert stop == 1.0           # floor seeded at lock_min
    assert anchor == 3.0         # anchor seeded at when_reach
    assert hit is False          # profit (3) still above floor (1)


def test_trailing_target_ratchets_floor_up():
    # Already active, anchor at 3.0; profit climbed to 5.5 → 2 full steps of 1.0.
    active, stop, anchor, hit = advance_trailing_target(
        True, 1.0, 3.0, profit_pct=5.5,
        when_reach=3.0, lock_min=1.0, every=1.0, by=0.5,
    )
    assert active is True
    assert stop == 2.0           # 1.0 + 2 steps × 0.5
    assert anchor == 5.0         # 3.0 + 2 steps × 1.0
    assert hit is False


def test_trailing_target_hit_when_profit_falls_to_floor():
    # Active with a locked floor of 2.0; profit retreated to 1.8 → exit.
    active, stop, anchor, hit = advance_trailing_target(
        True, 2.0, 5.0, profit_pct=1.8,
        when_reach=3.0, lock_min=1.0, every=1.0, by=0.5,
    )
    assert hit is True


def test_trailing_target_no_ratchet_when_every_zero():
    active, stop, anchor, hit = advance_trailing_target(
        True, 1.0, 3.0, profit_pct=99.0,
        when_reach=3.0, lock_min=1.0, every=0.0, by=0.5,
    )
    assert stop == 1.0 and hit is False


# ── ExitConfig — new field round-trip ──────────────────────────────────────

def test_exitconfig_new_target_fields_roundtrip():
    pf = portfolio_from_dict({
        "name": "t",
        "slots": [{
            "strategy_name": "EMA Cross",
            "exit_config": {
                "target_type": "atr",
                "tgt_atr_period": 14,
                "tgt_atr_multiplier": 2.5,
                "tgt_wait_sec": 30,
                "tgt_trail_enabled": True,
                "tgt_trail_when_profit_reach": 3.0,
                "tgt_trail_lock_min_profit": 1.0,
                "tgt_trail_every": 1.0,
                "tgt_trail_by": 0.5,
            },
        }],
    })
    ec = pf.slots[0].exit_config
    assert ec.target_type == "atr"
    assert ec.tgt_atr_period == 14
    assert ec.tgt_atr_multiplier == 2.5
    assert ec.tgt_wait_sec == 30
    assert ec.tgt_trail_enabled is True
    assert ec.tgt_trail_when_profit_reach == 3.0
    # Survives a save → load round-trip.
    rt = portfolio_to_dict(pf)
    assert rt["slots"][0]["exit_config"]["tgt_atr_period"] == 14
    assert rt["slots"][0]["exit_config"]["tgt_trail_by"] == 0.5


def test_exitconfig_defaults_for_old_json():
    # Portfolio JSON pre-dating these fields loads with safe defaults.
    pf = portfolio_from_dict({
        "name": "t2",
        "slots": [{"strategy_name": "EMA Cross", "exit_config": {"target_type": "none"}}],
    })
    ec = pf.slots[0].exit_config
    assert ec.tgt_atr_period == 0
    assert ec.tgt_wait_sec == 0
    assert ec.tgt_trail_enabled is False


def test_has_exit_management_true_for_trailing_target():
    # tgt_trail_enabled alone is enough to require the managed exit wrapper.
    ec = ExitConfig(tgt_trail_enabled=True)
    assert ec.has_exit_management() is True
    assert ExitConfig().has_exit_management() is False


# ── config_from_exit — ExitConfig → ManagedExitConfig threading ─────────────

def _managed_cfg(exit_config: ExitConfig):
    return config_from_exit(
        exit_config=exit_config,
        signal_name="EMA Cross",
        signal_params={},
        instrument_id=InstrumentId.from_str("EURUSD.SIM"),
        bar_type=BarType.from_str("EURUSD.SIM-1-MINUTE-MID-EXTERNAL"),
        trade_size=1000,
    )


def test_config_from_exit_threads_atr_target():
    cfg = _managed_cfg(ExitConfig(
        target_type="atr", tgt_atr_period=20, tgt_atr_multiplier=3.0,
    ))
    assert cfg.target_type == "atr"
    assert cfg.tgt_atr_period == 20
    assert cfg.tgt_atr_multiplier == 3.0


# ── _canon_exit_type — spec-name aliasing (spec §4.4) ───────────────────────

def test_canon_exit_type_accepts_spec_sl_names():
    assert _canon_exit_type("Premium", _SL_TYPE_CANON) == "percentage"
    assert _canon_exit_type("PremiumBased", _SL_TYPE_CANON) == "percentage"
    assert _canon_exit_type("AbsolutePremium", _SL_TYPE_CANON) == "points"


def test_canon_exit_type_passes_through_canonical():
    for v in ("none", "percentage", "points", "trailing", "atr"):
        assert _canon_exit_type(v, _SL_TYPE_CANON) == v


def test_canon_exit_type_case_and_space_insensitive():
    assert _canon_exit_type("  Absolute Premium ", _TGT_TYPE_CANON) == "points"
    assert _canon_exit_type("PERCENTAGE", _TGT_TYPE_CANON) == "percentage"


def test_canon_exit_type_empty_is_none():
    assert _canon_exit_type("", _SL_TYPE_CANON) == "none"
    assert _canon_exit_type(None, _SL_TYPE_CANON) == "none"


def test_config_from_exit_normalises_spec_type_names():
    cfg = config_from_exit(
        exit_config=ExitConfig(stop_loss_type="Premium", target_type="AbsolutePremium"),
        signal_name="EMA Cross", signal_params={},
        instrument_id=InstrumentId.from_str("EURUSD.SIM"),
        bar_type=BarType.from_str("EURUSD.SIM-1-MINUTE-MID-EXTERNAL"),
        trade_size=1000,
    )
    assert cfg.stop_loss_type == "percentage"
    assert cfg.target_type == "points"


def test_config_from_exit_threads_target_wait_and_trail():
    cfg = _managed_cfg(ExitConfig(
        tgt_wait_sec=45,
        tgt_trail_enabled=True,
        tgt_trail_when_profit_reach=2.0,
        tgt_trail_lock_min_profit=0.5,
        tgt_trail_every=0.5,
        tgt_trail_by=0.25,
    ))
    assert cfg.tgt_wait_sec == 45
    assert cfg.tgt_trail_enabled is True
    assert cfg.tgt_trail_when_profit_reach == 2.0
    assert cfg.tgt_trail_lock_min_profit == 0.5
    assert cfg.tgt_trail_every == 0.5
    assert cfg.tgt_trail_by == 0.25
