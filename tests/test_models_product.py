"""Tests for the PortfolioConfig.product (MIS/NRML) wiring.

Covers effective_portfolio_squareoff (the MIS-aware portfolio-level
contribution) and resolve_squareoff (the full leg > slot > portfolio
precedence including the MIS default).
"""

from core.models import (
    ExitConfig,
    PortfolioConfig,
    StrategySlotConfig,
    effective_portfolio_squareoff,
    portfolio_from_dict,
    resolve_squareoff,
)


def _slot(**kw):
    return StrategySlotConfig(**kw)


def test_no_product_no_squareoff_returns_none():
    pf = PortfolioConfig()
    assert effective_portfolio_squareoff(pf) == (None, None)


def test_mis_supplies_default_when_explicit_unset():
    pf = PortfolioConfig(
        product="MIS",
        mis_squareoff_time="15:15",
        mis_squareoff_tz="Asia/Kolkata",
    )
    assert effective_portfolio_squareoff(pf) == ("15:15", "Asia/Kolkata")


def test_explicit_portfolio_squareoff_wins_over_mis_default():
    pf = PortfolioConfig(
        product="MIS",
        mis_squareoff_time="15:15",
        mis_squareoff_tz="Asia/Kolkata",
        squareoff_time="14:00",
        squareoff_tz="America/New_York",
    )
    assert effective_portfolio_squareoff(pf) == ("14:00", "America/New_York")


def test_nrml_is_a_noop():
    pf = PortfolioConfig(
        product="NRML",
        mis_squareoff_time="15:15",  # present but ignored under NRML
        mis_squareoff_tz="Asia/Kolkata",
    )
    assert effective_portfolio_squareoff(pf) == (None, None)


def test_mis_with_no_mis_time_returns_none():
    pf = PortfolioConfig(product="MIS")
    assert effective_portfolio_squareoff(pf) == (None, None)


def test_mis_is_case_insensitive():
    pf = PortfolioConfig(product="mis", mis_squareoff_time="15:15")
    assert effective_portfolio_squareoff(pf)[0] == "15:15"


def test_resolve_squareoff_uses_mis_default_when_slot_leg_unset():
    pf = PortfolioConfig(
        product="MIS",
        mis_squareoff_time="15:15",
        mis_squareoff_tz="Asia/Kolkata",
    )
    slot = _slot()
    assert resolve_squareoff(pf, slot) == ("15:15", "Asia/Kolkata")


def test_resolve_squareoff_slot_override_wins_over_mis_default():
    pf = PortfolioConfig(product="MIS", mis_squareoff_time="15:15", mis_squareoff_tz="Asia/Kolkata")
    slot = _slot(squareoff_time="16:00", squareoff_tz="UTC")
    assert resolve_squareoff(pf, slot) == ("16:00", "UTC")


def test_resolve_squareoff_leg_override_wins_over_mis_default():
    pf = PortfolioConfig(product="MIS", mis_squareoff_time="15:15", mis_squareoff_tz="Asia/Kolkata")
    slot = _slot(exit_config=ExitConfig(squareoff_time="17:00", squareoff_tz="Europe/London"))
    assert resolve_squareoff(pf, slot) == ("17:00", "Europe/London")


def test_portfolio_from_dict_round_trips_product_fields():
    raw = {
        "name": "p1",
        "product": "MIS",
        "mis_squareoff_time": "15:15",
        "mis_squareoff_tz": "Asia/Kolkata",
        "slots": [],
    }
    pf = portfolio_from_dict(raw)
    assert pf.product == "MIS"
    assert pf.mis_squareoff_time == "15:15"
    assert pf.mis_squareoff_tz == "Asia/Kolkata"


def test_portfolio_from_dict_ignores_unknown_fields_still_loads_product():
    raw = {
        "name": "p1",
        "product": "NRML",
        "some_unknown_future_field": True,
        "slots": [],
    }
    pf = portfolio_from_dict(raw)
    assert pf.product == "NRML"


def test_legacy_portfolio_without_product_loads_with_none():
    # Pre-MIS-wiring portfolios omit the product field entirely.
    raw = {"name": "legacy", "slots": []}
    pf = portfolio_from_dict(raw)
    assert pf.product is None
    assert pf.mis_squareoff_time is None
    assert pf.mis_squareoff_tz is None
