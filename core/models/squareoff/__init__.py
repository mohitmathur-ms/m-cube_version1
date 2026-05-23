"""Square-off time resolution across the leg → slot → portfolio hierarchy.

Two helpers that decide the effective force-close time for a slot. The
portfolio-level contribution honours the MIS product default; the slot-level
resolver then layers leg > slot > portfolio precedence on top. Both use
forward-reference type hints so this component carries no import dependency on
the dataclass components (keeps the package graph acyclic).
"""

from __future__ import annotations

from typing import Optional


def effective_portfolio_squareoff(
    portfolio: "PortfolioConfig",
) -> tuple[Optional[str], Optional[str]]:
    """Return the (squareoff_time, squareoff_tz) the portfolio contributes.

    Explicit ``portfolio.squareoff_time`` always wins. When that's unset
    AND ``product == "MIS"``, fall back to the portfolio's
    ``mis_squareoff_time`` / ``mis_squareoff_tz``. ``NRML`` (or any other
    value, or None) is a no-op — returns (None, None) when no explicit
    portfolio-level squareoff is configured.

    Slot- and leg-level overrides still win above this via
    ``resolve_squareoff``; this helper only decides what the
    PORTFOLIO level contributes as the lowest-priority default.
    """
    if portfolio.squareoff_time:
        return portfolio.squareoff_time, portfolio.squareoff_tz
    if (portfolio.product or "").upper() == "MIS" and portfolio.mis_squareoff_time:
        return portfolio.mis_squareoff_time, portfolio.mis_squareoff_tz
    return None, None


def resolve_squareoff(
    portfolio: "PortfolioConfig", slot: "StrategySlotConfig"
) -> tuple[Optional[str], Optional[str]]:
    """Resolve effective (squareoff_time, squareoff_tz) for a slot.

    Priority: leg (ExitConfig) > slot > portfolio. Each level is taken
    independently — e.g. a slot may set only the time and inherit the tz
    from the portfolio. Returns (None, None) if disabled at every level.

    The portfolio level is consulted via ``effective_portfolio_squareoff``
    so MIS product type can supply a default when no explicit value is set.
    """
    pf_time, pf_tz = effective_portfolio_squareoff(portfolio)
    leg_time = getattr(slot.exit_config, "squareoff_time", None)
    slot_time = getattr(slot, "squareoff_time", None)
    leg_tz = getattr(slot.exit_config, "squareoff_tz", None)
    slot_tz = getattr(slot, "squareoff_tz", None)
    time = leg_time or slot_time or pf_time
    tz = leg_tz or slot_tz or pf_tz
    return time, tz
