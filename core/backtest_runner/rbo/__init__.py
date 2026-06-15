"""Range-Breakout (RBO) configuration and the Winter Time Adjustment:
validated _RBOSettings, HH:MM[:SS] second parsing, add_one_hour, and the
portfolio-level resolver/mutator."""

from __future__ import annotations

import dataclasses


# ─────────────────────────────────────────────────────────────────────────────
# RBO (Range Breakout) — portfolio-level breakout-gated entry.
# Spec: 5. Logics/rbo_logics.html. Wired through ManagedExitStrategy: each
# slot's strategy maintains its own per-day state machine over its own bar
# type (the spec's "monitoring = Underlying"). Path-A and Path-B both run
# unchanged — RBO is applied at strategy-build time, not engine-build time.
# ─────────────────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class _RBOSettings:
    """Validated, time-parsed RBO configuration ready for ManagedExitStrategy.

    All HH:MM:SS portfolio fields are pre-converted to seconds-of-day so the
    strategy's hot path on every bar is integer comparisons only — no string
    parsing per tick.
    """
    monitoring_start_sec: int
    monitoring_end_sec: int
    entry_start_sec: int
    entry_end_sec: int
    range_buffer_sec: int  # rbo_range_buffer minutes → seconds
    entry_at: str  # "Any" / "RangeHigh" / "RangeLow" — already-downgraded
    cancel_other_side: bool


def _hms_to_sec(hms: str) -> int:
    """HH:MM[:SS] → seconds-of-day. ValueError on malformed input — fail loud."""
    parts = hms.split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    s = int(parts[2]) if len(parts) > 2 else 0
    return h * 3600 + m * 60 + s


def add_one_hour(t: str | None) -> str | None:
    """Shift an "HH:MM" / "HH:MM:SS" local time forward by one hour.

    Implements the spec's ``add_one_hour()`` for the Winter Time Adjustment
    (execution_logic_target.html §9). ``None`` / empty / malformed input is
    returned unchanged so callers can apply it unconditionally. A shift that
    would cross midnight is clamped to end-of-day (23:59[:59]) rather than
    wrapping — session times never legitimately wrap, and clamping preserves
    "late square-off" intent instead of silently moving it to 00:xx.
    """
    if not t:
        return t
    parts = str(t).split(":")
    try:
        h = int(parts[0])
        m = int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else None
    except (ValueError, IndexError):
        return t
    h += 1
    if h > 23:
        h, m = 23, 59
        if s is not None:
            s = 59
    if s is not None:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}"


# US venues that observe US DST → their IANA timezone. FX (FOREX_MS), crypto,
# and NSE/India venues are intentionally absent — they never take the US shift.
_US_VENUE_TZ = {
    "CME": "America/Chicago", "CBOT": "America/Chicago", "GLOBEX": "America/Chicago",
    "NYMEX": "America/New_York", "COMEX": "America/New_York",
    "NYSE": "America/New_York", "NASDAQ": "America/New_York", "ARCA": "America/New_York",
    "BATS": "America/New_York", "AMEX": "America/New_York", "CBOE": "America/Chicago",
}


def _us_winter_in_effect(portfolio) -> bool:
    """Auto US-DST detection (opt-in ``winter_time_auto``).

    Returns True when the portfolio's primary venue is a US DST-observing venue
    AND the run START date falls in US STANDARD time (winter). Uses ``zoneinfo``
    so no external metadata is needed beyond the venue→tz map. No-op (False) for
    FX/crypto/NSE venues or when the date can't be resolved. Whole-run
    determination keyed on start_date — runs spanning a DST boundary should be
    split (documented on the config field).
    """
    try:
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        slots = getattr(portfolio, "slots", None) or []
        if not slots:
            return False
        bt = str(getattr(slots[0], "bar_type_str", "") or "")
        # venue = token after "." up to the first "-": e.g. "ES.CME-1-MINUTE-..."
        venue = bt.split(".", 1)[1].split("-", 1)[0].upper() if "." in bt else ""
        tzname = _US_VENUE_TZ.get(venue)
        if not tzname:
            return False  # non-US venue → US DST never applies
        sd = getattr(portfolio, "start_date", None)
        if not sd:
            return False
        d = datetime.fromisoformat(str(sd)[:10]).replace(tzinfo=ZoneInfo(tzname))
        # Standard time (winter) ⇔ DST offset is zero on that date.
        return d.dst() == timedelta(0)
    except Exception:
        return False


def _apply_winter_time(portfolio) -> bool:
    """Apply the Winter Time Adjustment to all configured local times in place.

    Spec execution_logic_target.html §9: when winter time is in effect for a
    US-listed instrument, the engine shifts the raw configured times by +1 hour
    before applying them. We mutate the (per-run, freshly-loaded) portfolio's
    intraday time fields — entry window, portfolio/MIS square-off, RBO windows,
    and every slot/leg square-off override — so all downstream paths (Path A
    per-slot, grouped, Path B) see the shifted values uniformly. Dates are not
    touched. Returns True when a shift was applied (for logging).

    Applied when the explicit ``winter_time_adjust`` flag is set OR when
    ``winter_time_auto`` is on and ``_us_winter_in_effect`` resolves True
    (US venue + run-start date in standard time).
    """
    if not (getattr(portfolio, "winter_time_adjust", False)
            or (getattr(portfolio, "winter_time_auto", False)
                and _us_winter_in_effect(portfolio))):
        return False
    portfolio.entry_start_time = add_one_hour(portfolio.entry_start_time)
    portfolio.entry_end_time = add_one_hour(portfolio.entry_end_time)
    portfolio.squareoff_time = add_one_hour(portfolio.squareoff_time)
    portfolio.mis_squareoff_time = add_one_hour(portfolio.mis_squareoff_time)
    portfolio.range_monitoring_start = add_one_hour(portfolio.range_monitoring_start)
    portfolio.range_monitoring_end = add_one_hour(portfolio.range_monitoring_end)
    portfolio.rbo_entry_start = add_one_hour(portfolio.rbo_entry_start)
    portfolio.rbo_entry_end = add_one_hour(portfolio.rbo_entry_end)
    for slot in portfolio.slots:
        if getattr(slot, "squareoff_time", None):
            slot.squareoff_time = add_one_hour(slot.squareoff_time)
        ec = getattr(slot, "exit_config", None)
        if ec is not None and getattr(ec, "squareoff_time", None):
            ec.squareoff_time = add_one_hour(ec.squareoff_time)
    return True


def _resolve_rbo(portfolio) -> tuple[_RBOSettings | None, str | None]:
    """Validate portfolio.rbo_* fields per rbo_logics.html.

    Returns (settings, message):
      - (None, None)           → RBO disabled, no error.
      - (None, error_message)  → RBO requested but invalid; caller falls back
                                 to standard time-based entry per spec.
      - (settings, None)       → Valid; ready to wire into ManagedExitConfig.
      - (settings, warning)    → Valid but with a downgrade — e.g. options-only
                                 entry_at value silently coerced to "Any" for
                                 FX/crypto (spec assumes options).
    """
    if not getattr(portfolio, "rbo_enabled", False):
        return None, None

    if not portfolio.range_monitoring_start or not portfolio.range_monitoring_end:
        return None, "RBO Monitoring times missing"

    if portfolio.rbo_monitoring != "Underlying":
        return None, "RBO Monitoring must be set to 'Underlying'"

    entry_at = portfolio.rbo_entry_at or "Any"
    warning: str | None = None
    if entry_at in ("C_OnHigh_P_OnLow", "P_OnHigh_C_OnLow"):
        warning = (
            f"rbo_entry_at='{entry_at}' is options-only; downgraded to 'Any' "
            f"for FX/crypto. (Spec rbo_logics.html P7 — Call/Put routing has "
            f"no analogue without options legs.)"
        )
        entry_at = "Any"
    elif entry_at not in ("Any", "RangeHigh", "RangeLow"):
        return None, f"Invalid rbo_entry_at: '{entry_at}'"

    # Per spec P4: rbo_entry_start defaults to range_monitoring_end (no quiet gap).
    entry_start = portfolio.rbo_entry_start or portfolio.range_monitoring_end
    entry_end = portfolio.rbo_entry_end or "16:15:00"

    return _RBOSettings(
        monitoring_start_sec=_hms_to_sec(portfolio.range_monitoring_start),
        monitoring_end_sec=_hms_to_sec(portfolio.range_monitoring_end),
        entry_start_sec=_hms_to_sec(entry_start),
        entry_end_sec=_hms_to_sec(entry_end),
        range_buffer_sec=int(portfolio.rbo_range_buffer or 0) * 60,
        entry_at=entry_at,
        cancel_other_side=bool(portfolio.rbo_cancel_other_side),
    ), warning
