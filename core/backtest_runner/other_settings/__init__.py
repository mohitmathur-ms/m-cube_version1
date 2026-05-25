"""The Other Settings tab: validated _OtherSettings (delay-between-legs and
on-SL/on-Target action gating) and its resolver."""

from __future__ import annotations

import dataclasses


# ─────────────────────────────────────────────────────────────────────────────
# Other Settings tab. Spec: 5. Logics/Other_Settings_Logic.html.
# Wired through ManagedExitStrategy at slot level (the spec is portfolio-level
# but we adapt to our slot-independent architecture). delay_between_legs_sec
# gates re-entries; on_sl_action_on / on_target_action_on filter the configured
# exit action based on whether the SL/TP was the fixed level or a trailing one.
# ─────────────────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class _OtherSettings:
    """Validated Other Settings for ManagedExitStrategy.

    All fields default-equivalent to off, so a slot wired with a freshly-built
    instance behaves exactly like the legacy pre-Other-Settings code path.
    """
    delay_between_legs_sec: int = 0
    on_sl_action_on: str = "OnSL_N_Trailing_Both"
    on_target_action_on: str = "OnTarget_N_Trailing_Both"


_VALID_ON_SL_ACTION_ON = ("OnSL_N_Trailing_Both", "OnSL_Only", "OnSL_Trailing_Only")
_VALID_ON_TARGET_ACTION_ON = ("OnTarget_N_Trailing_Both", "OnTarget_Only", "OnTarget_Trailing_Only")


def _resolve_other_settings(portfolio) -> tuple[_OtherSettings, list[str]]:
    """Validate portfolio Other Settings fields and produce a ready-to-use struct.

    Returns (settings, warnings). ``warnings`` is empty unless the user has set
    options-only fields (Straddle Width Multiplier) or invalid enum values; we
    surface those via stdout in run_portfolio_backtest so they're visible in
    the worker log.
    """
    warnings: list[str] = []

    delay = int(getattr(portfolio, "delay_between_legs_sec", 0) or 0)
    if delay < 0:
        warnings.append(f"delay_between_legs_sec={delay} clamped to 0 (negative)")
        delay = 0

    on_sl = getattr(portfolio, "on_sl_action_on", "OnSL_N_Trailing_Both") or "OnSL_N_Trailing_Both"
    if on_sl not in _VALID_ON_SL_ACTION_ON:
        warnings.append(
            f"on_sl_action_on={on_sl!r} not in {_VALID_ON_SL_ACTION_ON} — "
            f"falling back to default 'OnSL_N_Trailing_Both'."
        )
        on_sl = "OnSL_N_Trailing_Both"

    on_tgt = getattr(portfolio, "on_target_action_on", "OnTarget_N_Trailing_Both") or "OnTarget_N_Trailing_Both"
    if on_tgt not in _VALID_ON_TARGET_ACTION_ON:
        warnings.append(
            f"on_target_action_on={on_tgt!r} not in {_VALID_ON_TARGET_ACTION_ON} — "
            f"falling back to default 'OnTarget_N_Trailing_Both'."
        )
        on_tgt = "OnTarget_N_Trailing_Both"

    # Options-only fields — surface a warning if user set non-default values.
    swm = float(getattr(portfolio, "straddle_width_multiplier", 0.0) or 0.0)
    if swm != 0.0:
        warnings.append(
            f"straddle_width_multiplier={swm} is options-only (CE/PE strike "
            f"override); ignored for FX/crypto. See Other_Settings_Logic.html."
        )

    if getattr(portfolio, "trail_wait_trade", False):
        warnings.append(
            "trail_wait_trade=True has no documented spec; the field is stored "
            "but not yet wired. No effect on this run."
        )

    return _OtherSettings(
        delay_between_legs_sec=delay,
        on_sl_action_on=on_sl,
        on_target_action_on=on_tgt,
    ), warnings
