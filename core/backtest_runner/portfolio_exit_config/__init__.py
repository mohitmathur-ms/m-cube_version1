"""Portfolio-level Stoploss / Target / Move-SL-to-Cost configuration: the
frozen settings dataclasses, their FX/options type+action allow-lists, and
the portfolio resolvers that validate and build them."""

from __future__ import annotations

import dataclasses

from core.backtest_runner.cross_portfolio import (
    _CROSS_PORTFOLIO_ACTIONS,
    _OPTIONS_ONLY_PF_ACTIONS,
    _VALID_PF_ACTIONS_FX,
    _normalize_pf_action,
)


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio-level Stoploss / Target. Spec: 5. Logics/portfolio_sl_tgt.html.
# Applied post-hoc in _merge_portfolio_results via _apply_portfolio_clip.
# Move SL to Cost is applied per-slot through ManagedExitStrategy (separate
# wiring) — captured in _MoveSLConfig below for that path.
# ─────────────────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class _PfStoplossSettings:
    """Validated portfolio-level Stoploss config ready for _apply_portfolio_clip.

    All values pre-validated and non-negative. Default-equivalent to "off"
    when ``enabled=False`` — caller may treat this as None semantically.
    """
    enabled: bool = False
    value: float = 0.0
    action: str = "SqOff"  # SqOff | ReExecute | cross-portfolio variants
    delay_sec: int = 0
    reexecute_count: int = 0  # 0 = unlimited per spec §1.7
    sqoff_only_loss_legs: bool = False
    sqoff_only_profit_legs: bool = False
    trail_enabled: bool = False
    trail_every: float = 0.0
    trail_by: float = 0.0
    target_portfolio: str = ""  # spec §2.1(m) — only used for cross-portfolio actions
    # SL type (spec §2.1): "Combined Loss" (PnL-based) | "Underlying Movement"
    # (price-cross) | "Loss and Underlying Range" (hybrid). Underlying-based
    # types default the underlying to the portfolio's primary instrument.
    sl_type: str = "Combined Loss"
    underlying_below: float = 0.0
    underlying_above: float = 0.0


@dataclasses.dataclass(frozen=True)
class _PfTargetSettings:
    """Validated portfolio-level Target config."""
    enabled: bool = False
    # Target type (spec §5.1): "Combined Profit" (PnL-based) or
    # "Underlying Movement" (primary-instrument price crosses ``value``).
    tgt_type: str = "Combined Profit"
    value: float = 0.0
    action: str = "SqOff"
    delay_sec: int = 0
    reexecute_count: int = 0
    trail_enabled: bool = False
    trail_lock_min_profit: float = 0.0
    trail_when_profit_reach: float = 0.0
    trail_every: float = 0.0
    trail_by: float = 0.0
    target_portfolio: str = ""  # spec §2.4(d)
    # Selective partial-close on a Target hit (sl_features.html §2.4). Mutually
    # exclusive; default False = full SqOff. TARGET_TRAIL ignores these (§5).
    sqoff_only_loss_legs: bool = False
    sqoff_only_profit_legs: bool = False


@dataclasses.dataclass(frozen=True)
class _MoveSLConfig:
    """Per-slot Move SL to Cost config (spec §3, adapted to slot-independent
    architecture: each slot raises its own SL to entry once safety_sec elapsed
    and position is in profit). Threaded through config_from_exit into
    ManagedExitConfig fields move_sl_*.

    ``no_reexec_sl_cost`` (ReExecute_Logics.html P1) is bundled here because
    it modifies Move-SL behaviour: when True, the re_execute action is
    suppressed if the SL that fired had been raised to entry by Move SL to
    Cost. It's meaningful only in conjunction with Move SL to Cost — when
    Move SL is disabled the flag is benignly always-no-op (the strategy's
    _move_sl_fired_this_position flag never flips True).
    """
    enabled: bool = False
    safety_sec: int = 0
    action: str = "Move Only for Profitable Legs"
    trail_after: bool = False
    no_buy_legs: bool = False
    no_reexec_sl_cost: bool = False
    # ReExecute_Logics.html P2 / P3 / P5 — wired regardless of move_sl_enabled.
    # no_wait_trade_reexec: skip the slot re-execution delay on re-executions.
    # no_reentry_sl_cost: block the re_entry action when SL was moved to cost.
    # no_reentry_after_end: block ReExecute / ReEntry past Portfolio End Time.
    no_wait_trade_reexec: bool = False
    no_reentry_sl_cost: bool = True
    no_reentry_after_end: bool = False
    # LTP-buffer variant (spec §3 action v3, leg-level): on a losing leg, slide
    # SL toward LTP by this buffer (price units). 0 = exit at current price.
    ltp_buffer: float = 0.0
    # Hit-On-Leg cross-slot triggers (spec §3 1.3(f)/(g)).
    hit_on_leg_sl: bool = False
    hit_on_leg_target: bool = False
    # Portfolio-aggregate Move SL trigger (spec §2.3). agg_pnl_* mirror the
    # PortfolioConfig fields. agg_trigger_ns / preseeded_bus are the pass-2
    # inputs the two-pass runner injects (0 / None during pass 1 and whenever
    # the feature is off — both are benign no-ops in ManagedExitStrategy).
    agg_pnl_enabled: bool = False
    agg_pnl_threshold: float = 0.0
    agg_pnl_direction: str = "loss"
    agg_trigger_ns: int = 0
    preseeded_bus: dict | None = None


# Underlying-based SL types are now universal for FX/crypto — D5 "underlying
# defaults to self" makes them apply against the portfolio's primary
# instrument price. Only the premium-based types remain options-only.
_VALID_PF_SL_TYPES_FX = (
    "Combined Loss", "Underlying Movement", "Loss and Underlying Range",
)
_UNDERLYING_PF_SL_TYPES = ("Underlying Movement", "Loss and Underlying Range")
# Underlying-based Target is now universal (D5 "underlying = self"), mirroring
# the SL side. Only the premium-based types remain options-only.
_VALID_PF_TGT_TYPES_FX = ("Combined Profit", "Underlying Movement")
_UNDERLYING_PF_TGT_TYPES = ("Underlying Movement",)
_OPTIONS_ONLY_PF_SL_TYPES = ("Combined Premium", "Absolute Combined Premium")
_OPTIONS_ONLY_PF_TGT_TYPES = ("Combined Premium", "Absolute Combined Premium")
_VALID_MOVE_SL_ACTIONS_FX = ("Move Only for Profitable Legs",
                             "Move SL for All Legs Despite Loss/Profit",
                             "Move SL to LTP + Buffer for Loss Making Legs")
_OPTIONS_ONLY_MOVE_SL_ACTIONS: tuple[str, ...] = ()


def _canon_move_sl_action(action: str | None) -> str:
    """Map any Move-SL-action spelling to one of the 3 canonical engine values.

    Tolerant of spacing/case so the UI's "Move SL for All Legs Despite Loss / Profit"
    (spaces around the slash), the spec's shorter "Move SL for All Legs", and the
    engine's "…Loss/Profit" all resolve to the same variant — instead of failing
    the exact-string allow-list and silently degrading to profitable-only. Mirrors
    the token match in ManagedExitStrategy._check_exits.
    """
    a = (action or "").lower().replace(" ", "")
    if "ltp" in a and "buffer" in a:
        return "Move SL to LTP + Buffer for Loss Making Legs"
    if "alllegs" in a:
        return "Move SL for All Legs Despite Loss/Profit"
    return "Move Only for Profitable Legs"


def _resolve_pf_stoploss(portfolio) -> tuple[_PfStoplossSettings, list[str]]:
    """Validate portfolio.pf_sl_* fields per portfolio_sl_tgt.html §1-§2.

    Returns (settings, warnings). settings.enabled=False when the user hasn't
    enabled the feature OR validation falls back to safe defaults. Warnings
    surface options-only types/actions silently downgraded to FX/crypto
    universals.
    """
    warnings: list[str] = []
    if not getattr(portfolio, "pf_sl_enabled", False):
        return _PfStoplossSettings(enabled=False), warnings

    sl_type = getattr(portfolio, "pf_sl_type", "Combined Loss") or "Combined Loss"
    if sl_type in _OPTIONS_ONLY_PF_SL_TYPES:
        warnings.append(
            f"pf_sl_type={sl_type!r} is options-only (premium/underlying-based); "
            f"downgraded to 'Combined Loss' for FX/crypto. Spec §1.2."
        )
        sl_type = "Combined Loss"
    elif sl_type not in _VALID_PF_SL_TYPES_FX:
        warnings.append(
            f"Invalid pf_sl_type={sl_type!r} — falling back to 'Combined Loss'."
        )
        sl_type = "Combined Loss"

    action = _normalize_pf_action(getattr(portfolio, "pf_sl_action", "SqOff"))
    if action in _OPTIONS_ONLY_PF_ACTIONS:
        warnings.append(
            f"pf_sl_action={action!r} requires cross-portfolio dispatch infrastructure "
            f"that is not yet wired; closing local portfolio only (SqOff fallback)."
        )
        action = "SqOff"
    elif action not in _VALID_PF_ACTIONS_FX:
        warnings.append(f"Invalid pf_sl_action={action!r} — falling back to 'SqOff'.")
        action = "SqOff"

    value = max(0.0, float(getattr(portfolio, "pf_sl_value", 0.0) or 0.0))
    # Value input mode (spec §2.1): when flagged percent AND the type is
    # PnL-based (Combined Loss), interpret pf_sl_value as % of starting_capital
    # and convert to a currency amount (the clip compares combined PnL in
    # currency). % is meaningless for underlying-price types (value is a price
    # level) — ignored there with a warning.
    if bool(getattr(portfolio, "pf_sl_value_is_pct", False)) and value > 0:
        if sl_type == "Combined Loss":
            cap0 = float(getattr(portfolio, "starting_capital", 0.0) or 0.0)
            value = value / 100.0 * cap0
        else:
            warnings.append(
                f"pf_sl_value_is_pct ignored for pf_sl_type={sl_type!r} "
                f"(% applies only to the PnL-based 'Combined Loss' type)."
            )
    delay = max(0, int(getattr(portfolio, "pf_sl_delay_sec", 0) or 0))
    reexec = max(0, int(getattr(portfolio, "pf_sl_reexecute_count", 0) or 0))

    sqoff_loss = bool(getattr(portfolio, "pf_sl_sqoff_only_loss_legs", False))
    sqoff_profit = bool(getattr(portfolio, "pf_sl_sqoff_only_profit_legs", False))
    if sqoff_loss and sqoff_profit:
        warnings.append(
            "pf_sl_sqoff_only_loss_legs and pf_sl_sqoff_only_profit_legs are "
            "mutually exclusive (spec §1.10) — disabling both."
        )
        sqoff_loss = sqoff_profit = False

    trail_enabled = bool(getattr(portfolio, "pf_sl_trail_enabled", False))
    trail_every = max(0.0, float(getattr(portfolio, "pf_sl_trail_every", 0.0) or 0.0))
    trail_by = max(0.0, float(getattr(portfolio, "pf_sl_trail_by", 0.0) or 0.0))
    if trail_enabled and (trail_every == 0 or trail_by == 0):
        warnings.append(
            "pf_sl_trail_enabled=True but trail_every=0 or trail_by=0 — trailing "
            "SL will have no effect."
        )

    target_pf = str(getattr(portfolio, "pf_sl_target_portfolio", "") or "")
    if action in _CROSS_PORTFOLIO_ACTIONS and not target_pf:
        warnings.append(
            f"pf_sl_action={action!r} requires pf_sl_target_portfolio to be set — "
            f"downgrading to 'SqOff' (local close)."
        )
        action = "SqOff"

    # Underlying-based SL bounds (spec §2.1). Only meaningful for the two
    # underlying SL types; for "Combined Loss" they're inert.
    u_below = max(0.0, float(getattr(portfolio, "pf_sl_underlying_below", 0.0) or 0.0))
    u_above = max(0.0, float(getattr(portfolio, "pf_sl_underlying_above", 0.0) or 0.0))
    if sl_type == "Underlying Movement" and value <= 0:
        warnings.append(
            "pf_sl_type='Underlying Movement' needs a non-zero pf_sl_value "
            "(the underlying price level to fire at) — SL disabled."
        )
        return _PfStoplossSettings(enabled=False), warnings
    if sl_type == "Loss and Underlying Range" and u_below <= 0 and u_above <= 0:
        warnings.append(
            "pf_sl_type='Loss and Underlying Range' needs pf_sl_underlying_below "
            "or pf_sl_underlying_above to be set — SL disabled."
        )
        return _PfStoplossSettings(enabled=False), warnings

    return _PfStoplossSettings(
        enabled=True, value=value, action=action, delay_sec=delay,
        reexecute_count=reexec,
        sqoff_only_loss_legs=sqoff_loss,
        sqoff_only_profit_legs=sqoff_profit,
        trail_enabled=trail_enabled,
        trail_every=trail_every, trail_by=trail_by,
        target_portfolio=target_pf,
        sl_type=sl_type,
        underlying_below=u_below,
        underlying_above=u_above,
    ), warnings


def _resolve_pf_target(portfolio) -> tuple[_PfTargetSettings, list[str]]:
    """Validate portfolio.pf_tgt_* fields per portfolio_sl_tgt.html §4-§5."""
    warnings: list[str] = []
    if not getattr(portfolio, "pf_tgt_enabled", False):
        return _PfTargetSettings(enabled=False), warnings

    tgt_type = getattr(portfolio, "pf_tgt_type", "Combined Profit") or "Combined Profit"
    if tgt_type in _OPTIONS_ONLY_PF_TGT_TYPES:
        warnings.append(
            f"pf_tgt_type={tgt_type!r} is options-only; downgraded to "
            f"'Combined Profit' for FX/crypto. Spec §4.2."
        )
        tgt_type = "Combined Profit"
    elif tgt_type not in _VALID_PF_TGT_TYPES_FX:
        warnings.append(
            f"Invalid pf_tgt_type={tgt_type!r} — falling back to 'Combined Profit'."
        )
        tgt_type = "Combined Profit"

    action = _normalize_pf_action(getattr(portfolio, "pf_tgt_action", "SqOff"))
    if action in _OPTIONS_ONLY_PF_ACTIONS:
        warnings.append(
            f"pf_tgt_action={action!r} requires cross-portfolio dispatch infrastructure "
            f"that is not yet wired; closing local portfolio only (SqOff fallback)."
        )
        action = "SqOff"
    elif action not in _VALID_PF_ACTIONS_FX:
        warnings.append(f"Invalid pf_tgt_action={action!r} — falling back to 'SqOff'.")
        action = "SqOff"

    value = max(0.0, float(getattr(portfolio, "pf_tgt_value", 0.0) or 0.0))
    # Value input mode (spec §2.4): % of starting_capital for the PnL-based
    # "Combined Profit" type; ignored (warn) for the underlying-price type.
    if bool(getattr(portfolio, "pf_tgt_value_is_pct", False)) and value > 0:
        if tgt_type == "Combined Profit":
            cap0 = float(getattr(portfolio, "starting_capital", 0.0) or 0.0)
            value = value / 100.0 * cap0
        else:
            warnings.append(
                f"pf_tgt_value_is_pct ignored for pf_tgt_type={tgt_type!r} "
                f"(% applies only to the PnL-based 'Combined Profit' type)."
            )
    delay = max(0, int(getattr(portfolio, "pf_tgt_delay_sec", 0) or 0))
    reexec = max(0, int(getattr(portfolio, "pf_tgt_reexecute_count", 0) or 0))

    # Selective partial-close on a Target hit (sl_features.html §2.4) — mirror of
    # the SL pair (§1.9/§1.10), mutually exclusive.
    sqoff_loss = bool(getattr(portfolio, "pf_tgt_sqoff_only_loss_legs", False))
    sqoff_profit = bool(getattr(portfolio, "pf_tgt_sqoff_only_profit_legs", False))
    if sqoff_loss and sqoff_profit:
        warnings.append(
            "pf_tgt_sqoff_only_loss_legs and pf_tgt_sqoff_only_profit_legs are "
            "mutually exclusive (sl_features.html §2.4) — disabling both."
        )
        sqoff_loss = sqoff_profit = False

    trail_enabled = bool(getattr(portfolio, "pf_tgt_trail_enabled", False))
    trail_lock = max(0.0, float(getattr(portfolio, "pf_tgt_trail_lock_min_profit", 0.0) or 0.0))
    trail_reach = max(0.0, float(getattr(portfolio, "pf_tgt_trail_when_profit_reach", 0.0) or 0.0))
    trail_every = max(0.0, float(getattr(portfolio, "pf_tgt_trail_every", 0.0) or 0.0))
    trail_by = max(0.0, float(getattr(portfolio, "pf_tgt_trail_by", 0.0) or 0.0))
    if trail_enabled and trail_reach < trail_lock:
        warnings.append(
            f"pf_tgt_trail_when_profit_reach={trail_reach} < lock_min_profit={trail_lock} — "
            f"per spec §5.3 the activation threshold should be >= lock_min_profit."
        )

    target_pf = str(getattr(portfolio, "pf_tgt_target_portfolio", "") or "")
    if action in _CROSS_PORTFOLIO_ACTIONS and not target_pf:
        warnings.append(
            f"pf_tgt_action={action!r} requires pf_tgt_target_portfolio to be set — "
            f"downgrading to 'SqOff' (local close)."
        )
        action = "SqOff"

    # Underlying-Movement Target (spec §5.1) needs a non-zero price level
    # (pf_tgt_value is the underlying price to fire at, not a PnL amount).
    if tgt_type == "Underlying Movement" and value <= 0:
        warnings.append(
            "pf_tgt_type='Underlying Movement' needs a non-zero pf_tgt_value "
            "(the underlying price level to fire at) — Target disabled."
        )
        return _PfTargetSettings(enabled=False), warnings

    return _PfTargetSettings(
        enabled=True, tgt_type=tgt_type, value=value, action=action, delay_sec=delay,
        reexecute_count=reexec,
        trail_enabled=trail_enabled,
        trail_lock_min_profit=trail_lock,
        trail_when_profit_reach=trail_reach,
        trail_every=trail_every, trail_by=trail_by,
        target_portfolio=target_pf,
        sqoff_only_loss_legs=sqoff_loss,
        sqoff_only_profit_legs=sqoff_profit,
    ), warnings


def _resolve_move_sl_to_cost(portfolio) -> tuple[_MoveSLConfig, list[str]]:
    """Validate portfolio.move_sl_* fields per portfolio_sl_tgt.html §3.

    Returns the per-slot config (threaded into ManagedExitConfig). Default-off
    is the no-op state.
    """
    warnings: list[str] = []
    # ReExecute gating flags are read regardless of move_sl_enabled — see dataclass docs.
    no_reexec_sl_cost = bool(getattr(portfolio, "no_reexec_sl_cost", False))
    no_wait_trade_reexec = bool(getattr(portfolio, "no_wait_trade_reexec", False))
    no_reentry_sl_cost = bool(getattr(portfolio, "no_reentry_sl_cost", True))
    no_reentry_after_end = bool(getattr(portfolio, "no_reentry_after_end", False))

    # Portfolio-aggregate Move SL trigger (spec §2.3). Read regardless of
    # move_sl_enabled — it is an independent portfolio-level trigger, not a
    # sub-option of the per-slot Move SL to Cost feature.
    agg_pnl_enabled = bool(getattr(portfolio, "move_sl_agg_pnl_enabled", False))
    agg_pnl_threshold = float(getattr(portfolio, "move_sl_agg_pnl_threshold", 0.0) or 0.0)
    agg_pnl_direction = str(getattr(portfolio, "move_sl_agg_pnl_direction", "loss") or "loss").lower()
    if agg_pnl_direction not in ("loss", "profit"):
        warnings.append(
            f"Invalid move_sl_agg_pnl_direction={agg_pnl_direction!r} — falling back to 'loss'."
        )
        agg_pnl_direction = "loss"
    if agg_pnl_enabled and agg_pnl_threshold <= 0:
        warnings.append(
            "move_sl_agg_pnl_enabled but threshold <= 0 - aggregate Move SL trigger disabled."
        )
        agg_pnl_enabled = False

    if not getattr(portfolio, "move_sl_enabled", False):
        return _MoveSLConfig(
            enabled=False, no_reexec_sl_cost=no_reexec_sl_cost,
            no_wait_trade_reexec=no_wait_trade_reexec,
            no_reentry_sl_cost=no_reentry_sl_cost,
            no_reentry_after_end=no_reentry_after_end,
            agg_pnl_enabled=agg_pnl_enabled,
            agg_pnl_threshold=agg_pnl_threshold,
            agg_pnl_direction=agg_pnl_direction,
        ), warnings

    # Canonicalise tolerantly (spacing/case-insensitive) so UI / spec / engine
    # spellings of the same variant all resolve correctly instead of failing the
    # exact-string allow-list and degrading to profitable-only.
    _raw_action = getattr(portfolio, "move_sl_action", "Move Only for Profitable Legs")
    action = _canon_move_sl_action(_raw_action)
    if action in _OPTIONS_ONLY_MOVE_SL_ACTIONS:
        warnings.append(
            f"move_sl_action={_raw_action!r} (LTP + Buffer variant) is options-flavoured; "
            f"downgraded to 'Move Only for Profitable Legs' for FX/crypto."
        )
        action = "Move Only for Profitable Legs"

    safety = max(0, int(getattr(portfolio, "move_sl_safety_sec", 0) or 0))
    ltp_buffer = max(0.0, float(getattr(portfolio, "move_sl_ltp_buffer", 0.0) or 0.0))

    return _MoveSLConfig(
        enabled=True, safety_sec=safety, action=action,
        trail_after=bool(getattr(portfolio, "move_sl_trail_after", False)),
        no_buy_legs=bool(getattr(portfolio, "move_sl_no_buy_legs", False)),
        no_reexec_sl_cost=no_reexec_sl_cost,
        no_wait_trade_reexec=no_wait_trade_reexec,
        no_reentry_sl_cost=no_reentry_sl_cost,
        no_reentry_after_end=no_reentry_after_end,
        ltp_buffer=ltp_buffer,
        hit_on_leg_sl=bool(getattr(portfolio, "move_sl_hit_on_leg_sl", False)),
        hit_on_leg_target=bool(getattr(portfolio, "move_sl_hit_on_leg_target", False)),
        agg_pnl_enabled=agg_pnl_enabled,
        agg_pnl_threshold=agg_pnl_threshold,
        agg_pnl_direction=agg_pnl_direction,
    ), warnings
