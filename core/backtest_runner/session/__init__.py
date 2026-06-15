"""Multi-portfolio SESSION runner (production wiring).

Runs SEVERAL portfolios in ONE unified engine via
``unified._run_portfolio_unified(session_pf_specs=[...], slot_pf_ids={...})`` —
the validated multi-portfolio architecture. This is the production entry point
that the test harnesses (`multiportfolio_*`, `crosspf_*`) exercise directly.

For each portfolio it builds the SAME per-portfolio args/spec the single-portfolio
orchestration builds for the unified call (``orchestration._run_all_slots``), and
ADDITIONALLY wires the cross-portfolio action from the portfolio's config:
``pf_sl_action`` / ``pf_tgt_action`` == "SqOff|Execute|Start Other Portfolio" +
``pf_sl_target_portfolio`` / ``pf_tgt_target_portfolio`` → the spec's
``cross_pf_target`` / ``cross_pf_verb`` (consumed live, same-bar, by the target
portfolio's legs). The source still squares itself (its pf-SL "sqoff") AND fires
the verb at the target — matching the legacy post-run "each clip also publishes"
semantic.

Slot ids are namespaced per portfolio (``<pf name>__<slot id>``) so two portfolios
with colliding slot ids stay distinct in the shared engine; portfolio NAMES are the
portfolio ids (and cross-pf targets), so they must be unique across the session.
"""
from __future__ import annotations

import dataclasses

from core.models import PortfolioConfig, effective_portfolio_squareoff
from core.backtest_runner.unified import _run_portfolio_unified
from core.backtest_runner.bar_filters import _allowed_weekdays
from core.backtest_runner.other_settings import _resolve_other_settings
from core.backtest_runner.portfolio_exit_config import (
    _resolve_pf_stoploss, _resolve_pf_target, _resolve_move_sl_to_cost,
)
from core.backtest_runner.rbo import _resolve_rbo, _apply_winter_time
from core.backtest_runner.cross_portfolio import (
    _is_reexec_action, _is_entry_price_reexec, cross_pf_verb, clear_cross_pf_live_bus,
)
from core.backtest_runner.portfolio_results import _merge_portfolio_results


def _pf_action_triple(settings):
    """(action, market_mode, reexec_cap) for the monitor — mirrors orchestration.
    A cross-portfolio action ("… Other Portfolio") is NOT reexec, so it resolves to
    a plain "sqoff" of the SOURCE; the cross-pf verb is wired separately."""
    a = getattr(settings, "action", "") or ""
    if _is_reexec_action(a):
        return ("reexecute", not _is_entry_price_reexec(a), int(getattr(settings, "reexecute_count", 0) or 0))
    return ("sqoff", False, 0)


def _capitals_for(pf: PortfolioConfig) -> dict:
    slots = pf.enabled_slots
    n = len(slots)
    caps = {}
    if pf.allocation_mode == "percentage":
        for s in slots:
            pct = s.allocation_pct if s.allocation_pct > 0 else (100.0 / n)
            caps[s.slot_id] = pf.starting_capital * pct / 100.0
    else:
        for s in slots:
            caps[s.slot_id] = pf.starting_capital / n
    return caps


def build_pf_session_args(pf: PortfolioConfig, catalog_path: str,
                          custom_strategies_dir: str | None, user_id: str | None):
    """Build (args, spec, slot_pf_ids, capitals) for ONE portfolio in a session.

    ``args`` are the ``_run_portfolio_unified`` kwargs (the per-portfolio defaults);
    ``spec`` is that portfolio's session spec (scoped monitor + cross-pf wiring);
    ``slot_pf_ids`` maps each slot id → this portfolio's id (its name)."""
    _apply_winter_time(pf)
    rbo, _ = _resolve_rbo(pf)
    other, _ = _resolve_other_settings(pf)
    move_sl, _ = _resolve_move_sl_to_cost(pf)
    sq_time, sq_tz = effective_portfolio_squareoff(pf)
    sl_set, _ = _resolve_pf_stoploss(pf)
    tgt_set, _ = _resolve_pf_target(pf)
    sl_act, sl_mkt, sl_cap = _pf_action_triple(sl_set)
    tgt_act, tgt_mkt, tgt_cap = _pf_action_triple(tgt_set)

    slots = pf.enabled_slots
    caps = _capitals_for(pf)
    vwap_fill = bool(getattr(pf, "vwap_exit_fill", False))
    dir_fill = bool(getattr(pf, "directional_close_fill", False))
    pairs = [(s, caps[s.slot_id]) for s in slots]
    pid = getattr(pf, "name", "")

    args = dict(
        catalog_path=catalog_path, slot_capital_pairs=pairs,
        custom_strategies_dir=custom_strategies_dir,
        default_start_date=pf.start_date, default_end_date=pf.end_date,
        default_squareoff_time=sq_time, default_squareoff_tz=sq_tz,
        default_run_on_days=pf.run_on_days,
        default_entry_start_time=pf.entry_start_time, default_entry_end_time=pf.entry_end_time,
        default_rbo_settings=rbo, default_other_settings=other, default_move_sl_settings=move_sl,
        user_id=user_id, portfolio_name=pid,
        default_vwap_fill=vwap_fill, default_directional_fill=dir_fill,
        default_capture_underlying=False,
        default_pf_sl_enabled=bool(getattr(sl_set, "enabled", False)),
        default_pf_sl_value=float(getattr(sl_set, "value", 0.0) or 0.0),
        default_day_tz=(sq_tz or "UTC"),
        default_pf_sl_action=sl_act, default_pf_sl_reexec_cap=sl_cap, default_pf_sl_market_mode=sl_mkt,
        default_pf_tgt_enabled=bool(getattr(tgt_set, "enabled", False)),
        default_pf_tgt_value=float(getattr(tgt_set, "value", 0.0) or 0.0),
        default_pf_tgt_action=tgt_act, default_pf_tgt_reexec_cap=tgt_cap, default_pf_tgt_market_mode=tgt_mkt,
        default_pf_sl_type=str(getattr(sl_set, "sl_type", "Combined Loss") or "Combined Loss"),
        default_pf_sl_underlying_below=float(getattr(sl_set, "underlying_below", 0.0) or 0.0),
        default_pf_sl_underlying_above=float(getattr(sl_set, "underlying_above", 0.0) or 0.0),
        default_pf_sl_delay_sec=int(getattr(sl_set, "delay_sec", 0) or 0),
        default_pf_tgt_delay_sec=int(getattr(tgt_set, "delay_sec", 0) or 0),
    )

    # enforce = same gate the orchestration/test use: a pf feature is configured + flags on.
    import os
    post_run = os.environ.get("_USE_POST_RUN_PF", "0") == "1"
    enf_flag = (not post_run) and os.environ.get("_USE_PF_ENFORCE", "1") == "1"
    agg = bool(move_sl and getattr(move_sl, "agg_pnl_enabled", False)
               and float(getattr(move_sl, "agg_pnl_threshold", 0) or 0) > 0)
    hit = bool(move_sl and (getattr(move_sl, "hit_on_leg_sl", False) or getattr(move_sl, "hit_on_leg_target", False)))
    sl_on = bool(getattr(sl_set, "enabled", False)) and float(getattr(sl_set, "value", 0.0) or 0.0) > 0
    tgt_on = bool(getattr(tgt_set, "enabled", False)) and float(getattr(tgt_set, "value", 0.0) or 0.0) > 0
    enforce = enf_flag and (sl_on or tgt_on or agg or hit)

    # Cross-portfolio wiring: SL-side action first, else target-side. The verb is
    # published to the named target portfolio when THIS portfolio's monitor breaches.
    x_verb = cross_pf_verb(getattr(pf, "pf_sl_action", "")) or ""
    x_target = getattr(pf, "pf_sl_target_portfolio", "") or ""
    if not x_verb:
        x_verb = cross_pf_verb(getattr(pf, "pf_tgt_action", "")) or ""
        x_target = getattr(pf, "pf_tgt_target_portfolio", "") or ""
    # A cross-pf action must enforce (its monitor has to run to publish the verb).
    if x_verb and x_target:
        enforce = enf_flag and (sl_on or tgt_on or agg or hit or True)

    spec = {
        "id": pid, "enforce": enforce,
        "pf_sl_enabled": args["default_pf_sl_enabled"], "pf_sl_value": args["default_pf_sl_value"],
        "pf_sl_action": sl_act, "pf_sl_reexec_cap": sl_cap, "pf_sl_market_mode": sl_mkt,
        "pf_sl_type": args["default_pf_sl_type"],
        "pf_sl_underlying_below": args["default_pf_sl_underlying_below"],
        "pf_sl_underlying_above": args["default_pf_sl_underlying_above"],
        "pf_sl_delay_sec": args["default_pf_sl_delay_sec"],
        "pf_tgt_enabled": args["default_pf_tgt_enabled"], "pf_tgt_value": args["default_pf_tgt_value"],
        "pf_tgt_action": tgt_act, "pf_tgt_reexec_cap": tgt_cap, "pf_tgt_market_mode": tgt_mkt,
        "pf_tgt_delay_sec": args["default_pf_tgt_delay_sec"],
        "move_sl_settings": move_sl, "rbo_settings": rbo, "other_settings": other,
        "squareoff_time": sq_time, "squareoff_tz": sq_tz,
        "entry_start_time": pf.entry_start_time, "entry_end_time": pf.entry_end_time,
        "allowed_weekdays": _allowed_weekdays(pf.run_on_days),
        "day_tz": (sq_tz or "UTC"),
        "underlying_bar_type": (pairs[0][0].bar_type_str if pairs else ""),
        "vwap_fill": vwap_fill, "dir_fill": dir_fill, "pf_feature": (sl_on or tgt_on or agg or hit),
        "cross_pf_target": x_target, "cross_pf_verb": x_verb,
    }
    slot_pf_ids = {s.slot_id: pid for s in slots}
    return args, spec, slot_pf_ids, caps


def _namespaced(pf: PortfolioConfig) -> PortfolioConfig:
    """Return a copy of pf whose ENABLED slots have ``<pf name>__`` slot ids so two
    portfolios with colliding slot ids stay distinct in the shared engine."""
    pid = getattr(pf, "name", "") or "PF"
    ns_slots = [dataclasses.replace(s, slot_id=f"{pid}__{s.slot_id}") for s in pf.enabled_slots]
    return dataclasses.replace(pf, slots=ns_slots)


def run_session_backtest(catalog_path: str, portfolios: list[PortfolioConfig],
                         custom_strategies_dir: str | None = None,
                         user_id: str | None = None) -> dict:
    """Run several portfolios in ONE session engine; return {portfolio_name: result}.

    Each portfolio's result has the SAME shape as ``run_portfolio_backtest`` (built
    by ``_merge_portfolio_results`` on that portfolio's slice of the shared run)."""
    names = [getattr(p, "name", "") for p in portfolios]
    if len(set(names)) != len(names):
        raise ValueError("Session portfolios must have unique names "
                         f"(cross-pf targets are by name); got {names}")
    clear_cross_pf_live_bus()

    specs, all_pairs, all_maps = [], [], {}
    base_args = None
    merge_inputs = []  # (namespaced_pf, capitals)
    # Union date range so every portfolio's legs get their bars even if ranges differ.
    starts = [p.start_date for p in portfolios if getattr(p, "start_date", None)]
    ends = [p.end_date for p in portfolios if getattr(p, "end_date", None)]
    union_start = min(starts) if starts else None
    union_end = max(ends) if ends else None

    for pf in portfolios:
        ns_pf = _namespaced(pf)
        args, spec, smap, caps = build_pf_session_args(
            ns_pf, catalog_path, custom_strategies_dir, user_id)
        if base_args is None:
            base_args = args
        specs.append(spec)
        all_pairs += list(args["slot_capital_pairs"])
        all_maps.update(smap)
        merge_inputs.append((ns_pf, caps))

    merged = dict(base_args)
    merged["slot_capital_pairs"] = all_pairs
    merged["portfolio_name"] = "SESSION"
    if union_start is not None:
        merged["default_start_date"] = union_start
    if union_end is not None:
        merged["default_end_date"] = union_end

    slot_results, errors = _run_portfolio_unified(
        session_pf_specs=specs, slot_pf_ids=all_maps, **merged)

    out = {}
    for ns_pf, caps in merge_inputs:
        ids = {s.slot_id for s in ns_pf.enabled_slots}
        sub = {sid: r for sid, r in slot_results.items() if sid in ids}
        out[getattr(ns_pf, "name", "")] = _merge_portfolio_results(
            ns_pf, sub, caps, errors, user_id=user_id)
    return out
