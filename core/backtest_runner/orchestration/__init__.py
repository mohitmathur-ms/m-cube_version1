"""Top-level portfolio entry point: run_portfolio_backtest."""

from __future__ import annotations

import dataclasses
import pandas as pd
from core.models import PortfolioConfig
from core.models import effective_portfolio_squareoff

from core.backtest_runner.bar_types import _group_slots
from core.backtest_runner.unified import _run_portfolio_unified, _unified_active
from core.backtest_runner.cross_portfolio import (
    _is_entry_price_reexec,
    _is_reexec_action,
    consume_cross_portfolio_events,
)
from core.backtest_runner.other_settings import _resolve_other_settings
from core.backtest_runner.portfolio_clip import (
    _compute_agg_coordination,
    _entry_at_clip,
    _opened_in_window,
    _ts_iso_to_ns,
)
from core.backtest_runner.portfolio_exit_config import (
    _UNDERLYING_PF_SL_TYPES,
    _UNDERLYING_PF_TGT_TYPES,
    _resolve_move_sl_to_cost,
    _resolve_pf_stoploss,
    _resolve_pf_target,
)
from core.backtest_runner.portfolio_results import (
    _leg_key_of,
    _merge_portfolio_results,
    _splice_merged_results,
)
from core.backtest_runner.rbo import (
    _apply_winter_time,
    _resolve_rbo,
)
from core.backtest_runner.slot_execution import (
    _run_single_slot,
    _run_slot_group,
    _worker_init_ignore_sigint,
)


def run_portfolio_backtest(
    catalog_path: str,
    portfolio: PortfolioConfig,
    custom_strategies_dir: str | None = None,
    on_slot_complete=None,
    user_id: str | None = None,
) -> dict:
    """
    Run a portfolio backtest with multiple strategy slots in parallel.

    Each slot runs in its own engine with allocated capital.
    Results are merged into portfolio-level metrics.

    ``user_id`` threads through to ``effective_slot_qty`` so the per-user
    multiplier (from ``config/users.json``) scales every slot's order
    quantity. None preserves single-user behavior (multiplier=1.0).
    """
    import os
    from concurrent.futures import ProcessPoolExecutor, as_completed

    # Reset the cross-slot event bus for this portfolio so a prior run's
    # SL/target events don't leak into the new run (spec §3 1.3(f)/(g)).
    from core.managed_strategy import clear_cross_slot_bus
    pf_name = getattr(portfolio, "name", "") or "_standalone_"
    clear_cross_slot_bus(pf_name)

    # Cross-portfolio dispatch consumption (spec §2.1(h)/(i)/(j) + §2.4
    # mirror). Pending events were written by an earlier portfolio's clip.
    pending_events = consume_cross_portfolio_events(pf_name)
    pending_sqoff = any(e["action"] == "sqoff" for e in pending_events)
    pending_execute = any(e["action"] == "execute" for e in pending_events)
    if pending_events:
        for e in pending_events:
            print(f"[XPF] {pf_name!r} consumed {e['action']!r} from clip @ {e.get('ts')}")
    if pending_sqoff:
        # SqOff Other Portfolio from a prior run: short-circuit this run.
        # Return a zero-trade result that downstream callers handle the same
        # way they handle an immediately-clipped portfolio.
        print(f"[XPF] {pf_name!r} suppressed by SqOff Other Portfolio event")
        return {
            "portfolio_name": pf_name,
            "starting_capital": getattr(portfolio, "starting_capital", 0.0),
            "final_balance": getattr(portfolio, "starting_capital", 0.0),
            "total_pnl": 0.0,
            "total_trades": 0,
            "wins": 0, "losses": 0,
            "max_loss_hit": False, "max_profit_hit": False,
            "per_strategy": {},
            "cross_portfolio_dispatch": {"suppressed_by": "sqoff", "events": pending_events},
        }
    # "execute"/"start" verbs just confirm normal run; armed_at_start handles
    # per-slot activation. The flag is surfaced in results for observability.

    enabled_slots = portfolio.enabled_slots

    if not enabled_slots:
        raise ValueError("No enabled strategy slots in portfolio")

    # Winter Time Adjustment (spec execution_logic_target.html §9). Shift all
    # configured local times +1h in place BEFORE resolving RBO / square-off /
    # entry-window, so every downstream path sees the adjusted values.
    if _apply_winter_time(portfolio):
        print(f"[WINTER] {pf_name!r}: configured times shifted +1h (winter_time_adjust)")

    # Resolve RBO once at the orchestrator. Failures fall back to standard
    # time-based entry per spec (rbo_logics.html validation rules); we surface
    # the message via print so it shows up in worker output even when the
    # caller doesn't pipe a logger.
    rbo_settings, rbo_msg = _resolve_rbo(portfolio)
    if rbo_msg:
        if rbo_settings is None:
            print(f"[RBO] disabled: {rbo_msg}")
        else:
            print(f"[RBO] warning: {rbo_msg}")

    # Resolve Other Settings (delay_between_legs, on_sl_action_on,
    # on_target_action_on, plus options-only fields). Spec:
    # 5. Logics/Other_Settings_Logic.html.
    other_settings, other_warnings = _resolve_other_settings(portfolio)
    for w in other_warnings:
        print(f"[OTHER] {w}")

    # Resolve Move SL to Cost (per-slot adaptation). Threaded into
    # ManagedExitConfig via config_from_exit alongside other slot params.
    # Spec: 5. Logics/portfolio_sl_tgt.html §3.
    move_sl_settings, move_sl_warnings = _resolve_move_sl_to_cost(portfolio)
    for w in move_sl_warnings:
        print(f"[MOVE_SL] {w}")

    # Conservative VWAP exit-fill model (spec §4.2 / §8.1). Enabled per-portfolio
    # via the saved config, or globally via the _USE_VWAP_FILL dev/parity flag.
    # Threaded into every slot worker as default_vwap_fill.
    vwap_fill_enabled = bool(getattr(portfolio, "vwap_exit_fill", False)) or (
        os.environ.get("_USE_VWAP_FILL", "0") == "1"
    )
    if vwap_fill_enabled:
        print(f"[VWAP_FILL] {pf_name!r}: conservative VWAP exit-fill enabled")

    # Directional-close exit-fill model (spec execution_logic.html §8.1). The
    # directional bid/ask close (long→bid, short→ask) is the base price for
    # every exit, modelling the half-spread paid on exit. Enabled per-portfolio
    # via ``directional_close_fill``, or globally via _USE_DIRECTIONAL_FILL.
    # Composes with the VWAP fill per spec: VWAP owns the SL/Target leg exits
    # (§4.2), the directional close is the base for the rest (squareoff/EOD,
    # §8.1). Threaded to workers as default_directional_fill.
    directional_fill_enabled = (
        bool(getattr(portfolio, "directional_close_fill", False))
        or os.environ.get("_USE_DIRECTIONAL_FILL", "0") == "1"
    )
    if directional_fill_enabled:
        print(f"[DIRECTIONAL_FILL] {pf_name!r}: directional-close exit-fill enabled")

    # Calculate capital allocation per slot
    n = len(enabled_slots)
    capitals = {}
    if portfolio.allocation_mode == "percentage":
        for slot in enabled_slots:
            pct = slot.allocation_pct if slot.allocation_pct > 0 else (100.0 / n)
            capitals[slot.slot_id] = portfolio.starting_capital * pct / 100.0
    else:  # equal
        per_slot = portfolio.starting_capital / n
        for slot in enabled_slots:
            capitals[slot.slot_id] = per_slot

    # Run all slots in parallel (executor block lives in _run_all_slots below).

    # Raise cap from 8 → 32 so 16-core boxes actually utilize their cores.
    max_workers = min(n, (os.cpu_count() or 2), 32)

    # LPT scheduling: submit longest-expected slots first so shorter ones
    # can tail-fill behind them, minimizing max-worker-runtime imbalance.
    # History-aware: if we've run (bar_type, strategy) before, use the
    # observed per-day runtime instead of the span heuristic — this fixes
    # the USDJPY-tail case where three pairs share a span but one is 10x
    # heavier due to trade volume.
    from core import runtime_history
    history = runtime_history.load()

    def _span_days(slot):
        s = slot.start_date or portfolio.start_date
        e = slot.end_date or portfolio.end_date
        if s and e:
            try:
                return max(1, (pd.Timestamp(e) - pd.Timestamp(s)).days)
            except Exception:
                return 1
        return 1

    def _duration_estimate(slot):
        span = _span_days(slot)
        hist = runtime_history.estimate(history, slot.bar_type_str, slot.strategy_name, span)
        if hist is not None:
            return hist
        # Bollinger Bands empirically ran 15-20% slower than EMA/RSI in benchmarks.
        mult = 1.2 if "bollinger" in slot.strategy_name.lower() else 1.0
        return span * mult

    # Direction B: group slots that share (bar_type, start, end, custom_strategies_dir)
    # and submit one future per group. Size-1 groups still run via _run_single_slot
    # (zero-behavior-change fallback). Size-≥2 groups run in a shared engine via
    # _run_slot_group. Gated behind _USE_GROUPING env flag for safe rollout.
    use_grouping = os.environ.get("_USE_GROUPING", "0") == "1"

    # Capture each slot's underlying price series only when the portfolio uses
    # an underlying-price-based SL or Target type (spec §2.1 / §5.1) — otherwise
    # the result dict stays lean. Resolved here so it threads into slot workers.
    _capture_underlying = (
        bool(getattr(portfolio, "pf_sl_enabled", False))
        and str(getattr(portfolio, "pf_sl_type", "") or "") in _UNDERLYING_PF_SL_TYPES
    ) or (
        bool(getattr(portfolio, "pf_tgt_enabled", False))
        and str(getattr(portfolio, "pf_tgt_type", "") or "") in _UNDERLYING_PF_TGT_TYPES
    )

    if use_grouping:
        groups = _group_slots(
            enabled_slots, capitals,
            default_start_date=portfolio.start_date,
            default_end_date=portfolio.end_date,
            custom_strategies_dir=custom_strategies_dir,
        )
        # LPT at group level — sum member-slot durations so the longest group submits first
        def _group_duration(grp):
            return sum(_duration_estimate(slot) for slot, _cap in grp)
        sorted_groups = sorted(groups, key=_group_duration, reverse=True)
        # max_workers capped by n_groups (no point spawning more workers than groups)
        max_workers = min(len(sorted_groups), (os.cpu_count() or 2), 32)
    else:
        sorted_slots = sorted(enabled_slots, key=_duration_estimate, reverse=True)

    def _run_all_slots(active_move_sl, fire_callbacks: bool, replay_cutoff_ns: int = 0,
                       reexec_entry_prices: dict | None = None,
                       reexec_market_mode: bool = False):
        """Submit every slot/group to a fresh ProcessPoolExecutor and collect
        results into ``{slot_id: result}``.

        The two-pass aggregate-Move-SL feature (spec §2.3) calls this twice
        with a different ``active_move_sl``; a normal run calls it once.
        ``fire_callbacks`` gates ``on_slot_complete`` and runtime-history
        recording so only the final (reported) pass drives the UI / history.
        ``replay_cutoff_ns`` > 0 makes every slot start flat at that timestamp
        — used by the portfolio ReExecute replay (spec §2.4).
        Returns ``(slot_results, errors)``.
        """
        slot_results: dict = {}
        errors: list = []
        # Portfolio-level squareoff routed through the helper so MIS product
        # type can supply a default when no explicit squareoff_time is set.
        # Slot/leg overrides still win at resolve-time inside the slot worker.
        _pf_sq_time, _pf_sq_tz = effective_portfolio_squareoff(portfolio)

        # ── Phase 1: unified single-engine path (flag _USE_UNIFIED_ENGINE) ──
        # Runs ALL legs in ONE engine. Restricted (for now) to the plain
        # single-pass case: no ReExecute replay, no aggregate/cross-leg Move-SL
        # two-pass, and no entry-window bar-filter (Phase 2). Portfolio SL/Target
        # still applies via the existing post-run clip on these results.
        _ms = active_move_sl
        _agg_active = bool(_ms and (getattr(_ms, "agg_pnl_enabled", False)
                                    or getattr(_ms, "hit_on_leg_sl", False)
                                    or getattr(_ms, "hit_on_leg_target", False)))
        # Phase 2: entry-window + run_on_days are now handled in the unified path
        # (start-side pre-filter + strategy gate, matching the per-slot MANAGED
        # behaviour). For a windowed portfolio only take the unified path when
        # every leg is managed (RAW end-side bar-filtering isn't replicated yet).
        _has_window = bool(portfolio.entry_start_time or portfolio.entry_end_time)
        _all_managed = all(
            (s.exit_config.has_exit_management()
             or s.exit_config.squareoff_time or s.squareoff_time
             or _pf_sq_time or rbo_settings is not None)
            for s in enabled_slots
        )
        # Portfolio SL/Target (SqOff / Target / trailing / selective / day-scoped)
        # run on the unified path — the clip sites are keyed by the per-leg
        # (trader_id|strategy_id) combo, correct in one shared engine. ReExecute
        # (both market AND "at Entry Price") ALSO runs live one-pass now (Phase 3
        # done): the monitor fires the breach and each leg either re-enters at
        # market or arms a genuine resting LIMIT at the captured entry price and
        # re-enters when price returns (managed_strategy `_reexec_limit_px`), capped
        # at reexecute_count. The old per-slot + post-run replay (portfolio_clip)
        # is now ONLY the _USE_POST_RUN_PF escape hatch.
        _sl_reexec = bool(portfolio.pf_sl_enabled and _is_reexec_action(portfolio.pf_sl_action))
        _tgt_reexec = bool(portfolio.pf_tgt_enabled and _is_reexec_action(portfolio.pf_tgt_action))
        _reexec_cfg = _sl_reexec or _tgt_reexec
        # Live portfolio enforcement is the DEFAULT (one-pass live engine): the
        # monitor signals legs to close (SqOff) / re-arm re-entry (ReExecute) /
        # move SL (aggregate + Hit-On-Leg) live on base bars — NO post-run clip,
        # NO two-pass replay/discovery. Reverted only by _USE_POST_RUN_PF=1.
        # pf_sl AND pf_tgt ReExecute both run live now.
        _enforce_active = (os.environ.get("_USE_POST_RUN_PF", "0") != "1"
                           and os.environ.get("_USE_PF_ENFORCE", "1") == "1")
        _live_reexec_ok = _enforce_active  # both SL- and Target-side ReExecute are live
        # Aggregate-P&L + Hit-On-Leg Move-SL also run one-pass when enforcing.
        _live_move_sl_ok = _enforce_active and _agg_active
        if (_unified_active() and replay_cutoff_ns == 0 and not reexec_entry_prices
                and (not _agg_active or _live_move_sl_ok)
                and (not _reexec_cfg or _live_reexec_ok)
                and (not _has_window or _all_managed)):
            print("[UNIFIED] single-engine path active — running all legs in one engine")
            pairs = [(slot, capitals[slot.slot_id]) for slot in enabled_slots]
            _sl_set_u, _ = _resolve_pf_stoploss(portfolio)
            _tgt_set_u, _ = _resolve_pf_target(portfolio)

            def _pf_action_triple(_settings):
                _a = getattr(_settings, "action", "") or ""
                if _is_reexec_action(_a):
                    return ("reexecute", not _is_entry_price_reexec(_a),
                            int(getattr(_settings, "reexecute_count", 0) or 0))
                return ("sqoff", False, 0)

            _pf_action_u, _pf_market_u, _pf_cap_u = _pf_action_triple(_sl_set_u)
            _tgt_action_u, _tgt_market_u, _tgt_cap_u = _pf_action_triple(_tgt_set_u)
            return _run_portfolio_unified(
                catalog_path=catalog_path,
                slot_capital_pairs=pairs,
                custom_strategies_dir=custom_strategies_dir,
                default_start_date=portfolio.start_date,
                default_end_date=portfolio.end_date,
                default_squareoff_time=_pf_sq_time,
                default_squareoff_tz=_pf_sq_tz,
                default_run_on_days=portfolio.run_on_days,
                default_entry_start_time=portfolio.entry_start_time,
                default_entry_end_time=portfolio.entry_end_time,
                default_rbo_settings=rbo_settings,
                default_other_settings=other_settings,
                default_move_sl_settings=active_move_sl,
                user_id=user_id,
                portfolio_name=getattr(portfolio, "name", ""),
                default_vwap_fill=vwap_fill_enabled,
                default_directional_fill=directional_fill_enabled,
                default_capture_underlying=_capture_underlying,
                default_pf_sl_enabled=bool(getattr(_sl_set_u, "enabled", False)),
                default_pf_sl_value=float(getattr(_sl_set_u, "value", 0.0) or 0.0),
                default_day_tz=(_pf_sq_tz or "UTC"),
                default_pf_sl_action=_pf_action_u,
                default_pf_sl_reexec_cap=_pf_cap_u,
                default_pf_sl_market_mode=_pf_market_u,
                default_pf_tgt_enabled=bool(getattr(_tgt_set_u, "enabled", False)),
                default_pf_tgt_value=float(getattr(_tgt_set_u, "value", 0.0) or 0.0),
                default_pf_tgt_action=_tgt_action_u,
                default_pf_tgt_reexec_cap=_tgt_cap_u,
                default_pf_tgt_market_mode=_tgt_market_u,
                # Underlying-price SL (spec §2.1/§5.1) — live in the monitor.
                default_pf_sl_type=str(getattr(_sl_set_u, "sl_type", "Combined Loss") or "Combined Loss"),
                default_pf_sl_underlying_below=float(getattr(_sl_set_u, "underlying_below", 0.0) or 0.0),
                default_pf_sl_underlying_above=float(getattr(_sl_set_u, "underlying_above", 0.0) or 0.0),
                default_pf_sl_delay_sec=int(getattr(_sl_set_u, "delay_sec", 0) or 0),
                default_pf_tgt_delay_sec=int(getattr(_tgt_set_u, "delay_sec", 0) or 0),
            )
        with ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=_worker_init_ignore_sigint,
        ) as executor:
            futures = {}

            if use_grouping:
                # One future per group. Size-1 groups route to _run_single_slot (unchanged path);
                # size-≥2 groups route to _run_slot_group (new shared-engine path).
                for group_idx, group in enumerate(sorted_groups):
                    if len(group) == 1:
                        slot, capital = group[0]
                        future = executor.submit(
                            _run_single_slot,
                            catalog_path=catalog_path,
                            slot=slot,
                            capital=capital,
                            custom_strategies_dir=custom_strategies_dir,
                            slot_index=group_idx,
                            default_start_date=portfolio.start_date,
                            default_end_date=portfolio.end_date,
                            default_squareoff_time=_pf_sq_time,
                            default_squareoff_tz=_pf_sq_tz,
                            default_run_on_days=portfolio.run_on_days,
                            default_entry_start_time=portfolio.entry_start_time,
                            default_entry_end_time=portfolio.entry_end_time,
                            default_rbo_settings=rbo_settings,
                            default_other_settings=other_settings,
                            default_move_sl_settings=active_move_sl,
                            user_id=user_id,
                            default_capture_underlying=_capture_underlying,
                            default_replay_cutoff_ns=replay_cutoff_ns,
                            default_vwap_fill=vwap_fill_enabled,
                            default_directional_fill=directional_fill_enabled,
                            default_reexec_entry_price=(reexec_entry_prices or {}).get(
                                slot.slot_id, (0.0, True))[0],
                            default_reexec_entry_was_long=(reexec_entry_prices or {}).get(
                                slot.slot_id, (0.0, True))[1],
                            default_reexec_market_mode=reexec_market_mode,
                        )
                        futures[future] = ("single", [slot])
                    else:
                        future = executor.submit(
                            _run_slot_group,
                            catalog_path=catalog_path,
                            group=group,
                            custom_strategies_dir=custom_strategies_dir,
                            group_index=group_idx,
                            default_start_date=portfolio.start_date,
                            default_end_date=portfolio.end_date,
                            default_squareoff_time=_pf_sq_time,
                            default_squareoff_tz=_pf_sq_tz,
                            default_run_on_days=portfolio.run_on_days,
                            default_entry_start_time=portfolio.entry_start_time,
                            default_entry_end_time=portfolio.entry_end_time,
                            default_rbo_settings=rbo_settings,
                            default_other_settings=other_settings,
                            default_move_sl_settings=active_move_sl,
                            user_id=user_id,
                            portfolio_name=pf_name,
                            default_replay_cutoff_ns=replay_cutoff_ns,
                            default_vwap_fill=vwap_fill_enabled,
                            default_directional_fill=directional_fill_enabled,
                            default_reexec_entry_prices=reexec_entry_prices,
                            default_reexec_market_mode=reexec_market_mode,
                            default_capture_underlying=_capture_underlying,
                        )
                        futures[future] = ("group", [slot for slot, _cap in group])
            else:
                for i, slot in enumerate(sorted_slots):
                    future = executor.submit(
                        _run_single_slot,
                        catalog_path=catalog_path,
                        slot=slot,
                        capital=capitals[slot.slot_id],
                        custom_strategies_dir=custom_strategies_dir,
                        slot_index=i,
                        default_start_date=portfolio.start_date,
                        default_end_date=portfolio.end_date,
                        default_squareoff_time=_pf_sq_time,
                        default_squareoff_tz=_pf_sq_tz,
                        default_run_on_days=portfolio.run_on_days,
                        default_entry_start_time=portfolio.entry_start_time,
                        default_entry_end_time=portfolio.entry_end_time,
                        default_rbo_settings=rbo_settings,
                        default_other_settings=other_settings,
                        default_move_sl_settings=active_move_sl,
                        user_id=user_id,
                        default_capture_underlying=_capture_underlying,
                        default_replay_cutoff_ns=replay_cutoff_ns,
                        default_vwap_fill=vwap_fill_enabled,
                        default_directional_fill=directional_fill_enabled,
                        default_reexec_entry_price=(reexec_entry_prices or {}).get(
                            slot.slot_id, (0.0, True))[0],
                        default_reexec_entry_was_long=(reexec_entry_prices or {}).get(
                            slot.slot_id, (0.0, True))[1],
                        default_reexec_market_mode=reexec_market_mode,
                    )
                    futures[future] = ("single", [slot])

            try:
                for future in as_completed(futures):
                    kind, slots_in_future = futures[future]
                    try:
                        result = future.result()
                        if kind == "group":
                            # _run_slot_group returns list[dict], one per slot in insertion order
                            for slot, r in zip(slots_in_future, result):
                                slot_results[slot.slot_id] = r
                                elapsed = r.get("elapsed_seconds")
                                if fire_callbacks and elapsed is not None:
                                    runtime_history.record(
                                        history, slot.bar_type_str, slot.strategy_name,
                                        float(elapsed), _span_days(slot),
                                    )
                                if fire_callbacks and on_slot_complete:
                                    try:
                                        on_slot_complete(slot.slot_id)
                                    except Exception:
                                        pass
                        else:
                            slot = slots_in_future[0]
                            slot_results[slot.slot_id] = result
                            elapsed = result.get("elapsed_seconds")
                            if fire_callbacks and elapsed is not None:
                                runtime_history.record(
                                    history, slot.bar_type_str, slot.strategy_name,
                                    float(elapsed), _span_days(slot),
                                )
                            if fire_callbacks and on_slot_complete:
                                try:
                                    on_slot_complete(slot.slot_id)
                                except Exception:
                                    pass
                    except Exception as e:
                        for slot in slots_in_future:
                            errors.append({
                                "slot_id": slot.slot_id,
                                "display_name": slot.display_name,
                                "error": str(e),
                            })
            except KeyboardInterrupt:
                # Parent main thread saw Ctrl+C. Cancel queued futures; in-flight
                # workers (which ignore SIGINT) finish their current engine.run()
                # and the pool drains cleanly. Re-raise so the caller sees the KI.
                executor.shutdown(wait=False, cancel_futures=True)
                raise
        return slot_results, errors

    # Two-pass portfolio-aggregate Move SL (spec §2.3). When active: pass 1
    # discovers the combined-P&L timeline + per-leg SL/target hits, the parent
    # computes the aggregate trigger ts and a cross-process event bus, then
    # pass 2 replays with those injected. ~2x runtime.
    #
    # Now a real per-portfolio feature: the two-pass runs whenever the
    # aggregate-P&L trigger or a cross-slot Hit-On-Leg trigger is configured on
    # the portfolio (cross-process Hit-On-Leg only works correctly via the
    # two-pass pre-seeded bus, since the in-process bus doesn't span workers).
    # When nothing is configured, agg_active stays False → single pass, zero
    # behavior change. The legacy _USE_PF_AGG_MOVE_SL env flag is still honoured
    # as a manual override for parity tooling.
    agg_active = (
        move_sl_settings.agg_pnl_enabled
        or move_sl_settings.hit_on_leg_sl
        or move_sl_settings.hit_on_leg_target
        or os.environ.get("_USE_PF_AGG_MOVE_SL", "0") == "1"
    )
    # Phase 4: when the live monitor enforces (unified path), aggregate-P&L +
    # Hit-On-Leg Move-SL run ONE-PASS in the engine — skip the two-pass discovery.
    _live_move_sl_active = (
        os.environ.get("_USE_POST_RUN_PF", "0") != "1"
        and os.environ.get("_USE_PF_ENFORCE", "1") == "1"
        and _unified_active()
        and (move_sl_settings.agg_pnl_enabled
             or move_sl_settings.hit_on_leg_sl
             or move_sl_settings.hit_on_leg_target)
    )
    if not agg_active or _live_move_sl_active:
        slot_results, errors = _run_all_slots(move_sl_settings, fire_callbacks=True)
    else:
        print("[PF_AGG_MOVE_SL] two-pass active — running discovery pass 1")
        pass1_results, pass1_errors = _run_all_slots(move_sl_settings, fire_callbacks=False)
        coord = _compute_agg_coordination(portfolio, pass1_results, move_sl_settings)
        for log_line in coord.logs:
            print(f"[PF_AGG_MOVE_SL] {log_line}")
        # Pass-1 wrote live SL/target events into the per-process bus; clear it
        # so pass 2 starts from only the explicitly pre-seeded events.
        clear_cross_slot_bus(pf_name)
        move_sl_pass2 = dataclasses.replace(
            move_sl_settings,
            agg_trigger_ns=coord.agg_trigger_ns,
            preseeded_bus=coord.event_bus,
        )
        slot_results, errors = _run_all_slots(move_sl_pass2, fire_callbacks=True)
        # If pass 2 produced nothing, surface pass-1's errors for diagnostics.
        if not slot_results and not errors:
            errors = pass1_errors

    # Persist once after the whole run — cheap single JSON write.
    try:
        runtime_history.save(history)
    except Exception:
        pass

    if not slot_results and errors:
        raise ValueError(f"All slots failed: {errors}")

    # Merge results into portfolio-level metrics
    result = _merge_portfolio_results(portfolio, slot_results, capitals, errors, user_id=user_id)

    # ── Portfolio ReExecute replay (spec §2.4) ──────────────────────────────
    # When the portfolio SL/Target fires a ReExecute-family action (plain
    # ReExecute, "ReExecute at Entry Price", or "ReExecute Same Contract at
    # EntryPrice"), re-run every slot FLAT from the clip timestamp and splice
    # that segment onto the pre-clip trades — a genuine re-execution instead of
    # the default "keep trades" approximation. The segment is itself merged (so
    # it re-evaluates the portfolio limit) and the loop recurses on its first
    # ReExecute clip, up to the configured ReExecute count (0 = unlimited,
    # hard-capped at 50 to bound runtime).
    #
    # Now config-driven: the replay runs whenever a ReExecute-family action is
    # actually configured on the portfolio SL or Target (the entry-price
    # variants replay as plain ReExecute — the FX adaptation per spec, which
    # marks them ⚙️ since price-wait re-entry lives at the leg level §1.2(d)).
    # When no ReExecute action is configured, this is a no-op single pass. The
    # legacy _USE_PF_REEXEC_REPLAY env flag is still honoured as an override.
    _sl_set, _ = _resolve_pf_stoploss(portfolio)
    _tgt_set, _ = _resolve_pf_target(portfolio)
    _reexec_configured = (
        (_sl_set.enabled and _is_reexec_action(_sl_set.action))
        or (_tgt_set.enabled and _is_reexec_action(_tgt_set.action))
    )
    # When the unified live monitor already re-executed in-engine (one-pass), the
    # result carries pf_monitor_enforced=True → skip the two-pass replay entirely
    # (it would double-count on top of the live re-entries).
    if result.get("pf_monitor_enforced"):
        return result
    if _reexec_configured or os.environ.get("_USE_PF_REEXEC_REPLAY", "0") == "1":
        _cap = max(int(getattr(_sl_set, "reexecute_count", 0) or 0),
                   int(getattr(_tgt_set, "reexecute_count", 0) or 0))
        _cap = _cap if _cap > 0 else 50
        _replays = 0
        _last_clip_ns = 0
        _pass_slots = slot_results  # per-slot results of the pass that produced the clip
        # Pending "ReExecute at Entry Price" arms ({slot_id: (entry_px, was_long)})
        # carried across replays. A leg armed to wait for its entry price E may
        # still be waiting (flat) when a LATER breach fires from another leg;
        # without this it would be dropped (no open position at the new clip) and
        # revert to normal signals. Carrying it forward keeps it waiting for E.
        _pending_eps: dict = {}
        while _replays < _cap:
            _events = result.get("pf_clip_events") or []
            _first = next((e for e in _events if _is_reexec_action(e[2])), None)
            if _first is None:
                break
            _clip_ns = _ts_iso_to_ns(_first[0])
            if _clip_ns <= 0 or _clip_ns <= _last_clip_ns:
                break  # no forward progress — guard against a degenerate loop
            # ReExecute-family re-entry mode (spec §5.2 / portfolio_sl_tgt.html).
            # Both variants capture each slot's pre-clip (entry_price, side):
            #   • "at Entry Price"/"Same Contract" → resting LIMIT at the entry
            #     price ("waits to re-enter each leg at its original entry price").
            #   • plain "ReExecute" → immediate MARKET re-entry on the original
            #     side on the next bar ("immediately re-opens them"); the price
            #     is carried only as a "re-entry pending" flag.
            _reexec_eps = None
            _reexec_market = False
            if _is_reexec_action(_first[2]):
                _reexec_market = not _is_entry_price_reexec(_first[2])
                _reexec_eps = {}
                for _sid, _sr in (_pass_slots or {}).items():
                    _ep = _entry_at_clip(_sr.get("positions_report"), _clip_ns)
                    if _ep is not None:
                        # Leg held an open position at this breach → (re-)arm at it.
                        _reexec_eps[_sid] = _ep
                    elif (not _reexec_market) and _sid in _pending_eps:
                        # Entry-price re-exec only: leg was armed in a prior replay
                        # and opened NO position between the prior clip and this one
                        # → it is still waiting for E (price never returned). A
                        # breach from another leg must not wipe that — carry the
                        # original (entry_px, side) forward so it keeps waiting
                        # instead of resuming signals. (The windowed check matters:
                        # _pass_slots is the prior FULL segment, which also holds
                        # this leg's later trades — so a plain "empty" test fails.)
                        _pr = _sr.get("positions_report") if _sr else None
                        if not _opened_in_window(_pr, _last_clip_ns, _clip_ns):
                            _reexec_eps[_sid] = _pending_eps[_sid]
                _pending_eps = dict(_reexec_eps)  # remember for the next replay
                _mode = "market re-entry (next bar)" if _reexec_market else "entry-price limit"
                print(f"[PF_REEXEC] {_mode}: {len(_reexec_eps)} slot(s) re-enter")
            # Head-truncation (close-at-breach, spec "Closes all legs"): per-trader
            # equity curve from the pre-clip pass, so the splice closes each slot's
            # open-at-breach position AT clip_ns instead of at its natural exit.
            _trader_curves = {}
            for _sid, _sr in (_pass_slots or {}).items():
                if not _sr:
                    continue
                _pr = _sr.get("positions_report")
                # Prefer the per-bar mark-to-market curve so an INTRADAY portfolio
                # clip closes the open leg at its live (unrealized) P&L at the
                # breach bar; fall back to the realized curve when MtM is absent.
                _curve = _sr.get("equity_curve_mtm") or _sr.get("equity_curve_ts")
                if (_pr is not None and not getattr(_pr, "empty", True)
                        and ("trader_id" in _pr.columns or "strategy_id" in _pr.columns)
                        and _curve):
                    try:
                        _trader_curves[_leg_key_of(_pr)] = _curve
                    except Exception:  # noqa: BLE001 — best effort
                        pass
            print(f"[PF_REEXEC] replay #{_replays + 1}: re-running slots flat from {_first[0]}")
            _seg_results, _seg_errors = _run_all_slots(
                move_sl_settings, fire_callbacks=False, replay_cutoff_ns=_clip_ns,
                reexec_entry_prices=_reexec_eps, reexec_market_mode=_reexec_market,
            )
            if not _seg_results:
                break
            _pass_slots = _seg_results  # next replay captures from this segment
            _seg_merged = _merge_portfolio_results(
                portfolio, _seg_results, capitals, _seg_errors, user_id=user_id,
            )
            # Orderbook reason for the legs the portfolio SL/Target closed at the
            # breach — uses the ACTUAL configured threshold (resolved), not a
            # constant, and reflects this clip's reason + action.
            _is_tgt = "TARGET" in _first[1]
            _clip_val = (_tgt_set.value if _is_tgt else _sl_set.value)
            _clip_reason_tag = (
                f"Portfolio {'Target' if _is_tgt else 'Stoploss'}: "
                f"combined {_clip_val:g} hit -> {_first[2]}"
            )
            result = _splice_merged_results(
                result, _seg_merged, _clip_ns, portfolio.starting_capital,
                trader_curves=_trader_curves, clip_reason_tag=_clip_reason_tag,
            )
            result["pf_reexec_replays"] = _replays + 1
            _last_clip_ns = _clip_ns
            _replays += 1
        if _replays > 0:
            print(f"[PF_REEXEC] spliced {_replays} replay segment(s)")

    return result
