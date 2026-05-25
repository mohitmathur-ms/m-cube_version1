"""Portfolio SL/Target clip detection and application: the equity-curve walk,
trailing/delay logic, selective square-off, underlying-price clips, and the
aggregate-Move-SL pass-1->pass-2 coordination payload."""

from __future__ import annotations

import dataclasses
import pandas as pd
from core.managed_strategy import advance_trailing_target

from core.backtest_runner.cross_portfolio import (
    _CROSS_PORTFOLIO_ACTIONS,
    _is_reexec_action,
    publish_cross_portfolio_event,
)
from core.backtest_runner.equity_curves import _merge_equity_curves
from core.backtest_runner.exit_fill import (
    _vwap_row_is_long,
    _vwap_ts_to_ns,
)
from core.backtest_runner.portfolio_exit_config import (
    _PfStoplossSettings,
    _PfTargetSettings,
)
from core.backtest_runner.report_utils import _pick_col


def _entry_at_clip(positions_report, clip_ns: int):
    """Return ``(entry_price, was_long)`` of the position open at ``clip_ns``.

    For the portfolio "ReExecute at Entry Price" replay (spec §5.2): inspects a
    slot's positions_report for the position spanning the clip timestamp
    (``ts_opened ≤ clip_ns ≤ ts_closed``) and returns its open price + side, so
    the replayed slot can wait for price to return to that level before
    re-entering. Returns ``None`` when no position was open at the clip or the
    report lacks the needed columns — caller then replays that slot as plain
    ReExecute (no pin). Defensive: never raises.
    """
    try:
        if positions_report is None or positions_report.empty:
            return None
        open_col = _pick_col(positions_report, ["ts_opened", "ts_init"])
        close_col = _pick_col(positions_report, ["ts_closed", "ts_last"])
        px_col = _pick_col(positions_report, ["avg_px_open", "AvgPxOpen", "avg_open"])
        if not open_col or not close_col or not px_col:
            return None
        sqty_col = _pick_col(positions_report, ["signed_qty", "SignedQty"])
        entry_col = _pick_col(positions_report, ["entry", "Entry"])
        side_col = _pick_col(positions_report, ["side", "Side"])
        best = None  # (ts_opened, idx)
        for idx in positions_report.index:
            o_ns = _vwap_ts_to_ns(positions_report.at[idx, open_col])
            c_ns = _vwap_ts_to_ns(positions_report.at[idx, close_col])
            if o_ns and o_ns <= clip_ns and (c_ns == 0 or c_ns >= clip_ns):
                if best is None or o_ns > best[0]:
                    best = (o_ns, idx)
        if best is None:
            return None
        idx = best[1]
        px = float(positions_report.at[idx, px_col])
        was_long = _vwap_row_is_long(positions_report, idx, sqty_col, entry_col, side_col)
        if px <= 0 or was_long is None:
            return None
        return (px, bool(was_long))
    except Exception:
        return None


@dataclasses.dataclass(frozen=True)
class _AggCoordination:
    """Pass-1 → pass-2 hand-off for the portfolio-aggregate Move SL trigger
    (spec §2.3). ``agg_trigger_ns`` is the UTC-ns timestamp at which the
    combined portfolio P&L first crossed the configured threshold (0 = never).
    ``event_bus`` is the union of every leg's pass-1 SL/target hit timestamps,
    ``{slot_id: {"sl_ns","tgt_ns"}}`` — pre-seeded into each pass-2 worker so
    the cross-slot Hit-On-Leg trigger works across process boundaries.
    """
    agg_trigger_ns: int = 0
    agg_trigger_ts: str | None = None
    event_bus: dict = dataclasses.field(default_factory=dict)
    logs: tuple[str, ...] = ()


def _compute_agg_coordination(portfolio, pass1_results: dict, move_sl) -> _AggCoordination:
    """Build the pass-2 coordination payload from pass-1 slot results.

    Merges every slot's per-bar equity curve into one combined-P&L timeline,
    finds the first timestamp the combined P&L crosses ``agg_pnl_threshold``
    in the configured direction, and unions every leg's SL/target hit
    timestamps into a cross-process event bus. Pure function of pass-1 output
    — keeps the two-pass run deterministic.
    """
    logs: list[str] = []

    # Cross-process event bus: union of pass-1 per-leg SL/target hits.
    event_bus: dict[str, dict[str, int]] = {}
    for r in pass1_results.values():
        if not r:
            continue
        sid = r.get("slot_id")
        ev = r.get("leg_exit_events") or {}
        if not sid or not ev:
            continue
        entry = event_bus.setdefault(str(sid), {})
        for k, v in ev.items():
            try:
                iv = int(v)
            except (TypeError, ValueError):
                continue
            if iv > entry.get(k, 0):
                entry[k] = iv

    agg_trigger_ns = 0
    agg_trigger_ts: str | None = None
    if move_sl.agg_pnl_enabled and move_sl.agg_pnl_threshold > 0:
        curves = [
            r.get("equity_curve_ts", [])
            for r in pass1_results.values()
            if r and r.get("equity_curve_ts")
        ]
        merged = _merge_equity_curves(curves)
        start_cap = float(getattr(portfolio, "starting_capital", 0.0) or 0.0)
        thr = abs(float(move_sl.agg_pnl_threshold))
        is_loss = move_sl.agg_pnl_direction == "loss"
        for pt in merged:
            ts_iso = pt.get("timestamp")
            if not ts_iso:
                continue  # seed point carries no timestamp
            pnl = float(pt.get("balance", start_cap)) - start_cap
            crossed = (pnl <= -thr) if is_loss else (pnl >= thr)
            if crossed:
                agg_trigger_ns = _ts_iso_to_ns(ts_iso)
                agg_trigger_ts = ts_iso
                break
        if agg_trigger_ns:
            logs.append(
                f"aggregate {move_sl.agg_pnl_direction} trigger: combined PnL crossed "
                f"{thr:g} at {agg_trigger_ts} — open legs move SL to cost from there"
            )
        else:
            logs.append(
                f"aggregate {move_sl.agg_pnl_direction} trigger: combined PnL never "
                f"crossed {thr:g} — no aggregate Move SL applied"
            )
    if event_bus:
        logs.append(
            f"cross-slot bus: {len(event_bus)} slot(s) published SL/target hits to pass 2"
        )
    return _AggCoordination(
        agg_trigger_ns=agg_trigger_ns,
        agg_trigger_ts=agg_trigger_ts,
        event_bus=event_bus,
        logs=tuple(logs),
    )


@dataclasses.dataclass(frozen=True)
class _ClipResult:
    """Outcome of _apply_portfolio_clip — drives report-level enforcement
    of portfolio Stoploss/Target. ``clip_ts is None`` means no enforcement
    fired (or feature disabled). ``clipped_slots`` is the set of slot_ids
    whose trades after clip_ts should be dropped — for full SqOff that's
    every enabled slot, for selective SqOff it's only the matching subset.
    """
    clip_ts: str | None = None  # ISO UTC string from equity_curve_ts (FIRST clip)
    clip_reason: str | None = None  # STOPLOSS | STOPLOSS_TRAIL | TARGET | TARGET_TRAIL
    clip_action: str | None = None  # SqOff | ReExecute
    clipped_slots: tuple[str, ...] = ()
    would_reexecute: bool = False
    reexec_count: int = 0
    logs: tuple[str, ...] = ()
    # All clip events fired during this portfolio run, in chronological order.
    # For non-ReExecute actions there's at most one entry. For ReExecute,
    # honours `pf_sl_reexecute_count` (0 = unlimited; otherwise capped).
    # Each tuple: (clip_ts_iso, reason, action).
    clip_events: tuple[tuple[str, str, str], ...] = ()


def _ts_iso_to_ns(ts_iso: str | None) -> int:
    """Convert an ISO timestamp string from equity_curve_ts to UTC nanoseconds.
    Returns 0 for the seed/None entry. The merged equity curve uses ISO with
    explicit '+00:00' offset (per _build_equity_curve_from_account)."""
    if not ts_iso:
        return 0
    try:
        return pd.Timestamp(ts_iso).value
    except Exception:
        return 0


def _apply_portfolio_clip(
    equity_curve_ts: list[dict],
    starting_capital: float,
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None = None,  # slot_id -> pnl_at_clip (for selective sqoff)
    slot_curves: dict | None = None,       # slot_id -> equity_curve_ts (selective at clip ts)
) -> _ClipResult:
    """Walk the unified equity curve in time order and decide where the
    portfolio-level Stoploss/Target would have triggered. Returns a
    _ClipResult that the caller uses to drop post-clip trades from the
    merged outputs.

    Spec evaluation order (portfolio_sl_tgt.html §8) per tick:
      1. Trailing SL ratchet
      2. Fixed SL check
      3. Trailing Target activate / ratchet
      4. Fixed Target check
    Delay confirmation: when a hit fires, defer clip by N seconds; cancel
    if the condition clears before delay expires (oscillation guard, §1.6).
    Trailing-Target SqOff always uses local SqOff regardless of action (§5).
    """
    if not pf_sl.enabled and not pf_tgt.enabled:
        return _ClipResult()
    if len(equity_curve_ts) < 2:
        return _ClipResult()  # need at least one real tick after the seed

    logs: list[str] = []

    # Trailing SL state
    sl_current = pf_sl.value if pf_sl.enabled else 0.0
    sl_trail_anchor = 0.0
    # Trailing Target state
    tgt_trail_active = False
    tgt_floor = 0.0
    tgt_trail_anchor = 0.0
    # Delay state — shared between SL and TGT per spec §4.6
    pending_reason: str | None = None  # STOPLOSS | STOPLOSS_TRAIL | TARGET | TARGET_TRAIL
    pending_at_ns: int = 0
    pending_clip_ts: str | None = None

    def _condition_holds(reason: str, pnl: float) -> bool:
        """Re-check whether the same condition still holds for a pending clip."""
        if reason == "STOPLOSS":
            return pf_sl.enabled and pnl <= -sl_current
        if reason == "STOPLOSS_TRAIL":
            return pf_sl.enabled and pf_sl.trail_enabled and pnl <= -sl_current
        if reason == "TARGET":
            return pf_tgt.enabled and pnl >= pf_tgt.value
        if reason == "TARGET_TRAIL":
            return tgt_trail_active and pnl <= tgt_floor
        return False

    # Cumulative ReExecute clip events. When pf_sl_action == "ReExecute" and
    # the cap allows, each SL clip resets trailing/pending state and continues
    # walking instead of stopping. reexecute_count=0 means unlimited;
    # otherwise the (count+1)-th SL hit ends the run.
    clip_events_acc: list[tuple[str, str, str]] = []
    reexec_cap = int(getattr(pf_sl, "reexecute_count", 0) or 0)

    for pt in equity_curve_ts:
        ts = pt.get("timestamp")
        if ts is None:
            continue  # seed entry
        balance = float(pt.get("balance", starting_capital))
        pnl = balance - starting_capital
        ts_ns = _ts_iso_to_ns(ts)

        # ── Step 1: Trailing SL ratchet ──
        if pf_sl.enabled and pf_sl.trail_enabled and pf_sl.trail_every > 0:
            gain = pnl - sl_trail_anchor
            if gain >= pf_sl.trail_every:
                steps = int(gain / pf_sl.trail_every)
                old_sl = sl_current
                sl_current = max(0.0, sl_current - steps * pf_sl.trail_by)
                sl_trail_anchor += steps * pf_sl.trail_every
                if sl_current != old_sl:
                    logs.append(
                        f"TRAIL_SL_UPDATED | Steps={steps} | Combined_SL={sl_current:.2f} | PnL={pnl:.2f}"
                    )

        # ── Step 2: Fixed SL check ──
        sl_hit_now = pf_sl.enabled and pnl <= -sl_current
        # Distinguish trailed-SL from fixed-SL hit (analogous to spec)
        sl_reason = "STOPLOSS_TRAIL" if (sl_hit_now and pf_sl.trail_enabled and sl_current < pf_sl.value) else "STOPLOSS"

        # ── Step 3: Trailing Target activate / ratchet ──
        if pf_tgt.enabled and pf_tgt.trail_enabled:
            if not tgt_trail_active and pnl >= pf_tgt.trail_when_profit_reach and pf_tgt.trail_when_profit_reach > 0:
                tgt_trail_active = True
                tgt_floor = pf_tgt.trail_lock_min_profit
                tgt_trail_anchor = pf_tgt.trail_when_profit_reach
                logs.append(
                    f"TRAIL_TARGET_ACTIVATED | Lock={tgt_floor:.2f} | WhenReach={tgt_trail_anchor:.2f}"
                )
            if tgt_trail_active and pf_tgt.trail_every > 0:
                gain = pnl - tgt_trail_anchor
                if gain >= pf_tgt.trail_every:
                    steps = int(gain / pf_tgt.trail_every)
                    old_floor = tgt_floor
                    tgt_floor += steps * pf_tgt.trail_by
                    tgt_trail_anchor += steps * pf_tgt.trail_every
                    if tgt_floor != old_floor:
                        logs.append(
                            f"TRAIL_TARGET_UPDATED | current_stop={tgt_floor:.2f}"
                        )

        # Trailing-Target exit (always SqOff per spec §5)
        tgt_trail_hit_now = tgt_trail_active and pnl <= tgt_floor

        # ── Step 4: Fixed Target check ──
        tgt_hit_now = pf_tgt.enabled and pnl >= pf_tgt.value and pf_tgt.value > 0

        # ── Delay confirmation handling ──
        # Determine if any new condition fires at this tick
        new_hit_reason: str | None = None
        if sl_hit_now:
            new_hit_reason = sl_reason
        elif tgt_trail_hit_now:
            new_hit_reason = "TARGET_TRAIL"
        elif tgt_hit_now:
            new_hit_reason = "TARGET"

        def _fire_clip(clip_ts_iso: str, reason: str):
            """Handle a confirmed clip. Returns a `_ClipResult` to stop the
            walk, or None to continue (when action=ReExecute and cap allows)."""
            action_local = pf_sl.action if reason.startswith("STOPLOSS") else (
                "SqOff" if reason == "TARGET_TRAIL" else pf_tgt.action
            )
            # Spec: all ReExecute-family actions replay (subject to cap).
            if _is_reexec_action(action_local) and (reexec_cap == 0 or len(clip_events_acc) < reexec_cap):
                clip_events_acc.append((clip_ts_iso, reason, action_local))
                logs.append(
                    f"PORTFOLIO_REEXECUTE_REPLAY | Reason={reason} | "
                    f"ReExec#{len(clip_events_acc)}/{reexec_cap or 'unlimited'}"
                )
                return None  # Continue walking
            # Final clip — stop the walk.
            return _build_clip_result(
                clip_ts=clip_ts_iso, reason=reason,
                pf_sl=pf_sl, pf_tgt=pf_tgt,
                slot_pnl_at_clip=slot_pnl_at_clip, logs=logs,
                prior_events=tuple(clip_events_acc),
                slot_curves=slot_curves,
            )

        if pending_reason is not None:
            # Has the condition cleared?
            if not _condition_holds(pending_reason, pnl):
                logs.append(f"PORTFOLIO_DELAY_CLEARED | Condition persistent=False | Reason={pending_reason}")
                pending_reason = None
                pending_at_ns = 0
                pending_clip_ts = None
            else:
                # Condition still holds; check if delay elapsed
                delay_sec = pf_sl.delay_sec if pending_reason.startswith("STOPLOSS") else pf_tgt.delay_sec
                if delay_sec == 0 or (ts_ns - pending_at_ns) >= delay_sec * 1_000_000_000:
                    result = _fire_clip(pending_clip_ts, pending_reason)
                    if result is not None:
                        return result
                    # ReExecute replay: reset trail/pending state and continue.
                    sl_trail_anchor = pnl
                    tgt_trail_active = False
                    tgt_floor = 0.0
                    tgt_trail_anchor = 0.0
                    pending_reason = None
                    pending_at_ns = 0
                    pending_clip_ts = None

        if new_hit_reason is not None and pending_reason is None:
            delay_sec = pf_sl.delay_sec if new_hit_reason.startswith("STOPLOSS") else pf_tgt.delay_sec
            if delay_sec > 0:
                pending_reason = new_hit_reason
                pending_at_ns = ts_ns
                pending_clip_ts = ts
                logs.append(
                    f"PORTFOLIO_DELAY_PENDING | Reason={new_hit_reason} | Delay={delay_sec}s"
                )
            else:
                # No delay — clip immediately (or replay if ReExecute + under cap)
                result = _fire_clip(ts, new_hit_reason)
                if result is not None:
                    return result
                # ReExecute replay: reset trail state and continue.
                sl_trail_anchor = pnl
                tgt_trail_active = False
                tgt_floor = 0.0
                tgt_trail_anchor = 0.0

    # Walked the whole curve without a final (non-replayed) clip.
    # Surface any accumulated ReExecute replays for the caller.
    return _ClipResult(logs=tuple(logs), clip_events=tuple(clip_events_acc))


def _slot_pnl_at_ts(curve: list[dict] | None, clip_ns: int) -> float:
    """Per-slot PnL as of ``clip_ns`` — the slot's balance at the last equity
    point at-or-before the clip timestamp, minus its seed capital."""
    if not curve:
        return 0.0
    seed = float(curve[0].get("balance", 0.0) or 0.0)
    last = seed
    for pt in curve:
        ts = pt.get("timestamp")
        if ts is None:
            continue
        if _ts_iso_to_ns(ts) <= clip_ns:
            last = float(pt.get("balance", last) or last)
        else:
            break
    return last - seed


def _build_clip_result(
    clip_ts: str,
    reason: str,
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None,
    logs: list,
    prior_events: tuple[tuple[str, str, str], ...] = (),
    slot_curves: dict | None = None,
) -> _ClipResult:
    """Construct the ClipResult, applying selective SqOff filtering and
    classifying the action. Trailing-Target hits ignore configured action
    (always SqOff per spec §5).

    When ``slot_curves`` (slot_id → equity_curve_ts) is supplied, the
    loss/profit selective filter uses each slot's PnL **at the clip timestamp**
    rather than its end-of-run PnL — spec-accurate per §1.9-§1.10."""
    if reason.startswith("STOPLOSS"):
        action = pf_sl.action
        sqoff_loss = pf_sl.sqoff_only_loss_legs
        sqoff_profit = pf_sl.sqoff_only_profit_legs
    else:
        action = "SqOff" if reason == "TARGET_TRAIL" else pf_tgt.action
        sqoff_loss = False  # selective filters are SL-only per spec §1.9-§1.10
        sqoff_profit = False

    # Prefer per-slot PnL evaluated at the clip timestamp when slot curves are
    # available; fall back to the end-of-run proxy in slot_pnl_at_clip.
    if slot_curves and (sqoff_loss or sqoff_profit):
        _clip_ns = _ts_iso_to_ns(clip_ts)
        slot_pnl_at_clip = {
            sid: _slot_pnl_at_ts(curve, _clip_ns) for sid, curve in slot_curves.items()
        }

    # Determine which slots are clipped
    if slot_pnl_at_clip is not None and (sqoff_loss or sqoff_profit):
        if sqoff_loss:
            clipped = tuple(sid for sid, p in slot_pnl_at_clip.items() if p < 0)
            filter_label = "LOSS_LEGS_ONLY"
        else:
            clipped = tuple(sid for sid, p in slot_pnl_at_clip.items() if p > 0)
            filter_label = "PROFIT_LEGS_ONLY"
        logs = list(logs) + [f"PORTFOLIO_PARTIAL_SQOFF | Reason={reason} | Filter={filter_label}"]
    else:
        # Full SqOff — every slot in slot_pnl_at_clip (or empty if not provided)
        clipped = tuple(slot_pnl_at_clip.keys()) if slot_pnl_at_clip else ()
        logs = list(logs) + [f"PORTFOLIO_SQOFF | Reason={reason}"]

    would_reexec = _is_reexec_action(action)
    final_events = prior_events + ((clip_ts, reason, action),)
    total_reexec = len(prior_events) + (1 if would_reexec else 0)
    if would_reexec:
        logs.append(
            f"PORTFOLIO_REEXECUTE_FINAL | Reason={reason} | TotalReExec={total_reexec} (cap reached → stop)"
        )

    # Cross-portfolio dispatch (spec §2.1(h)/(i)/(j) and §2.4 mirror). The
    # event is queued for the target portfolio; the runner consumes it at
    # its next start. Action name is normalised to a single-word verb the
    # consumer recognises.
    if action in _CROSS_PORTFOLIO_ACTIONS:
        target_pf = pf_sl.target_portfolio if reason.startswith("STOPLOSS") else pf_tgt.target_portfolio
        verb_map = {
            "SqOff Other Portfolio": "sqoff",
            "Execute Other Portfolio": "execute",
            "Start Other Portfolio": "start",
        }
        verb = verb_map.get(action, "sqoff")
        if target_pf:
            publish_cross_portfolio_event(target_pf, verb, "", clip_ts)
            logs.append(
                f"CROSS_PORTFOLIO_DISPATCH | Action={action!r} | Target={target_pf!r} | Verb={verb}"
            )

    return _ClipResult(
        clip_ts=clip_ts, clip_reason=reason, clip_action=action,
        clipped_slots=clipped,
        would_reexecute=would_reexec, reexec_count=total_reexec,
        logs=tuple(logs),
        clip_events=final_events,
    )


def _build_underlying_curve(engine, bar_type) -> list[dict]:
    """Extract the (timestamp, close) price series the engine processed.

    Used by the portfolio-level "Underlying Movement" / "Loss and Underlying
    Range" SL types (spec §2.1). The underlying defaults to the slot's own
    instrument (D5 "underlying = self"). Returns ``[]`` when bars can't be
    read — the caller degrades to "underlying SL not enforced" with a warning.
    """
    try:
        bars = engine.cache.bars(bar_type)
    except Exception:
        return []
    if not bars:
        return []
    out: list[dict] = []
    for b in bars:
        try:
            out.append({
                "timestamp": pd.Timestamp(b.ts_event, unit="ns", tz="UTC").isoformat(),
                "close": float(b.close),
            })
        except Exception:
            continue
    # cache.bars() returns most-recent-first; sort ascending by ts.
    out.sort(key=lambda p: p["timestamp"])
    return out


def _underlying_sl_clip(
    underlying_curve: list[dict] | None,
    equity_curve_ts: list[dict],
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None,
    starting_capital: float,
    slot_curves: dict | None = None,
) -> _ClipResult:
    """Portfolio SL for the underlying-price-based types (spec §2.1).

    • "Underlying Movement"      → fire when the primary instrument price
      crosses ``pf_sl.value``.
    • "Loss and Underlying Range" → fire when combined PnL ≤ −value AND the
      underlying price is outside [underlying_below, underlying_above].

    ``Delay (sec)`` shifts the clip timestamp forward by that many seconds
    (a confirmed-after-N-seconds approximation). Returns a ``_ClipResult``;
    ``clip_ts is None`` means the SL never fired.
    """
    logs: list[str] = []
    if not underlying_curve:
        logs.append(
            "UNDERLYING_SL_SKIPPED | no underlying price series available "
            "(grouped / Path-B run, or missing data) — underlying SL not enforced"
        )
        return _ClipResult(logs=tuple(logs))

    # Underlying price series, ascending by ts.
    u: list[tuple[int, float]] = []
    for pt in underlying_curve:
        ts_ns = _ts_iso_to_ns(pt.get("timestamp"))
        if ts_ns:
            u.append((ts_ns, float(pt.get("close", 0.0) or 0.0)))
    u.sort()
    if not u:
        return _ClipResult(logs=tuple(logs))

    # Combined-PnL step series from the merged equity curve.
    eq: list[tuple[int, float]] = []
    for pt in equity_curve_ts:
        ts = pt.get("timestamp")
        if ts is None:
            continue
        eq.append((_ts_iso_to_ns(ts), float(pt.get("balance", starting_capital)) - starting_capital))
    eq.sort()

    is_movement = pf_sl.sl_type == "Underlying Movement"
    level = pf_sl.value
    below = pf_sl.underlying_below
    above = pf_sl.underlying_above
    delay_ns = int(pf_sl.delay_sec) * 1_000_000_000

    prev_close: float | None = None
    eq_idx = 0
    cur_pnl = 0.0
    for ts_ns, close in u:
        while eq_idx < len(eq) and eq[eq_idx][0] <= ts_ns:
            cur_pnl = eq[eq_idx][1]
            eq_idx += 1

        hit = False
        if is_movement:
            if prev_close is not None and (
                (prev_close <= level <= close) or (prev_close >= level >= close)
            ):
                hit = True
        else:  # Loss and Underlying Range
            range_breached = (below > 0 and close <= below) or (above > 0 and close >= above)
            if cur_pnl <= -level and range_breached:
                hit = True
        prev_close = close

        if hit:
            clip_ns = ts_ns + delay_ns
            clip_iso = pd.Timestamp(clip_ns, unit="ns", tz="UTC").isoformat()
            logs.append(
                f"UNDERLYING_SL_HIT | type={pf_sl.sl_type!r} | underlying={close:.5f} "
                f"| pnl={cur_pnl:.2f} | clip_ts={clip_iso}"
            )
            return _build_clip_result(
                clip_ts=clip_iso, reason="STOPLOSS",
                pf_sl=pf_sl, pf_tgt=pf_tgt,
                slot_pnl_at_clip=slot_pnl_at_clip, logs=logs,
                slot_curves=slot_curves,
            )

    return _ClipResult(logs=tuple(logs))


def _user_sl_clip(
    equity_curve_ts: list[dict],
    starting_capital: float,
    cum_user_pnl: float,
    eff_max_loss: float | None,
    user_trail_sl: dict | None,
    all_slot_ids: list[str],
    scope_label: str = "USER",
) -> _ClipResult:
    """User-level (or tag-level) SL clip (spec §3 Level 3 / §6 / §11).

    Walks the merged equity curve in combined-PnL terms (this portfolio's PnL
    plus ``cum_user_pnl`` — the cumulative PnL the scope has accrued from
    earlier portfolios). The Max-Loss cap — optionally ratcheted tighter each
    bar by the Trailing SL — is a real force-sqoff: at the first breaching bar
    the whole portfolio is clipped (every slot), and post-clip trades are
    dropped by the caller.

    ``scope_label`` ("USER" or "TAG") only flavours the clip reason / log
    strings so the two tiers are distinguishable downstream; the arithmetic is
    identical. The tag tier reuses this exact walk one level down (spec §11).
    """
    if eff_max_loss is None:
        return _ClipResult()
    u_sl = abs(float(eff_max_loss))
    anchor = 0.0
    logs: list[str] = []
    for pt in equity_curve_ts:
        ts = pt.get("timestamp")
        if ts is None:
            continue
        combined = float(pt.get("balance", starting_capital)) - starting_capital + cum_user_pnl
        if user_trail_sl:
            gain = combined - anchor
            if gain >= user_trail_sl["every"]:
                steps = int(gain / user_trail_sl["every"])
                u_sl = max(0.0, u_sl - steps * user_trail_sl["by"])
                anchor += steps * user_trail_sl["every"]
        if combined <= -u_sl:
            ratcheted = bool(user_trail_sl) and u_sl < abs(float(eff_max_loss))
            reason = f"{scope_label}_TRAIL_STOPLOSS" if ratcheted else f"{scope_label}_STOPLOSS"
            logs.append(
                f"{scope_label}_SL_HIT | reason={reason} | combined_pnl={combined:.2f} "
                f"| effective_sl={u_sl:.2f} | clip_ts={ts}"
            )
            return _ClipResult(
                clip_ts=ts, clip_reason=reason, clip_action="SqOff",
                clipped_slots=tuple(all_slot_ids), logs=tuple(logs),
            )
    return _ClipResult()


def _underlying_tgt_clip(
    underlying_curve: list[dict] | None,
    equity_curve_ts: list[dict],
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None,
    starting_capital: float,
    slot_curves: dict | None = None,
) -> _ClipResult:
    """Portfolio Target for the "Underlying Movement" type (spec §5.1).

    Mirror of the ``is_movement`` branch of ``_underlying_sl_clip`` on the
    profit side: fires the first time the primary instrument's price crosses
    ``pf_tgt.value``. ``Delay (sec)`` shifts the clip timestamp forward.
    Returns a ``_ClipResult``; ``clip_ts is None`` means the Target never fired.
    """
    logs: list[str] = []
    if not underlying_curve:
        logs.append(
            "UNDERLYING_TGT_SKIPPED | no underlying price series available "
            "(grouped / Path-B run, or missing data) — underlying Target not enforced"
        )
        return _ClipResult(logs=tuple(logs))

    u: list[tuple[int, float]] = []
    for pt in underlying_curve:
        ts_ns = _ts_iso_to_ns(pt.get("timestamp"))
        if ts_ns:
            u.append((ts_ns, float(pt.get("close", 0.0) or 0.0)))
    u.sort()
    if not u:
        return _ClipResult(logs=tuple(logs))

    level = pf_tgt.value
    delay_ns = int(pf_tgt.delay_sec) * 1_000_000_000
    prev_close: float | None = None
    for ts_ns, close in u:
        hit = (
            prev_close is not None
            and ((prev_close <= level <= close) or (prev_close >= level >= close))
        )
        prev_close = close
        if hit:
            clip_ns = ts_ns + delay_ns
            clip_iso = pd.Timestamp(clip_ns, unit="ns", tz="UTC").isoformat()
            logs.append(
                f"UNDERLYING_TGT_HIT | underlying={close:.5f} | level={level:.5f} "
                f"| clip_ts={clip_iso}"
            )
            return _build_clip_result(
                clip_ts=clip_iso, reason="TARGET",
                pf_sl=pf_sl, pf_tgt=pf_tgt,
                slot_pnl_at_clip=slot_pnl_at_clip, logs=logs,
                slot_curves=slot_curves,
            )

    return _ClipResult(logs=tuple(logs))


def _user_tgt_clip(
    equity_curve_ts: list[dict],
    starting_capital: float,
    cum_user_pnl: float,
    eff_max_profit: float | None,
    user_trail_tgt: dict | None,
    all_slot_ids: list[str],
    scope_label: str = "USER",
) -> _ClipResult:
    """User-level (or tag-level) Target clip (spec §3 / target §6 / §11).

    Mirror of ``_user_sl_clip`` on the profit side. Walks the merged equity
    curve in combined-PnL terms (this portfolio's PnL plus the user's
    cumulative PnL from earlier portfolios). Two ceilings, checked per bar:

      * **Max Profit** — fixed cap; first bar combined PnL ≥ cap force-sqoffs
        every slot.
      * **Trailing Target / Profit-Lock** — once combined PnL reaches the
        activation threshold a floor is locked and ratcheted up; a fall back
        to the floor force-sqoffs every slot.

    Fixed Max Profit is checked before the trailing lock within a bar so the
    hard ceiling always wins a tie. Returns a ``_ClipResult``; ``clip_ts is
    None`` means neither ceiling fired.
    """
    if eff_max_profit is None and not user_trail_tgt:
        return _ClipResult()
    cap = abs(float(eff_max_profit)) if eff_max_profit is not None else None
    logs: list[str] = []
    tt_active = False
    tt_stop = 0.0
    tt_anchor = 0.0
    for pt in equity_curve_ts:
        ts = pt.get("timestamp")
        if ts is None:
            continue
        combined = float(pt.get("balance", starting_capital)) - starting_capital + cum_user_pnl

        # Fixed Max Profit ceiling.
        if cap is not None and cap > 0 and combined >= cap:
            logs.append(
                f"{scope_label}_TARGET_HIT | reason={scope_label}_TARGET | combined_pnl={combined:.2f} "
                f"| max_profit={cap:.2f} | clip_ts={ts}"
            )
            return _ClipResult(
                clip_ts=ts, clip_reason=f"{scope_label}_TARGET", clip_action="SqOff",
                clipped_slots=tuple(all_slot_ids), logs=tuple(logs),
            )

        # Trailing Target / Profit-Lock — reuses the leg-level pure ratchet.
        if user_trail_tgt:
            tt_active, tt_stop, tt_anchor, tt_hit = advance_trailing_target(
                tt_active, tt_stop, tt_anchor, combined,
                user_trail_tgt["when_reach"], user_trail_tgt["lock"],
                user_trail_tgt["every"], user_trail_tgt["by"],
            )
            if tt_hit:
                logs.append(
                    f"{scope_label}_TARGET_HIT | reason={scope_label}_TRAIL_TARGET | combined_pnl={combined:.2f} "
                    f"| locked_floor={tt_stop:.2f} | clip_ts={ts}"
                )
                return _ClipResult(
                    clip_ts=ts, clip_reason=f"{scope_label}_TRAIL_TARGET", clip_action="SqOff",
                    clipped_slots=tuple(all_slot_ids), logs=tuple(logs),
                )
    return _ClipResult()


def _earliest_clip(*results: _ClipResult) -> _ClipResult:
    """Return the _ClipResult with the earliest non-None clip_ts.

    Logs from every result are merged onto the winner so nothing is lost.
    When no result fired, returns the first with merged logs.
    """
    merged_logs: tuple[str, ...] = ()
    for r in results:
        merged_logs += r.logs
    fired = [r for r in results if r.clip_ts is not None]
    if not fired:
        return _ClipResult(logs=merged_logs)
    winner = min(fired, key=lambda r: _ts_iso_to_ns(r.clip_ts))
    return dataclasses.replace(winner, logs=merged_logs)
