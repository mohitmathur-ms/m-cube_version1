"""Portfolio-level aggregation: per-strategy breakdown, trade-PnL extraction,
two-pass result splicing, the _merge_portfolio_results orchestrator, and the
portfolio result-dict builder."""

from __future__ import annotations

import dataclasses
import pandas as pd
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.identifiers import Venue
from core.models import PortfolioConfig

from core.backtest_runner.equity_curves import (
    _build_equity_curve_from_account,
    _merge_equity_curves,
)
from core.backtest_runner.portfolio_clip import (
    _ClipResult,
    _apply_portfolio_clip,
    _earliest_clip,
    _ts_iso_to_ns,
    _underlying_sl_clip,
    _underlying_tgt_clip,
    _user_sl_clip,
    _user_tgt_clip,
)
from core.backtest_runner.portfolio_exit_config import (
    _UNDERLYING_PF_SL_TYPES,
    _UNDERLYING_PF_TGT_TYPES,
    _resolve_pf_stoploss,
    _resolve_pf_target,
)


def _positions_pnl_series(df) -> "pd.Series":
    """Numeric realized-PnL series for a positions report.

    Prefers the FX-converted base-currency column added by
    ``positions_report_with_base`` (``realized_pnl_<CCY>``); falls back to
    parsing the Nautilus Money-string ``realized_pnl`` ("X.XX CCY").
    Returns an empty float Series when the report has no usable column.
    """
    if df is None or df.empty:
        return pd.Series([], dtype=float)
    base_col = next(
        (c for c in df.columns if c.startswith("realized_pnl_") and c != "realized_pnl_"),
        None,
    )
    if base_col is not None:
        return pd.to_numeric(df[base_col], errors="coerce").fillna(0.0)
    if "realized_pnl" in df.columns:
        return df["realized_pnl"].map(
            lambda x: float(str(x).split(" ")[0]) if x and " " in str(x) else 0.0
        )
    return pd.Series([0.0] * len(df), dtype=float)


def _per_strategy_breakdown(positions, slot_to_strategy_id: dict | None) -> dict:
    """Per-slot ``{pnl, trades, wins, losses, trade_pnls}`` from a positions
    report, keyed by slot_id.

    Used by the ReExecute splice to recover each slot's *pre-clip* stats from
    the truncated head positions report — ``positions`` rows carry the run's
    ``strategy_id``, which ``slot_to_strategy_id`` maps back to a slot_id.
    """
    out: dict = {}
    if positions is None or positions.empty or "strategy_id" not in positions.columns:
        return out
    inv = {str(sid): slot for slot, sid in (slot_to_strategy_id or {}).items()}
    pnl = _positions_pnl_series(positions).reset_index(drop=True)
    sids = positions["strategy_id"].astype(str).reset_index(drop=True)
    for i in range(len(sids)):
        slot_id = inv.get(sids.iloc[i])
        if slot_id is None:
            continue
        p = float(pnl.iloc[i])
        d = out.setdefault(slot_id, {"pnl": 0.0, "trades": 0, "wins": 0,
                                     "losses": 0, "trade_pnls": []})
        d["pnl"] += p
        d["trades"] += 1
        d["trade_pnls"].append(p)
        if p > 0:
            d["wins"] += 1
        elif p < 0:
            d["losses"] += 1
    return out


def _splice_merged_results(head: dict, tail: dict, clip_ns: int,
                           starting_capital: float) -> dict:
    """Splice a pass-1 merged result (``head``) with a ReExecute replay
    segment (``tail``) at ``clip_ns``.

    The head contributes every fill/position strictly before the clip; the
    tail — already bar-cutoff-filtered to start flat at the clip — contributes
    the whole post-clip regime. Top-level portfolio metrics (PnL, trades,
    win/loss, equity curve, drawdown) AND the per-slot ``per_strategy`` block
    are re-derived from the spliced reports — the head's pre-clip per-slot
    stats (mapped via its ``slot_to_strategy_id``) plus the tail segment's.
    """
    def _before(df, col):
        if df is None or getattr(df, "empty", True) or col not in df.columns:
            return df.iloc[0:0] if df is not None else pd.DataFrame()
        ts_int = pd.to_datetime(df[col], errors="coerce", utc=True).astype("int64")
        return df.loc[ts_int < clip_ns]

    hf = _before(head.get("fills_report"), "ts_init")
    hp = _before(head.get("positions_report"), "ts_opened")
    tf = tail.get("fills_report")
    tp = tail.get("positions_report")
    fills = pd.concat([d for d in (hf, tf) if d is not None and not d.empty],
                      ignore_index=True) if (hf is not None or tf is not None) else pd.DataFrame()
    positions = pd.concat([d for d in (hp, tp) if d is not None and not d.empty],
                          ignore_index=True) if (hp is not None or tp is not None) else pd.DataFrame()

    pnl = _positions_pnl_series(positions)
    total_pnl = float(pnl.sum())
    total_trades = int(len(positions))
    wins = int((pnl > 0).sum())
    losses = int((pnl < 0).sum())
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
    final_balance = starting_capital + total_pnl
    total_return_pct = (total_pnl / starting_capital * 100) if starting_capital > 0 else 0.0

    # Equity-curve splice: head points before the clip, then the tail curve
    # shifted to continue from the head's balance at the clip.
    head_eq = head.get("equity_curve_ts") or []
    tail_eq = tail.get("equity_curve_ts") or []
    kept_head = [p for p in head_eq
                 if p.get("timestamp") is None or _ts_iso_to_ns(p["timestamp"]) < clip_ns]
    head_bal_at_clip = next(
        (p["balance"] for p in reversed(kept_head) if p.get("timestamp") is not None),
        starting_capital,
    )
    shift = head_bal_at_clip - starting_capital
    spliced_eq = list(kept_head) + [
        {"timestamp": p["timestamp"], "balance": float(p.get("balance", starting_capital)) + shift}
        for p in tail_eq if p.get("timestamp") is not None
    ]
    balances = [float(p.get("balance", starting_capital)) for p in spliced_eq]
    # Running-peak drawdown over the spliced balance series.
    max_dd = 0.0
    peak = balances[0] if balances else starting_capital
    for b in balances:
        peak = max(peak, b)
        if peak > 0:
            max_dd = max(max_dd, (peak - b) / peak * 100)

    # Per-slot breakdown: head's pre-clip stats (recovered from the truncated
    # head positions via its slot_to_strategy_id) + the tail segment's stats.
    head_pre = _per_strategy_breakdown(hp, head.get("slot_to_strategy_id"))
    head_per = head.get("per_strategy") or {}
    tail_per = tail.get("per_strategy") or {}
    combined_per: dict = {}
    for slot_id in set(head_pre) | set(tail_per):
        h = head_pre.get(slot_id)
        t = tail_per.get(slot_id) or {}
        meta = tail_per.get(slot_id) or head_per.get(slot_id) or {}
        s_pnl = (h["pnl"] if h else 0.0) + float(t.get("pnl", 0.0))
        s_trades = (h["trades"] if h else 0) + int(t.get("trades", 0))
        s_wins = (h["wins"] if h else 0) + int(t.get("wins", 0))
        s_losses = (h["losses"] if h else 0) + int(t.get("losses", 0))
        s_tpnls = (h["trade_pnls"] if h else []) + list(t.get("trade_pnls", []))
        combined_per[slot_id] = {
            "display_name": meta.get("display_name", ""),
            "strategy_name": meta.get("strategy_name", ""),
            "bar_type": meta.get("bar_type", ""),
            "pnl": s_pnl,
            "trades": s_trades,
            "wins": s_wins,
            "losses": s_losses,
            "win_rate": (s_wins / s_trades * 100) if s_trades > 0 else 0.0,
            "trade_pnls": s_tpnls,
        }

    out = dict(tail)
    out.update(
        fills_report=fills,
        positions_report=positions,
        total_pnl=total_pnl,
        final_balance=final_balance,
        total_return_pct=total_return_pct,
        total_trades=total_trades,
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        equity_curve_ts=spliced_eq,
        equity_curve=balances,
        max_drawdown=max_dd,
        per_strategy=combined_per,
    )
    return out


def _merge_portfolio_results(
    portfolio: PortfolioConfig,
    slot_results: dict,
    capitals: dict,
    errors: list,
    user_id: str | None = None,
) -> dict:
    """Merge individual slot results into portfolio-level metrics."""
    total_pnl = 0.0
    total_trades = 0
    total_wins = 0
    total_losses = 0
    total_flat = 0
    all_positions_reports = []
    all_fills_reports = []

    per_strategy = {}

    for slot in portfolio.enabled_slots:
        r = slot_results.get(slot.slot_id)
        if not r:
            continue

        slot_pnl = r["total_pnl"]
        total_pnl += slot_pnl
        total_trades += r["total_trades"]
        total_wins += r["wins"]
        total_losses += r["losses"]
        total_flat += r.get("flat_trades", max(r["total_trades"] - r["wins"] - r["losses"], 0))

        # Collect reports for merging
        if r.get("positions_report") is not None and not r["positions_report"].empty:
            all_positions_reports.append(r["positions_report"])
        if r.get("fills_report") is not None and not r["fills_report"].empty:
            all_fills_reports.append(r["fills_report"])

        # Extract trade PnLs from positions_report. Prefer the base-currency
        # column added by positions_report_with_base — falling back to the
        # native column means JPY pnl would be summed alongside USD pnl,
        # which is exactly the bug we fixed upstream.
        trade_pnls: list[float] = []
        pos_report = r.get("positions_report")
        if pos_report is not None and not pos_report.empty:
            base_col = next(
                (c for c in pos_report.columns if c.startswith("realized_pnl_")
                 and c not in ("realized_pnl_",)),
                None,
            )
            pnl_col = base_col or next(
                (c for c in ["realized_pnl", "RealizedPnl", "pnl"]
                 if c in pos_report.columns),
                None,
            )
            if pnl_col:
                trade_pnls = _extract_trade_pnls(pos_report, pnl_col)

        per_strategy[slot.slot_id] = {
            "display_name": r.get("display_name", slot.display_name),
            "strategy_name": r.get("strategy_name", slot.strategy_name),
            "bar_type": r.get("bar_type", slot.bar_type_str),
            "pnl": slot_pnl,
            "trades": r["total_trades"],
            "wins": r["wins"],
            "losses": r["losses"],
            "flat_trades": r.get("flat_trades", max(r["total_trades"] - r["wins"] - r["losses"], 0)),
            "win_rate": r["win_rate"],
            "decisive_win_rate": r.get("decisive_win_rate"),
            "total_days": r.get("total_days", 0),
            "winning_days": r.get("winning_days", 0),
            "losing_days": r.get("losing_days", 0),
            "win_pct_days": r.get("win_pct_days", 0.0),
            "loss_pct_days": r.get("loss_pct_days", 0.0),
            "trade_pnls": trade_pnls,
            "allocated_capital": capitals.get(slot.slot_id, 0),
            "elapsed_seconds": r.get("elapsed_seconds"),
            "worker_pid": r.get("worker_pid"),
            "cache_hits": r.get("cache_hits"),
            "cache_misses": r.get("cache_misses"),
            "cache_currsize": r.get("cache_currsize"),
            "worker_rss_mb": r.get("worker_rss_mb"),
            "warning": r.get("warning"),
            # True when this slot ran via BacktestNode (Path B). Falsy means
            # the slot stayed on Path A — either because _USE_BACKTEST_NODE
            # wasn't set, or the gate auto-fell-back due to filter config.
            "path_b": bool(r.get("path_b")),
            # VWAP proxy-fill provenance (spec §4.2) — count of SL/Target exit
            # fills repriced for this slot; 0 / False when _USE_VWAP_FILL off.
            "vwap_fill_applied": bool(r.get("vwap_fill_applied")),
            "vwap_fill_adjustments": int(r.get("vwap_fill_adjustments", 0) or 0),
            # Directional-close fill provenance (spec §8.1) — count of exit
            # fills repriced to the directional bid/ask close for this slot.
            "directional_fill_applied": bool(r.get("directional_fill_applied")),
            "directional_fill_adjustments": int(r.get("directional_fill_adjustments", 0) or 0),
        }

    # Merge equity curves — sum balances at each timestamp
    all_curves = []
    for r in slot_results.values():
        curve = r.get("equity_curve_ts", [])
        if curve:
            all_curves.append(curve)

    equity_curve_ts = _merge_equity_curves(all_curves)
    equity = [pt["balance"] for pt in equity_curve_ts] if equity_curve_ts else [portfolio.starting_capital]

    # Max drawdown from merged equity
    peak = equity[0] if equity else portfolio.starting_capital
    max_dd = 0.0
    for val in equity:
        if val > peak:
            peak = val
        dd = ((val - peak) / peak) * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    final_balance = portfolio.starting_capital + total_pnl
    total_return_pct = (total_pnl / portfolio.starting_capital) * 100 if portfolio.starting_capital > 0 else 0
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    # Decisive win rate excludes flat trades (P&L rounded to zero) — see
    # _extract_results for rationale. ``None`` when no decisive trades exist
    # so the UI can distinguish "0% decisive" from "no signal yet".
    _decisive_n = total_wins + total_losses
    decisive_win_rate = (total_wins / _decisive_n * 100) if _decisive_n > 0 else None

    # Portfolio-level day-based win% — merge daily PnLs across all slots
    portfolio_daily_pnl: dict[str, float] = {}
    for slot in portfolio.enabled_slots:
        r = slot_results.get(slot.slot_id)
        if not r:
            continue
        for day_key, pv in r.get("daily_pnl", {}).items():
            portfolio_daily_pnl[day_key] = portfolio_daily_pnl.get(day_key, 0.0) + pv
    portfolio_total_days = len(portfolio_daily_pnl)
    portfolio_winning_days = sum(1 for v in portfolio_daily_pnl.values() if v > 0)
    portfolio_losing_days = sum(1 for v in portfolio_daily_pnl.values() if v < 0)
    portfolio_win_pct_days = (portfolio_winning_days / portfolio_total_days * 100) if portfolio_total_days > 0 else 0.0
    portfolio_loss_pct_days = (portfolio_losing_days / portfolio_total_days * 100) if portfolio_total_days > 0 else 0.0

    # Merge DataFrames
    merged_positions = pd.concat(all_positions_reports, ignore_index=True) if all_positions_reports else pd.DataFrame()
    merged_fills = pd.concat(all_fills_reports, ignore_index=True) if all_fills_reports else pd.DataFrame()

    # Global user-level caps (spec §3 Level 3). Prefer user-scoped limits from
    # config/users.json over the legacy portfolio-level fallback. PnL is the
    # sum of this portfolio's PnL plus any cumulative PnL the user has
    # accrued from prior portfolios in the same orchestrator run.
    from core.users import (
        get_user_max_loss, get_user_max_profit,
        get_user_cumulative_pnl, add_user_pnl, get_user_trailing_sl,
        get_user_trailing_target,
    )
    user_max_loss = get_user_max_loss(user_id) if user_id else None
    user_max_profit = get_user_max_profit(user_id) if user_id else None
    cum_user_pnl = get_user_cumulative_pnl(user_id) if user_id else 0.0
    combined_pnl = total_pnl + cum_user_pnl
    eff_max_loss = user_max_loss if user_max_loss is not None else portfolio.max_loss
    eff_max_profit = user_max_profit if user_max_profit is not None else portfolio.max_profit
    max_loss_hit = eff_max_loss is not None and combined_pnl <= -abs(eff_max_loss)
    max_profit_hit = eff_max_profit is not None and combined_pnl >= eff_max_profit

    # User-level SL (spec §3 Level 3 / execution_logic.html §6) — Max Loss,
    # optionally ratcheted tighter by the user Trailing SL. Resolved into a
    # real equity-curve clip below (_user_sl_clip); the result drives an
    # actual force-sqoff, not just a flag.
    user_trail_sl = get_user_trailing_sl(user_id) if user_id else None
    # User-level Target — Max Profit ceiling, optionally with a Trailing
    # Target / Profit-Lock (spec §6 target doc). Resolved into a real
    # equity-curve clip below (_user_tgt_clip), same as the SL side.
    user_trail_tgt = get_user_trailing_target(user_id) if user_id else None

    # Tag-level caps (spec §11) — the tier BETWEEN portfolio and user. A
    # portfolio carrying ``portfolio_tag`` shares a Max Loss / Max Profit /
    # trailing cap (defined in config/tags.json) with every other portfolio in
    # the same tag; the cap is evaluated on the tag's cumulative combined PnL,
    # exactly like the user tier one level up.
    from core.tags import (
        get_tag_max_loss, get_tag_max_profit, get_tag_trailing_sl,
        get_tag_trailing_target, get_tag_cumulative_pnl, add_tag_pnl,
    )
    pf_tag = getattr(portfolio, "portfolio_tag", None) or None
    tag_max_loss = get_tag_max_loss(pf_tag) if pf_tag else None
    tag_max_profit = get_tag_max_profit(pf_tag) if pf_tag else None
    tag_trail_sl = get_tag_trailing_sl(pf_tag) if pf_tag else None
    tag_trail_tgt = get_tag_trailing_target(pf_tag) if pf_tag else None
    cum_tag_pnl = get_tag_cumulative_pnl(pf_tag) if pf_tag else 0.0

    # Roll this portfolio's PnL into the user AND tag aggregators so subsequent
    # portfolios (or repeat runs in the same orchestrator session) see it.
    if user_id:
        add_user_pnl(user_id, total_pnl)
    if pf_tag:
        add_tag_pnl(pf_tag, total_pnl)

    # Portfolio-level Stoploss / Target post-hoc clip. Spec:
    # 5. Logics/portfolio_sl_tgt.html. Walks the merged equity curve, finds
    # the trigger point, drops post-clip trades from the merged outputs.
    pf_sl_settings, pf_sl_warnings = _resolve_pf_stoploss(portfolio)
    pf_tgt_settings, pf_tgt_warnings = _resolve_pf_target(portfolio)
    for w in pf_sl_warnings:
        print(f"[PF_SL] {w}")
    for w in pf_tgt_warnings:
        print(f"[PF_TGT] {w}")

    # User-level SL clip (spec §3 / §6) — evaluated regardless of portfolio SL.
    _user_clip_slot_ids = [
        s.slot_id for s in portfolio.enabled_slots if slot_results.get(s.slot_id)
    ]
    user_clip = _user_sl_clip(
        equity_curve_ts, portfolio.starting_capital, cum_user_pnl,
        eff_max_loss, user_trail_sl, _user_clip_slot_ids,
    )
    user_trail_sl_hit = user_clip.clip_reason == "USER_TRAIL_STOPLOSS"
    user_trail_sl_effective = None  # surfaced via the clip log when it fires
    # User-level Target clip — Max Profit ceiling + Trailing Target.
    user_tgt_clip = _user_tgt_clip(
        equity_curve_ts, portfolio.starting_capital, cum_user_pnl,
        eff_max_profit, user_trail_tgt, _user_clip_slot_ids,
    )
    user_trail_tgt_hit = user_tgt_clip.clip_reason == "USER_TRAIL_TARGET"

    # Tag-level SL + Target clips (spec §11) — same machinery one tier down,
    # evaluated on the tag's cumulative combined PnL. No-op when the portfolio
    # has no tag or the tag defines no caps.
    tag_clip = _user_sl_clip(
        equity_curve_ts, portfolio.starting_capital, cum_tag_pnl,
        tag_max_loss, tag_trail_sl, _user_clip_slot_ids, scope_label="TAG",
    ) if pf_tag else _ClipResult()
    tag_tgt_clip = _user_tgt_clip(
        equity_curve_ts, portfolio.starting_capital, cum_tag_pnl,
        tag_max_profit, tag_trail_tgt, _user_clip_slot_ids, scope_label="TAG",
    ) if pf_tag else _ClipResult()

    clip_result = _ClipResult()
    if (pf_sl_settings.enabled or pf_tgt_settings.enabled
            or user_clip.clip_ts is not None or user_tgt_clip.clip_ts is not None
            or tag_clip.clip_ts is not None or tag_tgt_clip.clip_ts is not None):
        # Compute per-slot final P&L for selective sqoff (used at clip-point
        # to decide which slots to clip — uses end-of-run P&L as a proxy for
        # P&L at clip_ts, which is close enough for v1 since selective sqoff
        # at the clip moment matters only for which slots survive past it).
        slot_pnl_at_clip = {
            slot.slot_id: float(slot_results[slot.slot_id]["total_pnl"])
            for slot in portfolio.enabled_slots
            if slot_results.get(slot.slot_id) is not None
        }
        # Per-slot equity curves let the selective SqOff filter use each slot's
        # PnL *at the clip timestamp* rather than the end-of-run proxy.
        slot_curves = {
            slot.slot_id: slot_results[slot.slot_id].get("equity_curve_ts", [])
            for slot in portfolio.enabled_slots
            if slot_results.get(slot.slot_id) is not None
        }
        # Underlying-based SL/Target types (spec §2.1 / §5.1) are evaluated
        # against the primary slot's price series, separately from the PnL
        # clip. For each side that is underlying-based we disable its branch
        # in _apply_portfolio_clip and run the dedicated underlying clip, then
        # keep whichever clip fires first.
        sl_is_underlying = (
            pf_sl_settings.enabled
            and pf_sl_settings.sl_type in _UNDERLYING_PF_SL_TYPES
        )
        tgt_is_underlying = (
            pf_tgt_settings.enabled
            and pf_tgt_settings.tgt_type in _UNDERLYING_PF_TGT_TYPES
        )
        if sl_is_underlying or tgt_is_underlying:
            underlying_curve = next(
                (slot_results[s.slot_id].get("underlying_curve")
                 for s in portfolio.enabled_slots
                 if slot_results.get(s.slot_id)
                 and slot_results[s.slot_id].get("underlying_curve")),
                None,
            )
            # The PnL clip handles whichever side is NOT underlying-based.
            pf_sl_for_clip = (dataclasses.replace(pf_sl_settings, enabled=False)
                              if sl_is_underlying else pf_sl_settings)
            pf_tgt_for_clip = (dataclasses.replace(pf_tgt_settings, enabled=False)
                               if tgt_is_underlying else pf_tgt_settings)
            candidates = [_apply_portfolio_clip(
                equity_curve_ts, portfolio.starting_capital,
                pf_sl_for_clip, pf_tgt_for_clip, slot_pnl_at_clip,
                slot_curves=slot_curves,
            )]
            if sl_is_underlying:
                candidates.append(_underlying_sl_clip(
                    underlying_curve, equity_curve_ts, pf_sl_settings,
                    pf_tgt_settings, slot_pnl_at_clip, portfolio.starting_capital,
                    slot_curves=slot_curves,
                ))
            if tgt_is_underlying:
                candidates.append(_underlying_tgt_clip(
                    underlying_curve, equity_curve_ts, pf_sl_settings,
                    pf_tgt_settings, slot_pnl_at_clip, portfolio.starting_capital,
                    slot_curves=slot_curves,
                ))
            clip_result = _earliest_clip(*candidates)
        else:
            clip_result = _apply_portfolio_clip(
                equity_curve_ts, portfolio.starting_capital,
                pf_sl_settings, pf_tgt_settings, slot_pnl_at_clip,
                slot_curves=slot_curves,
            )
        # Merge the portfolio, tag and user clips — whichever fires earliest
        # wins (spec §8 evaluation order). The tag tier (§11) sits between
        # portfolio and user; all are evaluated and the earliest breach clips.
        clip_result = _earliest_clip(
            clip_result, tag_clip, tag_tgt_clip, user_clip, user_tgt_clip,
        )
        for log_line in clip_result.logs:
            print(f"[PF_CLIP] {log_line}")

        if clip_result.clip_ts is not None and clip_result.clipped_slots:
            # Drop post-clip rows from merged_fills / merged_positions for the
            # clipped slot set. We match by trader_id (slot_to_trader_id is
            # built below — compute it inline here since we need it earlier).
            _slot_to_trader_pre = {}
            for slot in portfolio.enabled_slots:
                r = slot_results.get(slot.slot_id)
                if r and r.get("positions_report") is not None and not r["positions_report"].empty:
                    tids = r["positions_report"]["trader_id"].unique()
                    if len(tids) > 0:
                        _slot_to_trader_pre[slot.slot_id] = str(tids[0])

            clipped_traders = {
                _slot_to_trader_pre[sid] for sid in clip_result.clipped_slots
                if sid in _slot_to_trader_pre
            }
            clip_ns = _ts_iso_to_ns(clip_result.clip_ts)

            def _filter_post_clip(df: "pd.DataFrame") -> "pd.DataFrame":
                if df.empty or not clipped_traders:
                    return df
                if "trader_id" not in df.columns or "ts_init" not in df.columns:
                    return df
                # Drop rows where trader_id ∈ clipped_traders AND ts_init > clip_ns.
                # ts_init in the report is typically a pandas Timestamp object;
                # convert to int ns for comparison.
                ts_int = pd.to_datetime(df["ts_init"], errors="coerce", utc=True).astype("int64")
                mask = (df["trader_id"].astype(str).isin(clipped_traders)) & (ts_int > clip_ns)
                return df.loc[~mask].reset_index(drop=True)

            # v1 ReExecute is documented as "clip + flag, no replay" (see
            # _apply_portfolio_clip log line). For ReExecute, do NOT actually
            # drop trades — the flag is informational only, the trades really
            # happened. Filtering here would empty the orderbook/positions
            # whenever the clip fires near the start of the run (e.g. tight
            # pf_sl_value with bid/ask spread immediately tipping combined
            # PnL negative). SqOff still filters: post-clip trades genuinely
            # "shouldn't have happened" once the portfolio was squared off.
            if not clip_result.would_reexecute:
                merged_fills = _filter_post_clip(merged_fills)
                merged_positions = _filter_post_clip(merged_positions)

            # Recompute aggregate stats from the clipped positions (PnL/trades
            # for slots that were clipped). For v1 we only update the totals;
            # per-slot stats remain pre-clip (would require recomputing each
            # slot's win/loss from clipped merged_positions — defer).
            #
            # The "realized_pnl" column from Nautilus is a Money-string
            # ("-2.20 USD", "-321 JPY") — calling .sum() on it concatenates
            # rather than adds. Prefer the base-currency numeric column added
            # by positions_report_with_base (e.g. "realized_pnl_USD"), which
            # is FX-converted and float-typed. Same lookup pattern as
            # backtest_runner.py:2978-2991.
            if not merged_positions.empty:
                _base_col = next(
                    (c for c in merged_positions.columns
                     if c.startswith("realized_pnl_") and c != "realized_pnl_"),
                    None,
                )
                if _base_col is not None:
                    _clipped_pnl = float(merged_positions[_base_col].sum())
                elif "realized_pnl" in merged_positions.columns:
                    # Fallback: parse Money strings ("X.XX CCY" -> X.XX). This
                    # ignores cross-currency conversion and is only correct for
                    # single-currency catalogs. The base-currency column above
                    # is the right path; this branch exists for safety.
                    _clipped_pnl = sum(
                        float(str(x).split(" ")[0]) if x and " " in str(x) else 0.0
                        for x in merged_positions["realized_pnl"]
                    )
                else:
                    _clipped_pnl = total_pnl  # nothing to recompute
                if _clipped_pnl != total_pnl:
                    print(f"[PF_CLIP] Recomputed total_pnl after clip: {total_pnl:.2f} -> {_clipped_pnl:.2f}")
                total_pnl = _clipped_pnl
                final_balance = portfolio.starting_capital + total_pnl
                total_return_pct = (total_pnl / portfolio.starting_capital) * 100 if portfolio.starting_capital > 0 else 0
                total_trades = len(merged_positions)

    # Build slot_to_strategy_id mapping from positions_report
    slot_to_strategy_id = {}
    for slot in portfolio.enabled_slots:
        r = slot_results.get(slot.slot_id)
        if r and r.get("positions_report") is not None and not r["positions_report"].empty:
            sids = r["positions_report"]["strategy_id"].unique()
            if len(sids) > 0:
                slot_to_strategy_id[slot.slot_id] = str(sids[0])

    # Build slot_to_trader_id mapping — trader_id is unique per slot even when
    # multiple slots share the same strategy_id (e.g. grouped ManagedExitStrategy).
    slot_to_trader_id = {}
    for slot in portfolio.enabled_slots:
        r = slot_results.get(slot.slot_id)
        if r and r.get("positions_report") is not None and not r["positions_report"].empty:
            tids = r["positions_report"]["trader_id"].unique()
            if len(tids) > 0:
                slot_to_trader_id[slot.slot_id] = str(tids[0])

    # Portfolio-level Path B summary. "all" when every slot ran on Path B,
    # "none" when every slot stayed on Path A, "mixed" when some did and some
    # didn't (e.g. one slot had a filter that triggered Path A fallback).
    _slot_path_b_flags = [
        bool(r.get("path_b"))
        for r in slot_results.values()
        if r is not None
    ]
    if not _slot_path_b_flags:
        path_b_summary = "none"
    elif all(_slot_path_b_flags):
        path_b_summary = "all"
    elif any(_slot_path_b_flags):
        path_b_summary = "mixed"
    else:
        path_b_summary = "none"

    return {
        "starting_capital": portfolio.starting_capital,
        "final_balance": final_balance,
        "total_pnl": total_pnl,
        "total_return_pct": total_return_pct,
        "total_trades": total_trades,
        "wins": total_wins,
        "losses": total_losses,
        "flat_trades": total_flat,
        "win_rate": win_rate,
        "decisive_win_rate": decisive_win_rate,
        "path_b": path_b_summary,  # "all" | "mixed" | "none"
        "total_days": portfolio_total_days,
        "winning_days": portfolio_winning_days,
        "losing_days": portfolio_losing_days,
        "win_pct_days": portfolio_win_pct_days,
        "loss_pct_days": portfolio_loss_pct_days,
        "max_drawdown": max_dd,
        "equity_curve": equity,
        "equity_curve_ts": equity_curve_ts,
        "per_strategy": per_strategy,
        "max_loss_hit": max_loss_hit,
        "max_profit_hit": max_profit_hit,
        # User-level Trailing SL (spec execution_logic.html §6.1) — detection
        # flag + the ratcheted-tighter effective Max-Loss cap.
        "user_trail_sl_hit": user_trail_sl_hit,
        "user_trail_sl_effective": user_trail_sl_effective,
        # User-level Trailing Target / Profit-Lock (spec §6.1 target doc).
        "user_trail_tgt_hit": user_trail_tgt_hit,
        # Portfolio-level Stoploss/Target post-hoc clip (spec
        # 5. Logics/portfolio_sl_tgt.html). Null/empty when not enabled or
        # when the clip never triggered. clip_action is informational —
        # ReExecute is treated as clip+flag in v1, no actual replay.
        "pf_clip_ts": clip_result.clip_ts,
        "pf_clip_reason": clip_result.clip_reason,
        "pf_clip_action": clip_result.clip_action,
        "pf_clipped_slot_ids": list(clip_result.clipped_slots),
        "pf_would_reexecute": clip_result.would_reexecute,
        "pf_reexec_count": clip_result.reexec_count,
        # Chronological clip events (ts, reason, action) — drives the
        # ReExecute replay loop (_USE_PF_REEXEC_REPLAY). Empty when no clip.
        "pf_clip_events": list(clip_result.clip_events),
        "portfolio_name": portfolio.name,
        "allocation_mode": portfolio.allocation_mode,
        "fills_report": merged_fills,
        "positions_report": merged_positions,
        "account_report": None,
        "slot_to_strategy_id": slot_to_strategy_id,
        "slot_to_trader_id": slot_to_trader_id,
        "errors": errors,
        "warnings": [
            {"slot_id": sid, "display_name": info.get("display_name", sid), "warning": info["warning"]}
            for sid, info in per_strategy.items() if info.get("warning")
        ],
    }


def _extract_trade_pnls(pos_report: pd.DataFrame, pnl_col: str) -> list[float]:
    """Pull a list of float PnLs from a positions_report column.

    Replaces an iterrows() walk: extracting the column once with .tolist()
    avoids allocating one Series per row, which is the dominant cost on
    portfolios with thousands of trades.

    Tolerates the two shapes the column ever takes:
      * numeric (int / float) — used by `realized_pnl_<base>` after
        positions_report_with_base() has converted to base currency.
      * money-string ("123.45 USD", "0 JPY", or unparseable) — the raw
        Nautilus output. Unparseable cells fall through to 0.0.
    """
    values = pos_report[pnl_col].tolist()
    out: list[float] = []
    for val in values:
        if isinstance(val, (int, float)):
            out.append(float(val))
            continue
        try:
            out.append(float(str(val).split()[0]))
        except (ValueError, IndexError):
            out.append(0.0)
    return out


def _extract_portfolio_results(
    engine: BacktestEngine,
    portfolio: PortfolioConfig,
    slot_strategy_map: dict,
    user_id: str | None = None,
) -> dict:
    """Extract portfolio-level and per-strategy results."""
    # Get actual strategy IDs from engine
    actual_strategies = engine.trader.strategies()
    actual_strategy_ids = [str(s.id) for s in actual_strategies]

    # Map slot_id -> actual strategy_id
    slot_to_actual = {}
    strategy_list = list(slot_strategy_map.items())
    for i, (slot_id, strategy) in enumerate(strategy_list):
        if i < len(actual_strategy_ids):
            slot_to_actual[slot_id] = actual_strategy_ids[i]

    # Get all positions (both closed and open)
    all_positions = engine.kernel.cache.positions()
    closed_positions = [p for p in all_positions if p.is_closed]
    open_positions = [p for p in all_positions if p.is_open]

    # Get final balance
    accounts = list(engine.kernel.cache.accounts())
    final_balance = portfolio.starting_capital
    if accounts:
        try:
            balance = accounts[0].balance_total(USD)
            if balance is not None:
                final_balance = float(balance)
        except Exception:
            pass

    total_pnl = final_balance - portfolio.starting_capital

    # Include unrealized P&L from open positions in total P&L
    unrealized_pnl = 0.0
    for pos in open_positions:
        try:
            unrealized_pnl += float(pos.unrealized_pnl(pos.last_price))
        except Exception:
            pass

    total_pnl_with_unrealized = total_pnl + unrealized_pnl
    total_return_pct = (total_pnl_with_unrealized / portfolio.starting_capital) * 100 if portfolio.starting_capital > 0 else 0

    # Generate reports for accurate trade counting and CSV export.
    # In NETTING mode, cache.positions() returns only 1 position per instrument,
    # but positions_report has the actual round-trip trades.
    positions_report = None
    fills_report = None
    account_report = None
    try:
        positions_report = engine.trader.generate_positions_report()
    except Exception:
        pass
    try:
        fills_report = engine.trader.generate_order_fills_report()
    except Exception:
        pass
    try:
        accs = list(engine.kernel.cache.accounts())
        if accs:
            venue = accs[0].id.get_issuer()
            account_report = engine.trader.generate_account_report(Venue(str(venue)))
    except Exception:
        pass

    # Portfolio-level stats — use positions_report for accurate trade counts
    all_pnls = []
    total_wins = 0
    total_losses = 0
    total_trades = 0

    # Build per-strategy PnL lookup from positions_report
    strategy_pnls = {}  # strategy_id -> list of pnl values

    if positions_report is not None and not positions_report.empty:
        pnl_col = None
        for col_name in ["realized_pnl", "RealizedPnl", "pnl"]:
            if col_name in positions_report.columns:
                pnl_col = col_name
                break

        strat_col = None
        for col_name in ["strategy_id", "StrategyId"]:
            if col_name in positions_report.columns:
                strat_col = col_name
                break

        if pnl_col:
            for _, row in positions_report.iterrows():
                try:
                    pnl_val = float(str(row[pnl_col]).split()[0])
                except (ValueError, IndexError):
                    pnl_val = 0.0
                all_pnls.append(pnl_val)
                total_trades += 1
                if pnl_val > 0:
                    total_wins += 1
                elif pnl_val < 0:
                    total_losses += 1

                # Track per-strategy
                if strat_col:
                    sid = str(row[strat_col])
                    strategy_pnls.setdefault(sid, []).append(pnl_val)
    else:
        # Fallback to cache positions
        for pos in closed_positions:
            try:
                pnl = float(pos.realized_pnl)
            except (TypeError, ValueError):
                pnl = 0.0
            all_pnls.append(pnl)
            total_trades += 1
            if pnl > 0:
                total_wins += 1
            elif pnl < 0:
                total_losses += 1

            sid = str(pos.strategy_id)
            strategy_pnls.setdefault(sid, []).append(pnl)

    # Count open positions as trades too
    for pos in open_positions:
        total_trades += 1
        try:
            pnl = float(pos.unrealized_pnl(pos.last_price))
        except Exception:
            pnl = 0.0
        all_pnls.append(pnl)
        if pnl > 0:
            total_wins += 1
        elif pnl < 0:
            total_losses += 1

        sid = str(pos.strategy_id)
        strategy_pnls.setdefault(sid, []).append(pnl)

    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0

    # Build timestamped equity curve from account events
    actual_final = final_balance + unrealized_pnl
    equity_curve_ts = _build_equity_curve_from_account(accounts, portfolio.starting_capital)

    # Compute max drawdown from the timestamped equity curve
    balances = [pt["balance"] for pt in equity_curve_ts] if equity_curve_ts else [portfolio.starting_capital]
    peak = balances[0]
    max_dd = 0.0
    for val in balances:
        if val > peak:
            peak = val
        dd = ((val - peak) / peak) * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    # Backward-compat equity_curve (just balance values)
    equity = balances

    # Per-strategy results using strategy_pnls from positions_report
    per_strategy = {}
    for slot in portfolio.enabled_slots:
        actual_sid = slot_to_actual.get(slot.slot_id)
        if not actual_sid:
            continue

        slot_pnls = strategy_pnls.get(actual_sid, [])
        slot_wins = sum(1 for p in slot_pnls if p > 0)
        slot_losses = sum(1 for p in slot_pnls if p < 0)
        slot_trades = len(slot_pnls)
        slot_pnl = sum(slot_pnls)

        per_strategy[slot.slot_id] = {
            "display_name": slot.display_name,
            "strategy_name": slot.strategy_name,
            "bar_type": slot.bar_type_str,
            "pnl": slot_pnl,
            "trades": slot_trades,
            "wins": slot_wins,
            "losses": slot_losses,
            "win_rate": (slot_wins / slot_trades * 100) if slot_trades > 0 else 0,
            "trade_pnls": slot_pnls,
        }

    # Global user-level caps (spec §3 Level 3). Same precedence rule as the
    # path-A site: user-level limits override portfolio-level when present.
    from core.users import (
        get_user_max_loss, get_user_max_profit,
        get_user_cumulative_pnl, add_user_pnl,
    )
    user_max_loss = get_user_max_loss(user_id) if user_id else None
    user_max_profit = get_user_max_profit(user_id) if user_id else None
    cum_user_pnl = get_user_cumulative_pnl(user_id) if user_id else 0.0
    combined_pnl = total_pnl_with_unrealized + cum_user_pnl
    eff_max_loss = user_max_loss if user_max_loss is not None else portfolio.max_loss
    eff_max_profit = user_max_profit if user_max_profit is not None else portfolio.max_profit
    max_loss_hit = eff_max_loss is not None and combined_pnl <= -abs(eff_max_loss)
    max_profit_hit = eff_max_profit is not None and combined_pnl >= eff_max_profit
    if user_id:
        add_user_pnl(user_id, total_pnl_with_unrealized)

    return {
        "starting_capital": portfolio.starting_capital,
        "final_balance": actual_final,
        "total_pnl": total_pnl_with_unrealized,
        "total_return_pct": total_return_pct,
        "total_trades": total_trades,
        "wins": total_wins,
        "losses": total_losses,
        "win_rate": win_rate,
        "max_drawdown": max_dd,
        "equity_curve": equity,
        "equity_curve_ts": equity_curve_ts,
        "per_strategy": per_strategy,
        "max_loss_hit": max_loss_hit,
        "max_profit_hit": max_profit_hit,
        "portfolio_name": portfolio.name,
        # Raw report DataFrames for CSV export
        "fills_report": fills_report,
        "positions_report": positions_report,
        "account_report": account_report,
        # Mapping of slot_id -> actual engine strategy_id
        "slot_to_strategy_id": slot_to_actual,
    }
