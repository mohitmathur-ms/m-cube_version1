"""Phase-0 session-engine parity gate.

Runs ONE portfolio two ways through the SAME unified engine and asserts the
per-slot results are byte-identical:

  (A) single-portfolio path  (session_pf_specs=None)  — the trusted reference
  (B) multi-portfolio SESSION path of ONE portfolio  (session_pf_specs=[spec])

If A == B, the new per-portfolio scoped-monitor session block produces the same
result as the proven single path for one portfolio — the no-regression gate
before running 2+ portfolios together.

Run: venv\\Scripts\\python.exe multiportfolio_session_parity_test.py [portfolio.json ...]
"""
import os
import sys
import json
from pathlib import Path

os.environ.setdefault("_USE_UNIFIED_ENGINE", "1")

from core.models import portfolio_from_dict
from core.backtest_runner.unified import _run_portfolio_unified, _allowed_weekdays
from core.backtest_runner.orchestration import (
    _resolve_pf_stoploss, _resolve_pf_target, _resolve_rbo,
    _resolve_other_settings, _resolve_move_sl_to_cost,
    effective_portfolio_squareoff, _apply_winter_time,
)
from core.backtest_runner.cross_portfolio import _is_reexec_action, _is_entry_price_reexec


def _pf_action_triple(settings):
    a = getattr(settings, "action", "") or ""
    if _is_reexec_action(a):
        return ("reexecute", not _is_entry_price_reexec(a), int(getattr(settings, "reexecute_count", 0) or 0))
    return ("sqoff", False, 0)


def _common_args(pf, catalog_path, custom_dir, user_id):
    """Replicate orchestration's unified-call arg build for one portfolio."""
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
    n = len(slots)
    caps = {}
    if pf.allocation_mode == "percentage":
        for s in slots:
            pct = s.allocation_pct if s.allocation_pct > 0 else (100.0 / n)
            caps[s.slot_id] = pf.starting_capital * pct / 100.0
    else:
        for s in slots:
            caps[s.slot_id] = pf.starting_capital / n
    vwap_fill = bool(getattr(pf, "vwap_exit_fill", False))
    dir_fill = bool(getattr(pf, "directional_close_fill", False))
    pairs = [(s, caps[s.slot_id]) for s in slots]
    args = dict(
        catalog_path=catalog_path, slot_capital_pairs=pairs, custom_strategies_dir=custom_dir,
        default_start_date=pf.start_date, default_end_date=pf.end_date,
        default_squareoff_time=sq_time, default_squareoff_tz=sq_tz,
        default_run_on_days=pf.run_on_days,
        default_entry_start_time=pf.entry_start_time, default_entry_end_time=pf.entry_end_time,
        default_rbo_settings=rbo, default_other_settings=other, default_move_sl_settings=move_sl,
        user_id=user_id, portfolio_name=getattr(pf, "name", ""),
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
    # Build the SESSION spec (one portfolio) + per-slot pid map.
    post_run = os.environ.get("_USE_POST_RUN_PF", "0") == "1"
    enf_flag = (not post_run) and os.environ.get("_USE_PF_ENFORCE", "1") == "1"
    agg = bool(move_sl and getattr(move_sl, "agg_pnl_enabled", False)
               and float(getattr(move_sl, "agg_pnl_threshold", 0) or 0) > 0)
    hit = bool(move_sl and (getattr(move_sl, "hit_on_leg_sl", False) or getattr(move_sl, "hit_on_leg_target", False)))
    sl_on = bool(getattr(sl_set, "enabled", False)) and float(getattr(sl_set, "value", 0.0) or 0.0) > 0
    tgt_on = bool(getattr(tgt_set, "enabled", False)) and float(getattr(tgt_set, "value", 0.0) or 0.0) > 0
    enforce = enf_flag and (sl_on or tgt_on or agg or hit)
    pid = getattr(pf, "name", "")
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
        # Per-portfolio conservative-fill regime (so a session of mixed regimes
        # gives each leg its OWN fill treatment, not the first portfolio's).
        "vwap_fill": vwap_fill, "dir_fill": dir_fill, "pf_feature": (sl_on or tgt_on or agg or hit),
    }
    slot_pf_ids = {s.slot_id: pid for s in slots}
    return args, spec, slot_pf_ids


def _summary(results):
    """slot_id -> (n_trades, total_realized_pnl) from positions_report."""
    import pandas as pd
    out = {}
    for sid, r in results.items():
        pr = r.get("positions_report")
        if pr is None or pr.empty:
            out[sid] = (0, 0.0); continue
        col = next((c for c in ("realized_pnl", "Realized PnL", "pnl") if c in pr.columns), None)
        tot = 0.0
        if col:
            for v in pr[col]:
                try:
                    tot += float(str(v).split()[0])
                except (ValueError, IndexError):
                    pass
        out[sid] = (len(pr), round(tot, 6))
    return out


def _run_dup(pf_path):
    """2-portfolio session: P + an identical in-memory duplicate (renamed slots/name).
    Same instrument → SHARED → exercises the per-position scoped P&L path. Each
    portfolio's per-slot result in the session must equal P's standalone result."""
    import copy
    name = Path(pf_path).stem
    d = json.loads(Path(pf_path).read_text(encoding="utf-8"))
    d2 = copy.deepcopy(d)
    d2["name"] = (d.get("name") or "P") + "_B"
    for s in d2.get("slots", []):
        s["slot_id"] = s["slot_id"] + "_B"
    pfA = portfolio_from_dict(d)
    pfB = portfolio_from_dict(d2)
    argsA, specA, mapA = _common_args(pfA, "catalog", "custom_strategies", "_default")
    std = _summary(_run_portfolio_unified(**argsA)[0])
    argsB, specB, mapB = _common_args(pfB, "catalog", "custom_strategies", "_default")
    merged = dict(argsA)
    merged["slot_capital_pairs"] = list(argsA["slot_capital_pairs"]) + list(argsB["slot_capital_pairs"])
    merged["portfolio_name"] = "SESSION_AB"   # session id → shared per-venue fill bus
    sess = _summary(_run_portfolio_unified(
        session_pf_specs=[specA, specB], slot_pf_ids={**mapA, **mapB}, **merged)[0])
    ok = True
    for sid, val in std.items():
        if sess.get(sid) != val:
            ok = False; print(f"    A DIVERGE {sid}: std={val} sess={sess.get(sid)}")
        if sess.get(sid + "_B") != val:
            ok = False; print(f"    B DIVERGE {sid}_B: std={val} sess={sess.get(sid + '_B')}")
    print(f"[{'PASS' if ok else 'FAIL'}] {name} (2-pf session): std={std} sess={sess}")
    return ok


def main():
    pfs = sys.argv[1:] or ["portfolios/_default/PF_SL_TEST.json"]
    ok_all = True
    for pf_path in pfs:
        name = Path(pf_path).stem
        pf = portfolio_from_dict(json.loads(Path(pf_path).read_text(encoding="utf-8")))
        args, spec, slot_pf_ids = _common_args(pf, "catalog", "custom_strategies", "_default")
        single, _ = _run_portfolio_unified(**args)
        session, _ = _run_portfolio_unified(session_pf_specs=[spec], slot_pf_ids=slot_pf_ids, **args)
        a, b = _summary(single), _summary(session)
        ok = a == b
        ok_all &= ok
        print(f"[{'PASS' if ok else 'FAIL'}] {name} (1-pf session): single={a} session={b}")
        if not ok:
            for sid in set(a) | set(b):
                if a.get(sid) != b.get(sid):
                    print(f"    DIVERGE slot {sid}: single={a.get(sid)} session={b.get(sid)}")
        ok_all &= _run_dup(pf_path)
    print("SESSION_PARITY_OK" if ok_all else "SESSION_PARITY_FAIL")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
