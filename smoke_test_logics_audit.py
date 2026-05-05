"""Comprehensive smoke tests for the Add Portfolio backend audit.

Runs 16 backtest variants combining MULTIPLE portfolio settings each (no
single-knob tests). Each test models a realistic trading scenario with
4-10 settings tuned together. The week 2024-12-02 .. 2024-12-06 is used as
the common date span for all tests except T15 (which spans a weekend).

Invoke from the project root:

    python smoke_test_logics_audit.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

PROJECT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_DIR))

from core.backtest_runner import run_portfolio_backtest  # noqa: E402
from core.models import portfolio_from_dict  # noqa: E402

CATALOG_PATH = str(PROJECT_DIR / "catalog")
PORTFOLIO_PATH = PROJECT_DIR / "portfolios" / "FX_9_Slot_2015_2024.json"
RESULTS_PATH = PROJECT_DIR / "5. Logics" / "audit_smoke_results.json"

DEFAULT_START = "2024-12-02"
DEFAULT_END = "2024-12-06"


def _load_baseline_dict(start: str = DEFAULT_START, end: str = DEFAULT_END) -> dict:
    with open(PORTFOLIO_PATH) as f:
        d = json.load(f)
    d["start_date"] = start
    d["end_date"] = end
    for slot in d.get("slots", []):
        slot["start_date"] = start
        slot["end_date"] = end
    return d


def _trade_times_from_fills(fills_report) -> tuple[Counter, Counter]:
    weekdays: Counter = Counter()
    hours: Counter = Counter()
    if fills_report is None:
        return weekdays, hours
    try:
        if hasattr(fills_report, "empty") and fills_report.empty:
            return weekdays, hours
        ts_col = None
        for c in ("ts_event", "ts_init", "timestamp", "ts_open"):
            if hasattr(fills_report, "columns") and c in fills_report.columns:
                ts_col = c
                break
        if ts_col is None:
            return weekdays, hours
        for raw in fills_report[ts_col]:
            try:
                if isinstance(raw, (int, float)):
                    dt = datetime.fromtimestamp(int(raw) / 1e9, tz=timezone.utc)
                elif isinstance(raw, str):
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                else:
                    dt = raw.to_pydatetime() if hasattr(raw, "to_pydatetime") else raw
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                weekdays[dt.strftime("%a")] += 1
                hours[dt.hour] += 1
            except Exception:
                continue
    except Exception:
        pass
    return weekdays, hours


def _summarise(results: dict) -> dict:
    per_strategy = results.get("per_strategy", {}) or {}
    weekdays, hours = _trade_times_from_fills(results.get("fills_report"))
    return {
        "total_pnl": round(float(results.get("total_pnl", 0.0) or 0.0), 4),
        "total_trades": int(results.get("total_trades", 0) or 0),
        "wins": int(results.get("wins", 0) or 0),
        "losses": int(results.get("losses", 0) or 0),
        "path_b_summary": results.get("path_b"),
        "slot_count": len(per_strategy),
        "fills_weekdays": dict(weekdays),
        "fills_hours_utc": {str(k): v for k, v in sorted(hours.items())},
        "max_loss_hit": bool(results.get("max_loss_hit")),
        "max_profit_hit": bool(results.get("max_profit_hit")),
        "pf_clip_ts": results.get("pf_clip_ts"),
        "pf_clip_reason": results.get("pf_clip_reason"),
        "pf_clip_action": results.get("pf_clip_action"),
        "pf_clipped_slots": list(results.get("pf_clipped_slot_ids") or []),
        "warnings": [w.get("warning", "") for w in (results.get("warnings") or [])][:10],
    }


def run_test(test_id: str, scenario: str, params: dict, mutate, expected: str,
             env_overrides: dict | None = None,
             date_range: tuple[str, str] | None = None) -> dict:
    print(f"[{test_id}] {scenario}", flush=True)

    saved_env = {}
    if env_overrides:
        for k, v in env_overrides.items():
            saved_env[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    start, end = date_range if date_range else (DEFAULT_START, DEFAULT_END)
    pf_dict = _load_baseline_dict(start=start, end=end)
    if mutate is not None:
        mutate(pf_dict)
    try:
        pf = portfolio_from_dict(pf_dict)
    except Exception as e:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return {
            "test_id": test_id, "name": scenario, "params": params,
            "expected": expected, "status": "config_error",
            "error": repr(e), "elapsed_sec": 0.0,
        }

    t0 = time.time()
    try:
        results = run_portfolio_backtest(catalog_path=CATALOG_PATH, portfolio=pf)
        elapsed = time.time() - t0
        summary = _summarise(results)
        summary["elapsed_sec"] = round(elapsed, 1)
        summary["status"] = "ok"
        summary["test_id"] = test_id
        summary["name"] = scenario
        summary["params"] = params
        summary["expected"] = expected
        summary["date_range"] = f"{start} to {end}"
        clip = ""
        if summary.get("pf_clip_reason"):
            clip = f"  CLIP={summary['pf_clip_reason']}"
        flags = ""
        if summary.get("max_loss_hit"): flags += "  max_loss_hit"
        if summary.get("max_profit_hit"): flags += "  max_profit_hit"
        print(
            f"   -> ok  pnl=${summary['total_pnl']:.2f}  "
            f"trades={summary['total_trades']}  "
            f"path_b={summary['path_b_summary']}  ({summary['elapsed_sec']}s){clip}{flags}",
            flush=True,
        )
        return summary
    except Exception as e:
        elapsed = time.time() - t0
        tb = traceback.format_exc(limit=5)
        print(f"   -> ERROR after {elapsed:.1f}s: {e!r}", flush=True)
        return {
            "test_id": test_id, "name": scenario, "params": params,
            "expected": expected, "status": "error",
            "error": repr(e), "traceback": tb,
            "elapsed_sec": round(elapsed, 1),
            "date_range": f"{start} to {end}",
        }
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def main():
    os.environ["_USE_BACKTEST_NODE"] = "1"
    out = []

    # ---------------------------------------------------------------------
    # T01 — Conservative day trader
    # ---------------------------------------------------------------------
    def _t01(d):
        d["rbo_enabled"] = True
        d["range_monitoring_start"] = "09:00:00"
        d["range_monitoring_end"] = "10:00:00"
        d["rbo_entry_start"] = "10:00:00"
        d["rbo_entry_end"] = "15:00:00"
        d["rbo_entry_at"] = "Any"
        d["rbo_cancel_other_side"] = True
        d["entry_start_time"] = "10:00:00"
        d["entry_end_time"] = "15:00:00"
        d["squareoff_time"] = "15:30"
        d["squareoff_tz"] = "UTC"
        d["delay_between_legs_sec"] = 120
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 50.0
        d["pf_sl_action"] = "SqOff"
        d["move_sl_enabled"] = True
        d["move_sl_safety_sec"] = 600
    out.append(run_test(
        "T01", "Conservative day trader (RBO + tight window + squareoff + pf_sl + move_sl)",
        params={
            "rbo_enabled": True, "rbo_entry_at": "Any", "rbo_cancel_other_side": True,
            "range_monitoring": "09:00-10:00", "entry_window": "10:00-15:00",
            "squareoff": "15:30 UTC", "delay_between_legs_sec": 120,
            "pf_sl_value": 50.0, "move_sl_safety_sec": 600,
        },
        mutate=_t01,
        expected="Path B; entries 10:00-15:00; closes by 15:30; pf_sl may fire if cumulative loss exceeds $50",
    ))

    # ---------------------------------------------------------------------
    # T02 — Aggressive scalper
    # ---------------------------------------------------------------------
    def _t02(d):
        d["rbo_enabled"] = True
        d["rbo_entry_at"] = "Any"
        d["rbo_cancel_other_side"] = False
        d["rbo_range_buffer"] = 15
        d["delay_between_legs_sec"] = 0
        d["run_on_days"] = ["Mon", "Tue", "Wed", "Thu", "Fri"]
        d["squareoff_time"] = "22:00"
        d["squareoff_tz"] = "UTC"
        d["on_sl_action_on"] = "OnSL_N_Trailing_Both"
        d["on_target_action_on"] = "OnTarget_N_Trailing_Both"
    out.append(run_test(
        "T02", "Aggressive scalper (RBO Any, both directions, no leg delay)",
        params={
            "rbo_entry_at": "Any", "rbo_cancel_other_side": False,
            "rbo_range_buffer": 15, "delay_between_legs_sec": 0,
            "run_on_days": ["Mon-Fri"], "squareoff": "22:00 UTC",
        },
        mutate=_t02,
        expected="Higher trade count than T01 (no cancel_other_side, no leg delay)",
    ))

    # ---------------------------------------------------------------------
    # T03 — Defensive grid
    # ---------------------------------------------------------------------
    def _t03(d):
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 80.0
        d["pf_sl_action"] = "SqOff"
        d["pf_sl_trail_enabled"] = True
        d["pf_sl_trail_every"] = 20.0
        d["pf_sl_trail_by"] = 10.0
        d["move_sl_enabled"] = True
        d["move_sl_safety_sec"] = 300
        d["move_sl_trail_after"] = True
        d["delay_between_legs_sec"] = 180
        d["run_on_days"] = ["Mon", "Wed", "Fri"]
    out.append(run_test(
        "T03", "Defensive grid (pf_sl + trailing pf_sl + move_sl trail_after + leg delay + Mon/Wed/Fri only)",
        params={
            "pf_sl_value": 80.0, "pf_sl_trail_every": 20, "pf_sl_trail_by": 10,
            "move_sl_safety_sec": 300, "move_sl_trail_after": True,
            "delay_between_legs_sec": 180, "run_on_days": ["Mon", "Wed", "Fri"],
        },
        mutate=_t03,
        expected="Trades only on Mon/Wed/Fri; trailing SL ratchets without firing $80 threshold",
    ))

    # ---------------------------------------------------------------------
    # T04 — Profit lock chaser
    # ---------------------------------------------------------------------
    def _t04(d):
        d["pf_tgt_enabled"] = True
        d["pf_tgt_type"] = "Combined Profit"
        d["pf_tgt_value"] = 30.0
        d["pf_tgt_action"] = "SqOff"
        d["pf_tgt_trail_enabled"] = True
        d["pf_tgt_trail_lock_min_profit"] = 15.0
        d["pf_tgt_trail_when_profit_reach"] = 20.0
        d["pf_tgt_trail_every"] = 10.0
        d["pf_tgt_trail_by"] = 5.0
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 60.0
        d["pf_sl_action"] = "SqOff"
        d["run_on_days"] = ["Tue", "Wed", "Thu"]
    out.append(run_test(
        "T04", "Profit lock chaser (pf_tgt + trailing target + pf_sl backstop + Tue-Thu only)",
        params={
            "pf_tgt_value": 30.0, "trail_lock_min_profit": 15, "trail_when_reach": 20,
            "trail_every": 10, "trail_by": 5,
            "pf_sl_value": 60.0, "run_on_days": ["Tue", "Wed", "Thu"],
        },
        mutate=_t04,
        expected="Trades only Tue/Wed/Thu; profit-lock activates at +$20 if reached",
    ))

    # ---------------------------------------------------------------------
    # T05 — Asia session (early UTC)
    # ---------------------------------------------------------------------
    def _t05(d):
        d["entry_start_time"] = "05:00:00"
        d["entry_end_time"] = "10:00:00"
        d["squareoff_time"] = "11:00"
        d["squareoff_tz"] = "UTC"
        d["run_on_days"] = ["Mon", "Tue", "Wed", "Thu"]
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 40.0
        d["pf_sl_action"] = "SqOff"
        d["rbo_enabled"] = False
    out.append(run_test(
        "T05", "Asia session (05:00-10:00 entry, 11:00 squareoff, Mon-Thu, pf_sl=$40, RBO off)",
        params={
            "entry_start_time": "05:00:00", "entry_end_time": "10:00:00",
            "squareoff": "11:00 UTC", "run_on_days": ["Mon-Thu"],
            "pf_sl_value": 40.0, "rbo_enabled": False,
        },
        mutate=_t05,
        expected="Entries 05:00-10:00 UTC; closes 11:00; no Friday trades",
    ))

    # ---------------------------------------------------------------------
    # T06 — Trend follower (RBO RangeHigh)
    # ---------------------------------------------------------------------
    def _t06(d):
        d["rbo_enabled"] = True
        d["rbo_entry_at"] = "RangeHigh"
        d["rbo_cancel_other_side"] = True
        d["rbo_range_buffer"] = 10
        d["range_monitoring_start"] = "09:00:00"
        d["range_monitoring_end"] = "10:30:00"
        d["rbo_entry_start"] = "10:30:00"
        d["rbo_entry_end"] = "16:00:00"
        d["delay_between_legs_sec"] = 300
        d["move_sl_enabled"] = True
        d["move_sl_safety_sec"] = 600
        d["move_sl_trail_after"] = True
        d["no_reexec_sl_cost"] = True
    out.append(run_test(
        "T06", "Trend follower (RBO RangeHigh + 5min leg delay + move_sl trail_after + no_reexec_sl_cost)",
        params={
            "rbo_entry_at": "RangeHigh", "rbo_range_buffer": 10,
            "range_monitoring": "09:00-10:30", "rbo_entry": "10:30-16:00",
            "delay_between_legs_sec": 300, "move_sl_safety_sec": 600,
            "move_sl_trail_after": True, "no_reexec_sl_cost": True,
        },
        mutate=_t06,
        expected="Upward breakouts only; SL pin via move_sl; no re_execute after move_sl",
    ))

    # ---------------------------------------------------------------------
    # T07 — Mean reversion (RBO RangeLow)
    # ---------------------------------------------------------------------
    def _t07(d):
        d["rbo_enabled"] = True
        d["rbo_entry_at"] = "RangeLow"
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 30.0
        d["pf_sl_action"] = "SqOff"
        d["pf_tgt_enabled"] = True
        d["pf_tgt_type"] = "Combined Profit"
        d["pf_tgt_value"] = 20.0
        d["pf_tgt_action"] = "SqOff"
        d["allocation_mode"] = "percentage"
        slots = d.get("slots", [])
        for i, slot in enumerate(slots):
            slot["allocation_pct"] = 30.0 if i < 3 else round(10.0 / max(1, len(slots) - 3), 4)
        d["on_sl_action_on"] = "OnSL_Only"
    out.append(run_test(
        "T07", "Mean reversion (RBO RangeLow + pf_sl=$30 + pf_tgt=$20 + percentage alloc + OnSL_Only)",
        params={
            "rbo_entry_at": "RangeLow", "pf_sl_value": 30.0, "pf_tgt_value": 20.0,
            "allocation_mode": "percentage", "first_3_slots": "30% each",
            "on_sl_action_on": "OnSL_Only",
        },
        mutate=_t07,
        expected="Downward breakouts only; clip on $30 loss or $20 profit",
    ))

    # ---------------------------------------------------------------------
    # T08 — Risk-averse (all defenses)
    # ---------------------------------------------------------------------
    def _t08(d):
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 25.0
        d["pf_sl_action"] = "SqOff"
        d["pf_sl_trail_enabled"] = True
        d["pf_sl_trail_every"] = 10.0
        d["pf_sl_trail_by"] = 5.0
        d["move_sl_enabled"] = True
        d["move_sl_safety_sec"] = 240
        d["move_sl_trail_after"] = True
        d["max_loss"] = 30
        d["max_profit"] = 50
        d["squareoff_time"] = "15:00"
        d["squareoff_tz"] = "UTC"
        d["entry_start_time"] = "10:00:00"
        d["entry_end_time"] = "14:00:00"
    out.append(run_test(
        "T08", "Risk-averse (tight pf_sl + trail + move_sl + max_loss + squareoff + narrow window)",
        params={
            "pf_sl_value": 25.0, "trail_every": 10, "trail_by": 5,
            "move_sl_safety_sec": 240, "max_loss": 30, "max_profit": 50,
            "squareoff": "15:00 UTC", "entry_window": "10:00-14:00",
        },
        mutate=_t08,
        expected="Tight pf_sl ($25) likely fires; max_loss flag if total > $30",
    ))

    # ---------------------------------------------------------------------
    # T09 — High-frequency (no filters)
    # ---------------------------------------------------------------------
    def _t09(d):
        d["rbo_enabled"] = False
        d["entry_start_time"] = "09:30:00"
        d["entry_end_time"] = "23:59:00"
        d["delay_between_legs_sec"] = 0
        d["allocation_mode"] = "percentage"
        slots = d.get("slots", [])
        for i, slot in enumerate(slots):
            slot["allocation_pct"] = 25.0 if i < 3 else round(25.0 / max(1, len(slots) - 3), 4)
        d["on_sl_action_on"] = "OnSL_N_Trailing_Both"
        d["on_target_action_on"] = "OnTarget_N_Trailing_Both"
    out.append(run_test(
        "T09", "High-frequency (RBO off + 09:30-23:59 window + delay=0 + percentage alloc 25/25/25/8.3...)",
        params={
            "rbo_enabled": False, "entry_window": "09:30-23:59",
            "delay_between_legs_sec": 0, "allocation_mode": "percentage",
            "first_3_slots": "25%", "rest": "~4.2%",
        },
        mutate=_t09,
        expected="Higher trade count than baseline (wider entry window, no leg delay)",
    ))

    # ---------------------------------------------------------------------
    # T10 — Weekly cycle
    # ---------------------------------------------------------------------
    def _t10(d):
        d["run_on_days"] = ["Mon", "Tue", "Wed", "Thu"]
        d["entry_start_time"] = "11:00:00"
        d["entry_end_time"] = "16:00:00"
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 70.0
        d["pf_sl_action"] = "SqOff"
        d["pf_sl_trail_enabled"] = True
        d["pf_sl_trail_every"] = 15.0
        d["pf_sl_trail_by"] = 8.0
        d["squareoff_time"] = "17:00"
        d["squareoff_tz"] = "UTC"
        d["rbo_enabled"] = False
    out.append(run_test(
        "T10", "Weekly cycle (Mon-Thu + 11:00-16:00 entry + trailing pf_sl + 17:00 squareoff)",
        params={
            "run_on_days": ["Mon-Thu"], "entry_window": "11:00-16:00",
            "pf_sl_value": 70.0, "trail_every": 15, "trail_by": 8,
            "squareoff": "17:00 UTC", "rbo_enabled": False,
        },
        mutate=_t10,
        expected="No Fri trades; trailing pf_sl ratchets",
    ))

    # ---------------------------------------------------------------------
    # T11 — Both pf_sl & pf_tgt with trailing on both
    # ---------------------------------------------------------------------
    def _t11(d):
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 50.0
        d["pf_sl_action"] = "SqOff"
        d["pf_sl_trail_enabled"] = True
        d["pf_sl_trail_every"] = 15.0
        d["pf_sl_trail_by"] = 8.0
        d["pf_tgt_enabled"] = True
        d["pf_tgt_value"] = 40.0
        d["pf_tgt_action"] = "SqOff"
        d["pf_tgt_trail_enabled"] = True
        d["pf_tgt_trail_lock_min_profit"] = 20.0
        d["pf_tgt_trail_when_profit_reach"] = 30.0
        d["pf_tgt_trail_every"] = 10.0
        d["pf_tgt_trail_by"] = 5.0
        d["move_sl_enabled"] = True
        d["move_sl_safety_sec"] = 180
    out.append(run_test(
        "T11", "Both SL & Target with trailing both + move_sl",
        params={
            "pf_sl_value": 50.0, "pf_sl_trail": "15/8",
            "pf_tgt_value": 40.0, "pf_tgt_trail": "lock=20 reach=30 every=10 by=5",
            "move_sl_safety_sec": 180,
        },
        mutate=_t11,
        expected="Both SL and target trailing run; move_sl pins entry SL after 3min",
    ))

    # ---------------------------------------------------------------------
    # T12 — Path A parity (combo on Path A)
    # ---------------------------------------------------------------------
    def _t12(d):
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 50.0
        d["pf_sl_action"] = "SqOff"
        d["pf_tgt_enabled"] = True
        d["pf_tgt_value"] = 40.0
        d["pf_tgt_action"] = "SqOff"
        d["run_on_days"] = ["Mon", "Tue", "Wed"]
        d["entry_start_time"] = "10:00:00"
        d["entry_end_time"] = "14:00:00"
        d["squareoff_time"] = "14:30"
        d["squareoff_tz"] = "UTC"
    out.append(run_test(
        "T12", "Path A combo (env=0; same scenario should run as on Path B)",
        params={
            "_USE_BACKTEST_NODE": "0",
            "pf_sl_value": 50.0, "pf_tgt_value": 40.0,
            "run_on_days": ["Mon", "Tue", "Wed"],
            "entry_window": "10:00-14:00", "squareoff": "14:30 UTC",
        },
        mutate=_t12,
        expected='path_b_summary == "none"; path A handles all settings same as Path B',
        env_overrides={"_USE_BACKTEST_NODE": "0"},
    ))

    # ---------------------------------------------------------------------
    # T13 — Edge: very tight settings (likely triggers Money-string bug)
    # ---------------------------------------------------------------------
    def _t13(d):
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 15.0
        d["pf_sl_action"] = "SqOff"
        d["entry_start_time"] = "11:30:00"
        d["entry_end_time"] = "12:30:00"
        d["move_sl_enabled"] = True
        d["move_sl_safety_sec"] = 60
        d["delay_between_legs_sec"] = 60
        d["rbo_enabled"] = False
    out.append(run_test(
        "T13", "Edge: very tight settings (pf_sl=$15 + 1h window + 1min move_sl safety)",
        params={
            "pf_sl_value": 15.0, "entry_window": "11:30-12:30",
            "move_sl_safety_sec": 60, "delay_between_legs_sec": 60,
            "rbo_enabled": False,
        },
        mutate=_t13,
        expected="pf_sl likely fires (tight $15 threshold); may trip Money-string bug",
    ))

    # ---------------------------------------------------------------------
    # T14 — Edge: zero filters (full week, no constraints)
    # ---------------------------------------------------------------------
    def _t14(d):
        d["rbo_enabled"] = False
        d["run_on_days"] = None
        d["entry_start_time"] = "00:00:00"
        d["entry_end_time"] = "23:59:00"
        d["squareoff_time"] = None
        d["squareoff_tz"] = None
        d["pf_sl_enabled"] = False
        d["pf_tgt_enabled"] = False
        d["move_sl_enabled"] = False
        d["delay_between_legs_sec"] = 0
    out.append(run_test(
        "T14", "Edge: zero filters (no RBO, no day filter, 24h window, no clips)",
        params={
            "rbo_enabled": False, "run_on_days": None,
            "entry_window": "00:00-23:59", "squareoff": None,
            "pf_sl_enabled": False, "pf_tgt_enabled": False,
            "move_sl_enabled": False,
        },
        mutate=_t14,
        expected="Maximum trade count; no clips or boundary flags",
    ))

    # ---------------------------------------------------------------------
    # T15 — Edge: weekend-only filter (no FX trading data)
    # ---------------------------------------------------------------------
    def _t15(d):
        d["run_on_days"] = ["Sat", "Sun"]
        d["entry_start_time"] = "09:30:00"
        d["entry_end_time"] = "16:00:00"
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 20.0
        d["rbo_enabled"] = False
    out.append(run_test(
        "T15", "Edge: weekend-only run_on_days (no FX market data on Sat/Sun)",
        params={
            "run_on_days": ["Sat", "Sun"], "pf_sl_value": 20.0,
            "rbo_enabled": False,
        },
        mutate=_t15,
        expected="Zero trades (no FX data on weekends); run completes without error",
        date_range=("2024-12-02", "2024-12-08"),  # spans Sat-Sun
    ))

    # ---------------------------------------------------------------------
    # T16 — Stress test: everything on
    # ---------------------------------------------------------------------
    def _t16(d):
        d["rbo_enabled"] = True
        d["rbo_entry_at"] = "Any"
        d["rbo_cancel_other_side"] = True
        d["rbo_range_buffer"] = 30
        d["range_monitoring_start"] = "09:00:00"
        d["range_monitoring_end"] = "10:00:00"
        d["rbo_entry_start"] = "10:00:00"
        d["rbo_entry_end"] = "15:00:00"
        d["run_on_days"] = ["Mon", "Wed", "Fri"]
        d["entry_start_time"] = "10:00:00"
        d["entry_end_time"] = "15:00:00"
        d["squareoff_time"] = "16:00"
        d["squareoff_tz"] = "UTC"
        d["delay_between_legs_sec"] = 120
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 40.0
        d["pf_sl_action"] = "SqOff"
        d["pf_sl_trail_enabled"] = True
        d["pf_sl_trail_every"] = 15.0
        d["pf_sl_trail_by"] = 8.0
        d["pf_tgt_enabled"] = True
        d["pf_tgt_value"] = 25.0
        d["pf_tgt_action"] = "SqOff"
        d["pf_tgt_trail_enabled"] = True
        d["pf_tgt_trail_lock_min_profit"] = 10.0
        d["pf_tgt_trail_when_profit_reach"] = 20.0
        d["pf_tgt_trail_every"] = 10.0
        d["pf_tgt_trail_by"] = 5.0
        d["move_sl_enabled"] = True
        d["move_sl_safety_sec"] = 300
        d["move_sl_trail_after"] = True
        d["move_sl_no_buy_legs"] = False
        d["no_reexec_sl_cost"] = True
        d["max_loss"] = 50
        d["on_sl_action_on"] = "OnSL_Only"
        d["on_target_action_on"] = "OnTarget_Only"
    out.append(run_test(
        "T16", "Stress test: every active setting on with realistic values",
        params={
            "rbo": "Any cancel=T buf=30", "run_on_days": ["Mon", "Wed", "Fri"],
            "entry_window": "10:00-15:00", "squareoff": "16:00 UTC",
            "delay_between_legs_sec": 120, "pf_sl": "$40 trail 15/8",
            "pf_tgt": "$25 trail lock=10 reach=20",
            "move_sl_safety": 300, "move_sl_trail_after": True,
            "no_reexec_sl_cost": True, "max_loss": 50,
            "on_sl_action_on": "OnSL_Only", "on_target_action_on": "OnTarget_Only",
        },
        mutate=_t16,
        expected="All gates active; trades only Mon/Wed/Fri 10-15 UTC; clip on $40 loss or $25 profit; closes by 16:00",
    ))

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nWrote {len(out)} test results to {RESULTS_PATH}", flush=True)


if __name__ == "__main__":
    main()
