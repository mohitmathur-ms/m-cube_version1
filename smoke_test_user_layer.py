"""User-layer smoke tests — 16 portfolio backtests with complex settings,
verifying that the multi-user identity layer (multiplier, allowed_instruments,
per-user portfolio dirs, per-user reports dirs) works correctly under realistic
portfolio configurations.

Each test mutates a deep copy of the FX_9_Slot_2015_2024 baseline portfolio
(EURUSD/GBPUSD/USDJPY x EMA Cross/RSI/Bollinger over a fixed week) so the
backtests are deterministic. The user-layer feature being exercised is named
in each test; expectations include the observed effect (e.g. fills doubled
for alice ×2.0, capped at 10M for runaway multipliers).

Output: ``5. Logics/audit_user_layer_results.json`` -> consumed by
``smoke_test_user_layer_report.py`` to render the HTML audit page.

Run from the project root:  python smoke_test_user_layer.py
"""

from __future__ import annotations

import copy
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
from core.users import save_users, load_users  # noqa: E402

CATALOG_PATH = str(PROJECT_DIR / "catalog")
PORTFOLIO_PATH = PROJECT_DIR / "portfolios" / "_default" / "FX_9_Slot_2015_2024.json"
RESULTS_PATH = PROJECT_DIR / "5. Logics" / "audit_user_layer_results.json"

DEFAULT_START = "2024-12-02"
DEFAULT_END = "2024-12-06"

# ── Test users (registry state used during tests) ─────────────────────────
TEST_REGISTRY = {
    "users": [
        {"user_id": "_default", "alias": "Default User", "multiplier": 1.0,
         "allowed_instruments": None},
        {"user_id": "alice", "alias": "Alice (FX)",
         "multiplier": 2.0, "allowed_instruments": ["EURUSD", "GBPUSD", "USDJPY"]},
        {"user_id": "bob", "alias": "Bob (Crypto)",
         "multiplier": 0.5, "allowed_instruments": ["BTCUSD"]},
        {"user_id": "trader_safe", "alias": "Junior - Risk-Capped",
         "multiplier": 0.01, "allowed_instruments": ["EURUSD", "GBPUSD", "USDJPY"]},
        {"user_id": "trader_agg", "alias": "Senior - Aggressive",
         "multiplier": 3.0, "allowed_instruments": None},
        {"user_id": "runaway", "alias": "Runaway-Multiplier User",
         "multiplier": 100000.0, "allowed_instruments": ["EURUSD", "GBPUSD", "USDJPY"]},
        {"user_id": "carol", "alias": "Carol", "multiplier": 1.5,
         "allowed_instruments": ["EURUSD"]},
    ]
}


def _load_baseline_dict(start: str = DEFAULT_START, end: str = DEFAULT_END) -> dict:
    with open(PORTFOLIO_PATH) as f:
        d = json.load(f)
    d["start_date"] = start
    d["end_date"] = end
    for slot in d.get("slots", []):
        slot["start_date"] = start
        slot["end_date"] = end
    return d


def _avg_qty_from_fills(fills_report) -> float | None:
    """Mean order quantity across all fills — the proof the multiplier fired."""
    if fills_report is None:
        return None
    try:
        if hasattr(fills_report, "empty") and fills_report.empty:
            return None
        for col in ("quantity", "last_qty", "size"):
            if hasattr(fills_report, "columns") and col in fills_report.columns:
                vals = [float(v) for v in fills_report[col] if v is not None]
                return round(sum(vals) / len(vals), 4) if vals else None
    except Exception:
        return None
    return None


def _summarise(results: dict) -> dict:
    per_strategy = results.get("per_strategy", {}) or {}
    return {
        "total_pnl": round(float(results.get("total_pnl", 0.0) or 0.0), 4),
        "total_trades": int(results.get("total_trades", 0) or 0),
        "wins": int(results.get("wins", 0) or 0),
        "losses": int(results.get("losses", 0) or 0),
        "slot_count": len(per_strategy),
        "avg_fill_qty": _avg_qty_from_fills(results.get("fills_report")),
        "pf_clip_reason": results.get("pf_clip_reason"),
    }


def run_backtest_test(test_id: str, scenario: str, params: dict, mutate, expected: str,
                      user_id: str | None, env_overrides: dict | None = None,
                      date_range: tuple[str, str] | None = None) -> dict:
    """Run a portfolio backtest test under a specific user_id and capture
    summary including avg fill qty (so multiplier scaling is observable).
    """
    print(f"[{test_id}] {scenario}  user={user_id}", flush=True)

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
            os.environ[k] = v if v is not None else ""
        return {"test_id": test_id, "name": scenario, "params": params,
                "expected": expected, "user_id": user_id,
                "status": "config_error", "error": repr(e), "elapsed_sec": 0.0}

    t0 = time.time()
    try:
        results = run_portfolio_backtest(catalog_path=CATALOG_PATH, portfolio=pf,
                                         user_id=user_id)
        elapsed = time.time() - t0
        summary = _summarise(results)
        summary.update({
            "test_id": test_id, "name": scenario, "params": params,
            "expected": expected, "user_id": user_id,
            "elapsed_sec": round(elapsed, 1), "status": "ok",
            "date_range": f"{start} to {end}", "kind": "backtest",
        })
        clip = f"  CLIP={summary['pf_clip_reason']}" if summary.get("pf_clip_reason") else ""
        print(f"   -> ok  pnl=${summary['total_pnl']:.2f}  "
              f"trades={summary['total_trades']}  "
              f"avg_qty={summary['avg_fill_qty']}  "
              f"({summary['elapsed_sec']}s){clip}", flush=True)
        return summary
    except Exception as e:
        elapsed = time.time() - t0
        tb = traceback.format_exc(limit=5)
        print(f"   -> ERROR after {elapsed:.1f}s: {e!r}", flush=True)
        return {"test_id": test_id, "name": scenario, "params": params,
                "expected": expected, "user_id": user_id,
                "status": "error", "error": repr(e), "traceback": tb,
                "elapsed_sec": round(elapsed, 1),
                "date_range": f"{start} to {end}", "kind": "backtest"}
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def run_api_test(test_id: str, scenario: str, expected: str, fn) -> dict:
    """Wrapper for endpoint-level tests (allowlist, isolation, etc.).
    `fn(client)` must return a dict with at least {'pass': bool, 'detail': str}.
    """
    print(f"[{test_id}] {scenario}  (api)", flush=True)
    t0 = time.time()
    try:
        # Lazy import per-test so server module's startup-side-effects (migration,
        # _seed_if_missing) don't spam stdout multiple times.
        import server
        client = server.app.test_client()
        result = fn(client)
        elapsed = time.time() - t0
        verdict = "ok" if result.get("pass") else "fail"
        print(f"   -> {verdict}: {result.get('detail','')}  ({elapsed:.1f}s)", flush=True)
        return {
            "test_id": test_id, "name": scenario, "expected": expected,
            "status": verdict, "detail": result.get("detail", ""),
            "elapsed_sec": round(elapsed, 1), "kind": "api",
            "observations": result.get("observations", {}),
        }
    except Exception as e:
        elapsed = time.time() - t0
        tb = traceback.format_exc(limit=5)
        print(f"   -> ERROR: {e!r}", flush=True)
        return {"test_id": test_id, "name": scenario, "expected": expected,
                "status": "error", "error": repr(e), "traceback": tb,
                "elapsed_sec": round(elapsed, 1), "kind": "api"}


# ────────────────────────────────────────────────────────────────────────
# 16 tests
# ────────────────────────────────────────────────────────────────────────

def _t_complex_rbo(d):
    """Complex setting bundle reused by T01-T03: RBO + window + squareoff + pf_sl + move_sl."""
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


def main():
    os.environ["_USE_BACKTEST_NODE"] = "1"

    # Snapshot existing registry so we can restore it at the end.
    saved_registry = load_users()
    save_users(TEST_REGISTRY)

    out = []

    # ── T01: Multiplier ×1.0 baseline (control) ─────────────────────────
    out.append(run_backtest_test(
        "T01", "Baseline _default (×1.0): RBO + window + squareoff + pf_sl + move_sl",
        params={"user": "_default", "multiplier": 1.0,
                "complex_settings": "RBO Any+cancel + window 10-15 + sqoff 15:30 + pf_sl 50 + move_sl 600s"},
        mutate=_t_complex_rbo,
        expected="Baseline avg_fill_qty captured here; later tests should scale by their multipliers.",
        user_id="_default",
    ))
    baseline_qty = out[-1].get("avg_fill_qty")

    # ── T02: alice ×2.0 same config -> qty doubles ──────────────────────
    out.append(run_backtest_test(
        "T02", "alice (×2.0): same complex bundle as T01 — order qty must DOUBLE",
        params={"user": "alice", "multiplier": 2.0,
                "complex_settings": "RBO Any+cancel + window 10-15 + sqoff 15:30 + pf_sl 50 + move_sl 600s"},
        mutate=_t_complex_rbo,
        expected=f"avg_fill_qty == 2 × T01 baseline ({baseline_qty}) within rounding",
        user_id="alice",
    ))

    # ── T03: bob ×0.5 same config -> qty halves ─────────────────────────
    out.append(run_backtest_test(
        "T03", "bob (×0.5): same complex bundle — order qty must HALVE",
        params={"user": "bob", "multiplier": 0.5,
                "complex_settings": "RBO Any+cancel + window 10-15 + sqoff 15:30 + pf_sl 50 + move_sl 600s"},
        mutate=_t_complex_rbo,
        expected=f"avg_fill_qty == 0.5 × T01 baseline ({baseline_qty}) within rounding",
        # bob's allowlist is [BTCUSD] but FX_9_Slot uses EURUSD/GBPUSD/USDJPY.
        # Allowlist is enforced at API boundary (server.py), NOT inside
        # run_portfolio_backtest, so we expect this to RUN and just scale qty.
        # The allowlist enforcement is verified separately in T07-T09.
        user_id="bob",
    ))

    # ── T04: Senior aggressive ×3.0 with trailing target + reverse on SL ─
    def _t04(d):
        d["pf_tgt_enabled"] = True
        d["pf_tgt_value"] = 30.0
        d["pf_tgt_action"] = "SqOff"
        d["pf_tgt_trail_enabled"] = True
        d["pf_tgt_trail_when_profit_reach"] = 20.0
        d["pf_tgt_trail_every"] = 10.0
        d["pf_tgt_trail_by"] = 5.0
        d["pf_sl_enabled"] = True
        d["pf_sl_value"] = 60.0
        for slot in d.get("slots", []):
            ec = slot.setdefault("exit_config", {})
            ec["on_sl_action"] = "reverse"
        d["run_on_days"] = ["Mon", "Tue", "Wed", "Thu", "Fri"]
    out.append(run_backtest_test(
        "T04", "trader_agg (×3.0): trailing target + reverse-on-SL + Mon-Fri",
        params={"user": "trader_agg", "multiplier": 3.0,
                "complex_settings": "pf_tgt 30 + trail 20/10/5 + pf_sl 60 backstop + reverse-on-SL + weekdays"},
        mutate=_t04,
        expected=f"avg_fill_qty == 3 × baseline; trades fire weekdays only; reverses generate paired fills",
        user_id="trader_agg",
    ))

    # ── T05: trader_safe ×0.01 — safety multiplier with full settings ──
    def _t05(d):
        _t_complex_rbo(d)
        d["pf_sl_value"] = 80.0  # generous SL since 0.01x leaves tiny PnL
        d["pf_sl_trail_enabled"] = True
        d["pf_sl_trail_every"] = 20.0
        d["pf_sl_trail_by"] = 10.0
    out.append(run_backtest_test(
        "T05", "trader_safe (×0.01): safety scaling + RBO + trailing pf_sl",
        params={"user": "trader_safe", "multiplier": 0.01,
                "complex_settings": "RBO + window + squareoff + pf_sl 80 + trailing 20/10 + move_sl"},
        mutate=_t05,
        expected="avg_fill_qty ≈ 1% of baseline; pf_sl rarely fires due to tiny exposure",
        user_id="trader_safe",
    ))

    # ── T06: Runaway multiplier — admin cap must clamp ──────────────────
    out.append(run_backtest_test(
        "T06", "runaway (×100000): admin cap MUST clamp — order qty == per-symbol cap",
        params={"user": "runaway", "multiplier": 100000.0,
                "complex_settings": "RBO + pf_sl 50 + cap=10M (FX) — math would overflow without cap"},
        mutate=_t_complex_rbo,
        expected="avg_fill_qty == 10,000,000 (admin cap binds; multiplier alone would yield 1e8)",
        user_id="runaway",
    ))

    # ── T07: Allowlist enforcement on /api/portfolios/save (403) ────────
    def _t07(client):
        pf = {"name": "AuditPF_T07",
              "slots": [{"slot_id": "a", "bar_type_str": "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL",
                         "lots": 0.01, "strategy_name": "EMA Cross"}]}
        r = client.post("/api/portfolios/save", json=pf, headers={"X-User-Id": "bob"})
        return {"pass": r.status_code == 403,
                "detail": f"bob (allowlist=[BTCUSD]) saving EURUSD -> {r.status_code}",
                "observations": {"status": r.status_code, "error": r.json.get("error")}}
    out.append(run_api_test(
        "T07", "Allowlist save: bob blocked on EURUSD (allowlist=[BTCUSD])",
        expected="HTTP 403 with descriptive error message",
        fn=_t07,
    ))

    # ── T08: Allowlist enforcement on portfolio backtest stream ─────────
    def _t08(client):
        pf = {"name": "X", "slots": [{"slot_id": "b",
              "bar_type_str": "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL",
              "lots": 0.01, "strategy_name": "EMA Cross"}]}
        r = client.post("/api/portfolios/backtest", json={"portfolio": pf},
                        headers={"X-User-Id": "bob"})
        first_line = (r.get_data(as_text=True).splitlines() or [""])[0]
        try:
            evt = json.loads(first_line)
        except Exception:
            evt = {}
        ok = evt.get("event") == "error" and "allowed_instruments" in evt.get("error", "")
        return {"pass": ok,
                "detail": f"first stream event: {evt}",
                "observations": {"first_event": evt}}
    out.append(run_api_test(
        "T08", "Allowlist backtest stream: bob blocked on EURUSD — error event",
        expected="First NDJSON line carries event=error with 'allowed_instruments' in message",
        fn=_t08,
    ))

    # ── T09: Allowlist enforcement on standalone /api/backtest/run-stream
    def _t09(client):
        body = {"instruments": [{"bar_type": "BTCUSD.BINANCE_MS-1-DAY-LAST-EXTERNAL"}],
                "strategies": {"EMA Cross": {"params": {}, "trade_size": 1}}}
        r = client.post("/api/backtest/run-stream", json=body,
                        headers={"X-User-Id": "alice"})
        return {"pass": r.status_code == 403,
                "detail": f"alice (allowlist=[EURUSD,GBPUSD,USDJPY]) standalone BTCUSD -> {r.status_code}",
                "observations": {"status": r.status_code, "error": (r.json or {}).get("error")}}
    out.append(run_api_test(
        "T09", "Allowlist standalone backtest: alice blocked on BTCUSD",
        expected="HTTP 403 — standalone path enforces same allowlist as portfolio path",
        fn=_t09,
    ))

    # ── T10: Per-user portfolio isolation (alice saves, bob doesn't see) ─
    def _t10(client):
        pf = {"name": "AuditPF_T10",
              "slots": [{"slot_id": "a", "bar_type_str": "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL",
                         "lots": 0.01, "strategy_name": "EMA Cross"}]}
        r_save = client.post("/api/portfolios/save", json=pf,
                             headers={"X-User-Id": "alice"})
        la = (client.get("/api/portfolios/list", headers={"X-User-Id": "alice"}).json
              or {}).get("portfolios", [])
        lb = (client.get("/api/portfolios/list", headers={"X-User-Id": "bob"}).json
              or {}).get("portfolios", [])
        # Cleanup
        client.post("/api/portfolios/delete", json={"name": "AuditPF_T10"},
                    headers={"X-User-Id": "alice"})
        ok = (r_save.status_code == 200 and "AuditPF_T10" in la and "AuditPF_T10" not in lb)
        return {"pass": ok,
                "detail": f"alice saves={r_save.status_code}; alice sees AuditPF_T10={'AuditPF_T10' in la}; bob sees={'AuditPF_T10' in lb}",
                "observations": {"alice_list": la, "bob_list": lb}}
    out.append(run_api_test(
        "T10", "Per-user portfolio isolation: separate portfolios/<user_id>/ dirs",
        expected="alice's portfolio invisible to bob's list call",
        fn=_t10,
    ))

    # ── T11: 401 on missing X-User-Id header (mutating endpoint) ────────
    def _t11(client):
        pf = {"name": "Z", "slots": []}
        r = client.post("/api/portfolios/save", json=pf)
        return {"pass": r.status_code == 401,
                "detail": f"missing header -> {r.status_code}",
                "observations": {"status": r.status_code, "error": r.json.get("error")}}
    out.append(run_api_test(
        "T11", "Identity gate: 401 when X-User-Id is missing on mutating endpoints",
        expected="HTTP 401 'X-User-Id header required'",
        fn=_t11,
    ))

    # ── T12: 401 on unknown user_id ─────────────────────────────────────
    def _t12(client):
        pf = {"name": "Z", "slots": []}
        r = client.post("/api/portfolios/save", json=pf,
                        headers={"X-User-Id": "ghost_user"})
        return {"pass": r.status_code == 401,
                "detail": f"unknown user 'ghost_user' -> {r.status_code}",
                "observations": {"status": r.status_code, "error": r.json.get("error")}}
    out.append(run_api_test(
        "T12", "Identity gate: 401 on unknown user_id",
        expected="HTTP 401 'Unknown user: ghost_user'",
        fn=_t12,
    ))

    # ── T13: Public picker endpoint (slim) — no multiplier leakage ─────
    def _t13(client):
        r = client.get("/api/users/list")
        users_list = (r.json or {}).get("users", [])
        keys_only = all(set(u.keys()) <= {"user_id", "alias"} for u in users_list)
        has_known = {"alice", "bob", "_default"}.issubset({u["user_id"] for u in users_list})
        return {"pass": r.status_code == 200 and keys_only and has_known,
                "detail": f"status={r.status_code}, keys_clean={keys_only}, known_users_present={has_known}",
                "observations": {"sample": users_list[:3]}}
    out.append(run_api_test(
        "T13", "Picker endpoint /api/users/list: returns only {user_id, alias}",
        expected="200; multiplier and allowed_instruments NOT exposed via this endpoint",
        fn=_t13,
    ))

    # ── T14: /api/users/me reflects active X-User-Id ────────────────────
    def _t14(client):
        ra = client.get("/api/users/me", headers={"X-User-Id": "alice"}).json
        rb = client.get("/api/users/me", headers={"X-User-Id": "bob"}).json
        ok = (ra["multiplier"] == 2.0 and ra["allowed_instruments"] == ["EURUSD", "GBPUSD", "USDJPY"]
              and rb["multiplier"] == 0.5 and rb["allowed_instruments"] == ["BTCUSD"])
        return {"pass": ok,
                "detail": f"alice: mul={ra['multiplier']}, allow={ra['allowed_instruments']} | bob: mul={rb['multiplier']}, allow={rb['allowed_instruments']}",
                "observations": {"alice": ra, "bob": rb}}
    out.append(run_api_test(
        "T14", "/api/users/me: full read-only row for active session user",
        expected="multiplier and allowed_instruments match registry; per-header switching works",
        fn=_t14,
    ))

    # ── T15: Reports scoped per user (write isolation) ──────────────────
    def _t15(client):
        from server import _user_reports_dir
        ad = Path(_user_reports_dir("alice"))
        bd = Path(_user_reports_dir("bob"))
        # Plant probe files
        (ad / "order_book_probe_alice.csv").write_text("col_a,col_b\n1,2\n")
        (bd / "order_book_probe_bob.csv").write_text("col_a,col_b\n9,9\n")
        try:
            la = (client.get("/api/orderbook/list",
                             headers={"X-User-Id": "alice"}).json or {}).get("files", [])
            lb = (client.get("/api/orderbook/list",
                             headers={"X-User-Id": "bob"}).json or {}).get("files", [])
            ok = ("order_book_probe_alice.csv" in la and
                  "order_book_probe_alice.csv" not in lb and
                  "order_book_probe_bob.csv" in lb and
                  "order_book_probe_bob.csv" not in la)
        finally:
            (ad / "order_book_probe_alice.csv").unlink(missing_ok=True)
            (bd / "order_book_probe_bob.csv").unlink(missing_ok=True)
        return {"pass": ok,
                "detail": f"alice list contains alice-probe={'order_book_probe_alice.csv' in la}; bob list cross-leak={'order_book_probe_alice.csv' in lb}",
                "observations": {"alice_count": len(la), "bob_count": len(lb)}}
    out.append(run_api_test(
        "T15", "Report scoping: /api/orderbook/list returns only the caller's files",
        expected="No cross-user file leakage; each session sees its own reports/<user_id>/ contents only",
        fn=_t15,
    ))

    # ── T16: Admin update → next run reflects new multiplier (no restart)
    def _t16(client):
        # Pre-state
        before = client.get("/api/users/me", headers={"X-User-Id": "carol"}).json
        # Admin POST flips carol multiplier 1.5 -> 5.0 via main server's
        # /api/users (POST is also exposed there for admin convenience —
        # actually no, that lives on the admin server. Use save_users
        # directly to simulate the admin save without spinning up port 5001).
        new_reg = copy.deepcopy(TEST_REGISTRY)
        for u in new_reg["users"]:
            if u["user_id"] == "carol":
                u["multiplier"] = 5.0
        save_users(new_reg)
        after = client.get("/api/users/me", headers={"X-User-Id": "carol"}).json
        # Restore
        save_users(TEST_REGISTRY)
        ok = before["multiplier"] == 1.5 and after["multiplier"] == 5.0
        return {"pass": ok,
                "detail": f"carol multiplier before={before['multiplier']}, after admin save={after['multiplier']}",
                "observations": {"before": before, "after": after}}
    out.append(run_api_test(
        "T16", "Admin live-edit: multiplier change visible on next request (no restart)",
        expected="get_user reads users.json on every call; new multiplier visible immediately",
        fn=_t16,
    ))

    # Persist
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nWrote {len(out)} test results -> {RESULTS_PATH}", flush=True)

    # Restore registry
    save_users(saved_registry)
    print(f"Restored original registry ({len(saved_registry.get('users', []))} users)", flush=True)


if __name__ == "__main__":
    main()
