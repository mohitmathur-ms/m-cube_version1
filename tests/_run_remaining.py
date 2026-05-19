"""Run only T14-T16 of the stress suite and merge into _stress_results.json
(T01-T13 already completed and saved). Guarded for Windows spawn."""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main():
    from tests.run_stress_suite import (
        CASES, CATALOG, OUT, write_users, set_env, _summ, _run_t15)
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    from core.users import reset_user_pnl

    users = ROOT / "config" / "users.json"
    orig = users.read_text(encoding="utf-8")
    existing = json.loads(OUT.read_text(encoding="utf-8")) if OUT.exists() else []
    results = list(existing)
    skip = {f"T{n:02d}" for n in range(1, 14)}

    try:
        for c in CASES:
            if c["id"] in skip:
                continue
            t0 = time.time()
            rec = dict(id=c["id"], title=c["title"], instruments=c["instruments"],
                       range=c["range"], env=c["env"], user=c["user"])
            print(f"\n{'='*70}\n{c['id']} - {c['title']}\n{'='*70}", flush=True)
            try:
                write_users(c["user"]); set_env(c["env"])
                reset_user_pnl(); clear_cross_portfolio_bus()
                uid = c["user"]["user_id"]
                if c["builder"] == "MULTI":
                    rs = _run_t15(portfolio_from_dict, run_portfolio_backtest, uid)
                    rec["status"] = "COMPLETED"; rec["results"] = [_summ(x) for x in rs]
                    ok, note = c["verdict"](rs)
                else:
                    r = run_portfolio_backtest(CATALOG, portfolio_from_dict(c["builder"]()),
                                               user_id=uid)
                    rec["status"] = "COMPLETED"; rec["result"] = _summ(r)
                    ok, note = c["verdict"](r)
                rec["verdict"] = "PASS" if ok else "FAIL"; rec["note"] = note
            except Exception as e:
                rec["status"] = "ERROR"; rec["error"] = f"{type(e).__name__}: {e}"
                rec["trace"] = traceback.format_exc()[-1400:]
                rec["verdict"] = "DEGRADED-OK" if c["id"] == "T08" else "FAIL"
                rec["note"] = "raised a handled exception - see error"
            rec["seconds"] = round(time.time() - t0, 1)
            print(f"  -> {rec['status']} / {rec['verdict']} ({rec['seconds']}s)", flush=True)
            results = [x for x in results if x["id"] != c["id"]] + [rec]
            results.sort(key=lambda x: x["id"])
            OUT.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    finally:
        users.write_text(orig, encoding="utf-8")
    print(f"\nREMAINING DONE - {len(results)} total cases", flush=True)


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
