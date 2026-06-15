"""Cross-portfolio Execute / Start Other Portfolio — live, same-session test.

Two portfolios in ONE session engine:
  A — ETHUSD, tight Combined-Loss pf-SL that breaches early; on breach fires the
      cross-portfolio verb (execute / start) at B.
  B — SOLUSD, started DORMANT (exit_config.armed_at_start=False) so its signal
      never opens a position on its own. Its own pf SL/TP are off too.

B can ONLY ever trade if A's cross-portfolio Execute/Start verb arms it live,
same-session. So:
  • with cross verb  → B armed by A → B trades  (count > 0)
  • without cross    → B stays dormant forever  → B trades == 0
B(with) > 0 and B(without) == 0  ⇒  the live cross-portfolio arm demonstrably fired.

Run: venv\\Scripts\\python.exe crosspf_execute_test.py [execute|start]
"""
import os
import sys
import json
import copy
from pathlib import Path

os.environ.setdefault("_USE_UNIFIED_ENGINE", "1")

from core.models import portfolio_from_dict
from core.backtest_runner.unified import _run_portfolio_unified
from multiportfolio_session_parity_test import _common_args, _summary


def _mk(base, name, suffix, mut):
    d = copy.deepcopy(base)
    d["name"] = name
    for s in d.get("slots", []):
        s["slot_id"] = s["slot_id"] + suffix
    mut(d)
    return portfolio_from_dict(d)


def _run_session(A, B, verb):
    argsA, specA, mapA = _common_args(A, "catalog", "custom_strategies", "_default")
    argsB, specB, mapB = _common_args(B, "catalog", "custom_strategies", "_default")
    if verb:
        specA["cross_pf_target"] = B.name
        specA["cross_pf_verb"] = verb
    merged = dict(argsA)
    merged["slot_capital_pairs"] = list(argsA["slot_capital_pairs"]) + list(argsB["slot_capital_pairs"])
    merged["portfolio_name"] = "XPF_EXEC_SESSION"
    res, _ = _run_portfolio_unified(
        session_pf_specs=[specA, specB], slot_pf_ids={**mapA, **mapB}, **merged)
    return _summary(res)


def main():
    verb = sys.argv[1] if len(sys.argv) > 1 else "execute"
    base = json.loads(Path("portfolios/_default/CRYPTO_REEXEC_ENTRY.json").read_text(encoding="utf-8"))

    def _mut_a(d):
        # A holds ONLY ETHUSD, breaches early, fires the cross-pf verb at B.
        d["slots"] = [s for s in d["slots"] if "eth" in s["slot_id"].lower()]
        d.update(pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=0.01,
                 pf_sl_value_is_pct=False, pf_sl_action="SqOff")

    def _mut_b(d):
        # B holds ONLY SOLUSD, starts DORMANT — armed_at_start=False — and has no
        # own pf SL/TP. The only way B opens a position is A arming it cross-pf.
        d["slots"] = [s for s in d["slots"] if "sol" in s["slot_id"].lower()]
        for s in d["slots"]:
            s["exit_config"]["armed_at_start"] = False
        d.update(pf_sl_enabled=False, pf_tgt_enabled=False)

    A = _mk(base, "PFA", "_A", _mut_a)
    B = _mk(base, "PFB", "_B", _mut_b)
    bslot = B.enabled_slots[0].slot_id

    with_x = _run_session(A, B, verb)
    without_x = _run_session(A, B, "")
    bw = with_x.get(bslot, (0, 0.0))[0]
    bo = without_x.get(bslot, (0, 0.0))[0]
    ok = (bw > 0) and (bo == 0)
    print(f"verb='{verb}'  B('{bslot}') trades:  with cross verb = {bw}   without (dormant) = {bo}")
    print(f"[{'PASS' if ok else 'FAIL'}] cross-portfolio {verb} "
          f"{'armed B live, same-session (B was dormant and only traded because A armed it)' if ok else 'did NOT arm B'}")
    print(f"CROSSPF_{verb.upper()}_OK" if ok else f"CROSSPF_{verb.upper()}_FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
