"""Cross-portfolio SqOff Other Portfolio — live, same-session test.

Two portfolios in ONE session engine:
  A — tight Combined-Loss pf-SL that breaches; on breach fires SqOff -> B.
  B — its OWN pf SL/TP disabled, so B only ever stops because A force-squares it.

We run the session twice with the SAME breaching A and compare B's trade count:
  • cross_pf_verb="sqoff"  → A's breach force-squares B  → B cut short
  • cross_pf_verb=""        → A breaches but does NOT touch B → B runs freely
If B(with) < B(without), the live cross-portfolio SqOff demonstrably fired in-session.

Run: venv\\Scripts\\python.exe crosspf_live_test.py
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


def _run_session(A, B, cross):
    argsA, specA, mapA = _common_args(A, "catalog", "custom_strategies", "_default")
    argsB, specB, mapB = _common_args(B, "catalog", "custom_strategies", "_default")
    if cross:
        specA["cross_pf_target"] = B.name
        specA["cross_pf_verb"] = "sqoff"
    merged = dict(argsA)
    merged["slot_capital_pairs"] = list(argsA["slot_capital_pairs"]) + list(argsB["slot_capital_pairs"])
    merged["portfolio_name"] = "XPF_SESSION"
    res, _ = _run_portfolio_unified(
        session_pf_specs=[specA, specB], slot_pf_ids={**mapA, **mapB}, **merged)
    return _summary(res)


def main():
    base = json.loads(Path("portfolios/_default/CRYPTO_REEXEC_ENTRY.json").read_text(encoding="utf-8"))

    _trig = os.environ.get("CROSSPF_TRIG", "sl")     # "sl" | "target" — what breaches A
    _shared = bool(os.environ.get("CROSSPF_SHARED"))  # B on ETHUSD (shared) vs SOLUSD (disjoint)

    def _mut_a(d):
        # A holds ONLY ETHUSD. A breaches early (tight) → fires the cross-pf verb at B.
        d["slots"] = [s for s in d["slots"] if "eth" in s["slot_id"].lower()]
        if _trig == "target":
            d.update(pf_tgt_enabled=True, pf_tgt_type="Combined Profit", pf_tgt_value=0.01,
                     pf_tgt_action="SqOff", pf_sl_enabled=False)
        else:
            d.update(pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=0.01,
                     pf_sl_value_is_pct=False, pf_sl_action="SqOff")

    def _mut_b(d):
        # B's OWN SL/TP OFF so B only ever stops if A force-squares it cross-portfolio.
        # SOLUSD (disjoint) by default; ETHUSD (shared with A) when CROSSPF_SHARED.
        _coin = "eth" if _shared else "sol"
        d["slots"] = [s for s in d["slots"] if _coin in s["slot_id"].lower()]
        d.update(pf_sl_enabled=False, pf_tgt_enabled=False)

    A = _mk(base, "PFA", "_A", _mut_a)
    B = _mk(base, "PFB", "_B", _mut_b)
    bslot = B.enabled_slots[0].slot_id
    aslot = A.enabled_slots[0].slot_id

    with_x = _run_session(A, B, cross=True)
    without_x = _run_session(A, B, cross=False)
    # Use B's full (count, PnL): a single cross-pf SqOff may force-close + let B
    # re-enter to the SAME count, but the forced close still changes B's PnL (and,
    # on a shared instrument, perturbs A) — so compare the whole tuple, not count.
    bw = with_x.get(bslot, (0, 0.0))
    bo = without_x.get(bslot, (0, 0.0))
    # Diagnostic: A's own behaviour. A's pf SL/Target SqOff squares A itself when it
    # breaches (regardless of cross), so A's trade count reveals whether A breached.
    print(f"  [diag] A('{aslot}') = with={with_x.get(aslot)} without={without_x.get(aslot)}")
    # B's own SL/TP are OFF, so the ONLY thing that can change B between the two runs
    # is A's cross-portfolio SqOff firing on B's legs same-session. Identical A breaches
    # both times; the sole difference is cross_pf_verb. So bw != bo ⇒ the cross-pf SqOff
    # demonstrably reached B and force-closed its position(s).
    ok = bw != bo
    print(f"B('{bslot}') trades:  with cross-pf SqOff = {bw}   without (A breaches but no cross-pf) = {bo}")
    print(f"[{'PASS' if ok else 'FAIL'}] cross-portfolio SqOff "
          f"{'reached B live, same-session (B behaviour changed only due to the cross-pf verb)' if ok else 'had NO effect on B'}")
    print("CROSSPF_OK" if ok else "CROSSPF_FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
