"""HETEROGENEOUS multi-portfolio settings-parity test.

Proves that EVERY existing leg-level and portfolio-level setting still produces
the CORRECT result when many DIFFERENT portfolios run together in ONE session
engine — by comparing each portfolio's per-leg result in the session against the
SAME portfolio run standalone (the trusted single-portfolio unified path).

Default set (all NIFTY_SPOT, same range/strategy, differ ONLY in settings — so
they all SHARE the instrument → this also stresses the per-position scoped-P&L
path with 5 same-instrument legs the monitor must disentangle):
  BXT_01  leg SL + leg Target
  BXT_02  portfolio squareoff (time-based force-close)
  BXT_03  leg trailing SL
  BXT_05  leg reverse-on-SL action
  BXT_10  portfolio SL + portfolio Target  (scoped monitor on a shared instrument)

Each portfolio is namespaced (name + slot ids) so the 5 legs are distinct in the
shared engine. PASS = for every leg, session result == standalone result, exact.

Run: venv\\Scripts\\python.exe multiportfolio_heterogeneous_test.py [file.json ...]
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

DEFAULT_SET = [
    "BXT_01_sl_tgt_5m", "BXT_02_squareoff_5m", "BXT_03_trailing_5m",
    "BXT_05_sl_reverse_5m", "BXT_10_pf_sl_tgt_5m",
]


def _load(tag, name):
    """Load a portfolio, namespace its name + slot ids by tag so legs stay
    distinct inside the shared engine."""
    d = json.loads(Path(f"portfolios/_default/{name}.json").read_text(encoding="utf-8"))
    d["name"] = f"{tag}_{d.get('name') or name}"
    for s in d.get("slots", []):
        s["slot_id"] = f"{tag}_{s['slot_id']}"
    _cap = os.environ.get("MPH_CAPITAL")
    if _cap:
        d["starting_capital"] = float(_cap)
    if os.environ.get("MPH_CLEAR_WINDOW"):
        d["entry_start_time"] = None
        d["entry_end_time"] = None
    return portfolio_from_dict(d)


def main():
    names = sys.argv[1:] or DEFAULT_SET
    names = [Path(n).stem for n in names]

    # 1) Build + run each portfolio STANDALONE (trusted single-pf unified path).
    standalone = {}          # tag -> {slot_id: (trades, pnl)}
    specs = []               # session specs
    all_pairs = []           # concatenated legs
    all_maps = {}            # slot_id -> portfolio id
    base_args = None
    for i, nm in enumerate(names):
        tag = f"P{i}"
        pf = _load(tag, nm)
        args, spec, smap = _common_args(pf, "catalog", "custom_strategies", "_default")
        standalone[tag] = _summary(_run_portfolio_unified(**args)[0])
        if base_args is None:
            base_args = args
        specs.append(spec)
        all_pairs += list(args["slot_capital_pairs"])
        all_maps.update(smap)

    # 2) Run ALL portfolios together in ONE session engine.
    merged = dict(base_args)
    merged["slot_capital_pairs"] = all_pairs
    merged["portfolio_name"] = "HETERO_SESSION"
    sess = _summary(_run_portfolio_unified(
        session_pf_specs=specs, slot_pf_ids=all_maps, **merged)[0])

    # 3) Per-leg parity: session must reproduce each standalone result, exact.
    ok_all = True
    for i, nm in enumerate(names):
        tag = f"P{i}"
        ok = True
        for sid, val in standalone[tag].items():
            got = sess.get(sid)
            if got != val:
                ok = False
                print(f"    DIVERGE {nm} / {sid}: standalone={val}  session={got}")
        ok_all &= ok
        print(f"[{'PASS' if ok else 'FAIL'}] {nm:24s} standalone={standalone[tag]}  session={{"
              + ", ".join(f'{s!r}: {sess.get(s)}' for s in standalone[tag]) + "}")
    print("HETERO_PARITY_OK" if ok_all else "HETERO_PARITY_FAIL")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
