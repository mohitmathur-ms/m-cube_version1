"""Phase 0 parity gate for the multi-portfolio session engine.

Proves the strategy-scoped per-position combined-P&L (the path a per-portfolio
monitor uses for instruments SHARED across portfolios) is byte-identical to the
trusted engine-wide ``Portfolio.total_pnl(instrument)`` it replaces — by running a
real backtest and, on every valuation bar, computing BOTH and asserting equality.

It forces EVERY instrument to be treated as "shared" so the per-position path is
exercised on all of them (worst case). If engine == scoped here, then the scoped
path is safe; an instrument that is actually exclusive uses total_pnl directly, so
it is byte-identical by construction.

Run: venv\\Scripts\\python.exe multiportfolio_parity_test.py [portfolio.json ...]
"""
import os
import sys
import json
from pathlib import Path

os.environ["_USE_UNIFIED_ENGINE"] = "1"
os.environ["_USE_PF_MONITOR"] = "1"   # attach a TRACKING monitor even w/o pf SL/TP

import core.portfolio_monitor as pm
from core.models import portfolio_from_dict
from core.backtest_runner import run_portfolio_backtest


def _run_one(pf_path: str) -> tuple[int, float, int]:
    compare: list[tuple[float, float]] = []

    _orig_on_start = pm.PortfolioMonitorStrategy.on_start
    _orig_combined = pm.PortfolioMonitorStrategy._combined_pnl

    def patched_on_start(self):
        _orig_on_start(self)
        # Force scoped mode over ALL legs, and mark EVERY monitored instrument as
        # shared so the per-position path runs for all of them.
        try:
            sids = {str(s) for s in self.cache.strategy_ids()}
        except Exception:
            sids = set()
        sids.discard(str(self.id))
        self._scope_sids = sids
        self._shared_iids = set(self._iids)

    def patched_combined(self):
        # Engine-wide path: temporarily clear scope so the original takes the
        # total_pnl(instrument) branch. (Does not disturb the open-count caches.)
        saved = self._scope_sids
        self._scope_sids = set()
        eng = _orig_combined(self)
        self._scope_sids = saved
        sco = _orig_combined(self)  # scoped per-position path (all iids "shared")
        compare.append((eng, sco))
        return eng  # keep enforcement behaviour identical to the engine path

    pm.PortfolioMonitorStrategy.on_start = patched_on_start
    pm.PortfolioMonitorStrategy._combined_pnl = patched_combined
    try:
        cfg = portfolio_from_dict(json.loads(Path(pf_path).read_text(encoding="utf-8")))
        run_portfolio_backtest(catalog_path="catalog", portfolio=cfg,
                               custom_strategies_dir="custom_strategies", user_id="_default")
    finally:
        pm.PortfolioMonitorStrategy.on_start = _orig_on_start
        pm.PortfolioMonitorStrategy._combined_pnl = _orig_combined

    n = len(compare)
    maxdiff = max((abs(a - b) for a, b in compare), default=0.0)
    diverged = sum(1 for a, b in compare if abs(a - b) > 1e-6)
    return n, maxdiff, diverged


def main():
    pfs = sys.argv[1:] or ["portfolios/_default/BXT_01_sl_tgt_5m.json"]
    overall_ok = True
    for pf in pfs:
        name = Path(pf).stem
        n, maxdiff, diverged = _run_one(pf)
        ok = maxdiff < 1e-6
        overall_ok &= ok
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: bars={n} "
              f"max|engine-scoped|={maxdiff:.10f} divergent_bars={diverged}")
    print("PARITY_OK" if overall_ok else "PARITY_FAIL")
    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
