"""Run EVERY testing2 portfolio through the real portfolio-backtest endpoint so
each order book is built + saved exactly like the UI does (all 16 runnable cases
incl. VP-01, so every order book carries TODAY's date for the report builder).
OT-02 / OT-03 are negative tests — they must be REJECTED at save
(_validate_portfolio_sl) and are not run.

Writes order_book_portfolio_<name>_<today>.csv under reports/testing2/ as a side
effect, and prints a one-line status per case + a JSON manifest to
portfolios/testing2/_run_manifest.json for the report builder.

Run:  venv\\Scripts\\python.exe portfolios\\testing2\\_run_all_backtests.py
"""
from __future__ import annotations
import glob
import json
import os
import sys
import time
from datetime import datetime

# Run-as-file: ensure the repo root (two levels up) is importable.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import server
from core.models import portfolio_from_dict

TODAY = datetime.now().strftime("%d_%B_%Y").lower()
H = {"X-User-Id": "testing2"}
client = server.app.test_client()

manifest = []
# Skip helper/meta files (e.g. _run_manifest.json) — only real portfolio JSONs.
files = sorted(p for p in glob.glob("portfolios/testing2/*.json")
               if not os.path.basename(p).startswith("_"))
for path in files:
    cfg = json.load(open(path, encoding="utf-8"))
    name = cfg.get("name", os.path.basename(path))
    config = portfolio_from_dict(cfg)
    label = server.sanitize_filename(name)
    ob = f"reports/testing2/order_book_portfolio_{label}_{TODAY}.csv"

    ok, err = server._validate_portfolio_sl(config)
    if not ok:
        manifest.append({"name": name, "label": label, "runnable": False,
                         "rejected": True, "reject_msg": err, "orderbook": None})
        print(f"[REJECT ] {name}: {err[:80]}")
        continue

    t0 = time.time()
    try:
        r = client.post("/api/portfolios/backtest", headers=H,
                        json={"portfolio": cfg, "catalog_path": server.CATALOG_PATH})
        r.get_data()  # drain the stream so the run completes + files flush
        dt = time.time() - t0
        exists = os.path.exists(ob)
        manifest.append({"name": name, "label": label, "runnable": True,
                         "rejected": False, "orderbook": ob if exists else None,
                         "status": r.status_code, "elapsed": round(dt, 1)})
        print(f"[{'OK' if exists else 'NO OB':6}] {name}: {dt:.1f}s -> {ob if exists else 'MISSING'}")
    except Exception as e:
        manifest.append({"name": name, "label": label, "runnable": True,
                         "rejected": False, "orderbook": None, "error": str(e)})
        print(f"[ERROR ] {name}: {e}")

with open("portfolios/testing2/_run_manifest.json", "w", encoding="utf-8") as f:
    json.dump({"today": TODAY, "cases": manifest}, f, indent=2)
print(f"\nDONE. {sum(1 for m in manifest if m.get('orderbook'))} order books, "
      f"{sum(1 for m in manifest if m.get('rejected'))} rejected. Manifest written.")
