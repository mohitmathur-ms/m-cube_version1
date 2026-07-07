"""Sweep EMA Cross + 4 Moving Averages on EURUSD across multiple BID
timeframes for a single year via the /api/portfolios/backtest endpoint.

Each timeframe is materialised as its own portfolio JSON under
portfolios/_default/, run individually, and its result captured for the
side-by-side summary printed at the end.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests


REPO = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = REPO / "portfolios" / "_default" / "FX_9_Slot_2015_2024.json"
OUT_DIR = REPO / "portfolios" / "_default"

URL = "http://localhost:5000/api/portfolios/backtest"
USER_ID = "_default"
HEADERS = {"X-User-Id": USER_ID, "Content-Type": "application/json"}

# Single year run per user spec.
YEARS = [2024]

# Per-leg exit widths (percentages of entry price).
SL_PCT = 0.2
TP_PCT = 1.0

# Each entry: (short label used in the portfolio name, full Nautilus bar type).
TIMEFRAMES: list[tuple[str, str]] = [
    ("1MIN",  "EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL"),
    ("5MIN",  "EURUSD.FOREX_MS-5-MINUTE-BID-EXTERNAL"),
    ("15MIN", "EURUSD.FOREX_MS-15-MINUTE-BID-EXTERNAL"),
    ("30MIN", "EURUSD.FOREX_MS-30-MINUTE-BID-EXTERNAL"),
    ("1H",    "EURUSD.FOREX_MS-1-HOUR-BID-EXTERNAL"),
    ("2H",    "EURUSD.FOREX_MS-2-HOUR-BID-EXTERNAL"),
]


def build_portfolio(template: dict, year: int, tf_label: str, bar_type: str) -> dict:
    cfg = json.loads(json.dumps(template))  # deep copy
    cfg["name"] = f"EURUSD_EMAxMA_{year}_{tf_label}"
    cfg["description"] = (
        f"EMA Cross + 4 Moving Averages on {bar_type}, year {year} "
        f"(01-01 to 31-12), per-leg SL={SL_PCT}% TP={TP_PCT}% (Square_off), "
        f"portfolio SL/TP disabled, lots=0.01."
    )
    cfg["start_date"] = f"{year}-01-01"
    cfg["end_date"] = f"{year}-12-31"
    cfg["squareoff_time"] = None

    # Portfolio-level SL/TP: disabled (Combined SL=0, TP=0, action N/A).
    cfg["pf_sl_enabled"] = False
    cfg["pf_sl_value"] = 0
    cfg["pf_tgt_enabled"] = False
    cfg["pf_tgt_value"] = 0

    # Leg-level: bar_type per row, SL/TP per the SL_PCT / TP_PCT constants,
    # on_sl_action / on_target_action = "close" (Square_off).
    for slot in cfg.get("slots", []):
        slot["bar_type_str"] = bar_type
        ec = slot.get("exit_config") or {}
        ec["stop_loss_type"] = "percentage"
        ec["stop_loss_value"] = SL_PCT
        ec["target_type"] = "percentage"
        ec["target_value"] = TP_PCT
        ec["on_sl_action"] = "close"
        ec["on_target_action"] = "close"
        ec["max_re_executions"] = 0
        slot["exit_config"] = ec

    return cfg


def save_portfolio(cfg: dict) -> Path:
    path = OUT_DIR / f"{cfg['name']}.json"
    path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    return path


def run_one(cfg: dict, tag: str) -> dict:
    """POST to the streaming backtest endpoint and return the final 'complete' result."""
    print(f"\n=== {tag}: POST /api/portfolios/backtest ===", flush=True)
    t0 = time.time()
    resp = requests.post(URL, headers=HEADERS, json={"portfolio": cfg}, stream=True, timeout=None)
    resp.raise_for_status()

    last_event = None
    for raw in resp.iter_lines(decode_unicode=True):
        if not raw:
            continue
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            continue
        ev = event.get("event", "")
        if ev == "complete":
            last_event = event
            elapsed = time.time() - t0
            results = event.get("results", {})
            fb = results.get("final_balance")
            pnl = results.get("total_pnl")
            trades = results.get("total_trades")
            report = results.get("report_file")
            print(
                f"  [{tag}] DONE in {elapsed:.1f}s | "
                f"final_balance=${fb:,.2f} | total_pnl=${pnl:,.2f} | "
                f"trades={trades} | report={report}",
                flush=True,
            )
        elif ev == "error":
            print(f"  [{tag}] ERROR: {event.get('error')}", flush=True)
    return last_event or {}


def main() -> int:
    if not TEMPLATE_PATH.exists():
        print(f"Template not found: {TEMPLATE_PATH}", file=sys.stderr)
        return 1

    template = json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))

    summary = []
    for year in YEARS:
        for tf_label, bar_type in TIMEFRAMES:
            cfg = build_portfolio(template, year, tf_label, bar_type)
            path = save_portfolio(cfg)
            print(f"Saved {path.name}", flush=True)
            tag = f"{year}/{tf_label}"
            result = run_one(cfg, tag)
            r = result.get("results", {}) if result else {}
            summary.append({
                "year": year,
                "timeframe": tf_label,
                "bar_type": bar_type,
                "final_balance": r.get("final_balance"),
                "total_pnl": r.get("total_pnl"),
                "total_trades": r.get("total_trades"),
                "report_file": r.get("report_file"),
            })

    print("\n=== Summary ===", flush=True)
    print(
        f"{'Year':<6} {'TF':<7} {'Trades':<8} {'PnL':>14} "
        f"{'Final Balance':>16}  Report",
        flush=True,
    )
    for s in summary:
        pnl = s.get("total_pnl")
        fb = s.get("final_balance")
        pnl_s = f"${pnl:,.2f}" if isinstance(pnl, (int, float)) else "-"
        fb_s = f"${fb:,.2f}" if isinstance(fb, (int, float)) else "-"
        print(
            f"{s['year']:<6} {s['timeframe']:<7} "
            f"{str(s.get('total_trades') or '-'):<8} "
            f"{pnl_s:>14} {fb_s:>16}  {s.get('report_file') or '-'}",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
