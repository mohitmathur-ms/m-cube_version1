"""
Per-backtest aggregation_reports — path layout, JSON ledger, and the
single global HTML report rebuilt from the ledger on every backtest.

Output layout::

    aggregation_reports/
      _ledger.json
      combined_report.html
      {asset_class}/{symbol}/
        raw_engine_sniffer_{TIMEFRAME}_{DDMMMYYYY}_{DDMMMYYYY}.csv
        sniffer_data_engine_{TIMEFRAME}_{DDMMMYYYY}_{DDMMMYYYY}.csv
        sniffer_strategy_{TIMEFRAME}_{DDMMMYYYY}_{DDMMMYYYY}.csv

CSVs are written by :mod:`core.sniffers`; this module owns paths,
the ledger, and the HTML.

All public functions swallow + log their own errors. A report-pipeline
failure must never propagate out and kill a backtest.
"""

from __future__ import annotations

import html
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from core.venue_config import (
    load_adapter_config_for_bar_type,
    symbol_from_bar_type,
    venue_from_bar_type,
)


logger = logging.getLogger(__name__)


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_ROOT = _PROJECT_ROOT / "aggregation_reports"
LEDGER_PATH = REPORTS_ROOT / "_ledger.json"
COMBINED_HTML_PATH = REPORTS_ROOT / "combined_report.html"


# Map Nautilus's "1-MINUTE" / "5-HOUR" / etc. tokens to the compact tags used
# in the dumped CSV filenames. Keeps filenames short while staying unambiguous
# (the existing prior-art files in csv/ use "1min", "5min" — we normalize
# to upper-case to match the consolidated-FX naming convention).
_UNIT_COMPACT = {
    "MINUTE": "MIN",
    "MIN": "MIN",
    "HOUR": "H",
    "HR": "H",
    "DAY": "D",
    "WEEK": "WK",
    "MONTH": "MO",
    "SECOND": "S",
    "TICK": "T",
}


def _compact_timeframe(timeframe: str) -> str:
    """``"1-MINUTE"`` → ``"1MIN"``; ``"15-MINUTE"`` → ``"15MIN"``."""
    if not timeframe:
        return "NA"
    m = re.match(r"^\s*(\d+)\s*-\s*([A-Za-z]+)\s*$", timeframe)
    if not m:
        return timeframe.upper().replace("-", "")
    n, unit = m.group(1), m.group(2).upper()
    return f"{n}{_UNIT_COMPACT.get(unit, unit)}"


def parse_bar_type(bar_type_str: str) -> dict[str, str | None]:
    """Pull the parts out of ``EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL`` (or the
    ``5-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL`` form).

    Returns a dict with keys: symbol, venue, timeframe, price_type,
    aggregation_source ('EXTERNAL' | 'INTERNAL').
    """
    if not bar_type_str:
        return {"symbol": None, "venue": None, "timeframe": None,
                "price_type": None, "aggregation_source": None}
    instr_part, _, rest = bar_type_str.partition("-")  # "EURUSD.FOREX_MS", "1-MINUTE-BID-EXTERNAL[@...]"
    symbol = venue = None
    if "." in instr_part:
        symbol, venue = instr_part.split(".", 1)
    else:
        symbol = instr_part or None
    # Split the rest into the four ordered fields.
    parts = rest.split("-", 3)
    if len(parts) < 4:
        return {"symbol": symbol, "venue": venue, "timeframe": None,
                "price_type": None, "aggregation_source": None}
    n, unit, price_type, source_tail = parts[0], parts[1], parts[2], parts[3]
    # source_tail is "EXTERNAL" or "INTERNAL@1-MINUTE-EXTERNAL"
    source = "INTERNAL" if source_tail.upper().startswith("INTERNAL") else "EXTERNAL"
    return {
        "symbol": symbol,
        "venue": venue,
        "timeframe": f"{n}-{unit}",
        "price_type": price_type,
        "aggregation_source": source,
    }


def asset_class_for(bar_type_str: str) -> str:
    """Look up the venue's adapter config and return its ``asset_class``.

    Falls back to ``"unknown"`` if no matching adapter is configured (mirrors
    the existing FxRateResolver / venue_config contract).
    """
    cfg = load_adapter_config_for_bar_type(bar_type_str)
    if isinstance(cfg, dict):
        ac = cfg.get("asset_class")
        if isinstance(ac, str) and ac.strip():
            return ac.strip().lower().replace(" ", "_")
    return "unknown"


def _fmt_ddmmmyyyy(date_str: str | None) -> str:
    """``"2015-01-01"`` → ``"01JAN2015"``. Falls back to ``"NA"`` on bad input."""
    if not date_str:
        return "NA"
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        return d.strftime("%d%b%Y").upper()
    except Exception:
        return "NA"


def paths_for(
    asset_class: str,
    symbol: str,
    timeframe_external: str,
    timeframe_internal: str | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Path]:
    """Return absolute paths for the three sniffer CSVs.

    The ``sniffer_strategy`` CSV is tagged with the INTERNAL timeframe
    if there is one, else falls back to the EXTERNAL timeframe (in which
    case the file is expected to contain only the header — diagnostic of
    a strategy that doesn't use any aggregations).
    """
    out_dir = REPORTS_ROOT / asset_class / symbol
    range_tag = f"{_fmt_ddmmmyyyy(start_date)}_{_fmt_ddmmmyyyy(end_date)}"
    ext_tag = _compact_timeframe(timeframe_external)
    strat_tf = timeframe_internal or timeframe_external
    strat_tag = _compact_timeframe(strat_tf)
    return {
        "raw_engine_sniffer": out_dir / f"raw_engine_sniffer_{ext_tag}_{range_tag}.csv",
        "sniffer_data_engine": out_dir / f"sniffer_data_engine_{ext_tag}_{range_tag}.csv",
        "sniffer_strategy": out_dir / f"sniffer_strategy_{strat_tag}_{range_tag}.csv",
    }


def _row_count_or_zero(path: Path) -> int:
    """Count CSV data rows (excluding the header). Returns 0 if missing."""
    try:
        if not path.exists():
            return 0
        with open(path, "r", encoding="utf-8") as fh:
            # Skip header
            next(fh, None)
            return sum(1 for _ in fh)
    except Exception:
        return 0


def _safe_metrics(result: dict | None) -> dict:
    """Pluck a small set of headline metrics from a result dict."""
    if not isinstance(result, dict):
        return {}
    keys = (
        "net_pnl", "total_trades", "win_rate",
        "flat_trades", "decisive_win_rate",
        "max_drawdown", "sharpe_ratio", "warning",
    )
    out = {}
    for k in keys:
        if k in result:
            out[k] = result[k]
    return out


def _load_ledger() -> list[dict]:
    if not LEDGER_PATH.exists():
        return []
    try:
        with open(LEDGER_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, list) else []
    except Exception:
        logger.exception("aggregation_report: failed to read ledger; starting fresh")
        return []


def _save_ledger(rows: list[dict]) -> None:
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = LEDGER_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2, default=str)
    os.replace(tmp, LEDGER_PATH)


def record_run(
    *,
    run_id: str,
    run_kind: str,
    user_id: str | None,
    portfolio_name: str | None,
    bar_type_external: str,
    bar_type_internal: str | None,
    start_date: str | None,
    end_date: str | None,
    csv_paths: dict[str, Path | str],
    result: dict | None,
) -> None:
    """Append one row to the ledger and re-render the combined HTML."""
    try:
        ext = parse_bar_type(bar_type_external)
        symbol = ext.get("symbol") or "unknown"
        venue = ext.get("venue") or "unknown"
        timeframe_external = ext.get("timeframe") or "unknown"
        timeframe_internal = None
        if bar_type_internal:
            timeframe_internal = parse_bar_type(bar_type_internal).get("timeframe")
        asset_class = asset_class_for(bar_type_external)

        row_counts = {name: _row_count_or_zero(Path(p)) for name, p in csv_paths.items()}

        # Store CSV paths as relative to the project root when possible —
        # keeps the ledger portable across machines and renders nicer in
        # the HTML.
        rel_csv_paths: dict[str, str] = {}
        for name, p in csv_paths.items():
            pp = Path(p)
            try:
                rel = pp.resolve().relative_to(_PROJECT_ROOT)
                rel_csv_paths[name] = str(rel).replace("\\", "/")
            except Exception:
                rel_csv_paths[name] = str(pp)

        row = {
            "run_id": run_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "run_kind": run_kind,
            "portfolio_name": portfolio_name,
            "asset_class": asset_class,
            "symbol": symbol,
            "venue": venue,
            "timeframe_external": timeframe_external,
            "timeframe_internal": timeframe_internal,
            "start_date": start_date,
            "end_date": end_date,
            "csv_paths": rel_csv_paths,
            "row_counts": row_counts,
            "metrics": _safe_metrics(result),
        }
        ledger = _load_ledger()
        ledger.append(row)
        _save_ledger(ledger)
        _render_combined_html(ledger)
    except Exception:
        logger.exception("aggregation_report.record_run failed (run_id=%s)", run_id)


def record_runs(rows: Iterable[dict]) -> None:
    """Batch variant: append many rows from one backtest (e.g. a portfolio
    with N slots sharing one ``run_id``) and re-render the HTML once."""
    try:
        prepared: list[dict] = []
        for r in rows:
            bar_type_external = r["bar_type_external"]
            ext = parse_bar_type(bar_type_external)
            symbol = ext.get("symbol") or "unknown"
            venue = ext.get("venue") or "unknown"
            timeframe_external = ext.get("timeframe") or "unknown"
            timeframe_internal = None
            if r.get("bar_type_internal"):
                timeframe_internal = parse_bar_type(r["bar_type_internal"]).get("timeframe")
            asset_class = asset_class_for(bar_type_external)

            csv_paths = {k: Path(v) for k, v in r["csv_paths"].items()}
            row_counts = {name: _row_count_or_zero(p) for name, p in csv_paths.items()}
            rel_csv_paths: dict[str, str] = {}
            for name, p in csv_paths.items():
                try:
                    rel = p.resolve().relative_to(_PROJECT_ROOT)
                    rel_csv_paths[name] = str(rel).replace("\\", "/")
                except Exception:
                    rel_csv_paths[name] = str(p)

            prepared.append({
                "run_id": r["run_id"],
                "ts": datetime.now(timezone.utc).isoformat(),
                "user_id": r.get("user_id"),
                "run_kind": r.get("run_kind", "portfolio"),
                "portfolio_name": r.get("portfolio_name"),
                "asset_class": asset_class,
                "symbol": symbol,
                "venue": venue,
                "timeframe_external": timeframe_external,
                "timeframe_internal": timeframe_internal,
                "start_date": r.get("start_date"),
                "end_date": r.get("end_date"),
                "csv_paths": rel_csv_paths,
                "row_counts": row_counts,
                "metrics": _safe_metrics(r.get("result")),
            })
        if not prepared:
            return
        ledger = _load_ledger()
        ledger.extend(prepared)
        _save_ledger(ledger)
        _render_combined_html(ledger)
    except Exception:
        logger.exception("aggregation_report.record_runs failed")


# ─── HTML render ──────────────────────────────────────────────────────────────

_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>m-cube aggregation_reports</title>
<style>
  :root {{
    --bg: #0f1115; --panel: #161a22; --border: #262b36;
    --text: #e6e8ee; --muted: #8a93a6; --accent: #5aa9ff;
    --ok: #4caf50; --warn: #f0ad4e; --bad: #e74c3c;
  }}
  body {{
    margin: 0; padding: 24px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg); color: var(--text);
  }}
  h1 {{ margin: 0 0 4px; font-size: 22px; }}
  .sub {{ color: var(--muted); margin-bottom: 24px; }}
  .group {{ margin-top: 28px; }}
  .group h2 {{
    font-size: 15px; text-transform: uppercase; letter-spacing: 0.08em;
    color: var(--accent); border-bottom: 1px solid var(--border);
    padding-bottom: 6px; margin-bottom: 12px;
  }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(420px, 1fr)); gap: 14px; }}
  .card {{
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 8px; padding: 14px 16px;
  }}
  .card .hdr {{ display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 8px; }}
  .card .hdr .sym {{ font-weight: 600; font-size: 16px; }}
  .card .hdr .tf  {{ color: var(--muted); font-size: 12px; }}
  .card .meta {{ color: var(--muted); font-size: 12px; margin-bottom: 8px; }}
  .card .flow {{
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 11px; color: var(--muted);
    background: rgba(255,255,255,0.03); padding: 6px 8px;
    border-radius: 4px; margin: 8px 0;
  }}
  .card ul.csvs {{ list-style: none; padding: 0; margin: 8px 0; font-size: 12px; }}
  .card ul.csvs li {{ display: flex; justify-content: space-between; padding: 2px 0; }}
  .card ul.csvs a {{ color: var(--accent); text-decoration: none; }}
  .card ul.csvs a:hover {{ text-decoration: underline; }}
  .card ul.csvs .empty {{ color: var(--warn); }}
  .card .metrics {{
    display: grid; grid-template-columns: 1fr 1fr; gap: 4px 14px;
    font-size: 12px; margin-top: 8px;
  }}
  .card .metrics .k {{ color: var(--muted); }}
  .card .warn {{ color: var(--warn); font-size: 12px; margin-top: 6px; }}
</style>
</head>
<body>
<h1>m-cube aggregation_reports</h1>
<div class="sub">{total_runs} run{plural} captured across {asset_count} asset class{asset_plural}. Generated {generated_at}.</div>
{body}
<script type="application/json" id="ledger">{ledger_json}</script>
</body>
</html>
"""


def _render_card(row: dict) -> str:
    sym = html.escape(str(row.get("symbol") or "?"))
    venue = html.escape(str(row.get("venue") or "?"))
    tf_ext = html.escape(str(row.get("timeframe_external") or "?"))
    tf_int = row.get("timeframe_internal")
    tf_label = f"{tf_ext}" + (f" → {html.escape(str(tf_int))}" if tf_int else "")
    ts = html.escape(str(row.get("ts") or ""))
    kind = html.escape(str(row.get("run_kind") or "?"))
    pname = row.get("portfolio_name")
    pname_html = f"  ·  <span title=\"portfolio\">{html.escape(str(pname))}</span>" if pname else ""
    sdate = html.escape(str(row.get("start_date") or "?"))
    edate = html.escape(str(row.get("end_date") or "?"))

    csvs = row.get("csv_paths") or {}
    counts = row.get("row_counts") or {}
    csv_items = []
    for name in ("raw_engine_sniffer", "sniffer_data_engine", "sniffer_strategy"):
        href = csvs.get(name)
        rc = counts.get(name, 0)
        if href:
            cls = "" if rc > 0 else "empty"
            label = html.escape(name)
            url = html.escape(href)
            csv_items.append(
                f"<li><a class='{cls}' href=\"{url}\">{label}</a>"
                f"<span class='{cls}'>{rc:,} rows</span></li>"
            )
        else:
            csv_items.append(
                f"<li><span class='empty'>{html.escape(name)}</span>"
                f"<span class='empty'>missing</span></li>"
            )

    metrics = row.get("metrics") or {}
    metric_rows = []
    for k in ("net_pnl", "total_trades", "win_rate",
              "decisive_win_rate", "flat_trades",
              "max_drawdown", "sharpe_ratio"):
        if k in metrics:
            v = metrics[k]
            if isinstance(v, float):
                vs = f"{v:.4f}" if abs(v) < 1 else f"{v:,.2f}"
            else:
                vs = str(v)
            metric_rows.append(
                f"<div class='k'>{html.escape(k)}</div>"
                f"<div class='v'>{html.escape(vs)}</div>"
            )
    warning = metrics.get("warning")
    warn_html = (
        f"<div class='warn'>⚠ {html.escape(str(warning))}</div>" if warning else ""
    )

    return (
        "<div class='card'>"
        f"<div class='hdr'><span class='sym'>{sym}.{venue}</span>"
        f"<span class='tf'>{tf_label}</span></div>"
        f"<div class='meta'>{ts}  ·  {kind}{pname_html}  ·  {sdate} → {edate}</div>"
        "<div class='flow'>engine.data → DataEngine → TimeBarAggregator → Strategy</div>"
        f"<ul class='csvs'>{''.join(csv_items)}</ul>"
        f"<div class='metrics'>{''.join(metric_rows)}</div>"
        f"{warn_html}"
        "</div>"
    )


def _render_combined_html(ledger: list[dict]) -> None:
    try:
        # Group rows by asset_class for the section layout.
        buckets: dict[str, list[dict]] = {}
        for row in ledger:
            ac = str(row.get("asset_class") or "unknown")
            buckets.setdefault(ac, []).append(row)
        # Sort: newest run last within each bucket (chronological).
        for rows in buckets.values():
            rows.sort(key=lambda r: r.get("ts", ""))

        sections: list[str] = []
        for asset_class in sorted(buckets.keys()):
            rows = buckets[asset_class]
            cards = "".join(_render_card(r) for r in rows)
            sections.append(
                f"<div class='group'><h2>Asset class: {html.escape(asset_class)} "
                f"<span style='color:var(--muted);font-weight:400'>"
                f"({len(rows)} run{'s' if len(rows) != 1 else ''})</span></h2>"
                f"<div class='grid'>{cards}</div></div>"
            )

        total = len(ledger)
        asset_count = len(buckets)
        html_text = _HTML_TEMPLATE.format(
            total_runs=total,
            plural="s" if total != 1 else "",
            asset_count=asset_count,
            asset_plural="es" if asset_count != 1 else "",
            generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
            body="".join(sections) if sections else "<div class='sub'>No runs yet.</div>",
            ledger_json=json.dumps(ledger, default=str),
        )
        COMBINED_HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = COMBINED_HTML_PATH.with_suffix(".html.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(html_text)
        os.replace(tmp, COMBINED_HTML_PATH)
    except Exception:
        logger.exception("aggregation_report: failed to render combined_report.html")


# ─── helpers used by core/backtest_runner.py ──────────────────────────────────

def derive_sniffer_paths_for_bar_type(
    bar_type_str: str,
    extra_bar_types: Iterable[str] | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Path]:
    """Convenience: parse a primary bar_type + optional extras, find the
    INTERNAL aggregation timeframe (if any), and return the three CSV paths.

    Designed to be called inside backtest workers right before
    ``engine.add_strategy(BarSniffer(...))`` and ``dump_raw_engine(...)``.
    """
    primary = parse_bar_type(bar_type_str)
    symbol = primary.get("symbol") or "unknown"
    asset_class = asset_class_for(bar_type_str)
    timeframe_external = primary.get("timeframe") or "unknown"

    timeframe_internal: str | None = None
    for bt in (extra_bar_types or ()):
        info = parse_bar_type(bt)
        if info.get("aggregation_source") == "INTERNAL":
            timeframe_internal = info.get("timeframe")
            break

    return paths_for(
        asset_class=asset_class,
        symbol=symbol,
        timeframe_external=timeframe_external,
        timeframe_internal=timeframe_internal,
        start_date=start_date,
        end_date=end_date,
    )


def pick_internal_bar_type(bar_types: Iterable[str]) -> str | None:
    """Return the first bar_type string with INTERNAL aggregation, or None."""
    for bt in bar_types:
        if parse_bar_type(bt).get("aggregation_source") == "INTERNAL":
            return bt
    return None
