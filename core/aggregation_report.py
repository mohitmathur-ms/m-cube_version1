"""
Per-backtest aggregation_reports — path layout, JSON ledger, and the
single global HTML report rebuilt from the ledger on every backtest.

Output layout::

    aggregation_reports/
      _ledger.json
      combined_report.html
      {asset_class}/{symbol}/{target_timeframe}/
        raw_engine_sniffer_{EXT_TIMEFRAME}_{DDMMMYYYY}_{DDMMMYYYY}.csv
        sniffer_data_engine_{EXT_TIMEFRAME}_{DDMMMYYYY}_{DDMMMYYYY}.csv
        sniffer_strategy_{TARGET_TIMEFRAME}_{DDMMMYYYY}_{DDMMMYYYY}.csv

    ``{target_timeframe}`` is the INTERNAL aggregation timeframe the strategy
    trades on (e.g. ``5MIN`` for a 1-min→5-min run), or the EXTERNAL base
    timeframe when no aggregation is used.

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

    All three CSVs are filed under a per-run **target-timeframe** directory
    (``{asset_class}/{symbol}/{TARGET_TF}/``). The target timeframe is the
    INTERNAL aggregation timeframe when there is one, else the EXTERNAL (base)
    timeframe — i.e. the resolution the strategy actually trades on. So a
    1-min→5-min run drops all three CSVs into ``.../{symbol}/5MIN/``.

    The ``sniffer_strategy`` CSV is tagged with the INTERNAL timeframe
    if there is one, else falls back to the EXTERNAL timeframe (in which
    case the file is expected to contain only the header — diagnostic of
    a strategy that doesn't use any aggregations). The raw-engine and
    data-engine CSVs keep the EXTERNAL (base) timeframe in their filename
    but live under the target-timeframe folder.
    """
    range_tag = f"{_fmt_ddmmmyyyy(start_date)}_{_fmt_ddmmmyyyy(end_date)}"
    ext_tag = _compact_timeframe(timeframe_external)
    strat_tf = timeframe_internal or timeframe_external
    strat_tag = _compact_timeframe(strat_tf)
    # Segregate every run's CSVs by the target timeframe the strategy trades on.
    out_dir = REPORTS_ROOT / asset_class / symbol / strat_tag
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

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>m-cube aggregation_reports</title>
<style>
  :root {{
    --bg: #fafbfc; --panel: #fff; --border: #e1e4e8;
    --text: #1f2328; --muted: #57606a;
    --accent: #0969da;
    --green: #1a7f37; --green-bg: #dafbe1;
    --red: #cf222e; --red-bg: #ffebe9;
    --amber: #9a6700; --amber-bg: #fff8c5;
    --purple: #8250df; --purple-bg: #fbefff;
    --blue-bg: #ddf4ff;
  }}
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; line-height: 1.55; color: var(--text); background: var(--bg); margin: 0; }}
  .container {{ max-width: 1240px; margin: 0 auto; padding: 32px 40px 80px; }}
  header.cover {{ background: linear-gradient(135deg, #0969da 0%, #8250df 100%); color: white; padding: 36px; margin: -32px -40px 28px; border-radius: 0 0 12px 12px; }}
  header.cover h1 {{ margin: 0 0 8px; font-size: 26px; }}
  header.cover .subtitle {{ margin: 0; font-size: 14px; opacity: 0.92; }}
  header.cover .meta {{ margin-top: 10px; font-size: 12px; opacity: 0.8; }}
  h2 {{ font-size: 21px; margin-top: 36px; padding-bottom: 8px; border-bottom: 2px solid var(--border); }}
  h3 {{ font-size: 16px; margin-top: 22px; }}
  h4 {{ font-size: 13px; margin-top: 18px; margin-bottom: 6px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.4px; }}
  p {{ margin: 8px 0; }}
  table {{ width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 13.5px; background: var(--panel); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }}
  th, td {{ text-align: left; padding: 9px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }}
  th {{ background: #f6f8fa; font-weight: 600; font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.3px; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover {{ background: #f9fafb; }}
  td.center, th.center {{ text-align: center; white-space: nowrap; }}
  td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }}
  code {{ font-family: "SF Mono", Monaco, Consolas, monospace; font-size: 12.5px; background: #f6f8fa; padding: 1px 6px; border-radius: 3px; color: #d63384; }}
  pre {{ background: #f6f8fa; border: 1px solid var(--border); border-radius: 6px; padding: 0; overflow-x: auto; font-family: "SF Mono", Monaco, Consolas, monospace; font-size: 12px; line-height: 1.5; margin: 0 0 12px; }}
  pre code {{ background: transparent; padding: 0; color: var(--text); }}
  pre table {{ margin: 0; border: none; border-radius: 0; font-size: 12px; }}
  pre table th {{ background: #eef1f4; }}
  pre table tr:hover {{ background: transparent; }}
  .badge {{ display: inline-block; padding: 2px 8px; font-size: 11px; font-weight: 600; border-radius: 4px; line-height: 1.4; white-space: nowrap; vertical-align: middle; }}
  .badge-ok    {{ background: var(--green-bg); color: var(--green); }}
  .badge-bad   {{ background: var(--red-bg);   color: var(--red); }}
  .badge-warn  {{ background: var(--amber-bg); color: var(--amber); }}
  .badge-info  {{ background: var(--blue-bg); color: var(--accent); }}
  .badge-purple{{ background: var(--purple-bg); color: var(--purple); }}
  .callout       {{ background: var(--blue-bg);  border-left: 4px solid var(--accent); padding: 12px 16px; margin: 12px 0; border-radius: 0 6px 6px 0; font-size: 14px; }}
  .callout-warn  {{ background: var(--amber-bg); border-left: 4px solid var(--amber);  padding: 12px 16px; margin: 12px 0; border-radius: 0 6px 6px 0; font-size: 14px; }}
  .callout-ok    {{ background: var(--green-bg); border-left: 4px solid var(--green);  padding: 12px 16px; margin: 12px 0; border-radius: 0 6px 6px 0; font-size: 14px; }}
  .callout-tech  {{ background: var(--purple-bg); border-left: 4px solid var(--purple); padding: 12px 16px; margin: 12px 0; border-radius: 0 6px 6px 0; font-size: 14px; }}
  .callout-tech strong {{ color: var(--purple); }}
  .simple        {{ background: #f6f8fa; border-left: 4px solid #57606a; padding: 12px 16px; margin: 12px 0; border-radius: 0 6px 6px 0; font-size: 14.5px; }}
  .simple::before {{ content: "Plain English: "; font-weight: 700; color: var(--muted); font-size: 12px; letter-spacing: 0.5px; text-transform: uppercase; }}
  nav.toc {{ background: var(--panel); border: 1px solid var(--border); border-radius: 6px; padding: 14px 18px; margin: 20px 0; }}
  nav.toc h3 {{ margin: 0 0 8px; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--muted); }}
  nav.toc ol {{ margin: 0; padding-left: 20px; columns: 2; column-gap: 32px; }}
  nav.toc ol li {{ padding: 3px 0; break-inside: avoid; }}
  nav.toc a {{ color: var(--accent); text-decoration: none; }}
  nav.toc a:hover {{ text-decoration: underline; }}
  .small {{ font-size: 12px; color: var(--muted); }}
  .filebox {{ background: #2d2d2d; color: #f8f8f2; padding: 8px 14px; border-radius: 6px 6px 0 0; font-family: "SF Mono", Monaco, Consolas, monospace; font-size: 12px; margin-top: 12px; margin-bottom: 0; display: flex; justify-content: space-between; align-items: center; }}
  .filebox a {{ color: #79b8ff; text-decoration: none; }}
  .filebox a:hover {{ text-decoration: underline; }}
  .filebox .rows {{ color: #b9c1cc; font-size: 11px; }}
  .filebox + pre {{ margin-top: 0; border-radius: 0 0 6px 6px; }}
  hr {{ border: none; border-top: 1px solid var(--border); margin: 32px 0; }}
  .csv-row.bid td:first-child {{ color: var(--accent); font-weight: 600; }}
  .csv-row.ask td:first-child {{ color: var(--red);    font-weight: 600; }}
  .csv-row.mid td:first-child {{ color: var(--amber);  font-weight: 600; }}
  .flow {{ font-family: "SF Mono", Monaco, Consolas, monospace; font-size: 11.5px; color: var(--muted); background: #f6f8fa; border: 1px solid var(--border); padding: 6px 10px; border-radius: 6px; margin: 8px 0 12px; display: inline-block; }}
  .run-meta {{ font-size: 12.5px; color: var(--muted); margin: 4px 0 10px; }}
  .run-meta .badge {{ margin-right: 4px; }}
</style>
</head>
<body>
<div class="container">

<header class="cover">
  <h1>m-cube aggregation_reports</h1>
  <p class="subtitle">{total_runs} run{plural} captured across {asset_count} asset class{asset_plural}. One section per asset class; new runs append into their section.</p>
  <p class="meta">Generated {generated_at} · Source of truth: <code style="background:rgba(255,255,255,0.18); color:#fff;">aggregation_reports/_ledger.json</code></p>
</header>

<div class="callout">
  <strong>What this document is.</strong> A self-rebuilding catalog of every backtest run by the m-cube engine. After each run, <code>core.aggregation_report</code> appends a row to <code>_ledger.json</code> and re-renders this page. Sections are grouped by the venue's <code>asset_class</code> (from the adapter config), so a new FX run grows the FX section, a new crypto run grows the crypto section, and the others are untouched. Each run lists the three sniffer CSVs (<code>raw_engine_sniffer</code> → <code>sniffer_data_engine</code> → <code>sniffer_strategy</code>) with sample rows so you can audit data flow at each pipeline stage.
</div>

{toc}

{body}

<script type="application/json" id="ledger">{ledger_json}</script>

</div>
</body>
</html>
"""


# Headers we know about in the sniffer CSVs — used to pick the small
# subset we display in the sample table so cards stay narrow.
_SAMPLE_COLUMNS = ("price_type", "bar_type", "open", "high", "low", "close", "volume", "ts_event")


def _read_csv_sample(path_rel: str | None, n: int = 4) -> tuple[list[str], list[list[str]]] | None:
    """Read the first ``n`` data rows of a sniffer CSV.

    ``path_rel`` is the ledger-stored path (relative to the project root).
    Returns ``(header, rows)`` or ``None`` if the file is missing/empty.
    Reads at most ``n+1`` lines — does not slurp the whole CSV.
    """
    if not path_rel:
        return None
    try:
        p = Path(path_rel)
        if not p.is_absolute():
            p = _PROJECT_ROOT / path_rel
        if not p.exists():
            return None
        import csv as _csv
        with open(p, "r", encoding="utf-8", newline="") as fh:
            reader = _csv.reader(fh)
            header = next(reader, None)
            if not header:
                return None
            data: list[list[str]] = []
            for i, row in enumerate(reader):
                if i >= n:
                    break
                data.append(row)
        return header, data
    except Exception:
        return None


def _sample_table_html(header: list[str], rows: list[list[str]]) -> str:
    """Render a sniffer-CSV sample as a small colored table.

    Picks only the columns in ``_SAMPLE_COLUMNS`` so the row stays narrow,
    and tags each row with a class derived from its ``price_type`` (BID/ASK/MID)
    so CSS can tint the first cell. Output is wrapped in <pre> so it shares
    look-and-feel with the rest of the document's code blocks.
    """
    idx = {name: i for i, name in enumerate(header)}
    keep = [c for c in _SAMPLE_COLUMNS if c in idx]
    if not keep:
        keep = header[:8]
        idx = {name: i for i, name in enumerate(header)}
    th = "".join(f"<th>{html.escape(c)}</th>" for c in keep)
    body_rows = []
    pt_i = idx.get("price_type")
    for r in rows:
        cls = ""
        if pt_i is not None and pt_i < len(r):
            pt = (r[pt_i] or "").strip().upper()
            if pt == "BID":
                cls = "csv-row bid"
            elif pt == "ASK":
                cls = "csv-row ask"
            elif pt == "MID":
                cls = "csv-row mid"
        tds = "".join(
            f"<td>{html.escape(r[idx[c]]) if idx[c] < len(r) else ''}</td>"
            for c in keep
        )
        body_rows.append(f"<tr class='{cls}'>{tds}</tr>")
    return (
        "<pre><table>"
        f"<thead><tr>{th}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></pre>"
    )


def _plain_english_for(row: dict) -> str:
    """Single-sentence narrative for a non-technical reader."""
    sym = row.get("symbol") or "?"
    venue = row.get("venue") or "?"
    asset = row.get("asset_class") or "?"
    tf_ext = row.get("timeframe_external") or "?"
    tf_int = row.get("timeframe_internal")
    sd = row.get("start_date") or "?"
    ed = row.get("end_date") or "?"
    counts = row.get("row_counts") or {}
    raw = counts.get("raw_engine_sniffer", 0)
    strat = counts.get("sniffer_strategy", 0)

    if tf_int and tf_int != tf_ext:
        # Extract "5" from "5-MINUTE" and "1" from "1-MINUTE" for the ratio.
        try:
            ratio_int = int(str(tf_int).split("-", 1)[0])
            ratio_ext = int(str(tf_ext).split("-", 1)[0])
            per = max(1, ratio_int // max(1, ratio_ext))
            ratio_text = f"Every {per} of those {tf_ext.lower()} bars is glued together"
        except Exception:
            ratio_text = f"Those {tf_ext.lower()} bars are glued together"
        return (
            f"We loaded {raw:,} {tf_ext.lower()} {html.escape(sym)} bars "
            f"(the {html.escape(asset)} feed from {html.escape(venue)}) covering "
            f"{html.escape(sd)} → {html.escape(ed)}. "
            f"{ratio_text} into one {tf_int.lower()} bar — and that "
            f"larger {tf_int.lower()} bar ({strat:,} of them) is what the "
            f"strategy actually looks at when deciding to buy or sell. "
            f"The three tables below show the same data at three inspection "
            f"points so you can confirm nothing was lost or distorted along the way."
        )
    return (
        f"We loaded {raw:,} {tf_ext.lower()} {html.escape(sym)} bars "
        f"({html.escape(asset)} from {html.escape(venue)}) covering "
        f"{html.escape(sd)} → {html.escape(ed)} and fed them straight to the strategy — "
        f"no resampling involved. The three tables below show the same bars "
        f"at three inspection points so you can confirm nothing changed in between."
    )


def _tech_definition_for(row: dict) -> str:
    """One-paragraph engineering-accurate description of the data flow."""
    sym = row.get("symbol") or "?"
    venue = row.get("venue") or "?"
    tf_ext = row.get("timeframe_external") or "?"
    tf_int = row.get("timeframe_internal")
    counts = row.get("row_counts") or {}
    raw = counts.get("raw_engine_sniffer", 0)
    de = counts.get("sniffer_data_engine", 0)
    strat = counts.get("sniffer_strategy", 0)

    s = html.escape(sym)
    v = html.escape(venue)
    e = html.escape(tf_ext)

    if tf_int and tf_int != tf_ext:
        i = html.escape(tf_int)
        return (
            f"<code>BacktestEngine</code> streams "
            f"<code>{s}.{v}-{e}-BID/ASK-EXTERNAL</code> bars from the "
            f"<code>ParquetDataCatalog</code> ({raw:,} rows sniffed at "
            f"<code>engine.data</code>). <code>DataEngine</code> forwards them "
            f"({de:,} rows). A <code>TimeBarAggregator</code> subscribed to the "
            f"<code>INTERNAL</code> {i} aggregation consumes the EXTERNAL bars "
            f"and emits <code>{s}.{v}-{i}-BID/ASK-INTERNAL</code> bars "
            f"({strat:,} rows), which the strategy receives via "
            f"<code>on_bar()</code>."
        )
    return (
        f"<code>BacktestEngine</code> streams <code>{s}.{v}-{e}-BID/ASK-EXTERNAL</code> "
        f"bars ({raw:,} rows captured at <code>engine.data</code>). "
        f"<code>DataEngine</code> forwards them ({de:,} rows) and the strategy "
        f"consumes them directly via <code>on_bar()</code> — no "
        f"<code>TimeBarAggregator</code> is involved ({strat:,} rows reach the "
        f"strategy boundary)."
    )


_STAGE_LABEL = {
    "raw_engine_sniffer": "raw_engine_sniffer — what BacktestEngine emits",
    "sniffer_data_engine": "sniffer_data_engine — what DataEngine forwards",
    "sniffer_strategy": "sniffer_strategy — what the strategy receives",
}


def _format_metric(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if abs(value) < 1:
            return f"{value:.4f}"
        return f"{value:,.2f}"
    return str(value)


def _anchor_id(asset_class: str) -> str:
    """Stable in-page anchor for an asset class section."""
    safe = re.sub(r"[^a-z0-9]+", "-", asset_class.lower()).strip("-")
    return f"sec-{safe or 'unknown'}"


def _run_anchor_id(run_id: str, symbol: str) -> str:
    safe_id = re.sub(r"[^A-Za-z0-9]+", "-", str(run_id)[:8])
    safe_sym = re.sub(r"[^A-Za-z0-9]+", "-", str(symbol).lower())
    return f"run-{safe_sym}-{safe_id}"


def _render_run_block(row: dict) -> str:
    """One backtest run rendered as a linear <h3>…<hr> document block."""
    sym = html.escape(str(row.get("symbol") or "?"))
    venue = html.escape(str(row.get("venue") or "?"))
    tf_ext = html.escape(str(row.get("timeframe_external") or "?"))
    tf_int_raw = row.get("timeframe_internal")
    tf_label = tf_ext + (f" → {html.escape(str(tf_int_raw))}" if tf_int_raw else "")
    ts = html.escape(str(row.get("ts") or ""))
    kind_raw = str(row.get("run_kind") or "?")
    kind = html.escape(kind_raw)
    kind_badge_cls = "badge-info" if kind_raw == "single" else "badge-purple"
    pname = row.get("portfolio_name")
    pname_html = (
        f" · portfolio <code>{html.escape(str(pname))}</code>" if pname else ""
    )
    sdate = html.escape(str(row.get("start_date") or "?"))
    edate = html.escape(str(row.get("end_date") or "?"))
    run_id = str(row.get("run_id") or "")
    anchor = _run_anchor_id(run_id, row.get("symbol") or "x")

    # Sniffer CSV index table.
    csvs = row.get("csv_paths") or {}
    counts = row.get("row_counts") or {}
    csv_table_rows: list[str] = []
    sample_blocks: list[str] = []
    for name in ("raw_engine_sniffer", "sniffer_data_engine", "sniffer_strategy"):
        href = csvs.get(name)
        rc = int(counts.get(name, 0) or 0)
        stage_desc = html.escape(_STAGE_LABEL[name])
        if href:
            url = html.escape(href)
            name_cell = (
                f"<code>{html.escape(name)}</code><br>"
                f"<span class='small'>{stage_desc.split(' — ', 1)[-1]}</span>"
            )
            path_cell = f"<a href=\"{url}\"><code>{url}</code></a>"
            rows_cell = (
                f"<span class='badge badge-ok'>{rc:,} rows</span>"
                if rc > 0
                else "<span class='badge badge-warn'>empty</span>"
            )
            csv_table_rows.append(
                f"<tr><td>{name_cell}</td><td>{path_cell}</td>"
                f"<td class='num'>{rows_cell}</td></tr>"
            )
            sample = _read_csv_sample(href, n=4)
            if sample is not None:
                hdr, body = sample
                if body:
                    sample_blocks.append(
                        f"<div class='filebox'>"
                        f"<a href=\"{url}\">{url}</a>"
                        f"<span class='rows'>first {len(body)} of {rc:,}</span>"
                        f"</div>{_sample_table_html(hdr, body)}"
                    )
        else:
            csv_table_rows.append(
                f"<tr><td><code>{html.escape(name)}</code><br>"
                f"<span class='small'>{stage_desc.split(' — ', 1)[-1]}</span></td>"
                f"<td><span class='small'>missing</span></td>"
                f"<td class='num'><span class='badge badge-warn'>n/a</span></td></tr>"
            )

    csv_table_html = (
        "<table><thead><tr>"
        "<th>Stage</th><th>Path</th><th class='num'>Rows</th>"
        f"</tr></thead><tbody>{''.join(csv_table_rows)}</tbody></table>"
    )

    # Metrics table.
    metrics = row.get("metrics") or {}
    metric_rows_html: list[str] = []
    for key in (
        "net_pnl", "total_trades", "win_rate",
        "decisive_win_rate", "flat_trades",
        "max_drawdown", "sharpe_ratio",
    ):
        if key in metrics:
            metric_rows_html.append(
                f"<tr><td><code>{html.escape(key)}</code></td>"
                f"<td class='num'>{html.escape(_format_metric(metrics[key]))}</td></tr>"
            )
    metrics_html = (
        "<h4>Headline metrics</h4>"
        "<table><thead><tr>"
        "<th>Metric</th><th class='num'>Value</th>"
        f"</tr></thead><tbody>{''.join(metric_rows_html)}</tbody></table>"
        if metric_rows_html else ""
    )

    warning = metrics.get("warning")
    warn_html = (
        f"<div class='callout-warn'><strong>Warning.</strong> {html.escape(str(warning))}</div>"
        if warning else ""
    )

    flow_text = (
        "engine.data → DataEngine → TimeBarAggregator → Strategy"
        if tf_int_raw else
        "engine.data → DataEngine → Strategy"
    )

    samples_html = (
        f"<h4>Sample rows</h4>{''.join(sample_blocks)}"
        if sample_blocks else ""
    )

    plain = _plain_english_for(row)
    tech = _tech_definition_for(row)

    return (
        f"<h3 id=\"{anchor}\">{sym}.{venue} · {tf_label} "
        f"<span class='badge {kind_badge_cls}'>{kind}</span></h3>"
        f"<p class='run-meta'><span class='small'>{ts}</span>"
        f"{pname_html} · {sdate} → {edate}</p>"
        f"<div class='flow'>{flow_text}</div>"
        f"<div class='simple'>{plain}</div>"
        f"<div class='callout-tech'><strong>Technically:</strong> {tech}</div>"
        "<h4>Sniffer CSVs</h4>"
        f"{csv_table_html}"
        f"{samples_html}"
        f"{metrics_html}"
        f"{warn_html}"
        "<hr>"
    )


def _section_summary(rows: list[dict]) -> str:
    """One-sentence header above the run list for an asset class section."""
    symbols = sorted({(r.get("symbol") or "?") for r in rows})
    starts = [r.get("start_date") for r in rows if r.get("start_date")]
    ends = [r.get("end_date") for r in rows if r.get("end_date")]
    min_start = min(starts) if starts else None
    max_end = max(ends) if ends else None
    sym_text = ", ".join(html.escape(str(s)) for s in symbols[:8])
    if len(symbols) > 8:
        sym_text += f" (+{len(symbols) - 8} more)"
    span_text = ""
    if min_start and max_end:
        span_text = (
            f" Date coverage spans {html.escape(str(min_start))} → "
            f"{html.escape(str(max_end))}."
        )
    n = len(rows)
    plural = "s" if n != 1 else ""
    return (
        f"<p>Captured <strong>{n}</strong> run{plural} across symbols "
        f"<code>{sym_text}</code>.{span_text}</p>"
    )


def _render_toc(buckets: dict[str, list[dict]]) -> str:
    if not buckets:
        return ""
    items: list[str] = []
    for ac in sorted(buckets.keys()):
        n = len(buckets[ac])
        plural = "s" if n != 1 else ""
        items.append(
            f"<li><a href=\"#{_anchor_id(ac)}\">"
            f"{html.escape(ac)} <span class='small'>({n} run{plural})</span></a></li>"
        )
    return (
        "<nav class='toc'>"
        "<h3>Asset classes</h3>"
        f"<ol>{''.join(items)}</ol>"
        "</nav>"
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
            n = len(rows)
            plural = "s" if n != 1 else ""
            summary = _section_summary(rows)
            blocks = "".join(_render_run_block(r) for r in rows)
            sections.append(
                f"<h2 id=\"{_anchor_id(asset_class)}\">Asset class: "
                f"{html.escape(asset_class)} "
                f"<span class='small'>({n} run{plural})</span></h2>"
                f"{summary}"
                f"{blocks}"
            )

        total = len(ledger)
        asset_count = len(buckets)
        body_html = (
            "".join(sections) if sections else
            "<div class='callout-warn'>No runs recorded yet. "
            "Run any backtest to populate this report.</div>"
        )
        toc_html = _render_toc(buckets)

        html_text = _HTML_TEMPLATE.format(
            total_runs=total,
            plural="s" if total != 1 else "",
            asset_count=asset_count,
            asset_plural="es" if asset_count != 1 else "",
            generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
            toc=toc_html,
            body=body_html,
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
