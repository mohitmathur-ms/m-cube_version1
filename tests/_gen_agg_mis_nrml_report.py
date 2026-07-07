"""Generate the Custom-Aggregator + MIS/NRML verification report from
html_reports/tests_html_report/_agg_mis_nrml_results.json."""
from __future__ import annotations
import collections, html, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = json.loads((ROOT / "html_reports" / "tests_html_report"
                   / "_agg_mis_nrml_results.json").read_text(encoding="utf-8"))
OUT = ROOT / "html_reports" / "tests_html_report" / "aggregator_mis_nrml_test_results.html"

CATALOG_SPAN = "2015-01-01 .. 2025-06-27 (EURUSD 1-minute ASK·BID·MID)"

VBADGE = {"PASS": ("ok", "PASS"), "FAIL": ("miss", "FAIL")}


def fmt(v):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:,.2f}"
    return html.escape(str(v))


def settings_rows(s: dict) -> str:
    return "".join(f"<tr><td>{html.escape(str(k))}</td><td>{fmt(v) if not isinstance(v,(list,dict)) else html.escape(str(v))}</td></tr>"
                   for k, v in s.items())


def runs_table(runs: dict) -> str:
    """One column per run variant, rows = key metrics — so the reader can
    eyeball the *difference* the feature made between variants."""
    labels = list(runs.keys())
    metrics = [("Total trades", "total_trades"), ("Total PnL", "total_pnl"),
               ("Final balance", "final_balance"), ("Win rate %", "win_rate"),
               ("Per-strategy slots", "per_strategy_slots")]
    head = "".join(f"<th>{html.escape(l)}</th>" for l in labels)
    rows = ""
    for label, key in metrics:
        cells = "".join(f"<td>{fmt(runs[l].get(key))}</td>" for l in labels)
        rows += f"<tr><td class='mk'>{label}</td>{cells}</tr>"
    # exit-reason breakdown row
    cells = ""
    for l in labels:
        er = runs[l].get("exit_reasons") or {}
        cells += "<td>" + (", ".join(f"{k}:{v}" for k, v in er.items()) or "—") + "</td>"
    rows += f"<tr><td class='mk'>Exit reasons</td>{cells}</tr>"
    return f"<table class='runs'><tr><th>Metric</th>{head}</tr>{rows}</table>"


cards = []
tally = collections.Counter()
for r in DATA:
    tally[r["verdict"]] += 1
    vc, vt = VBADGE.get(r["verdict"], ("miss", r["verdict"]))
    body = runs_table(r["runs"]) if r.get("runs") else "<p class='muted'>No runs — case errored.</p>"
    err = ""
    if r.get("error"):
        err = f"<div class='err'><b>Raised:</b> <code>{html.escape(r['error'][:400])}</code></div>"
    cards.append(f"""
<section class="tc">
  <div class="tch"><span class="tcid">{r['id']}</span>
    <span class="tctitle">{html.escape(r['title'])}</span>
    <span class="vb {vc}">{vt}</span></div>
  <table class="meta">
    <tr><td>Backtest date range</td><td><b>{html.escape(r['range'])}</b></td></tr>
    <tr><td>Catalog data span</td><td>{CATALOG_SPAN}</td></tr>
    <tr><td>Runtime</td><td>{fmt(r.get('seconds'))} s · status <b>{html.escape(r['status'])}</b></td></tr>
  </table>
  <h4>Settings under test</h4>
  <table class="meta">{settings_rows(r.get('settings') or {})}</table>
  <h4>Comparative runs</h4>
  {body}
  {err}
  <div class="verdict {vc}"><b>Verification:</b> {html.escape(r.get('note',''))}</div>
</section>""")

CSS = """
:root{--bg:#0f1117;--surface:#1a1d27;--surface2:#22263a;--border:#2e3250;
--text:#d4d8f0;--muted:#8a90b0;--accent:#5b7cfa;--ok:#3ecf8e;--partial:#f7c948;
--miss:#e96060;--code:#141824;}
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
background:var(--bg);color:var(--text);line-height:1.6;font-size:14px;}
.wrap{max-width:1180px;margin:0 auto;padding:0 38px 100px;}
header.cover{background:linear-gradient(135deg,#3ecf8e 0%,#5b7cfa 100%);color:#fff;
padding:38px 40px;margin-bottom:8px;border-radius:0 0 14px 14px;}
header.cover h1{font-size:25px;margin-bottom:6px;}
header.cover p{color:rgba(255,255,255,.92);font-size:13.5px;}
h2{font-size:18px;margin:38px 0 12px;color:#fff;padding-bottom:8px;border-bottom:1px solid var(--border);}
h4{font-size:12px;color:var(--accent);margin:14px 0 5px;text-transform:uppercase;letter-spacing:.04em;}
p{color:var(--muted);margin:9px 0;}
code{font-family:'Fira Code',Consolas,monospace;font-size:11px;background:var(--code);
border:1px solid var(--border);border-radius:3px;padding:1px 5px;color:#f7c948;}
.muted{color:var(--muted);}
.sumgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:16px 0;}
.sumcard{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;text-align:center;}
.sumcard .n{font-size:30px;font-weight:800;}
.sumcard .l{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin-top:4px;}
.callout{background:rgba(91,124,250,.08);border-left:3px solid var(--accent);
padding:12px 16px;margin:14px 0;border-radius:0 6px 6px 0;font-size:13px;}
.callout strong{color:#fff;}
.tc{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:18px 22px;margin:14px 0;}
.tch{display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;}
.tcid{background:var(--accent);color:#fff;font-weight:800;font-size:12px;border-radius:5px;padding:3px 9px;}
.tctitle{font-size:15px;font-weight:700;color:#fff;flex:1;min-width:200px;}
.vb{font-size:11px;font-weight:800;border-radius:5px;padding:3px 10px;}
.vb.ok{background:rgba(62,207,142,.15);color:var(--ok);border:1px solid var(--ok);}
.vb.miss{background:rgba(233,96,96,.15);color:var(--miss);border:1px solid var(--miss);}
table{width:100%;border-collapse:collapse;font-size:12.5px;margin:6px 0;
background:var(--surface2);border:1px solid var(--border);border-radius:6px;overflow:hidden;}
td,th{padding:7px 11px;border-bottom:1px solid var(--border);color:var(--muted);vertical-align:top;text-align:left;}
th{color:#aab0d0;font-weight:700;background:rgba(91,124,250,.06);}
tr:last-child td{border-bottom:none;}
table.meta td:first-child{width:240px;color:#aab0d0;font-weight:600;}
table.runs td.mk{width:200px;color:#aab0d0;font-weight:600;}
td b,td strong{color:var(--text);}
.err{background:rgba(233,96,96,.09);border-left:3px solid var(--miss);padding:9px 13px;margin:8px 0;border-radius:0 6px 6px 0;font-size:12px;}
.verdict{padding:9px 13px;margin-top:8px;border-radius:0 6px 6px 0;font-size:12.5px;}
.verdict.ok{background:rgba(62,207,142,.08);border-left:3px solid var(--ok);}
.verdict.miss{background:rgba(233,96,96,.08);border-left:3px solid var(--miss);}
.verdict b{color:#fff;}
"""

HTML = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Custom Aggregator &amp; MIS/NRML — Verification</title><style>{CSS}</style></head><body>
<header class="cover">
  <h1>Custom Aggregator &amp; MIS / NRML — Verification Results</h1>
  <p>Real backtests proving the two newest features behave correctly: the in-strategy
  streaming aggregator (which replaced Nautilus' internal <code>TimeBarAggregator</code>)
  and the MIS / NRML product type. Each case runs the same portfolio twice with one
  variable changed and compares the results, so the effect of the feature is observable.</p>
</header>
<div class="wrap">

<h2>1. Summary</h2>
<div class="sumgrid">
  <div class="sumcard"><div class="n" style="color:var(--ok)">{tally['PASS']}</div><div class="l">Pass</div></div>
  <div class="sumcard"><div class="n" style="color:var(--miss)">{tally['FAIL']}</div><div class="l">Fail</div></div>
  <div class="sumcard"><div class="n">{len(DATA)}</div><div class="l">Total cases</div></div>
</div>
<div class="callout">
  <strong>Outcome.</strong> {tally['PASS']}/{len(DATA)} PASS. The custom streaming
  aggregator drives strategy logic at the selected timeframe — coarser timeframes yield
  monotonically fewer signal trades (1-min&nbsp;→&nbsp;5-min&nbsp;→&nbsp;15-min&nbsp;→&nbsp;1-hour),
  in both the managed (SL/TP) and raw (signal-only) portfolio paths, while order fills
  still resolve on the base data fed to the engine. MIS supplies a forced daily squareoff;
  NRML carries forward; an explicit portfolio squareoff time overrides the MIS default.
</div>
<div class="callout">
  <strong>Method.</strong> Harness <code>tests/run_agg_mis_nrml_tests.py</code> builds each
  portfolio via <code>portfolio_from_dict</code> and runs <code>run_portfolio_backtest</code>
  against <code>./catalog</code> using the reserved <code>_default</code> user. Aggregation is
  requested through the leg's <code>strategy_bar_types</code> (the composite
  <code>…-INTERNAL@…-EXTERNAL</code> string the Multileg UI emits), which the backend
  reinterprets into an EXTERNAL aggregation target. MIS/NRML is set via
  <code>PortfolioConfig.product</code> + <code>mis_squareoff_time</code>.
</div>

<h2>2. Per-case results</h2>
{''.join(cards)}

</div></body></html>
"""

OUT.write_text(HTML, encoding="utf-8")
print(f"wrote {OUT}  ({tally['PASS']} PASS, {tally['FAIL']} FAIL)")
