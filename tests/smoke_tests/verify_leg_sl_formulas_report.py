"""Generate sl_formula_report.html from sl_formula_results.json.

Output: 5. Logics/sl_formula_tests/sl_formula_report.html
Run after verify_leg_sl_formulas.py.

Renders the orderbook-only leg-SL formula verification (§1 + §2 of
leg_sl_test_scenarios.html): summary cards, a compact scenarios table, and
per-scenario cards showing each Stop Loss trade's realised entry, the
recomputed expected SL, the engine's SL, and the verdict.
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape
from pathlib import Path

# This file lives in tests/smoke_tests/; the verifier writes its output under
# the repo root's "5. Logics/" — walk up two levels to reach it.
PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
OUT_DIR = PROJECT_DIR / "5. Logics" / "sl_formula_tests"
RESULTS_PATH = OUT_DIR / "sl_formula_results.json"
OUTPUT_PATH = OUT_DIR / "sl_formula_report.html"


def _verdict(status: str) -> tuple[str, str]:
    return {"ok": ("v-pass", "PASS"), "fail": ("v-fail", "FAIL")}.get(
        status, ("v-error", status.upper()))


def _sample_table(sample: list[dict]) -> str:
    if not sample:
        return "<p class='muted'>(no Stop Loss trades to show)</p>"
    head = ("<tr><th>#</th><th>Dir</th><th>Entry</th><th>Expected SL</th>"
            "<th>Engine SL</th><th>ATR@entry</th><th>✓</th></tr>")
    rows = []
    for i, s in enumerate(sample, 1):
        ok = s.get("ok")
        mark = "<span class='chk-pass'>✓</span>" if ok else "<span class='chk-fail'>✗</span>"
        rowcls = "" if ok else " class='bad-row'"
        atr = s.get("atr_at_entry")
        rows.append(
            f"<tr{rowcls}><td>{i}</td><td>{escape(str(s.get('direction','')))}</td>"
            f"<td>{s.get('entry','')}</td><td>{s.get('expected_sl','')}</td>"
            f"<td>{s.get('engine_sl','')}</td>"
            f"<td>{'' if atr is None else atr}</td><td>{mark}</td></tr>")
    return (f"<table class='sample-table'><thead>{head}</thead>"
            f"<tbody>{''.join(rows)}</tbody></table>")


def _checks_html(passed: list, failed: list) -> str:
    rows = []
    for c in failed:
        rows.append(f"<li><span class='chk-fail'>✗</span> "
                    f"<strong>{escape(c['label'])}</strong>"
                    f"<div class='chk-detail'>{escape(c['detail'])}</div></li>")
    # Show a capped slice of passing checks (can be hundreds of trades).
    for c in passed[:8]:
        rows.append(f"<li><span class='chk-pass'>✓</span> "
                    f"<strong>{escape(c['label'])}</strong>"
                    f"<div class='chk-detail'>{escape(c['detail'])}</div></li>")
    extra = len(passed) - 8
    if extra > 0:
        rows.append(f"<li class='muted'>… and {extra} more passing trade checks</li>")
    if not rows:
        return "<p class='muted'>(no checks)</p>"
    return "<ul class='checks'>" + "".join(rows) + "</ul>"


def _params_html(params: dict) -> str:
    items = []
    for k, v in params.items():
        items.append(f"<li><code>{escape(str(k))}</code>: "
                     f"<code>{escape(json.dumps(v, default=str))}</code></li>")
    return "<ul class='params'>" + "".join(items) + "</ul>"


def _card(t: dict) -> str:
    vcls, vlabel = _verdict(t.get("status", ""))
    body_err = ""
    if t.get("status") == "error":
        body_err = (f"<div class='err-card'><h4>Engine error</h4>"
                    f"<pre>{escape(t.get('error',''))}</pre>"
                    f"<details><summary>traceback</summary>"
                    f"<pre>{escape(t.get('traceback',''))}</pre></details></div>")
    return f"""
    <div class="test-card">
      <div class="test-header">
        <span class="test-id">{escape(t.get('test_id','?'))}</span>
        <span class="test-name">{escape(t.get('name',''))}</span>
        <span class="verdict {vcls}">{vlabel}</span>
      </div>
      <div class="test-body">
        <div class="test-section">
          <h4>Parameters</h4>{_params_html(t.get('params') or {})}
          <h4 style='margin-top:.6rem;'>Expected</h4>
          <p class='expected'>{escape(t.get('expected',''))}</p>
          <h4 style='margin-top:.6rem;'>Observed</h4>
          <p class='muted'>trades={t.get('orderbook_trades',0)} ·
            sl_exits={t.get('n_sl_exits',0)} ·
            mean_sl_dist={t.get('mean_sl_distance','—')} ·
            checks={escape(str(t.get('verification_count','—')))}</p>
        </div>
        <div class="test-section">
          <h4>Per-trade SL recomputation (orderbook only)</h4>
          {_sample_table(t.get('sample') or [])}
        </div>
      </div>
      <div class="checks-row">
        <h4>Verification checks</h4>
        {_checks_html(t.get('checks_passed') or [], t.get('checks_failed') or [])}
      </div>
      {body_err}
    </div>"""


def _stat_cards(results: list[dict]) -> str:
    total = len(results)
    p = sum(1 for r in results if r.get("status") == "ok")
    f = sum(1 for r in results if r.get("status") == "fail")
    e = total - p - f
    sl = sum(int(r.get("n_sl_exits", 0) or 0) for r in results)
    return f"""
    <div class="stat-row">
      <div class="stat-card"><div class="num">{total}</div><div class="label">Scenarios</div></div>
      <div class="stat-card pass"><div class="num">{p}</div><div class="label">Pass</div></div>
      <div class="stat-card fail"><div class="num">{f}</div><div class="label">Fail</div></div>
      <div class="stat-card err"><div class="num">{e}</div><div class="label">Error</div></div>
      <div class="stat-card"><div class="num">{sl}</div><div class="label">SL exits verified</div></div>
    </div>"""


def _scen_table(results: list[dict]) -> str:
    rows = []
    for r in results:
        vcls, short = _verdict(r.get("status", ""))
        rows.append(
            f"<tr><td><code>{escape(r.get('test_id','?'))}</code></td>"
            f"<td>{escape(r.get('name',''))}</td>"
            f"<td>sl_exits={r.get('n_sl_exits',0)}, "
            f"mean_dist={r.get('mean_sl_distance','—')}</td>"
            f"<td><code>{escape(str(r.get('verification_count','—')))}</code></td>"
            f"<td><span class='verdict {vcls}'>{short}</span></td></tr>")
    return (f"<table class='scen-table'><thead><tr><th>ID</th><th>Scenario</th>"
            f"<th>Observation</th><th>Checks</th><th>Verdict</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table>")


def main() -> None:
    with open(RESULTS_PATH, encoding="utf-8") as fh:
        results = json.load(fh)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cards = "\n".join(_card(t) for t in results)

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Leg-SL Formula Verification — §1 + §2</title>
<style>
  :root {{ --bg:#0d1117; --surface:#161b22; --border:#30363d; --text:#e6edf3;
    --text-muted:#8b949e; --accent:#58a6ff; --accent2:#3fb950; --accent3:#d2a8ff;
    --accent4:#f0883e; --accent5:#f85149; --code-bg:#1c2128; }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;
    background:var(--bg); color:var(--text); line-height:1.55; font-size:14px; }}
  .header {{ background:linear-gradient(135deg,#1a1e2e,#0d1117 50%,#111a22);
    border-bottom:1px solid var(--border); padding:2rem; text-align:center; }}
  .header h1 {{ font-size:1.8rem;
    background:linear-gradient(90deg,var(--accent),var(--accent3));
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; margin-bottom:.3rem; }}
  .header p {{ color:var(--text-muted); font-size:.9rem; }}
  .container {{ max-width:1200px; margin:0 auto; padding:1.6rem; }}
  section {{ background:var(--surface); border:1px solid var(--border); border-radius:8px;
    padding:1.4rem 1.6rem; margin-bottom:1.2rem; }}
  h2 {{ color:var(--accent); font-size:1.3rem; margin-bottom:.7rem;
    padding-bottom:.4rem; border-bottom:1px solid var(--border); }}
  h4 {{ color:var(--accent4); font-size:.82rem; margin:.5rem 0 .3rem;
    text-transform:uppercase; letter-spacing:.04em; }}
  code {{ background:var(--code-bg); color:var(--accent2); padding:.1em .35em;
    border-radius:3px; font-size:.85em; font-family:Consolas,Menlo,monospace; }}
  pre {{ background:var(--code-bg); border:1px solid var(--border); border-radius:4px;
    padding:.6rem; overflow-x:auto; font-size:.78rem; }}
  .stat-row {{ display:flex; gap:.7rem; flex-wrap:wrap; margin:.6rem 0 1rem; }}
  .stat-card {{ flex:1 1 110px; background:var(--code-bg); border-radius:6px;
    border:1px solid var(--border); padding:.7rem .9rem; text-align:center; }}
  .stat-card .num {{ font-size:1.5rem; font-weight:700; color:var(--accent); }}
  .stat-card.pass .num {{ color:var(--accent2); }}
  .stat-card.fail .num {{ color:var(--accent5); }}
  .stat-card.err .num {{ color:var(--accent4); }}
  .stat-card .label {{ font-size:.72rem; color:var(--text-muted);
    text-transform:uppercase; letter-spacing:.05em; margin-top:.2rem; }}
  .test-card {{ background:var(--surface); border:1px solid var(--border);
    border-radius:6px; margin-bottom:.9rem; overflow:hidden; }}
  .test-header {{ display:flex; align-items:center; gap:.7rem; padding:.7rem 1rem;
    background:var(--code-bg); border-bottom:1px solid var(--border); }}
  .test-id {{ font-family:monospace; font-weight:700; color:var(--accent); }}
  .test-name {{ flex:1 1 auto; font-size:.92rem; }}
  .verdict {{ font-size:.72rem; font-weight:700; padding:.2rem .6rem; border-radius:3px;
    text-transform:uppercase; letter-spacing:.05em; }}
  .v-pass {{ background:#103018; color:var(--accent2); }}
  .v-fail {{ background:#2a1018; color:var(--accent5); }}
  .v-error {{ background:#2a2410; color:var(--accent4); }}
  .test-body {{ padding:.9rem 1rem; display:grid; grid-template-columns:1fr 1.3fr; gap:.9rem; }}
  .test-section {{ font-size:.85rem; }}
  .params {{ padding-left:1.1rem; }} .params li {{ margin:.12rem 0; font-size:.8rem; }}
  .expected {{ font-style:italic; color:var(--text-muted); }}
  .sample-table {{ width:100%; border-collapse:collapse; font-size:.78rem; }}
  .sample-table th {{ text-align:left; color:var(--text-muted); padding:.3rem .4rem;
    border-bottom:1px solid var(--border); font-weight:600; }}
  .sample-table td {{ padding:.25rem .4rem; border-bottom:1px solid var(--border);
    font-family:Consolas,Menlo,monospace; }}
  .sample-table tr.bad-row td {{ background:#2a1018; }}
  .checks-row {{ background:var(--code-bg); border-top:1px solid var(--border);
    padding:.7rem 1rem; }}
  .checks {{ list-style:none; padding-left:0; margin-top:.3rem; }}
  .checks li {{ font-size:.82rem; padding:.2rem 0; }}
  .chk-pass {{ color:var(--accent2); font-weight:700; margin-right:.4rem; }}
  .chk-fail {{ color:var(--accent5); font-weight:700; margin-right:.4rem; }}
  .chk-detail {{ color:var(--text-muted); font-size:.72rem; padding-left:1.2rem;
    word-break:break-word; }}
  .scen-table {{ width:100%; border-collapse:collapse; font-size:.82rem; }}
  .scen-table th {{ text-align:left; padding:.5rem .4rem; color:var(--text-muted);
    border-bottom:1px solid var(--border); font-weight:600; }}
  .scen-table td {{ padding:.45rem .4rem; border-bottom:1px solid var(--border); }}
  .scen-table tr:hover td {{ background:var(--code-bg); }}
  .err-card {{ background:linear-gradient(135deg,#2a1018,#161b22);
    border-left:3px solid var(--accent5); border-radius:0 6px 6px 0;
    padding:.8rem 1rem; margin:.4rem 1rem .9rem; }}
  .err-card h4 {{ color:var(--accent5); }}
  details summary {{ cursor:pointer; color:var(--text-muted); font-size:.78rem; }}
  .muted {{ color:var(--text-muted); font-size:.78rem; font-style:italic; }}
  @media (max-width:1000px) {{ .test-body {{ grid-template-columns:1fr; }} }}
</style></head><body>

<div class="header">
  <h1>Leg-SL Formula Verification &mdash; §1 + §2</h1>
  <p>Orderbook-only verification per Part B of leg_sl_test_scenarios.html:
     EMA Cross drives the entries; every Stop Loss is recomputed from the
     realised fill and matched to the engine's SL. Generated {ts}.</p>
</div>

<div class="container">
<section>
  <h2>Summary</h2>
  {_stat_cards(results)}
  <p class="muted">Each scenario runs a real EURUSD backtest, builds the
    orderbook with <code>build_orderbook_dataframe</code>, and for every
    <code>EXIT REASON == "Stop Loss"</code> row recomputes the expected SL from
    <code>ENTRY PRICE</code> using the type formula, tick-snapped, then asserts
    it equals the engine's SL (parsed from <code>EXIT DETAILED REASON</code>) to
    within half a tick. No engine internals are read.</p>
</section>

<section>
  <h2>Scenarios</h2>
  {_scen_table(results)}
</section>

<section>
  <h2>Detailed scenario cards</h2>
  {cards}
</section>

<section>
  <h2>How to reproduce</h2>
  <pre>python verify_leg_sl_formulas.py
python tests\\smoke_tests\\verify_leg_sl_formulas_report.py</pre>
</section>
</div></body></html>"""

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Wrote -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
