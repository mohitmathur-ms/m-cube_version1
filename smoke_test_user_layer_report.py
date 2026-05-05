"""Generate audit_user_layer_report.html from audit_user_layer_results.json.

Mirrors the visual style of the existing audit_test_report.html so the new
user-layer audit lives alongside the previous portfolio-feature audit
without a jarring style break. Run after smoke_test_user_layer.py:

    python smoke_test_user_layer_report.py
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.resolve()
RESULTS_PATH = PROJECT_DIR / "5. Logics" / "audit_user_layer_results.json"
OUTPUT_PATH = PROJECT_DIR / "5. Logics" / "audit_user_layer_report.html"


def _verdict_class(status: str) -> tuple[str, str]:
    if status == "ok":
        return "v-pass", "PASS"
    if status == "fail":
        return "v-fail", "FAIL"
    return "v-error", status.upper()


def _params_html(params: dict) -> str:
    if not params:
        return "<p class='expected'>(no parameters)</p>"
    lines = []
    for k, v in params.items():
        v_str = json.dumps(v, default=str) if not isinstance(v, str) else f'"{v}"'
        lines.append(f"<li><code>{escape(k)}</code>: <code>{escape(v_str)}</code></li>")
    return "<ul class='params'>" + "".join(lines) + "</ul>"


def _obs_table_backtest(t: dict) -> str:
    """Render observed metrics for a backtest test."""
    pnl = t.get("total_pnl", 0)
    pnl_cls = "pos" if (pnl or 0) > 0 else ("neg" if (pnl or 0) < 0 else "zero")
    rows = [
        ("user_id", f"<code>{escape(str(t.get('user_id') or '(none)'))}</code>"),
        ("avg_fill_qty", f"<strong>{escape(str(t.get('avg_fill_qty')))}</strong>"),
        ("total_pnl", f"<span class='{pnl_cls}'>${pnl:.2f}</span>"),
        ("total_trades", str(t.get("total_trades", 0))),
        ("wins / losses", f"{t.get('wins',0)} / {t.get('losses',0)}"),
        ("slot_count", str(t.get("slot_count", 0))),
        ("date_range", t.get("date_range") or ""),
        ("elapsed_sec", str(t.get("elapsed_sec", 0))),
    ]
    if t.get("pf_clip_reason"):
        rows.append(("pf_clip", f"<span class='flag flag-loss'>{escape(t['pf_clip_reason'])}</span>"))
    return "<table class='obs-table'>" + "".join(
        f"<tr><td>{escape(k)}</td><td>{v}</td></tr>" for k, v in rows
    ) + "</table>"


def _obs_table_api(t: dict) -> str:
    """Render observed details for an API test."""
    rows = [("detail", escape(t.get("detail", "")))]
    obs = t.get("observations") or {}
    for k, v in obs.items():
        v_str = json.dumps(v, default=str) if not isinstance(v, (str, int, float, bool)) else str(v)
        if len(v_str) > 200:
            v_str = v_str[:200] + " ..."
        rows.append((k, escape(v_str)))
    rows.append(("elapsed_sec", str(t.get("elapsed_sec", 0))))
    return "<table class='obs-table'>" + "".join(
        f"<tr><td>{escape(k)}</td><td>{v}</td></tr>" for k, v in rows
    ) + "</table>"


def _test_card(t: dict) -> str:
    vcls, vlabel = _verdict_class(t.get("status", ""))
    test_id = escape(t.get("test_id", "?"))
    name = escape(t.get("name", ""))
    params_block = _params_html(t.get("params") or {})
    expected = escape(t.get("expected", ""))
    if t.get("kind") == "api":
        obs_block = _obs_table_api(t)
    else:
        obs_block = _obs_table_backtest(t)
    err_block = ""
    if t.get("status") in ("error", "fail"):
        err = escape(t.get("error", "")) or escape(t.get("detail", ""))
        tb = t.get("traceback")
        err_block = f"<div class='err-card'><h4>Failure</h4><pre>{err}</pre>"
        if tb:
            err_block += f"<details><summary>traceback</summary><pre>{escape(tb)}</pre></details>"
        err_block += "</div>"
    return f"""
        <div class="test-card">
          <div class="test-header">
            <span class="test-id">{test_id}</span>
            <span class="test-name">{name}</span>
            <span class="verdict {vcls}">{vlabel}</span>
          </div>
          <div class="test-body">
            <div class="test-section">
              <h4>Parameters</h4>
              {params_block}
            </div>
            <div class="test-section">
              <h4>Expected</h4>
              <p class="expected">{expected}</p>
            </div>
            <div class="test-section">
              <h4>Observed</h4>
              {obs_block}
            </div>
          </div>
          {err_block}
        </div>"""


def _stat_cards(results: list[dict]) -> str:
    total = len(results)
    pass_n = sum(1 for r in results if r.get("status") == "ok")
    fail_n = sum(1 for r in results if r.get("status") == "fail")
    err_n = total - pass_n - fail_n
    return f"""
    <div class="stat-row">
      <div class="stat-card"><div class="num">{total}</div><div class="label">Total tests</div></div>
      <div class="stat-card pass"><div class="num">{pass_n}</div><div class="label">Pass</div></div>
      <div class="stat-card fail"><div class="num">{fail_n}</div><div class="label">Fail</div></div>
      <div class="stat-card err"><div class="num">{err_n}</div><div class="label">Error</div></div>
    </div>"""


def _multiplier_table(results: list[dict]) -> str:
    """A focused mini-table proving multiplier scaling — picks T01-T06 from
    the suite and shows user, multiplier, observed avg_fill_qty, and the
    ratio vs. the _default baseline."""
    bt = [r for r in results if r.get("kind") == "backtest" and r.get("avg_fill_qty") is not None]
    if not bt:
        return ""
    baseline = next((r["avg_fill_qty"] for r in bt if r.get("user_id") == "_default"), None)
    rows = []
    for r in bt:
        avg = r.get("avg_fill_qty")
        ratio = (avg / baseline) if baseline and avg else None
        ratio_str = f"{ratio:.4f}×" if ratio else "—"
        params = r.get("params", {})
        m = params.get("multiplier", "")
        rows.append(
            f"<tr><td>{escape(r['test_id'])}</td>"
            f"<td><code>{escape(str(r.get('user_id') or ''))}</code></td>"
            f"<td>{m}</td>"
            f"<td><strong>{avg}</strong></td>"
            f"<td>{ratio_str}</td></tr>"
        )
    return f"""
      <h3>Multiplier scaling — observed avg fill quantity</h3>
      <p style="font-size:0.86rem; color:var(--text-muted);">
        Each row ran the same FX_9_Slot baseline portfolio under a different user.
        avg_fill_qty is the mean order quantity across all fills the engine
        produced; ratio is vs. <code>_default</code> (×1.0). Anything that
        diverges from <code>multiplier</code> is either (a) cap-clamped (T06)
        or (b) a rounding artefact.
      </p>
      <table class='obs-table' style='margin-bottom:1rem;'>
        <tr><td style='font-weight:700;'>Test</td>
            <td style='font-weight:700;'>User</td>
            <td style='font-weight:700;'>Multiplier</td>
            <td style='font-weight:700;'>avg_fill_qty</td>
            <td style='font-weight:700;'>vs baseline</td></tr>
        {''.join(rows)}
      </table>"""


def main():
    with open(RESULTS_PATH, encoding="utf-8") as f:
        results = json.load(f)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cards = "\n".join(_test_card(t) for t in results)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Multi-User Identity Layer &mdash; Smoke Test Report</title>
<style>
  :root {{
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #e6edf3; --text-muted: #8b949e;
    --accent: #58a6ff; --accent2: #3fb950; --accent3: #d2a8ff;
    --accent4: #f0883e; --accent5: #f85149;
    --code-bg: #1c2128;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.55; font-size: 14px; }}
  .header {{ background: linear-gradient(135deg, #1a1e2e 0%, #0d1117 50%, #111a22 100%);
    border-bottom: 1px solid var(--border); padding: 2rem; text-align: center; }}
  .header h1 {{ font-size: 1.8rem;
    background: linear-gradient(90deg, var(--accent), var(--accent3));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.3rem; }}
  .header p {{ color: var(--text-muted); font-size: 0.9rem; }}
  .container {{ max-width: 1100px; margin: 0 auto; padding: 1.6rem; }}
  section {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
    padding: 1.4rem 1.6rem; margin-bottom: 1.2rem; }}
  h2 {{ color: var(--accent); font-size: 1.3rem; margin-bottom: 0.7rem;
    padding-bottom: 0.4rem; border-bottom: 1px solid var(--border); }}
  h3 {{ color: var(--accent3); font-size: 1.05rem; margin: 0.8rem 0 0.4rem; }}
  h4 {{ color: var(--accent4); font-size: 0.85rem; margin: 0.6rem 0 0.3rem;
    text-transform: uppercase; letter-spacing: 0.04em; }}
  code {{ background: var(--code-bg); color: var(--accent2); padding: 0.1em 0.35em;
    border-radius: 3px; font-size: 0.85em;
    font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace; }}
  pre {{ background: var(--code-bg); border: 1px solid var(--border); border-radius: 4px;
    padding: 0.6rem; overflow-x: auto; font-size: 0.78rem; }}
  .stat-row {{ display: flex; gap: 1rem; flex-wrap: wrap; margin: 0.6rem 0 1rem; }}
  .stat-card {{ flex: 1 1 140px; background: var(--code-bg); border-radius: 6px;
    border: 1px solid var(--border); padding: 0.8rem 1rem; text-align: center; }}
  .stat-card .num {{ font-size: 1.7rem; font-weight: 700; color: var(--accent); }}
  .stat-card.pass .num {{ color: var(--accent2); }}
  .stat-card.fail .num {{ color: var(--accent5); }}
  .stat-card.err .num {{ color: var(--accent4); }}
  .stat-card .label {{ font-size: 0.75rem; color: var(--text-muted);
    text-transform: uppercase; letter-spacing: 0.05em; margin-top: 0.2rem; }}
  .test-card {{ background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; margin-bottom: 0.9rem; overflow: hidden; }}
  .test-header {{ display: flex; align-items: center; gap: 0.7rem; padding: 0.7rem 1rem;
    background: var(--code-bg); border-bottom: 1px solid var(--border); }}
  .test-id {{ font-family: monospace; font-weight: 700; color: var(--accent);
    font-size: 0.95rem; flex: 0 0 auto; }}
  .test-name {{ flex: 1 1 auto; font-size: 0.92rem; }}
  .verdict {{ font-size: 0.72rem; font-weight: 700; padding: 0.2rem 0.6rem;
    border-radius: 3px; text-transform: uppercase; letter-spacing: 0.05em;
    flex: 0 0 auto; }}
  .v-pass {{ background: #103018; color: var(--accent2); }}
  .v-fail {{ background: #2a1018; color: var(--accent5); }}
  .v-error {{ background: #2a2410; color: var(--accent4); }}
  .test-body {{ padding: 0.9rem 1rem;
    display: grid; grid-template-columns: 1fr 1fr 1.4fr; gap: 0.9rem; }}
  .test-section {{ font-size: 0.85rem; }}
  .test-section ul.params {{ padding-left: 1.1rem; }}
  .test-section ul.params li {{ margin: 0.15rem 0; font-size: 0.8rem; }}
  .test-section .expected {{ font-style: italic; color: var(--text-muted); }}
  .obs-table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
  .obs-table td {{ padding: 0.25rem 0.5rem; border-top: 1px solid var(--border);
    vertical-align: top; }}
  .obs-table td:first-child {{ color: var(--text-muted); width: 35%; font-weight: 500; }}
  .obs-table .pos {{ color: var(--accent2); font-weight: 600; }}
  .obs-table .neg {{ color: var(--accent5); font-weight: 600; }}
  .obs-table .zero {{ color: var(--text-muted); }}
  .flag {{ display: inline-block; padding: 0.1rem 0.45rem; border-radius: 3px;
    font-size: 0.7rem; font-weight: 600; margin-right: 0.25rem; }}
  .flag-loss {{ background: #2a1018; color: var(--accent5); }}
  .flag-profit {{ background: #103018; color: var(--accent2); }}
  .err-card {{ background: linear-gradient(135deg, #2a1018, #161b22);
    border-left: 3px solid var(--accent5); border-radius: 0 6px 6px 0;
    padding: 0.8rem 1rem; margin: 0.4rem 1rem 0.9rem; }}
  .err-card h4 {{ color: var(--accent5); }}
  details {{ margin-top: 0.4rem; }}
  details summary {{ cursor: pointer; color: var(--text-muted); font-size: 0.78rem; }}
  @media (max-width: 900px) {{
    .test-body {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Multi-User Identity Layer &mdash; Smoke Test Report</h1>
  <p>16 tests covering multiplier scaling, allowlist enforcement, per-user isolation, and admin live-edit. Generated {timestamp}.</p>
</div>

<div class="container">

<section>
  <h2>Summary</h2>
  {_stat_cards(results)}
  <p style="font-size: 0.86rem; color: var(--text-muted);">
    Each test exercises the multi-user wiring under realistic portfolio
    configurations (RBO, exit windows, squareoff, pf_sl/pf_tgt, trailing,
    move_sl, allowlists). Backtest tests run <code>run_portfolio_backtest()</code>
    in-process with an explicit <code>user_id</code>; API tests hit Flask via
    its test client. The 6 backtest tests vary <em>only</em> the user — the
    portfolio config is identical — so any divergence in observed
    <code>avg_fill_qty</code> is the multiplier (or admin cap) doing its job.
  </p>
  {_multiplier_table(results)}
</section>

<section>
  <h2>Per-test details</h2>
  {cards}
</section>

<section>
  <h2>What this audit verifies</h2>
  <ul style="padding-left:1.4rem; line-height:1.7;">
    <li><strong>Multiplier scaling (T01–T06).</strong> Per-user multiplier scales every order quantity through <code>effective_slot_qty(slot, user_id)</code>. Cap-clamping is applied last so the admin's per-symbol <code>trade_size</code> ceiling cannot be bypassed.</li>
    <li><strong>Allowlist enforcement (T07–T09).</strong> Save, portfolio backtest, and standalone backtest all check <code>allowed_instruments</code>. Out-of-allowlist requests fail with descriptive messages — <em>before</em> any worker process is spawned or any catalog data is read.</li>
    <li><strong>Per-user isolation (T10).</strong> Portfolios live under <code>portfolios/&lt;user_id&gt;/</code>; alice's saves are invisible to bob's <code>/list</code> calls. Same shape applies to reports under <code>reports/&lt;user_id&gt;/</code>.</li>
    <li><strong>Identity gates (T11–T12).</strong> Mutating endpoints reject 401 on missing or unknown <code>X-User-Id</code>. Read endpoints fall back to <code>_default</code> for back-compat.</li>
    <li><strong>API contract (T13–T15).</strong> <code>/api/users/list</code> returns slim view (no multiplier leakage); <code>/api/users/me</code> returns full row for active session; report-listing endpoints scope per user.</li>
    <li><strong>Live admin edits (T16).</strong> Multiplier changes take effect on the very next request — no server restart, because <code>get_multiplier</code> reads <code>users.json</code> on every call.</li>
  </ul>
</section>

</div>
</body>
</html>
"""

    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Wrote -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
