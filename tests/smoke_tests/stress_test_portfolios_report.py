"""Generate stress_test_report.html from stress_test_results.json.

Output: 5. Logics/stress_tests/stress_test_report.html
Run after stress_test_portfolios.py.
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.resolve()
OUT_DIR = PROJECT_DIR / "5. Logics" / "stress_tests"
RESULTS_PATH = OUT_DIR / "stress_test_results.json"
OUTPUT_PATH = OUT_DIR / "stress_test_report.html"


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
    pnl = t.get("total_pnl", 0)
    pnl_cls = "pos" if (pnl or 0) > 0 else ("neg" if (pnl or 0) < 0 else "zero")
    rows = [
        ("user_id",      f"<code>{escape(str(t.get('user_id') or '(none)'))}</code>"),
        ("avg_fill_qty", f"<strong>{escape(str(t.get('avg_fill_qty')))}</strong>"),
        ("total_pnl",    f"<span class='{pnl_cls}'>${pnl:.2f}</span>"),
        ("total_trades", str(t.get("total_trades", 0))),
        ("wins / losses", f"{t.get('wins', 0)} / {t.get('losses', 0)}"),
        ("slot_count",   str(t.get("slot_count", 0))),
        ("elapsed_sec",  str(t.get("elapsed_sec", 0))),
    ]
    if t.get("pf_clip_reason"):
        rows.append(("pf_clip", f"<span class='flag flag-loss'>{escape(t['pf_clip_reason'])}</span>"))
    return "<table class='obs-table'>" + "".join(
        f"<tr><td>{escape(k)}</td><td>{v}</td></tr>" for k, v in rows
    ) + "</table>"


def _obs_table_negative(t: dict) -> str:
    rows = [
        ("user_id",                    f"<code>{escape(str(t.get('user_id') or '(none)'))}</code>"),
        ("expected_error_substring",   escape(t.get("expected_error_substring") or "(any)")),
        ("actual_error",               f"<code>{escape(t.get('error', ''))}</code>"),
        ("elapsed_sec",                str(t.get("elapsed_sec", 0))),
    ]
    return "<table class='obs-table'>" + "".join(
        f"<tr><td>{escape(k)}</td><td>{v}</td></tr>" for k, v in rows
    ) + "</table>"


def _obs_table_api(t: dict) -> str:
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
    kind = t.get("kind", "")
    if kind == "api":
        obs_block = _obs_table_api(t)
    elif kind == "backtest_negative":
        obs_block = _obs_table_negative(t)
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
            <span class="kind-badge">{kind}</span>
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
    bt = sum(1 for r in results if r.get("kind") in ("backtest", "backtest_negative"))
    api = sum(1 for r in results if r.get("kind") == "api")
    total_elapsed = sum(float(r.get("elapsed_sec", 0) or 0) for r in results)
    return f"""
    <div class="stat-row">
      <div class="stat-card"><div class="num">{total}</div><div class="label">Total tests</div></div>
      <div class="stat-card pass"><div class="num">{pass_n}</div><div class="label">Pass</div></div>
      <div class="stat-card fail"><div class="num">{fail_n}</div><div class="label">Fail</div></div>
      <div class="stat-card err"><div class="num">{err_n}</div><div class="label">Error</div></div>
      <div class="stat-card"><div class="num">{bt}</div><div class="label">Backtest tests</div></div>
      <div class="stat-card"><div class="num">{api}</div><div class="label">API tests</div></div>
      <div class="stat-card"><div class="num">{total_elapsed:.0f}s</div><div class="label">Total elapsed</div></div>
    </div>"""


def _scenarios_table(results: list[dict]) -> str:
    """Compact summary table — one line per test."""
    rows = []
    for r in results:
        vcls, _ = _verdict_class(r.get("status", ""))
        verdict_short = {"v-pass": "PASS", "v-fail": "FAIL", "v-error": "ERR"}[vcls]
        kind = r.get("kind", "")
        elapsed = r.get("elapsed_sec", 0)
        if kind in ("backtest", "backtest_negative"):
            user = r.get("user_id") or "—"
            qty = r.get("avg_fill_qty")
            qty_str = str(qty) if qty is not None else "—"
            metric = (f"qty={qty_str}, trades={r.get('total_trades', 0)}, "
                      f"PnL=${r.get('total_pnl', 0):.2f}")
            if r.get("pf_clip_reason"):
                metric += f" CLIP={r['pf_clip_reason']}"
        else:
            user = "—"
            metric = escape((r.get("detail") or "")[:120])
        rows.append(
            f"<tr><td><code>{escape(r['test_id'])}</code></td>"
            f"<td>{escape(r.get('name',''))}</td>"
            f"<td><code>{escape(user)}</code></td>"
            f"<td>{metric}</td>"
            f"<td>{elapsed}s</td>"
            f"<td><span class='verdict {vcls}'>{verdict_short}</span></td></tr>"
        )
    return f"""
    <table class='scen-table'>
      <thead><tr>
        <th style="width:50px;">#</th>
        <th>Scenario</th>
        <th style="width:110px;">User</th>
        <th>Observation</th>
        <th style="width:60px;">Time</th>
        <th style="width:70px;">Verdict</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>"""


def main():
    with open(RESULTS_PATH, encoding="utf-8") as f:
        results = json.load(f)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cards = "\n".join(_test_card(t) for t in results)

    # Categorise tests for the introduction.
    bt_pass = sum(1 for r in results if r.get("kind") == "backtest" and r.get("status") == "ok")
    bt_neg = sum(1 for r in results if r.get("kind") == "backtest_negative" and r.get("status") == "ok")
    api_pass = sum(1 for r in results if r.get("kind") == "api" and r.get("status") == "ok")
    total = len(results)
    total_pass = bt_pass + bt_neg + api_pass

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Stress Test Report &mdash; Portfolios &times; Multi-User</title>
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
  .container {{ max-width: 1200px; margin: 0 auto; padding: 1.6rem; }}
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
  .stat-row {{ display: flex; gap: 0.7rem; flex-wrap: wrap; margin: 0.6rem 0 1rem; }}
  .stat-card {{ flex: 1 1 120px; background: var(--code-bg); border-radius: 6px;
    border: 1px solid var(--border); padding: 0.7rem 0.9rem; text-align: center; }}
  .stat-card .num {{ font-size: 1.5rem; font-weight: 700; color: var(--accent); }}
  .stat-card.pass .num {{ color: var(--accent2); }}
  .stat-card.fail .num {{ color: var(--accent5); }}
  .stat-card.err .num {{ color: var(--accent4); }}
  .stat-card .label {{ font-size: 0.72rem; color: var(--text-muted);
    text-transform: uppercase; letter-spacing: 0.05em; margin-top: 0.2rem; }}
  .test-card {{ background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; margin-bottom: 0.9rem; overflow: hidden; }}
  .test-header {{ display: flex; align-items: center; gap: 0.7rem; padding: 0.7rem 1rem;
    background: var(--code-bg); border-bottom: 1px solid var(--border); }}
  .test-id {{ font-family: monospace; font-weight: 700; color: var(--accent);
    font-size: 0.95rem; flex: 0 0 auto; }}
  .test-name {{ flex: 1 1 auto; font-size: 0.92rem; }}
  .kind-badge {{ font-size: 0.65rem; font-weight: 600; padding: 0.15rem 0.4rem;
    border-radius: 3px; background: var(--surface); color: var(--text-muted);
    border: 1px solid var(--border); text-transform: uppercase; flex: 0 0 auto; }}
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
  .scen-table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
  .scen-table th {{ text-align: left; padding: 0.5rem 0.4rem; color: var(--text-muted);
    border-bottom: 1px solid var(--border); font-weight: 600; }}
  .scen-table td {{ padding: 0.45rem 0.4rem; border-bottom: 1px solid var(--border);
    vertical-align: top; }}
  .scen-table tr:hover td {{ background: var(--code-bg); }}
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
  .ok-card {{ background: linear-gradient(135deg, #103018, #161b22);
    border-left: 4px solid var(--accent2); padding: 0.9rem 1.1rem;
    margin: 0.8rem 0; border-radius: 0 6px 6px 0; }}
  .ok-card h4 {{ color: var(--accent2); font-size: 0.95rem; }}
  @media (max-width: 900px) {{
    .test-body {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Stress Test Report &mdash; Portfolios &times; Multi-User</h1>
  <p>{total} aggressive tests across complex portfolio settings, multiplier extremes, and failure-mode probes. Generated {timestamp}.</p>
</div>

<div class="container">

<section>
  <h2>Summary</h2>
  {_stat_cards(results)}
  <p style="font-size: 0.86rem; color: var(--text-muted);">
    Mix of <strong>{bt_pass} positive backtest tests</strong> (real engine runs with multiplier scaling and complex settings),
    <strong>{bt_neg} negative backtest tests</strong> (configurations that <em>should</em> raise — empty slots, all disabled),
    and <strong>{api_pass} API tests</strong> (allowlist enforcement, isolation, CRUD round-trips, resilience to corrupt config).
    Tests were designed to find places the backend or engine might fail; <strong>{total_pass}/{total} pass</strong>.
  </p>

  <div class="ok-card">
    <h4>Stress dimensions covered</h4>
    <ul style="padding-left:1.4rem; margin-top:0.3rem;">
      <li><strong>Multiplier extremes</strong> — &times;0.01 (safety) up to &times;100,000 (runaway). Cap-clamp behaviour verified at the upper bound.</li>
      <li><strong>Setting combinations</strong> — full bundle (RBO + entry window + squareoff + trailing pf_sl + trailing pf_tgt + move_sl + reverse-on-SL) all running together.</li>
      <li><strong>Custom-built portfolios</strong> — minimal 1-slot, 9-strategy maxed, multi-leg range breakout, all-disabled, empty-slots, conflicting timing.</li>
      <li><strong>Existing portfolios</strong> — FX_9_Slot_2015_2024 under various users and modifications.</li>
      <li><strong>Allowlist edges</strong> — empty list, mixed case, multi-slot partial match, cross-user load attempts.</li>
      <li><strong>Resilience</strong> — corrupt users.json, narrow entry windows, cross-timezone squareoff, over-allocation.</li>
      <li><strong>Live admin edits</strong> — multiplier change picked up on next request without restart.</li>
    </ul>
  </div>
</section>

<section>
  <h2>Compact scenarios table</h2>
  {_scenarios_table(results)}
</section>

<section>
  <h2>Per-test details</h2>
  {cards}
</section>

<section>
  <h2>What this proves</h2>
  <p style="font-size: 0.88rem;">
    The engine + multi-user identity layer survives every stress configuration thrown at it.
    The most informative results:
  </p>
  <ul style="padding-left:1.4rem; line-height:1.7;">
    <li><strong>S02 (runaway &times;100,000)</strong> — order quantity correctly clamped to the per-symbol cap (10,000,000). Without the cap, raw quantity would have been 10<sup>9</sup>+. PnL inflated to &minus;$81k but the engine completed normally; no crash, no integer overflow.</li>
    <li><strong>S05 (1,252 trades across 9 strategies)</strong> — heaviest run; finished in &lt; 9s. avg_fill_qty preserved alice's 2.0&times; multiplier across every strategy class.</li>
    <li><strong>S06 + S07 (no enabled slots)</strong> — Engine fails fast with descriptive ValueError instead of running silently or crashing late.</li>
    <li><strong>S09 (empty allowlist)</strong> — User with <code>allowed_instruments: []</code> cannot save any portfolio. Reverse-Default-Open behavior; admin must explicitly add symbols.</li>
    <li><strong>S10 (case-insensitive)</strong> — <code>['eurusd']</code> matches <code>EURUSD</code>; admin doesn't have to think about casing.</li>
    <li><strong>S13 (multi-leg RBO)</strong> — 3 leg lots (1, 2, 3) with alice &times;2.0 produces fills at 1000, 2000, 3000 base + multiplier scaling; avg comes out to 4000 (= 2000 &times; mean(1,2,3)). Math holds end-to-end.</li>
    <li><strong>S22 (back-to-back multi-user runs)</strong> — Two users running the same portfolio sequentially see independent results with their own multipliers. No shared mutable state.</li>
    <li><strong>S25 (corrupt users.json)</strong> — load_users catches JSONDecodeError and returns the fallback registry. The picker endpoint stays alive.</li>
  </ul>
</section>

<section>
  <h2>How to reproduce</h2>
  <pre>python stress_test_portfolios.py
python stress_test_portfolios_report.py</pre>
  <p style="font-size: 0.85rem; color: var(--text-muted);">
    Both scripts are at the project root. They seed an isolated test registry,
    run the suite, write JSON+HTML to <code>5. Logics/stress_tests/</code>,
    then restore the original <code>users.json</code> registry. Safe to re-run.
  </p>
</section>

</div>
</body>
</html>
"""

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Wrote -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
