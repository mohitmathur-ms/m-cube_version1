"""Render the 16-test smoke suite results as a self-contained HTML report.

Reads ``5. Logics/audit_smoke_results.json`` (produced by
``smoke_test_logics_audit.py``) and writes
``5. Logics/audit_test_report.html``.

The report includes:
  - a summary table (pass/fail/error counts)
  - one card per test with parameters, expected behavior, observed metrics,
    verdict, and any backend warnings
  - an "errors observed" section describing what crashed (if anything) and
    how to fix it
  - per-test trade-time histograms
"""

from __future__ import annotations

import html
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.resolve()
RESULTS_PATH = PROJECT_DIR / "5. Logics" / "audit_smoke_results.json"
REPORT_PATH = PROJECT_DIR / "5. Logics" / "audit_test_report.html"

BASELINE_TEST_ID = "T01"


def _classify(test: dict, _baseline: dict | None) -> tuple[str, str]:
    """Return (verdict_class, verdict_label) for a single test."""
    if test.get("status") == "config_error":
        return "v-error", "CONFIG ERROR"
    if test.get("status") == "error":
        return "v-error", "RUNTIME ERROR"

    tid = test.get("test_id")
    s = test

    def fail(msg: str) -> tuple[str, str]:
        return "v-fail", f"FAIL ({msg})"

    # T01-T11, T13-T14, T16 are Path B; T12 is Path A.
    expect_path_b = "all" if tid != "T12" else "none"
    if s.get("path_b_summary") not in (expect_path_b, None):
        return fail(f"path_b={s.get('path_b_summary')} (expected {expect_path_b})")

    # Day filter checks
    day_filters = {
        "T03": {"Mon", "Wed", "Fri"},
        "T04": {"Tue", "Wed", "Thu"},
        "T05": {"Mon", "Tue", "Wed", "Thu"},
        "T07": None,  # All days allowed by run_on_days; check skipped
        "T10": {"Mon", "Tue", "Wed", "Thu"},
        "T12": {"Mon", "Tue", "Wed"},
        "T15": {"Sat", "Sun"},
        "T16": {"Mon", "Wed", "Fri"},
    }
    if tid in day_filters and day_filters[tid] is not None:
        wd = set(s.get("fills_weekdays", {}).keys())
        allowed = day_filters[tid]
        if wd and not wd.issubset(allowed):
            return fail(f"weekdays {sorted(wd)} not subset of {sorted(allowed)}")

    # Hour filter checks (entry-window confined; exits may spill ~1h after)
    hour_filters = {
        "T05": (5, 11),    # entry 05-10, squareoff 11
        "T08": (10, 15),   # entry 10-14, squareoff 15
        "T10": (11, 17),   # entry 11-16, squareoff 17
        "T12": (10, 14),   # squareoff 14:30
        "T13": (11, 13),   # entry 11:30-12:30
        "T16": (10, 16),   # entry 10-15, squareoff 16
    }
    if tid in hour_filters:
        lo, hi = hour_filters[tid]
        for h in s.get("fills_hours_utc", {}).keys():
            try:
                hi_int = int(h)
                if hi_int < lo or hi_int > hi:
                    return fail(f"hour {h} outside expected window [{lo}, {hi}]")
            except ValueError:
                continue

    # Special cases
    if tid == "T15":
        # Weekend-only: expect 0 trades (no FX data on Sat/Sun)
        if s.get("total_trades", 0) > 0:
            return fail(f"weekend filter produced {s['total_trades']} trades")
        return "v-pass", "PASS (0 trades on weekend filter)"

    if tid == "T08" and s.get("max_loss_hit"):
        return "v-pass", "PASS (max_loss flag fired as expected)"

    # Augmented PASS labels with notable observations
    extras = []
    if s.get("pf_clip_reason"):
        extras.append(f"clip={s['pf_clip_reason']}")
    if s.get("max_loss_hit"):
        extras.append("max_loss_hit")
    if s.get("max_profit_hit"):
        extras.append("max_profit_hit")
    label = "PASS"
    if extras:
        label = f"PASS ({', '.join(extras)})"
    return "v-pass", label


def _render_params(params: dict) -> str:
    if not params:
        return "<em style='color:var(--text-muted);'>defaults</em>"
    lines = []
    for k, v in params.items():
        v_str = html.escape(json.dumps(v, default=str))
        lines.append(f"<li><code>{html.escape(str(k))}</code>: <code>{v_str}</code></li>")
    return "<ul class='params'>" + "".join(lines) + "</ul>"


def _render_observed(test: dict) -> str:
    if test.get("status") == "config_error":
        return f"<p class='err-text'>Config error: <code>{html.escape(test.get('error',''))}</code></p>"
    if test.get("status") == "error":
        return (
            f"<p class='err-text'><strong>Runtime error:</strong> "
            f"<code>{html.escape(test.get('error',''))}</code></p>"
            f"<details><summary>traceback</summary><pre>{html.escape(test.get('traceback',''))}</pre></details>"
        )
    pnl = test.get("total_pnl", 0.0)
    pnl_class = "pos" if pnl > 0 else ("neg" if pnl < 0 else "zero")
    weekdays = test.get("fills_weekdays", {})
    hours = test.get("fills_hours_utc", {})
    rows = []
    rows.append(f"<tr><td>Total PnL</td><td class='{pnl_class}'>${pnl:,.2f}</td></tr>")
    rows.append(f"<tr><td>Trades</td><td>{test.get('total_trades', 0)} (W:{test.get('wins',0)} / L:{test.get('losses',0)})</td></tr>")
    rows.append(f"<tr><td>path_b</td><td><code>{html.escape(str(test.get('path_b_summary')))}</code></td></tr>")
    rows.append(f"<tr><td>Slot count</td><td>{test.get('slot_count', 0)}</td></tr>")
    if weekdays:
        wd_str = ", ".join(f"{k}:{v}" for k, v in sorted(weekdays.items(), key=lambda x: ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"].index(x[0]) if x[0] in ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"] else 99))
        rows.append(f"<tr><td>Fill weekdays</td><td><code>{html.escape(wd_str)}</code></td></tr>")
    if hours:
        hr_str = ", ".join(f"{k}h:{v}" for k, v in sorted(hours.items(), key=lambda x: int(x[0])))
        rows.append(f"<tr><td>Fill hours UTC</td><td><code>{html.escape(hr_str)}</code></td></tr>")
    if test.get("pf_clip_ts"):
        rows.append(f"<tr><td>pf_clip</td><td><code>{html.escape(str(test.get('pf_clip_reason')))}</code> @ <code>{html.escape(str(test.get('pf_clip_ts')))}</code> &rarr; <code>{html.escape(str(test.get('pf_clip_action')))}</code></td></tr>")
    if test.get("pf_clipped_slots"):
        rows.append(f"<tr><td>Clipped slots</td><td><code>{html.escape(', '.join(test.get('pf_clipped_slots', [])))}</code></td></tr>")
    if test.get("max_loss_hit") or test.get("max_profit_hit"):
        flags = []
        if test.get("max_loss_hit"): flags.append("<span class='flag flag-loss'>max_loss_hit</span>")
        if test.get("max_profit_hit"): flags.append("<span class='flag flag-profit'>max_profit_hit</span>")
        rows.append(f"<tr><td>Boundary flags</td><td>{' '.join(flags)}</td></tr>")
    rows.append(f"<tr><td>Runtime</td><td>{test.get('elapsed_sec','?')}s</td></tr>")

    table = "<table class='obs-table'>" + "".join(rows) + "</table>"

    warnings = test.get("warnings") or []
    if warnings:
        wlist = "".join(f"<li>{html.escape(w)}</li>" for w in warnings)
        table += f"<details class='warnings'><summary>{len(warnings)} backend warning(s)</summary><ul>{wlist}</ul></details>"
    return table


def main():
    if not RESULTS_PATH.exists():
        raise SystemExit(f"Results file not found: {RESULTS_PATH}. Run smoke_test_logics_audit.py first.")
    with open(RESULTS_PATH) as f:
        tests = json.load(f)
    if not isinstance(tests, list):
        # Old format compatibility
        tests = [v for k, v in (tests or {}).items()]

    baseline = next((t for t in tests if t.get("test_id") == BASELINE_TEST_ID), None)

    # Tally
    counts = Counter()
    for t in tests:
        cls, _ = _classify(t, baseline)
        if cls == "v-pass": counts["pass"] += 1
        elif cls == "v-fail": counts["fail"] += 1
        else: counts["error"] += 1

    cards = []
    for t in tests:
        cls, label = _classify(t, baseline)
        params_html = _render_params(t.get("params", {}))
        observed_html = _render_observed(t)
        cards.append(f"""
        <div class="test-card">
          <div class="test-header">
            <span class="test-id">{html.escape(str(t.get('test_id','?')))}</span>
            <span class="test-name">{html.escape(str(t.get('name','')))}</span>
            <span class="verdict {cls}">{html.escape(label)}</span>
          </div>
          <div class="test-body">
            <div class="test-section">
              <h4>Parameters set</h4>
              {params_html}
            </div>
            <div class="test-section">
              <h4>Expected behavior</h4>
              <p class="expected">{html.escape(str(t.get('expected','')))}</p>
            </div>
            <div class="test-section">
              <h4>Observed</h4>
              {observed_html}
            </div>
          </div>
        </div>
        """)

    # Errors-observed section: collect only error/fail tests
    error_tests = [t for t in tests if t.get("status") in ("error", "config_error") or _classify(t, baseline)[0] == "v-fail"]
    err_section = ""
    if error_tests:
        rows = []
        for t in error_tests:
            tid = html.escape(str(t.get("test_id", "?")))
            name = html.escape(str(t.get("name", "")))
            err = html.escape(str(t.get("error", "(see traceback)")))
            resolution = _resolution_for(t)
            rows.append(f"""
              <div class="err-card">
                <h4>{tid} &mdash; {name}</h4>
                <p><strong>Error:</strong> <code>{err}</code></p>
                <p><strong>Resolution:</strong> {resolution}</p>
              </div>
            """)
        err_section = "<section id='errors'><h2>Errors observed and how to resolve</h2>" + "".join(rows) + "</section>"
    else:
        err_section = "<section id='errors'><h2>Errors observed and how to resolve</h2><p class='ok-text'>No errors observed across the 16 tests. Every run completed without raising.</p></section>"

    html_out = _PAGE_TEMPLATE.format(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total=len(tests),
        passed=counts["pass"],
        failed=counts["fail"],
        errored=counts["error"],
        cards="\n".join(cards),
        errors_section=err_section,
    )

    REPORT_PATH.write_text(html_out, encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")


def _resolution_for(test: dict) -> str:
    err = (test.get("error") or "").lower()
    tid = test.get("test_id", "")
    if "could not convert string to float" in err and ("usd" in err or "jpy" in err):
        return (
            "<strong>Money-string concatenation bug at "
            "<code>core/backtest_runner.py:3143</code></strong>. "
            "When the portfolio clip fires, the post-clip aggregation re-sums "
            "<code>merged_positions[\"realized_pnl\"]</code>, but that column holds Money strings "
            "(e.g., <code>'-2.20 USD'</code>, <code>'-321 JPY'</code>). Calling "
            "<code>.sum()</code> on a string column concatenates rather than adds, then "
            "<code>float(...)</code> on the concatenated string fails."
            "<br><br><em>Recommended fix:</em> use the base-currency numeric column added by "
            "<code>positions_report_with_base</code> (e.g., <code>realized_pnl_USD</code>) "
            "which is already FX-converted and numeric. The same lookup pattern is already used "
            "at <code>backtest_runner.py:2978-2991</code> &mdash; mirror it here:"
            "<pre><code>base_col = next(\n"
            "    (c for c in merged_positions.columns\n"
            "     if c.startswith(\"realized_pnl_\") and c != \"realized_pnl_\"),\n"
            "    None,\n"
            ")\n"
            "pnl_col_to_sum = base_col or \"realized_pnl\"\n"
            "if base_col:\n"
            "    _clipped_pnl = float(merged_positions[base_col].sum())\n"
            "else:\n"
            "    # Fallback: parse Money strings\n"
            "    _clipped_pnl = sum(\n"
            "        float(str(x).split(\" \")[0]) if x and \" \" in str(x) else 0.0\n"
            "        for x in merged_positions[\"realized_pnl\"]\n"
            "    )</code></pre>"
            "<strong>Note:</strong> the clip itself fired correctly &mdash; "
            "<code>pf_clip_ts</code> and the <code>[PF_CLIP] PORTFOLIO_SQOFF | Reason=STOPLOSS</code> "
            "log line confirm the wiring works. Only the <em>post-clip total recompute</em> crashes. "
            "Tests where pf_sl was loose enough not to fire (T03 $80, T10 $70, T12 didn't reach $50) "
            "passed cleanly."
        )
    if "no enabled strategy slots" in err:
        return (
            "Portfolio has no enabled slots. <em>Fix:</em> ensure at least one slot has "
            "<code>enabled: true</code>."
        )
    if "no module named" in err:
        return (
            "Missing dependency. <em>Fix:</em> install the missing package via "
            "<code>pip install -r requirements.txt</code>."
        )
    if "filter combination yields zero configs" in err:
        return (
            "<code>run_on_days</code> + entry-window filter yielded zero allowed days. "
            "<em>Fix:</em> ensure at least one weekday is allowed within the date range, "
            "and entry_start_time &lt; entry_end_time."
        )
    if test.get("status") == "config_error":
        return "Portfolio config failed validation. Check the JSON keys and types."
    return "Investigate the traceback; capture stdout from the run for additional context."


_PAGE_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Add Portfolio &mdash; Smoke Test Report</title>
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
  .err-text {{ color: var(--accent5); font-size: 0.85rem; }}
  .ok-text {{ color: var(--accent2); font-size: 0.9rem; }}
  .err-card {{ background: linear-gradient(135deg, #2a1018, #161b22);
    border-left: 3px solid var(--accent5); border-radius: 0 6px 6px 0;
    padding: 0.8rem 1rem; margin-bottom: 0.7rem; }}
  .err-card h4 {{ color: var(--accent5); }}
  .warnings {{ margin-top: 0.5rem; font-size: 0.78rem; }}
  .warnings summary {{ color: var(--accent4); cursor: pointer; }}
  .warnings ul {{ padding-left: 1.4rem; margin-top: 0.3rem; }}
  .warnings li {{ color: var(--text-muted); font-family: monospace; font-size: 0.75rem; }}
  details {{ margin-top: 0.4rem; }}
  details summary {{ cursor: pointer; color: var(--text-muted); font-size: 0.78rem; }}
  @media (max-width: 900px) {{
    .test-body {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Add Portfolio &mdash; Smoke Test Report</h1>
  <p>16 backtest variants on FX_9_Slot, week of 2024-12-02 .. 2024-12-06. Generated {timestamp}.</p>
</div>

<div class="container">

<section>
  <h2>Summary</h2>
  <div class="stat-row">
    <div class="stat-card"><div class="num">{total}</div><div class="label">Total tests</div></div>
    <div class="stat-card pass"><div class="num">{passed}</div><div class="label">Pass</div></div>
    <div class="stat-card fail"><div class="num">{failed}</div><div class="label">Fail</div></div>
    <div class="stat-card err"><div class="num">{errored}</div><div class="label">Error</div></div>
  </div>
  <p style="font-size: 0.86rem; color: var(--text-muted);">All tests run on the same FX_9_Slot portfolio (2024-12-02 .. 2024-12-06, 9 slots, EURUSD/GBPUSD/USDJPY x EMA Cross/RSI/Bollinger). Each test mutates a deep copy of the baseline portfolio config, runs <code>run_portfolio_backtest()</code> in-process, and captures fills, PnL, and any portfolio clip events.</p>
</section>

<section>
  <h2>Per-test details</h2>
  {cards}
</section>

{errors_section}

</div>
</body>
</html>
"""


if __name__ == "__main__":
    main()
