"""Generate extreme_stress_report.html from extreme_stress_results.json.

Output: 5. Logics/extreme_stress_tests/extreme_stress_report.html
Run after stress_test_extreme_path_b.py.
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.resolve()
OUT_DIR = PROJECT_DIR / "5. Logics" / "extreme_stress_tests"
RESULTS_PATH = OUT_DIR / "extreme_stress_results.json"
OUTPUT_PATH = OUT_DIR / "extreme_stress_report.html"


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
    pnl = t.get("total_pnl", 0) or 0
    pnl_cls = "pos" if pnl > 0 else ("neg" if pnl < 0 else "zero")
    rows = [
        ("user_id",      f"<code>{escape(str(t.get('user_id') or '(none)'))}</code>"),
        ("path",         f"<code>{escape(str(t.get('path') or '?'))}</code>"),
        ("path_b flag",  f"<code>{escape(str(t.get('path_b') or '—'))}</code>"),
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
        ("path",                       f"<code>{escape(str(t.get('path') or '?'))}</code>"),
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


def _obs_table_parity(t: dict) -> str:
    """Side-by-side Path A / Path B metrics with drift highlighting."""
    a = t.get("path_a") or {}
    b = t.get("path_b") or {}
    rows = []
    for label, key in (("total_pnl", "total_pnl"),
                       ("total_trades", "total_trades"),
                       ("wins", "wins"),
                       ("losses", "losses"),
                       ("slot_count", "slot_count"),
                       ("avg_fill_qty", "avg_fill_qty"),
                       ("pf_clip_reason", "pf_clip_reason"),
                       ("path_b flag", "path_b")):
        va = a.get(key)
        vb = b.get(key)
        same = (va == vb) or (va is None and vb is None)
        if not same and key == "total_pnl" and va is not None and vb is not None:
            # Apply same tolerance as the test runner before colouring red
            drift = abs(float(va) - float(vb))
            ratio = drift / max(1.0, abs(float(va) or 1.0))
            same = drift <= 1.0 or ratio <= 0.005
        cls = "pos" if same else "neg"
        if key == "total_pnl":
            va_str = f"${(va or 0):.2f}" if va is not None else "—"
            vb_str = f"${(vb or 0):.2f}" if vb is not None else "—"
        else:
            va_str = escape(str(va) if va is not None else "—")
            vb_str = escape(str(vb) if vb is not None else "—")
        rows.append(
            f"<tr><td>{escape(label)}</td>"
            f"<td><code>{va_str}</code></td>"
            f"<td><code>{vb_str}</code></td>"
            f"<td><span class='{cls}'>{'✓' if same else '✗'}</span></td></tr>"
        )
    extra = []
    if t.get("drift_pnl") is not None:
        extra.append(f"<tr><td colspan='4' style='color:var(--text-muted); "
                     f"font-size:0.78rem; padding-top:0.4rem;'>"
                     f"PnL drift: ${t.get('drift_pnl', 0):.2f} "
                     f"({t.get('drift_pct', 0):.4f}%); tolerance ≤ $1 or ≤ 0.5%"
                     f"</td></tr>")
    if t.get("diff_summary"):
        extra.append(f"<tr><td colspan='4' style='color:var(--accent4); "
                     f"font-size:0.78rem;'>{escape(t['diff_summary'])}</td></tr>")
    return ("<table class='obs-table parity-table'>"
            "<thead><tr><th>metric</th><th>Path A</th><th>Path B</th><th>match</th></tr></thead>"
            "<tbody>" + "".join(rows) + "".join(extra) + "</tbody></table>")


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
    elif kind == "parity":
        obs_block = _obs_table_parity(t)
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
    parity = sum(1 for r in results if r.get("kind") == "parity")
    bt = sum(1 for r in results if r.get("kind") in ("backtest", "backtest_negative"))
    api = sum(1 for r in results if r.get("kind") == "api")
    total_elapsed = sum(float(r.get("elapsed_sec", 0) or 0) for r in results)
    return f"""
    <div class="stat-row">
      <div class="stat-card"><div class="num">{total}</div><div class="label">Total tests</div></div>
      <div class="stat-card pass"><div class="num">{pass_n}</div><div class="label">Pass</div></div>
      <div class="stat-card fail"><div class="num">{fail_n}</div><div class="label">Fail</div></div>
      <div class="stat-card err"><div class="num">{err_n}</div><div class="label">Error</div></div>
      <div class="stat-card"><div class="num">{parity}</div><div class="label">Parity tests</div></div>
      <div class="stat-card"><div class="num">{bt}</div><div class="label">Backtest tests</div></div>
      <div class="stat-card"><div class="num">{api}</div><div class="label">API tests</div></div>
      <div class="stat-card"><div class="num">{total_elapsed:.0f}s</div><div class="label">Total elapsed</div></div>
    </div>"""


def _scenarios_table(results: list[dict]) -> str:
    rows = []
    for r in results:
        vcls, _ = _verdict_class(r.get("status", ""))
        verdict_short = {"v-pass": "PASS", "v-fail": "FAIL", "v-error": "ERR"}[vcls]
        kind = r.get("kind", "")
        elapsed = r.get("elapsed_sec", 0)
        if kind == "parity":
            user = r.get("user_id") or "—"
            a = r.get("path_a") or {}
            b = r.get("path_b") or {}
            metric = (f"A: ${a.get('total_pnl', 0):.2f}/{a.get('total_trades', 0)}t "
                      f"vs B: ${b.get('total_pnl', 0):.2f}/{b.get('total_trades', 0)}t "
                      f"(drift {r.get('drift_pnl', 0):.2f})")
        elif kind in ("backtest", "backtest_negative"):
            user = r.get("user_id") or "—"
            qty = r.get("avg_fill_qty")
            qty_str = str(qty) if qty is not None else "—"
            metric = (f"qty={qty_str}, trades={r.get('total_trades', 0)}, "
                      f"PnL=${r.get('total_pnl', 0):.2f}")
            if r.get("pf_clip_reason"):
                metric += f" CLIP={r['pf_clip_reason']}"
            if r.get("path"):
                metric = f"[{r['path']}] " + metric
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


def _group_section(title: str, blurb: str, results: list[dict],
                   id_prefix_letters: list[str]) -> str:
    """Render a per-group section. id_prefix_letters tells us which test_ids
    belong (e.g. ['E01', 'E02', 'E03', 'E04', 'E05'] for Group A)."""
    matching = [r for r in results if r.get("test_id") in set(id_prefix_letters)]
    if not matching:
        return ""
    cards = "\n".join(_test_card(t) for t in matching)
    pass_n = sum(1 for r in matching if r.get("status") == "ok")
    return f"""
    <section>
      <h2>{escape(title)} <span style="font-size:0.7em; color:var(--text-muted);">
        ({pass_n}/{len(matching)} pass)</span></h2>
      <p style="font-size:0.86rem; color:var(--text-muted); margin-bottom:0.8rem;">
        {escape(blurb)}
      </p>
      {cards}
    </section>"""


def main() -> None:
    with open(RESULTS_PATH, encoding="utf-8") as f:
        results = json.load(f)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    parity_pass = sum(1 for r in results if r.get("kind") == "parity" and r.get("status") == "ok")
    parity_total = sum(1 for r in results if r.get("kind") == "parity")
    bt_pass = sum(1 for r in results if r.get("kind") == "backtest" and r.get("status") == "ok")
    bt_neg_pass = sum(1 for r in results if r.get("kind") == "backtest_negative" and r.get("status") == "ok")
    api_pass = sum(1 for r in results if r.get("kind") == "api" and r.get("status") == "ok")
    total = len(results)
    total_pass = sum(1 for r in results if r.get("status") == "ok")

    groups = [
        ("A — Path A vs Path B parity",
         "Same portfolio config run twice (Path A engine, then Path B BacktestNode). "
         "All metrics must agree within $1 / 0.5% PnL tolerance. Trade counts and clip "
         "reasons must match exactly. Any divergence is a regression in one of the paths.",
         ["E01", "E02", "E03", "E04", "E05"]),
        ("B — Path B-specific stress",
         "Behaviours that exercise Path B's per-day chunked BacktestDataConfig pipeline: "
         "long ranges, weekday gaps, micro entry windows, RBO daily resets across multi-day backtests.",
         ["E06", "E07", "E08", "E09"]),
        ("C — RBO state-machine compound risks",
         "RBO interacted with cancel_other_side, weekday filters, UTC-midnight boundaries, "
         "and slot-level reverse-on-SL re-executions (which must bypass the RBO entry gate).",
         ["E10", "E11", "E12", "E13"]),
        ("D — Exit logic compound",
         "Trailing SL micro-ratchets, move-to-cost suppression of trailing until armed, "
         "portfolio-level trailing SL/TP across cumulative wins, profit-lock minimum thresholds.",
         ["E14", "E15", "E16", "E17"]),
        ("E — Multi-user × Path B",
         "Path B with extreme multipliers (×0.01 to ×100,000), allowlist-restricted users, "
         "and cross-path back-to-back runs to verify no shared mutable state between Path A and B.",
         ["E18", "E19", "E20", "E21a", "E21b"]),
        ("F — Time window edge cases",
         "Same-bar gate races (squareoff = entry_end), DST transitions on America/New_York, "
         "weekend-only filters on FX (no data), single-bar entry windows.",
         ["E22", "E23", "E24", "E25"]),
        ("G — Extreme configurations",
         "All-knobs-on portfolios, sub-pip stop-losses, 200% allocation (leverage), "
         "9 mixed-strategy slots under runaway multiplier with cap clamping.",
         ["E26", "E27", "E28", "E29"]),
        ("H — Failure-mode probes",
         "Inverted dates, exclusively-Sunday weekday filter on FX, empty slot lists, "
         "malformed bar-type strings — engine should fail fast or return clean 0-trades, "
         "never produce wrong numbers silently.",
         ["E30", "E31", "E32", "E33"]),
        ("I — API + Path B integration",
         "Path B-active backtests via the Flask streaming endpoint, allowlist enforcement at "
         "/api/portfolios/backtest, full per-user CRUD round-trip.",
         ["E34", "E35", "E36"]),
    ]

    sections_html = "\n".join(_group_section(t, b, results, ids) for t, b, ids in groups)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Extreme Stress Report &mdash; Path B &times; Full Project Surface</title>
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
  .parity-table thead th {{ text-align: left; padding: 0.3rem 0.5rem;
    color: var(--text-muted); border-bottom: 1px solid var(--border);
    font-weight: 600; font-size: 0.75rem; text-transform: uppercase; }}
  .parity-table td:first-child {{ width: 28%; }}
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
  <h1>Extreme Stress Report &mdash; Path B &times; Full Project Surface</h1>
  <p>{total} tests across 9 groups targeting Path B parity, RBO state machine, exit logic compounds,
     multi-user multipliers, time-window races, and failure modes. Generated {timestamp}.</p>
</div>

<div class="container">

<section>
  <h2>Summary</h2>
  {_stat_cards(results)}
  <p style="font-size: 0.86rem; color: var(--text-muted);">
    <strong>{parity_pass}/{parity_total} parity tests</strong> verify Path A and Path B produce
    identical metrics for the same input.
    <strong>{bt_pass} positive backtest tests</strong> stress compound configurations.
    <strong>{bt_neg_pass} failure-mode probes</strong> verify the engine fails fast on
    invalid inputs.
    <strong>{api_pass} API tests</strong> verify the Path B pipeline behaves correctly through
    the Flask layer with allowlist enforcement.
    Total: <strong>{total_pass}/{total} pass</strong>.
  </p>

  <div class="ok-card">
    <h4>Why these tests matter</h4>
    <ul style="padding-left:1.4rem; margin-top:0.3rem;">
      <li><strong>Parity tests (Group A)</strong> — Path B is opt-in via <code>_USE_BACKTEST_NODE=1</code>
        and uses a different engine construction path. Without strict parity, Path B silently
        diverges from Path A's results — invisible to anyone not specifically checking.</li>
      <li><strong>Path B chunking (Group B)</strong> — Path B emits one BacktestDataConfig per allowed
        day when <code>run_on_days</code> or <code>entry_start_time</code> is set. Bugs here
        produce wrong fills on weekday boundaries or near the entry window edges.</li>
      <li><strong>RBO compound (Group C)</strong> — RBO has a multi-state state machine that resets
        daily; interacting with weekday filters, midnight boundaries, and slot-level reverse-on-SL
        is the highest-risk surface in the codebase.</li>
      <li><strong>Exit logic (Group D)</strong> — trailing SL ratcheting + move-to-cost +
        portfolio-level trailing TP all depend on watermark state that must not roll back.</li>
      <li><strong>Multi-user × Path B (Group E)</strong> — multiplier scaling and cap clamping must
        work identically on both paths; back-to-back runs must not leak shared state.</li>
      <li><strong>Time windows (Group F)</strong> — same-bar gate races, DST transitions, and
        weekend-only filters are subtle bugs that surface only when an actual user hits them.</li>
      <li><strong>Failure modes (Group H)</strong> — wrong inputs should fail fast with descriptive
        errors. Silent 0-trade returns on bad config look like working backtests.</li>
    </ul>
  </div>
</section>

<section>
  <h2>Compact scenarios table</h2>
  {_scenarios_table(results)}
</section>

{sections_html}

<section>
  <h2>How to reproduce</h2>
  <pre>python stress_test_extreme_path_b.py
python stress_test_extreme_path_b_report.py</pre>
  <p style="font-size: 0.85rem; color: var(--text-muted);">
    Both scripts are at the project root. They seed an isolated test registry,
    run the suite, write JSON+HTML to <code>5. Logics/extreme_stress_tests/</code>,
    then restore the original <code>users.json</code> registry. Safe to re-run.
    The Parity tests run each backtest twice (Path A then Path B), so total
    runtime is roughly 1.5&times; <code>stress_test_portfolios.py</code>.
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
