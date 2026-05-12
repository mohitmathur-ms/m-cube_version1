"""Generate stoploss_report.html from stoploss_results.json.

Output: 5. Logics/stoploss_tests/stoploss_report.html
Run after stress_test_stoploss.py.

The report renders:
* Summary stat cards (total / pass / fail / error counts).
* Compact scenarios table with per-test verdicts.
* Per-test cards detailing parameters, expected behaviour, observed
  metrics (orderbook trades / pnl, fills_tag_counts,
  exit_reason_counts), and the per-check pass/fail breakdown.
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.resolve()
OUT_DIR = PROJECT_DIR / "5. Logics" / "stoploss_tests"
RESULTS_PATH = OUT_DIR / "stoploss_results.json"
OUTPUT_PATH = OUT_DIR / "stoploss_report.html"


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


def _counts_html(counts: dict, css_class: str = "") -> str:
    if not counts:
        return f"<span class='muted'>(none)</span>"
    rows = []
    for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
        rows.append(
            f"<span class='count-pill {css_class}'>"
            f"<code>{escape(str(k))}</code>"
            f"<span class='count-num'>{v}</span></span>"
        )
    return "<div class='count-row'>" + " ".join(rows) + "</div>"


def _checks_html(passed: list, failed: list) -> str:
    rows = []
    for c in passed:
        rows.append(
            f"<li><span class='chk-pass'>✓</span> "
            f"<strong>{escape(c['label'])}</strong>"
            f"<div class='chk-detail'>{escape(c['detail'])}</div></li>"
        )
    for c in failed:
        rows.append(
            f"<li><span class='chk-fail'>✗</span> "
            f"<strong>{escape(c['label'])}</strong>"
            f"<div class='chk-detail'>{escape(c['detail'])}</div></li>"
        )
    if not rows:
        return "<p class='muted'>(no verifier configured)</p>"
    return "<ul class='checks'>" + "".join(rows) + "</ul>"


def _obs_table(t: dict) -> str:
    pnl = t.get("orderbook_pnl", 0.0) or 0.0
    pnl_cls = "pos" if pnl > 0 else ("neg" if pnl < 0 else "zero")
    rows = [
        ("orderbook_trades",  str(t.get("orderbook_trades", 0))),
        ("wins / losses",     f"{t.get('orderbook_wins', 0)} / {t.get('orderbook_losses', 0)}"),
        ("orderbook_pnl",     f"<span class='{pnl_cls}'>${pnl:.2f}</span>"),
        ("engine_trades",     str(t.get("engine_trades", 0))),
        ("engine_pnl",        f"${(t.get('engine_pnl', 0) or 0):.2f}"),
        ("verification",      f"<strong>{escape(str(t.get('verification_count','—')))}</strong>"),
        ("elapsed_sec",       str(t.get("elapsed_sec", 0))),
    ]
    portfolio_path = t.get("portfolio_path")
    if portfolio_path:
        rows.append(("portfolio_json",
                     f"<code>{escape(portfolio_path)}</code>"))
    return "<table class='obs-table'>" + "".join(
        f"<tr><td>{escape(k)}</td><td>{v}</td></tr>" for k, v in rows
    ) + "</table>"


def _test_card(t: dict) -> str:
    vcls, vlabel = _verdict_class(t.get("status", ""))
    test_id = escape(t.get("test_id", "?"))
    name = escape(t.get("name", ""))
    params_block = _params_html(t.get("params") or {})
    expected = escape(t.get("expected", ""))
    obs_block = _obs_table(t)

    fills_block = _counts_html(t.get("fills_tag_counts") or {}, css_class="pill-engine")
    exit_block = _counts_html(t.get("exit_reason_counts") or {}, css_class="pill-orderbook")

    checks_block = _checks_html(t.get("checks_passed") or [],
                                t.get("checks_failed") or [])

    err_block = ""
    if t.get("status") == "error":
        err = escape(t.get("error", ""))
        tb = t.get("traceback")
        err_block = f"<div class='err-card'><h4>Engine error</h4><pre>{err}</pre>"
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
              <h4 style='margin-top:0.7rem;'>Expected behaviour</h4>
              <p class="expected">{expected}</p>
            </div>
            <div class="test-section">
              <h4>Engine fills tags</h4>
              <p class='hint'>Source of truth — tags emitted by ManagedExitStrategy on each close.</p>
              {fills_block}
              <h4 style='margin-top:0.7rem;'>Orderbook EXIT REASON</h4>
              <p class='hint'>Report-layer view — what the user sees in the orderbook UI.</p>
              {exit_block}
            </div>
            <div class="test-section">
              <h4>Observed</h4>
              {obs_block}
            </div>
          </div>
          <div class="checks-row">
            <h4>Verification checks</h4>
            {checks_block}
          </div>
          {err_block}
        </div>"""


def _stat_cards(results: list[dict]) -> str:
    total = len(results)
    pass_n = sum(1 for r in results if r.get("status") == "ok")
    fail_n = sum(1 for r in results if r.get("status") == "fail")
    err_n = total - pass_n - fail_n
    total_elapsed = sum(float(r.get("elapsed_sec", 0) or 0) for r in results)
    total_trades = sum(int(r.get("orderbook_trades", 0) or 0) for r in results)
    return f"""
    <div class="stat-row">
      <div class="stat-card"><div class="num">{total}</div><div class="label">Total tests</div></div>
      <div class="stat-card pass"><div class="num">{pass_n}</div><div class="label">Pass</div></div>
      <div class="stat-card fail"><div class="num">{fail_n}</div><div class="label">Fail</div></div>
      <div class="stat-card err"><div class="num">{err_n}</div><div class="label">Error</div></div>
      <div class="stat-card"><div class="num">{total_trades}</div><div class="label">Total trades</div></div>
      <div class="stat-card"><div class="num">{total_elapsed:.0f}s</div><div class="label">Total elapsed</div></div>
    </div>"""


def _scenarios_table(results: list[dict]) -> str:
    rows = []
    for r in results:
        vcls, _ = _verdict_class(r.get("status", ""))
        verdict_short = {"v-pass": "PASS", "v-fail": "FAIL", "v-error": "ERR"}[vcls]
        ec = r.get("exit_reason_counts") or {}
        ft = r.get("fills_tag_counts") or {}
        sl_total = (ft.get("Stop Loss", 0) + ft.get("Trailing SL", 0) +
                    ft.get("Reverse on SL", 0))
        tp_total = ft.get("Take Profit", 0) + ft.get("Reverse on TP", 0)
        metric = (f"trades={r.get('orderbook_trades',0)}, "
                  f"PnL=${r.get('orderbook_pnl',0):.2f}, "
                  f"sl_hits={sl_total}, tp_hits={tp_total}")
        rows.append(
            f"<tr><td><code>{escape(r['test_id'])}</code></td>"
            f"<td>{escape(r.get('name',''))}</td>"
            f"<td>{metric}</td>"
            f"<td><code>{escape(r.get('verification_count','—'))}</code></td>"
            f"<td>{r.get('elapsed_sec', 0)}s</td>"
            f"<td><span class='verdict {vcls}'>{verdict_short}</span></td></tr>"
        )
    return f"""
    <table class='scen-table'>
      <thead><tr>
        <th style="width:50px;">#</th>
        <th>Scenario</th>
        <th>Observation</th>
        <th style="width:80px;">Checks</th>
        <th style="width:60px;">Time</th>
        <th style="width:70px;">Verdict</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>"""


def _settings_reference() -> str:
    """Documentation of every stoploss tab setting being tested."""
    settings = [
        ("stop_loss_type", "none / percentage / points / trailing",
         "Selects the SL pricing model. percentage = pct of entry price; "
         "points = absolute distance from entry; trailing = ratchets with "
         "watermark via trailing_sl_step + trailing_sl_offset."),
        ("stop_loss_value", "float",
         "The configured SL distance. Interpreted as % for percentage/trailing, "
         "as absolute price points for points."),
        ("trailing_sl_step", "float (only for type=trailing)",
         "Each step (% of entry) that the highest_profit watermark passes "
         "advances the trailing offset by trailing_sl_offset."),
        ("trailing_sl_offset", "float (only for type=trailing)",
         "How tightly the SL trails behind the watermark per step."),
        ("target_type", "none / percentage / points",
         "Symmetric to stop_loss_type but for take-profit."),
        ("target_value", "float", "Configured TP distance."),
        ("target_lock_trigger", "float (% profit)",
         "When highest_profit ≥ trigger, raise SL to lock_minimum locking "
         "in profit. Subsequent SL hits tagged 'Trailing SL'."),
        ("target_lock_minimum", "float (% profit)",
         "The locked-in floor SL after target_lock_trigger fires."),
        ("sl_wait_bars", "int",
         "Require N consecutive bars hitting SL before honoring it. Brief "
         "wicks are filtered."),
        ("on_sl_action", "close / re_execute / reverse",
         "What the engine does after an SL hit. close=flat. re_execute=re-enter "
         "on next signal up to max_re_executions. reverse=immediately flip "
         "to opposite-side position."),
        ("on_target_action", "close / re_execute / reverse",
         "Symmetric to on_sl_action but for TP hits."),
        ("max_re_executions", "int",
         "Cap on re_execute retries per slot lifetime."),
    ]
    rows = []
    for name, typ, desc in settings:
        rows.append(
            f"<tr><td><code>{escape(name)}</code></td>"
            f"<td><code>{escape(typ)}</code></td>"
            f"<td>{escape(desc)}</td></tr>"
        )
    return ("<table class='ref-table'><thead><tr>"
            "<th>Setting</th><th>Type / values</th><th>Effect</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


def main() -> None:
    with open(RESULTS_PATH, encoding="utf-8") as f:
        results = json.load(f)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = len(results)
    total_pass = sum(1 for r in results if r.get("status") == "ok")
    total_fail = sum(1 for r in results if r.get("status") == "fail")
    total_err = total - total_pass - total_fail

    cards_html = "\n".join(_test_card(t) for t in results)

    # Identify reverse-action tests where the orderbook diverges from the
    # engine fills — that's the close+open same-bar tag-collision bug.
    # When all reverse tests pass, render an "issue resolved" card instead.
    reverse_test_ids = {"S09", "S11", "S15"}
    reverse_results = [r for r in results if r.get("test_id") in reverse_test_ids]
    reverse_failing = [r for r in reverse_results if r.get("status") != "ok"]
    if reverse_failing:
        issue_card_html = """
  <div class="warn-card">
    <h4>Known issue surfaced by this suite</h4>
    <p style="font-size:0.86rem; margin:0.3rem 0;">
      <strong>S09 / S11 / S15</strong> all show
      <code>fills_report.tags</code> contains the correct
      <code>Reverse on SL</code> / <code>Reverse on TP</code> values
      (engine layer correct), but the orderbook EXIT REASON column shows
      <code>Market Exit</code> instead.
    </p>
    <p style="font-size:0.86rem; margin:0.3rem 0; color:var(--text-muted);">
      Root cause: when <code>on_sl_action=&quot;reverse&quot;</code> fires,
      <code>_handle_exit</code> emits a close fill (with tag) and then
      immediately submits a fresh open fill on the opposite side. Both
      fills carry the same <code>ts_init</code> nanosecond timestamp. In
      <code>core/report_generator.py:_build_fills_lookup</code>, the
      <code>by_pos_ts</code> dict is keyed by
      <code>(trader, strategy, instrument, ts_seconds)</code> — so the later
      open fill (tag=None) overwrites the earlier close fill's tag, and
      the orderbook's EXIT REASON falls back to the order-type-derived
      &quot;Market Exit&quot;. The engine behaviour is correct; only the report is
      losing the tag. Fix: the lookup needs to disambiguate close vs. open
      fills (e.g. by also keying on <code>is_reduce_only</code>).
    </p>
  </div>"""
    else:
        issue_card_html = """
  <div class="ok-card">
    <h4>Reverse-action tag propagation: fixed</h4>
    <p style="font-size:0.86rem; margin:0.3rem 0;">
      <strong>S09 / S11 / S15</strong> all pass — the orderbook EXIT REASON
      column now correctly shows <code>Reverse on SL</code> /
      <code>Reverse on TP</code> labels matching what the engine emits.
    </p>
    <p style="font-size:0.86rem; margin:0.3rem 0; color:var(--text-muted);">
      Fix: <code>core/report_generator.py:_build_fills_lookup</code> was
      previously keying fills by
      <code>(trader, strategy, instrument, ts_seconds)</code> only — when
      <code>on_sl_action=&quot;reverse&quot;</code> fires a close+open pair on the
      same bar, the open fill (tag=None) was overwriting the tagged close
      fill. The lookup now splits into <code>by_pos_ts_open</code> /
      <code>by_pos_ts_close</code> dicts (keyed additionally by
      <code>is_reduce_only</code>), and <code>_build_orderbook</code>
      reads from the role-appropriate dict for entry vs. exit fills, so the
      structured close tag is preserved.
    </p>
  </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Stoploss Tab Test Report &mdash; 16 stress scenarios</title>
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
  .verdict {{ font-size: 0.72rem; font-weight: 700; padding: 0.2rem 0.6rem;
    border-radius: 3px; text-transform: uppercase; letter-spacing: 0.05em;
    flex: 0 0 auto; }}
  .v-pass {{ background: #103018; color: var(--accent2); }}
  .v-fail {{ background: #2a1018; color: var(--accent5); }}
  .v-error {{ background: #2a2410; color: var(--accent4); }}
  .test-body {{ padding: 0.9rem 1rem;
    display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.9rem; }}
  .test-section {{ font-size: 0.85rem; }}
  .test-section ul.params {{ padding-left: 1.1rem; }}
  .test-section ul.params li {{ margin: 0.15rem 0; font-size: 0.8rem; }}
  .test-section .expected {{ font-style: italic; color: var(--text-muted); }}
  .test-section .hint {{ color: var(--text-muted); font-size: 0.72rem;
    font-style: italic; margin-bottom: 0.3rem; }}
  .obs-table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
  .obs-table td {{ padding: 0.25rem 0.5rem; border-top: 1px solid var(--border);
    vertical-align: top; }}
  .obs-table td:first-child {{ color: var(--text-muted); width: 45%; font-weight: 500; }}
  .obs-table .pos {{ color: var(--accent2); font-weight: 600; }}
  .obs-table .neg {{ color: var(--accent5); font-weight: 600; }}
  .obs-table .zero {{ color: var(--text-muted); }}
  .count-row {{ display: flex; flex-wrap: wrap; gap: 0.3rem; margin-bottom: 0.4rem; }}
  .count-pill {{ background: var(--code-bg); border: 1px solid var(--border);
    border-radius: 3px; padding: 0.18rem 0.45rem; font-size: 0.74rem;
    display: inline-flex; align-items: center; gap: 0.35rem; }}
  .count-pill.pill-engine {{ border-color: var(--accent2); }}
  .count-pill.pill-orderbook {{ border-color: var(--accent3); }}
  .count-num {{ background: var(--surface); padding: 0 0.35rem;
    border-radius: 8px; font-weight: 700; font-size: 0.7rem; }}
  .checks-row {{ background: var(--code-bg); border-top: 1px solid var(--border);
    padding: 0.7rem 1rem; }}
  .checks {{ list-style: none; padding-left: 0; margin-top: 0.3rem; }}
  .checks li {{ font-size: 0.82rem; padding: 0.2rem 0; }}
  .checks .chk-pass {{ color: var(--accent2); font-weight: 700; margin-right: 0.4rem; }}
  .checks .chk-fail {{ color: var(--accent5); font-weight: 700; margin-right: 0.4rem; }}
  .checks .chk-detail {{ color: var(--text-muted); font-size: 0.72rem;
    padding-left: 1.2rem; word-break: break-word; }}
  .scen-table, .ref-table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
  .scen-table th, .ref-table th {{ text-align: left; padding: 0.5rem 0.4rem;
    color: var(--text-muted); border-bottom: 1px solid var(--border);
    font-weight: 600; }}
  .scen-table td, .ref-table td {{ padding: 0.45rem 0.4rem; border-bottom: 1px solid var(--border);
    vertical-align: top; }}
  .scen-table tr:hover td {{ background: var(--code-bg); }}
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
  .warn-card {{ background: linear-gradient(135deg, #2a2410, #161b22);
    border-left: 4px solid var(--accent4); padding: 0.9rem 1.1rem;
    margin: 0.8rem 0; border-radius: 0 6px 6px 0; }}
  .warn-card h4 {{ color: var(--accent4); font-size: 0.95rem; }}
  .muted {{ color: var(--text-muted); font-size: 0.78rem; font-style: italic; }}
  @media (max-width: 1000px) {{
    .test-body {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Stoploss Tab Test Report &mdash; 16 stress scenarios</h1>
  <p>Per-slot exit_config (stop_loss_type, trailing, target_lock, on_sl_action,
     re_execute, reverse, sl_wait_bars …) verified end-to-end against backtest
     output and orderbook reports. Generated {timestamp}.</p>
</div>

<div class="container">

<section>
  <h2>Summary</h2>
  {_stat_cards(results)}
  <p style="font-size: 0.86rem; color: var(--text-muted);">
    <strong>{total_pass}/{total} pass</strong> end-to-end (engine + orderbook).
    <strong>{total_fail} fail</strong> (engine emits the right tag but the
    orderbook layer drops it on close+open same-bar — see S09/S11/S15).
    <strong>{total_err} engine errors</strong> (none expected; any non-zero is a regression).
  </p>

  <div class="ok-card">
    <h4>What this report verifies</h4>
    <ul style="padding-left:1.4rem; margin-top:0.3rem;">
      <li><strong>Engine layer (fills_report.tags)</strong> — Each closing fill
        carries a structured tag like <code>"Stop Loss: price=… ≤ SL=… (entry …, …%)"</code>.
        The tag prefix (Stop Loss / Trailing SL / Take Profit / Reverse on SL /
        Reverse on TP) is the engine's source of truth for what fired.</li>
      <li><strong>Report layer (orderbook EXIT REASON)</strong> — The
        <code>build_orderbook_dataframe</code> function joins fills to positions
        by timestamp, splits the tag at the first colon, and surfaces the prefix
        in the orderbook UI. Each test asserts that the prefix the engine emitted
        also lands in the orderbook.</li>
      <li><strong>Fixed and trailing SL</strong> — Percentage, points, and trailing
        SL all produce expected exit reasons; trailing tightens with watermark.</li>
      <li><strong>Composite settings</strong> — Multi-slot portfolios with
        different SL types per slot, target_lock SL ratchet, sl_wait_bars
        confirmation delay, re_execute / reverse follow-up actions.</li>
    </ul>
  </div>

  {issue_card_html}
</section>

<section>
  <h2>Stoploss tab settings reference</h2>
  <p style="font-size:0.86rem; color:var(--text-muted); margin-bottom:0.6rem;">
    Every per-slot setting exposed in the stoploss tab of the slot/leg modal.
    These are the inputs that flow into <code>exit_config</code> on each slot
    and into <code>ManagedExitConfig</code> on the running strategy.
  </p>
  {_settings_reference()}
</section>

<section>
  <h2>Compact scenarios table</h2>
  <p style="font-size:0.82rem; color:var(--text-muted); margin-bottom:0.5rem;">
    sl_hits = Stop Loss + Trailing SL + Reverse on SL fills.
    tp_hits = Take Profit + Reverse on TP fills.
    Counts come from the engine's fills_report.tags (source of truth).
  </p>
  {_scenarios_table(results)}
</section>

<section>
  <h2>Detailed test cards</h2>
  <p style="font-size:0.86rem; color:var(--text-muted); margin-bottom:0.6rem;">
    Each card shows: parameters supplied to the slot's exit_config / portfolio,
    expected behaviour, the engine-level fills tag counts (source of truth),
    the orderbook EXIT REASON column (report layer), and per-check verifier
    pass/fail breakdown. Click the per-test JSON path to inspect the saved portfolio.
  </p>
  {cards_html}
</section>

<section>
  <h2>How to reproduce</h2>
  <pre>python stress_test_stoploss.py
python stress_test_stoploss_report.py</pre>
  <p style="font-size: 0.85rem; color: var(--text-muted);">
    Both scripts are at the project root. They write the per-test portfolio
    JSON files to <code>5. Logics/stoploss_tests/portfolios/</code>, raw
    results to <code>stoploss_results.json</code>, and this HTML report to
    <code>stoploss_report.html</code>. Safe to re-run; each invocation
    overwrites prior output.
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
