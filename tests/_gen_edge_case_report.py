"""Generate the edge-case portfolio HTML report from _edge_case_results.json.

For every portfolio: the date range, environment flags, the saved JSON path, the
FULL parameter dump (portfolio-level + each leg + each leg's exit config), and the
backtest result (trades, PnL, exit-reason breakdown, clip, verdict)."""
from __future__ import annotations
import html, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = json.loads((ROOT / "html_reports" / "tests_html_report"
                   / "_edge_case_results.json").read_text(encoding="utf-8"))
OUT = ROOT / "html_reports" / "tests_html_report" / "edge_case_portfolios_report.html"

VBADGE = {"PASS": ("ok", "PASS"), "DEGRADED-OK": ("partial", "DEGRADED-OK"), "FAIL": ("miss", "FAIL")}

# Portfolio-level scalar fields worth showing, grouped for readability.
PF_GROUPS = [
    ("Core", ["name", "starting_capital", "start_date", "end_date", "allocation_mode",
              "product", "mis_squareoff_time", "mis_squareoff_tz", "max_loss", "max_profit"]),
    ("Timing", ["squareoff_time", "squareoff_tz", "entry_start_time", "entry_end_time",
                "run_on_days", "delay_between_legs_sec", "no_reentry_after_end"]),
    ("RBO", ["rbo_enabled", "range_monitoring_start", "range_monitoring_end", "rbo_entry_start",
             "rbo_entry_end", "rbo_range_buffer", "rbo_entry_at", "rbo_monitoring", "rbo_cancel_other_side"]),
    ("Portfolio SL", ["pf_sl_enabled", "pf_sl_type", "pf_sl_value", "pf_sl_underlying_below",
                      "pf_sl_underlying_above", "pf_sl_action", "pf_sl_delay_sec", "pf_sl_reexecute_count",
                      "pf_sl_sqoff_only_loss_legs", "pf_sl_sqoff_only_profit_legs", "pf_sl_trail_enabled",
                      "pf_sl_trail_every", "pf_sl_trail_by", "pf_sl_target_portfolio"]),
    ("Portfolio Target", ["pf_tgt_enabled", "pf_tgt_type", "pf_tgt_value", "pf_tgt_action",
                          "pf_tgt_delay_sec", "pf_tgt_reexecute_count", "pf_tgt_trail_enabled",
                          "pf_tgt_trail_lock_min_profit", "pf_tgt_trail_when_profit_reach",
                          "pf_tgt_trail_every", "pf_tgt_trail_by", "pf_tgt_target_portfolio"]),
    ("Move-SL", ["move_sl_enabled", "move_sl_safety_sec", "move_sl_action", "move_sl_trail_after",
                 "move_sl_no_buy_legs", "move_sl_hit_on_leg_sl", "move_sl_hit_on_leg_target",
                 "move_sl_ltp_buffer", "move_sl_agg_pnl_enabled", "move_sl_agg_pnl_threshold",
                 "move_sl_agg_pnl_direction"]),
    ("ReExec gates / misc", ["no_reexec_sl_cost", "no_wait_trade_reexec", "no_strike_change_reexec",
                             "no_reentry_sl_cost", "on_sl_action_on", "on_target_action_on",
                             "straddle_width_multiplier", "trail_wait_trade", "exit_order_type", "exit_sell_first"]),
]

LEG_FIELDS = ["slot_id", "strategy_name", "bar_type_str", "strategy_bar_types", "lots",
              "allocation_pct", "strategy_params", "squareoff_time", "squareoff_tz",
              "start_date", "end_date"]
EC_FIELDS = ["exit_price_format", "stop_loss_type", "stop_loss_value", "trailing_sl_step",
             "trailing_sl_offset", "sl_atr_period", "sl_atr_multiplier", "target_type",
             "target_value", "tgt_atr_period", "tgt_atr_multiplier", "target_lock_trigger",
             "target_lock_minimum", "tgt_trail_enabled", "tgt_trail_when_profit_reach",
             "tgt_trail_lock_min_profit", "tgt_trail_every", "tgt_trail_by", "sl_wait_sec",
             "sl_wait_bars", "tgt_wait_sec", "tgt_wait_bars", "on_sl_action", "on_target_action",
             "max_re_executions", "execute_target_leg_id", "reentry_price", "max_re_entries",
             "armed_at_start", "squareoff_time", "squareoff_tz"]


def fmt(v):
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, float):
        return f"{v:,.4f}".rstrip("0").rstrip(".") if abs(v) < 1e6 else f"{v:,.2f}"
    if isinstance(v, (list, dict)):
        return html.escape(json.dumps(v, default=str))
    return html.escape(str(v))


def kv_table(d, keys, title):
    rows = ""
    for k in keys:
        if k in d and d[k] not in (None, "", 0, 0.0, False, [], {}):
            rows += f"<tr><td>{k}</td><td>{fmt(d[k])}</td></tr>"
    if not rows:
        return ""
    return f"<div class='grp'><div class='grpt'>{title}</div><table class='p'>{rows}</table></div>"


def leg_block(slot, i):
    leg_rows = "".join(f"<tr><td>{k}</td><td>{fmt(slot.get(k))}</td></tr>"
                       for k in LEG_FIELDS if slot.get(k) not in (None, "", [], {}))
    ec = slot.get("exit_config") or {}
    ec_rows = "".join(f"<tr><td>{k}</td><td>{fmt(ec.get(k))}</td></tr>"
                      for k in EC_FIELDS if ec.get(k) not in (None, "", 0, 0.0, False))
    return (f"<div class='leg'><div class='legh'>Leg {i+1}: "
            f"{html.escape(str(slot.get('strategy_name','?')))} on "
            f"{html.escape(str(slot.get('bar_type_str','?')))}</div>"
            f"<div class='legcols'><table class='p'><tr><th colspan=2>Leg</th></tr>{leg_rows}</table>"
            f"<table class='p'><tr><th colspan=2>Exit config (leg-level SL/Target)</th></tr>{ec_rows}</table></div></div>")


def result_table(r):
    rows = [
        ("Total trades", fmt(r.get("total_trades"))),
        ("Total PnL", fmt(r.get("total_pnl"))),
        ("Final balance", fmt(r.get("final_balance"))),
        ("Win rate %", fmt(r.get("win_rate"))),
        ("Max drawdown %", fmt(r.get("max_drawdown"))),
        ("Per-strategy slots", fmt(r.get("per_strategy_slots"))),
        ("Portfolio clip", f"{fmt(r.get('pf_clip_reason'))} @ {fmt(r.get('pf_clip_ts'))} (action {fmt(r.get('pf_clip_action'))})"),
        ("ReExecute replays", fmt(r.get("pf_reexec_replays"))),
        ("Max-Loss / Max-Profit hit", f"{fmt(r.get('max_loss_hit'))} / {fmt(r.get('max_profit_hit'))}"),
        ("Exit-reason breakdown", ", ".join(f"{k}:{v}" for k, v in (r.get("exit_reasons") or {}).items()) or "—"),
    ]
    return "".join(f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in rows)


def env_str(env):
    on = [k for k, v in (env or {}).items() if v]
    return " ".join(f"<code>{k}=1</code>" for k in on) or "<span class='muted'>(all default / off)</span>"


cards = []
tally = {"PASS": 0, "DEGRADED-OK": 0, "FAIL": 0}
for r in DATA:
    tally[r.get("verdict", "FAIL")] = tally.get(r.get("verdict", "FAIL"), 0) + 1
    vc, vt = VBADGE.get(r.get("verdict"), ("miss", r.get("verdict", "?")))
    params = r.get("params") or {}
    pf_tables = "".join(kv_table(params, keys, title) for title, keys in PF_GROUPS)
    legs = "".join(leg_block(s, i) for i, s in enumerate(params.get("slots", [])))
    res = (f"<table class='res'>{result_table(r['result'])}</table>" if r.get("result")
           else "<p class='muted'>No result — case raised an exception (see below).</p>")
    err = (f"<div class='err'><b>Raised:</b> <code>{html.escape(str(r.get('error'))[:500])}</code></div>"
           if r.get("error") else "")
    cards.append(f"""
<section class="tc">
  <div class="tch"><span class="tcid">{r['id']}</span>
    <span class="tctitle">{html.escape(r['title'])}</span>
    <span class="vb {vc}">{vt}</span></div>
  <p class="desc">{html.escape(r.get('desc',''))}</p>
  <table class="meta">
    <tr><td>Backtest date range</td><td><b>{html.escape(r['range'])}</b></td></tr>
    <tr><td>Environment flags</td><td>{env_str(r.get('env'))}</td></tr>
    <tr><td>Saved to project</td><td><code>{html.escape(r.get('saved_as','—'))}</code></td></tr>
    <tr><td>Runtime / status</td><td>{fmt(r.get('seconds'))} s · <b>{html.escape(r.get('status','?'))}</b></td></tr>
  </table>
  <h4>Portfolio-level parameters</h4>
  <div class="pgrid">{pf_tables}</div>
  <h4>Legs &amp; leg-level exit configs</h4>
  {legs}
  <h4>Backtest result</h4>
  {res}
  {err}
  <div class="verdict {vc}"><b>Verification:</b> {html.escape(r.get('note',''))}</div>
</section>""")

CSS = """
:root{--bg:#0f1117;--surface:#1a1d27;--surface2:#22263a;--border:#2e3250;--text:#d4d8f0;
--muted:#8a90b0;--accent:#5b7cfa;--ok:#3ecf8e;--partial:#f7c948;--miss:#e96060;--code:#141824;}
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:'Inter',-apple-system,'Segoe UI',sans-serif;background:var(--bg);color:var(--text);line-height:1.55;font-size:13.5px;}
.wrap{max-width:1240px;margin:0 auto;padding:0 34px 100px;}
header.cover{background:linear-gradient(135deg,#3ecf8e,#5b7cfa);color:#fff;padding:36px 40px;margin-bottom:8px;border-radius:0 0 14px 14px;}
header.cover h1{font-size:24px;margin-bottom:6px;}
header.cover p{color:rgba(255,255,255,.92);font-size:13px;}
h2{font-size:18px;margin:34px 0 12px;color:#fff;padding-bottom:8px;border-bottom:1px solid var(--border);}
h4{font-size:12px;color:var(--accent);margin:16px 0 6px;text-transform:uppercase;letter-spacing:.05em;}
p.desc{color:var(--muted);margin:8px 0 12px;font-size:13px;}
.muted{color:var(--muted);}
code{font-family:'Fira Code',Consolas,monospace;font-size:10.5px;background:var(--code);border:1px solid var(--border);border-radius:3px;padding:1px 5px;color:#f7c948;}
.sumgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:16px 0;}
.sumcard{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;text-align:center;}
.sumcard .n{font-size:30px;font-weight:800;}
.sumcard .l{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin-top:4px;}
.callout{background:rgba(91,124,250,.08);border-left:3px solid var(--accent);padding:12px 16px;margin:14px 0;border-radius:0 6px 6px 0;font-size:12.5px;}
.callout strong{color:#fff;}
.tc{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:18px 22px;margin:16px 0;}
.tch{display:flex;align-items:center;gap:10px;margin-bottom:6px;flex-wrap:wrap;}
.tcid{background:var(--accent);color:#fff;font-weight:800;font-size:12px;border-radius:5px;padding:3px 9px;}
.tctitle{font-size:14.5px;font-weight:700;color:#fff;flex:1;min-width:200px;}
.vb{font-size:11px;font-weight:800;border-radius:5px;padding:3px 10px;}
.vb.ok{background:rgba(62,207,142,.15);color:var(--ok);border:1px solid var(--ok);}
.vb.partial{background:rgba(247,201,72,.15);color:var(--partial);border:1px solid var(--partial);}
.vb.miss{background:rgba(233,96,96,.15);color:var(--miss);border:1px solid var(--miss);}
table{width:100%;border-collapse:collapse;font-size:12px;margin:4px 0;background:var(--surface2);border:1px solid var(--border);border-radius:6px;overflow:hidden;}
td,th{padding:5px 9px;border-bottom:1px solid var(--border);color:var(--muted);vertical-align:top;text-align:left;}
th{color:#aab0d0;font-weight:700;background:rgba(91,124,250,.07);font-size:11px;}
tr:last-child td{border-bottom:none;}
table.meta td:first-child,table.res td:first-child{width:220px;color:#aab0d0;font-weight:600;}
table.p td:first-child{width:50%;color:#9aa0c0;}
td b{color:var(--text);}
.pgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px;align-items:start;}
.grp .grpt{font-size:11px;font-weight:700;color:var(--accent);margin:2px 0;}
.leg{border:1px solid var(--border);border-radius:6px;padding:8px 10px;margin:6px 0;background:rgba(34,38,58,.4);}
.legh{font-size:12px;font-weight:700;color:#fff;margin-bottom:4px;}
.legcols{display:grid;grid-template-columns:1fr 1fr;gap:8px;}
.err{background:rgba(233,96,96,.09);border-left:3px solid var(--miss);padding:8px 12px;margin:8px 0;border-radius:0 6px 6px 0;font-size:11.5px;}
.verdict{padding:8px 12px;margin-top:8px;border-radius:0 6px 6px 0;font-size:12px;}
.verdict.ok{background:rgba(62,207,142,.08);border-left:3px solid var(--ok);}
.verdict.partial{background:rgba(247,201,72,.08);border-left:3px solid var(--partial);}
.verdict.miss{background:rgba(233,96,96,.08);border-left:3px solid var(--miss);}
.verdict b{color:#fff;}
@media(max-width:700px){.legcols{grid-template-columns:1fr;}}
"""

HTML = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Edge-Case Portfolios — Verification</title><style>{CSS}</style></head><body>
<header class="cover">
  <h1>Complex Edge-Case Portfolios — Full Parameters &amp; Results</h1>
  <p>{len(DATA)} deliberately maximal portfolios exercising every leg-level and
  portfolio-level setting — SL/Target (%, points, trailing, ATR, lock, target-trailing,
  wait-bars/secs, all SL/Target actions), portfolio SL &amp; Target (Combined Loss/Profit,
  Underlying Movement, Loss-and-Range, trailing, ReExecute), Move-SL, MIS/NRML, RBO,
  multi-zone squareoff, entry window, run_on_days — all with the custom streaming
  aggregator. Each card shows the date range, every parameter value, the saved JSON path
  (loadable from the m-cube UI), and the backtest result.</p>
</header>
<div class="wrap">

<h2>1. Summary</h2>
<div class="sumgrid">
  <div class="sumcard"><div class="n" style="color:var(--ok)">{tally['PASS']}</div><div class="l">Pass</div></div>
  <div class="sumcard"><div class="n" style="color:var(--partial)">{tally['DEGRADED-OK']}</div><div class="l">Degraded-OK</div></div>
  <div class="sumcard"><div class="n" style="color:var(--miss)">{tally['FAIL']}</div><div class="l">Fail</div></div>
  <div class="sumcard"><div class="n">{len(DATA)}</div><div class="l">Total portfolios</div></div>
</div>
<div class="callout">
  <strong>How to run these yourself.</strong> Every portfolio is saved to
  <code>portfolios/_default/EDGE_*.json</code> — open the m-cube app, select the
  <code>_default</code> user, and they appear in the portfolio list ready to run.
  Environment flags (grouping, agg-Move-SL, ReExecute-replay) are listed per card and were
  set for the automated run here.
</div>

<h2>2. Per-portfolio parameters &amp; results</h2>
{''.join(cards)}

</div></body></html>
"""

OUT.write_text(HTML, encoding="utf-8")
print(f"wrote {OUT}  ({tally['PASS']} PASS, {tally['DEGRADED-OK']} DEGRADED-OK, {tally['FAIL']} FAIL)")
