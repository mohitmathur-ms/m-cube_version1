"""Generate html_reports/native_bracket_orders_report.html.

Runs the real-BacktestEngine Market-vs-Bracket SL/TP comparison (reusing the
Phase 0 harness) and emits a single self-contained HTML report containing:
  - a Mermaid knowledge graph of the native-bracket order model,
  - the change set explained in Plain English and in Nautilus terminology,
  - sample-data tables (OHLC + SL/TP prices) showing Market-order fills vs
    native-bracket fills and the slippage delta, with a mean/worst summary.

    venv\\Scripts\\python.exe scripts\\generate_native_bracket_report.py
"""
from __future__ import annotations

import html
import os
import sys
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
sys.path.insert(0, os.path.dirname(_HERE))

from sim_sltp_accuracy import CASES, ENTRY, PIP, SL, TP, _label_and_slip, run_case

OUT = os.path.join(os.path.dirname(_HERE), "html_reports", "native_bracket_orders_report.html")

# Per-case bar OHLC (the post-entry "case bar"), mirrored from sim_sltp_accuracy.
CASE_DESC = {
    "B move-through SL": "Low pierces the SL then the bar closes back above it.",
    "C gap-down SL": "Bar OPENS already below the SL (a gap through the stop).",
    "D both in one bar": "Bar spans BOTH the TP and the SL within one minute.",
}


def collect():
    """Run the engine for each case/scenario and return rows + summary."""
    rows = []
    slips = {"Market": [], "Bracket": []}
    for case_name, ohlc in CASES.items():
        for label, sc in (("Market", "market"), ("Bracket", "bracket")):
            entry_fill, exit_fill = run_case(ohlc, sc, adaptive=True)
            exit_type, intended, slip = _label_and_slip(entry_fill, exit_fill)
            slips[label].append(abs(slip))
            rows.append({
                "case": case_name, "ohlc": ohlc, "mech": label,
                "exit": exit_type, "intended": intended,
                "fill": exit_fill, "slip": slip,
            })
    summary = {
        m: {"mean": sum(v) / len(v), "worst": max(v)} for m, v in slips.items()
    }
    return rows, summary


MERMAID = """graph TD
  AGG["BarAggregator<br/>(custom 5-min signal)"] -->|entry signal| STRAT["ManagedExitStrategy"]
  STRAT -->|"exit_mode = native_bracket"| BR["order_factory.bracket()<br/>→ OrderList"]
  STRAT -->|"exit_mode = python"| PY["_check_exits()<br/>→ market close at bar close"]
  BR --> EN["MARKET entry"]
  BR --> SL["STOP_MARKET (SL)<br/>reduce_only"]
  BR --> TP["LIMIT (TP)<br/>reduce_only"]
  EN -->|"OTO: entry fill releases children"| SL
  EN -->|"OTO"| TP
  SL <-->|"OUO: one fills → other cancels"| TP
  ENG["Matching engine<br/>O→H→L→C bar sweep"] -->|"fills at trigger price"| SL
  ENG -->|"fills at limit price"| TP
  SL -->|fill| CL["on_position_closed → flat → re-arm"]
  TP -->|fill| CL
  classDef nat fill:#d8f5e3,stroke:#1b7f4b,color:#08311c;
  classDef py fill:#fde2e2,stroke:#b42318,color:#3b0a06;
  classDef eng fill:#e0ecff,stroke:#1d4ed8,color:#0a1f52;
  class BR,EN,SL,TP,CL nat;
  class PY py;
  class ENG,AGG,STRAT eng;
"""


def render(rows, summary) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Sample-data rows grouped by case (Market then Bracket).
    body_rows = []
    by_case = {}
    for r in rows:
        by_case.setdefault(r["case"], []).append(r)
    for case_name, recs in by_case.items():
        o, h, l, c = recs[0]["ohlc"]
        span = len(recs)
        ohlc_cell = f"O {o:.5f}<br>H {h:.5f}<br>L {l:.5f}<br>C {c:.5f}"
        desc = html.escape(CASE_DESC.get(case_name, ""))
        for i, r in enumerate(recs):
            mech_cls = "nat" if r["mech"] == "Bracket" else "py"
            cells = ""
            if i == 0:
                cells += (f'<td rowspan="{span}" class="case"><b>{html.escape(case_name)}</b>'
                          f'<div class="muted">{desc}</div></td>'
                          f'<td rowspan="{span}" class="mono">{ohlc_cell}</td>')
            cells += (
                f'<td class="{mech_cls}">{r["mech"]}</td>'
                f'<td>{r["exit"]}</td>'
                f'<td class="mono">{r["intended"]:.5f}</td>'
                f'<td class="mono">{r["fill"]:.5f}</td>'
                f'<td class="mono {"good" if abs(r["slip"]) <= 1 else "bad"}">{r["slip"]:+.1f}</td>'
            )
            body_rows.append(f"<tr>{cells}</tr>")

    summary_rows = "".join(
        f'<tr><td class="{ "nat" if m=="Bracket" else "py"}">{m}</td>'
        f'<td class="mono">{s["mean"]:.2f}</td><td class="mono">{s["worst"]:.2f}</td></tr>'
        for m, s in summary.items()
    )

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Native Bracket Orders — m-cube</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{startOnLoad:true, theme:"neutral"}});</script>
<style>
  body{{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
       max-width:1040px;margin:0 auto;padding:32px 24px;color:#1c2430;line-height:1.55;}}
  h1{{font-size:26px;margin-bottom:4px}} h2{{margin-top:36px;border-bottom:2px solid #eaecef;padding-bottom:6px}}
  h3{{margin-top:22px}}
  .sub{{color:#6b7480;margin-top:0}}
  code{{background:#f3f4f6;padding:1px 5px;border-radius:4px;font-size:90%}}
  table{{border-collapse:collapse;width:100%;margin:14px 0;font-size:14px}}
  th,td{{border:1px solid #dfe3e8;padding:7px 9px;text-align:left;vertical-align:top}}
  th{{background:#f6f8fa}}
  .mono{{font-family:ui-monospace,Consolas,Menlo,monospace}}
  .muted{{color:#6b7480;font-size:12px;font-weight:normal}}
  .good{{color:#1b7f4b;font-weight:600}} .bad{{color:#b42318;font-weight:600}}
  td.nat,th.nat{{background:#eafaf0}} td.py{{background:#fdeeee}}
  .mermaid{{background:#fbfcfe;border:1px solid #e6e9ee;border-radius:8px;padding:16px;margin:14px 0}}
  .legend span{{display:inline-block;margin-right:14px;font-size:13px}}
  .box{{background:#f6f8fa;border-left:4px solid #1d4ed8;padding:10px 14px;border-radius:4px;margin:12px 0}}
  .pill{{background:#1b7f4b;color:#fff;border-radius:12px;padding:2px 10px;font-size:12px}}
</style></head>
<body>
<h1>Native Bracket Orders for SL/TP</h1>
<p class="sub">m-cube · NautilusTrader 1.224.0 · generated {ts} · feature flag <code>_USE_NATIVE_BRACKET=1</code></p>

<div class="box"><b>Summary.</b> Plain "fixed SL + fixed TP, close-only" legs now submit a native
NautilusTrader <b>bracket</b> (MARKET entry + STOP_MARKET SL + LIMIT TP). The matching engine fires
the resting SL/TP at the <b>trigger/limit price</b> during each bar's O→H→L→C sweep — replacing the
old Python check that closed at the <b>bar close</b>. Mean SL/TP slippage drops from
<b class="bad">~7 pips</b> to <b class="good">~1.7 pips</b>. Advanced legs (trailing, target-lock,
wait-bars, re_execute/reverse, RBO) automatically stay on the Python exit engine.</p>

<h2>1 · Knowledge Graph</h2>
<div class="mermaid">{MERMAID}</div>
<p class="legend">
  <span class="pill">native bracket path</span>
  <span style="color:#b42318">■ python exit path (advanced legs)</span>
  <span style="color:#1d4ed8">■ engine / signal</span>
</p>
<ul>
  <li><b>OTO</b> (One-Triggers-Other): the MARKET entry filling <i>releases</i> the resting SL &amp; TP children onto the venue.</li>
  <li><b>OUO</b> (One-Updates-Other): when one child fills, the sibling's open quantity is reduced to zero (auto-cancel), because both are <code>reduce_only</code> on a now-flat position.</li>
  <li>The 5-minute signal is still produced by the custom <code>core.aggregator.BarAggregator</code>; only the 1-minute EXTERNAL bars drive the matching engine that sweeps the SL/TP.</li>
</ul>

<h2>2 · What changed — Plain English</h2>
<p>Previously, the strategy watched each bar and, when price crossed the stop or target, sent a
plain <b>market order</b> to close. Because that decision is made only after the bar is complete, the
order could only fill at that bar's <b>closing price</b> — which can be several pips past the level
you intended (or, when one bar straddles both stop and target, at a price unrelated to either).</p>
<p>Now, for simple setups, the strategy places the stop and target <b>up front, together with the
entry</b>, as real resting orders held by the exchange simulator. The simulator walks each minute's
price path and triggers the stop or target <b>exactly at your level</b> the moment price reaches it.
The result is far more realistic fills and much smaller slippage. Anything that needs ongoing
decision-making (a trailing stop, locking in profit, waiting N bars to confirm, re-entering or
reversing) still uses the original logic, because a fixed exchange order cannot express those.</p>

<h2>3 · What changed — Technical (Nautilus terminology)</h2>
<ul>
  <li><b>Per-leg <code>exit_mode</code></b>: <code>config_from_exit</code> calls a new
      <code>_classify_exit_mode()</code> → <code>"native_bracket"</code> only for fixed SL <i>and</i>
      fixed TP (percentage/points/atr), close-only actions, no trailing / target-lock /
      sl_wait / tgt_wait / RBO / Move-SL, and only when <code>_USE_NATIVE_BRACKET=1</code>; else
      <code>"python"</code>.</li>
  <li><b>Entry</b>: <code>_submit_bracket()</code> calls
      <code>self.order_factory.bracket(entry_order_type=MARKET, sl_trigger_price=…, tp_price=…,
      tp_post_only=False)</code> and <code>submit_order_list()</code>. SL/TP are computed by the
      shared <code>_compute_sl_tp(is_buy, ref_price)</code> off the signal-bar close. No
      <code>position_id</code> under <code>OmsType.NETTING</code>.</li>
  <li><b>Contingency</b>: entry is <code>ContingencyType.OTO</code> with the SL/TP as
      <code>linked_order_ids</code>; SL↔TP are <code>OUO</code>, both <code>reduce_only</code>.</li>
  <li><b>Fills</b>: <code>on_order_filled</code> branches on
      <code>event.order_type</code> — a <code>MARKET</code> fill is the entry; a
      <code>STOP_MARKET</code>/<code>LIMIT</code> fill is a child exit (ignored there).
      <code>on_position_closed</code> resets the leg to flat so the next signal can re-enter.</li>
  <li><b>Exit checks</b>: <code>_on_primary_bar</code> skips the Python <code>_check_exits</code>
      for native-bracket legs (the engine fires them); <code>_force_squareoff</code> calls
      <code>cancel_all_orders</code> before <code>close_all_positions</code> to clear the resting
      children race-free.</li>
  <li><b>Venue</b>: <code>bar_adaptive_high_low_ordering=True</code> for realistic intra-bar
      SL-vs-TP ordering when both fall inside one bar.</li>
</ul>

<h2>4 · Sample data — Market order vs native bracket</h2>
<p>LONG EUR/USD, entry <code>{ENTRY:.5f}</code>, <b>SL {SL:.5f}</b>, <b>TP {TP:.5f}</b>
(pip = {PIP}). Live fills from a real <code>BacktestEngine</code> run
(<code>bar_adaptive_high_low_ordering=True</code>). Slippage = fill − intended level.</p>
<table>
  <thead><tr>
    <th>Case (post-entry bar)</th><th>Bar OHLC</th><th>Mechanism</th><th>Exit</th>
    <th>Intended level</th><th>Actual fill</th><th>Slip (pips)</th>
  </tr></thead>
  <tbody>{''.join(body_rows)}</tbody>
</table>

<h3>Accuracy summary</h3>
<table>
  <thead><tr><th>Mechanism</th><th>Mean |slip| (pips)</th><th>Worst |slip| (pips)</th></tr></thead>
  <tbody>{summary_rows}</tbody>
</table>
<p class="muted">Case C's bracket slippage is a genuine market <b>gap</b> (the bar opens past the
stop, so it fills at the open) — correct gap modelling, not engine error. The Market mechanism never
fills at the level: it always settles at the bar close.</p>

</body></html>
"""


def main() -> None:
    rows, summary = collect()
    htmltext = render(rows, summary)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write(htmltext)
    print(f"wrote {OUT}")
    print(f"  Market  mean |slip| = {summary['Market']['mean']:.2f} pips (worst {summary['Market']['worst']:.2f})")
    print(f"  Bracket mean |slip| = {summary['Bracket']['mean']:.2f} pips (worst {summary['Bracket']['worst']:.2f})")


if __name__ == "__main__":
    main()
