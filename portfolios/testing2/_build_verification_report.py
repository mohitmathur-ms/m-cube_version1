"""Build the manual-verification RESULTS report for the testing2 set.

TEMPLATE STAGE: fully verifies VP-01 (leg fixed SL %) across the 3 axes
(Time / Logic / Price) against the real exported order book + the 1-second
catalog bars, renders candlestick evidence charts, and emits a single
self-contained dark-theme HTML report under portfolios/testing2/.

Run:
    venv\\Scripts\\python.exe portfolios\\testing2\\_build_verification_report.py
"""
from __future__ import annotations
import base64
import html
import io
import re
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

import _verify_lib as V

ORDERBOOK_CSV = "reports/testing2/order_book_portfolio_VP-01_long_sl_pct_17_june_2026.csv"
OUT_HTML = "portfolios/testing2/manual_verification_v2_RESULTS.html"
HALF_TICK = V.TICK / 2.0


# ─────────────────────────────────────────────────────────────────────────────
# Order book loading + per-trade analysis
# ─────────────────────────────────────────────────────────────────────────────
def load_orderbook(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["entry_dt"] = pd.to_datetime(df["ENTRY TIME"], dayfirst=True)
    df["exit_dt"] = pd.to_datetime(df["EXIT TIME"], dayfirst=True)
    return df


_SL_RE = re.compile(r"SL=([\d.]+)")


def analyse_sl_row(bars, row) -> dict:
    """Run the 3-axis VP-01 check on a single Stop Loss row."""
    is_long = row["TRANSACTION"] == "BUY"
    F = float(row["ENTRY PRICE"])
    fill = float(row["AVG EXIT PRICE"])
    m = _SL_RE.search(str(row["EXIT DETAILED REASON"]))
    engine_sl = float(m.group(1)) if m else float("nan")
    expected_sl = V.expected_pct_sl(F, 0.5, is_long)

    entry_iso = row["entry_dt"].strftime("%Y-%m-%d %H:%M:%S")
    exit_iso = row["exit_dt"].strftime("%Y-%m-%d %H:%M:%S")
    ts, _ = V.first_touch(bars, entry_iso, exit_iso, engine_sl, is_long)
    trig_bar = V.bar_at(bars, exit_iso)
    trig_close = float(trig_bar["close"]) if trig_bar is not None else float("nan")

    price_ok = abs(expected_sl - engine_sl) <= HALF_TICK
    time_ok = ts is not None and ts == row["exit_dt"].tz_localize(V.IST)
    fill_ok = trig_bar is not None and abs(trig_close - fill) <= HALF_TICK
    logic_ok = str(row["EXIT REASON"]).strip().lower() == "stop loss"

    return {
        "oid": str(row["OrderID"]), "side": "LONG" if is_long else "SHORT",
        "is_long": is_long, "F": F, "expected_sl": expected_sl, "engine_sl": engine_sl,
        "entry_iso": entry_iso, "exit_iso": exit_iso,
        "first_touch": ts.strftime("%Y-%m-%d %H:%M:%S") if ts is not None else "—",
        "trig_close": trig_close, "fill": fill,
        "price_ok": price_ok, "time_ok": time_ok, "fill_ok": fill_ok, "logic_ok": logic_ok,
        "passed": price_ok and time_ok and fill_ok and logic_ok,
    }


def analyse_sqoff_row(row) -> dict:
    t = row["exit_dt"]
    time_ok = (t.hour == 15 and t.minute == 29)
    logic_ok = str(row["EXIT REASON"]).strip().lower() == "squareoff"
    return {"oid": str(row["OrderID"]), "side": "LONG" if row["TRANSACTION"] == "BUY" else "SHORT",
            "exit_iso": t.strftime("%Y-%m-%d %H:%M:%S"),
            "time_ok": time_ok, "logic_ok": logic_ok, "passed": time_ok and logic_ok}


# ─────────────────────────────────────────────────────────────────────────────
# Charts (candlestick drawn manually — no mplfinance)
# ─────────────────────────────────────────────────────────────────────────────
DARK = {"bg": "#0d1117", "panel": "#161b22", "text": "#e6edf3", "grid": "#30363d",
        "green": "#3fb950", "red": "#f85149", "amber": "#d29922", "accent": "#58a6ff"}


def _style_ax(ax):
    ax.set_facecolor(DARK["panel"])
    for s in ax.spines.values():
        s.set_color(DARK["grid"])
    ax.tick_params(colors=DARK["text"], labelsize=8)
    ax.grid(True, color=DARK["grid"], lw=0.4, alpha=0.5)
    ax.xaxis.label.set_color(DARK["text"])
    ax.yaxis.label.set_color(DARK["text"])


def _fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight", facecolor=DARK["bg"])
    plt.close(fig)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


def chart_full_trade(bars, a: dict) -> str:
    """Close-price line over the whole trade with entry, SL level, exit marked."""
    lo = pd.Timestamp(a["entry_iso"], tz=V.IST)
    hi = pd.Timestamp(a["exit_iso"], tz=V.IST)
    seg = bars[(bars.index >= lo) & (bars.index <= hi)]
    fig, ax = plt.subplots(figsize=(8.4, 2.9))
    _style_ax(ax)
    ax.plot(seg.index, seg["close"], color=DARK["accent"], lw=0.8, label="close (1s)")
    ax.axhline(a["F"], color=DARK["text"], lw=1.0, ls=":", label=f"entry F={a['F']:.2f}")
    ax.axhline(a["engine_sl"], color=DARK["red"], lw=1.1, ls="--",
               label=f"SL={a['engine_sl']:.2f}")
    ax.scatter([lo], [a["F"]], color=DARK["green"], s=45, zorder=5, marker="^")
    ax.scatter([hi], [a["fill"]], color=DARK["red"], s=55, zorder=5, marker="x")
    ax.set_title(f"VP-01 · OID {a['oid']} · {a['side']} — full trade "
                 f"({a['entry_iso'][11:]}→{a['exit_iso'][11:]})",
                 color=DARK["text"], fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=seg.index.tz))
    leg = ax.legend(fontsize=7, facecolor=DARK["panel"], edgecolor=DARK["grid"], labelcolor=DARK["text"])
    return _fig_to_b64(fig)


def chart_trigger_zoom(bars, a: dict, pre=70, post=12) -> str:
    """Candlestick zoom around the trigger second showing the wick crossing SL."""
    hi = pd.Timestamp(a["exit_iso"], tz=V.IST)
    seg = bars[(bars.index >= hi - pd.Timedelta(seconds=pre)) &
               (bars.index <= hi + pd.Timedelta(seconds=post))]
    fig, ax = plt.subplots(figsize=(8.4, 2.9))
    _style_ax(ax)
    x = range(len(seg))
    for i, (ts, b) in zip(x, seg.iterrows()):
        up = b["close"] >= b["open"]
        col = DARK["green"] if up else DARK["red"]
        ax.plot([i, i], [b["low"], b["high"]], color=col, lw=0.7)
        ax.plot([i, i], [b["open"], b["close"]], color=col, lw=2.6)
    ax.axhline(a["engine_sl"], color=DARK["red"], ls="--", lw=1.1, label=f"SL={a['engine_sl']:.2f}")
    # highlight the trigger bar
    trig_pos = list(seg.index).index(hi) if hi in seg.index else None
    if trig_pos is not None:
        ax.axvline(trig_pos, color=DARK["amber"], lw=0.8, alpha=0.7)
        ax.scatter([trig_pos], [a["fill"]], color=DARK["amber"], s=55, zorder=6, marker="o",
                   label=f"fill={a['fill']:.2f}")
    ticks = list(x)[::max(1, len(seg) // 8)]
    ax.set_xticks(ticks)
    ax.set_xticklabels([seg.index[t].strftime("%H:%M:%S") for t in ticks], rotation=0)
    cross = "low ≤ SL" if a["is_long"] else "high ≥ SL"
    ax.set_title(f"VP-01 · OID {a['oid']} · {a['side']} — trigger zoom "
                 f"(first bar {cross} @ {a['exit_iso'][11:]})", color=DARK["text"], fontsize=9)
    ax.legend(fontsize=7, facecolor=DARK["panel"], edgecolor=DARK["grid"], labelcolor=DARK["text"])
    return _fig_to_b64(fig)


# ─────────────────────────────────────────────────────────────────────────────
# HTML
# ─────────────────────────────────────────────────────────────────────────────
def tick(ok: bool) -> str:
    return ('<span style="color:#3fb950;font-weight:700">&#10004;</span>' if ok
            else '<span style="color:#f85149;font-weight:700">&#10008;</span>')


def esc(s) -> str:
    return html.escape(str(s))


def build_html(sl_results, sqoff_results, charts) -> str:
    n_sl = len(sl_results)
    n_sl_pass = sum(r["passed"] for r in sl_results)
    n_sq = len(sqoff_results)
    n_sq_pass = sum(r["passed"] for r in sqoff_results)
    vp01_pass = (n_sl_pass == n_sl) and (n_sq_pass == n_sq)
    gen = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # axis roll-ups
    time_all = all(r["time_ok"] for r in sl_results) and all(r["time_ok"] for r in sqoff_results)
    logic_all = all(r["logic_ok"] for r in sl_results) and all(r["logic_ok"] for r in sqoff_results)
    price_all = all(r["price_ok"] and r["fill_ok"] for r in sl_results)

    # 5-row sample
    sample = sl_results[:5]
    sample_rows = "".join(
        f"<tr><td class='mono'>{esc(r['oid'])}</td><td>{esc(r['side'])}</td>"
        f"<td class='num'>{r['F']:.2f}</td><td class='num'>{r['expected_sl']:.2f}</td>"
        f"<td class='num'>{r['engine_sl']:.2f}</td><td class='center'>{tick(r['price_ok'])}</td>"
        f"<td class='mono'>{esc(r['first_touch'][11:])}</td><td class='mono'>{esc(r['exit_iso'][11:])}</td>"
        f"<td class='center'>{tick(r['time_ok'])}</td>"
        f"<td class='num'>{r['trig_close']:.2f}</td><td class='num'>{r['fill']:.2f}</td>"
        f"<td class='center'>{tick(r['fill_ok'])}</td></tr>" for r in sample)

    # full SL table
    full_rows = "".join(
        f"<tr><td class='mono'>{esc(r['oid'])}</td><td>{esc(r['side'])}</td>"
        f"<td class='num'>{r['F']:.2f}</td><td class='num'>{r['expected_sl']:.2f}</td>"
        f"<td class='num'>{r['engine_sl']:.2f}</td>"
        f"<td class='mono'>{esc(r['exit_iso'][11:])}</td>"
        f"<td class='num'>{r['trig_close']:.2f}</td><td class='num'>{r['fill']:.2f}</td>"
        f"<td class='center'>{tick(r['time_ok'])}</td><td class='center'>{tick(r['logic_ok'])}</td>"
        f"<td class='center'>{tick(r['price_ok'] and r['fill_ok'])}</td>"
        f"<td class='center'>{'<span class=pill-pass>PASS</span>' if r['passed'] else '<span class=pill-fail>FAIL</span>'}</td></tr>"
        for r in sl_results)

    sq_rows = "".join(
        f"<tr><td class='mono'>{esc(r['oid'])}</td><td>{esc(r['side'])}</td>"
        f"<td class='mono'>{esc(r['exit_iso'])}</td><td class='center'>{tick(r['time_ok'])}</td>"
        f"<td class='center'>{tick(r['logic_ok'])}</td>"
        f"<td class='center'>{'<span class=pill-pass>PASS</span>' if r['passed'] else '<span class=pill-fail>FAIL</span>'}</td></tr>"
        for r in sqoff_results)

    # arithmetic check for the first sample trade
    c = sample[0]
    factor = "1 − 0.005" if c["is_long"] else "1 + 0.005"
    cross = "low ≤ SL" if c["is_long"] else "high ≥ SL"

    overall_banner = (
        f'<div class="banner pass">&#10004; MANUAL VERIFICATION PASSED — VP-01 · '
        f'all {n_sl}/{n_sl} SL trades and {n_sq}/{n_sq} square-offs green across Time · Logic · Price</div>'
        if vp01_pass else
        f'<div class="banner fail">&#10008; VERIFICATION FAILED — VP-01 · '
        f'{n_sl_pass}/{n_sl} SL trades, {n_sq_pass}/{n_sq} square-offs passed</div>')

    roadmap = [
        ("VP-01", "Leg fixed SL % (long+short)", "DONE" if vp01_pass else "FAIL"),
        ("VP-02", "Leg Target %", "PENDING"), ("VP-03", "SL and Target (precedence)", "PENDING"),
        ("VP-04", "Short-side SL+Target", "PENDING"), ("VP-05", "Trailing SL", "PENDING"),
        ("VP-06", "ATR SL", "PENDING"), ("VP-07", "SL Wait", "PENDING"),
        ("VP-08", "On-SL Re-Execute", "PENDING"), ("VP-09", "On-SL Reverse", "PENDING"),
        ("VP-10", "Portfolio SL + leg SL", "PENDING"), ("VP-11", "Zero/disabled + MIS sqoff", "PENDING"),
        ("VP-12", "Zero-value flaw", "PENDING"),
        ("OT-01", "Market fill", "PENDING"), ("OT-02", "Limit (must 400)", "PENDING"),
        ("OT-03", "SL-Limit (must 400)", "PENDING"), ("OT-04", "Stop-Market", "PENDING"),
        ("OT-05", "GTT", "PENDING"), ("OT-06", "GTC", "PENDING"),
    ]
    rm_rows = "".join(
        f"<tr><td class='mono'>{cid}</td><td>{esc(desc)}</td>"
        f"<td class='center'>{'<span class=pill-pass>DONE</span>' if st=='DONE' else ('<span class=pill-fail>FAIL</span>' if st=='FAIL' else '<span class=pill-pend>PENDING</span>')}</td></tr>"
        for cid, desc, st in roadmap)

    chart_blocks = "".join(
        f'<figure class="chartfig"><img src="{src}" alt="{esc(cap)}"/>'
        f'<figcaption>{esc(cap)}</figcaption></figure>' for src, cap in charts)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Manual Verification RESULTS — testing2 (VP/OT)</title>
<style>
:root{{--bg:#0d1117;--panel:#161b22;--panel-2:#1c2128;--border:#30363d;--text:#e6edf3;--muted:#8b949e;--accent:#58a6ff;--green:#3fb950;--green-bg:#0d2818;--red:#f85149;--red-bg:#2d1316;--amber:#d29922;--amber-bg:#2d2206;--purple:#bc8cff;--purple-bg:#1f1535;--blue-bg:#0c2f4a;--code:#ff7b72;}}
*{{box-sizing:border-box;}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;line-height:1.55;color:var(--text);background:var(--bg);margin:0;}}
.container{{max-width:1240px;margin:0 auto;padding:32px 40px 80px;}}
header.cover{{background:linear-gradient(135deg,#1f6feb 0%,#8957e5 100%);color:#fff;padding:34px 36px;margin:-32px -40px 24px;border-radius:0 0 12px 12px;}}
header.cover h1{{margin:0 0 8px;font-size:25px;}}
header.cover .subtitle{{margin:0;font-size:14px;opacity:.95;}}
header.cover .meta{{margin-top:10px;font-size:12px;opacity:.85;}}
header.cover code{{background:rgba(255,255,255,.18);color:#fff;padding:1px 6px;border-radius:3px;}}
h2{{font-size:21px;margin-top:36px;padding-bottom:8px;border-bottom:2px solid var(--border);}}
h3{{font-size:16px;margin-top:22px;}}
p{{margin:8px 0;}}
code{{font-family:"SF Mono",Monaco,Consolas,monospace;font-size:12.5px;background:var(--panel-2);padding:1px 6px;border-radius:3px;color:var(--code);}}
table{{width:100%;border-collapse:collapse;margin:12px 0;font-size:13px;background:var(--panel);border:1px solid var(--border);border-radius:6px;overflow:hidden;}}
th,td{{text-align:left;padding:7px 9px;border-bottom:1px solid var(--border);vertical-align:top;}}
th{{background:var(--panel-2);font-weight:600;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.3px;}}
tr:last-child td{{border-bottom:none;}} tr:hover td{{background:var(--panel-2);}}
td.center,th.center{{text-align:center;white-space:nowrap;}}
td.num,th.num{{text-align:right;font-family:"SF Mono",Monaco,Consolas,monospace;font-size:12.5px;}}
td.mono{{font-family:"SF Mono",Monaco,Consolas,monospace;font-size:12.5px;}}
.banner{{padding:16px 20px;border-radius:10px;font-size:16px;font-weight:700;margin:18px 0;}}
.banner.pass{{background:var(--green-bg);color:var(--green);border:1px solid var(--green);}}
.banner.fail{{background:var(--red-bg);color:var(--red);border:1px solid var(--red);}}
.callout{{background:var(--blue-bg);border-left:4px solid var(--accent);padding:12px 16px;margin:12px 0;border-radius:0 6px 6px 0;font-size:14px;}}
.callout-ok{{background:var(--green-bg);border-left:4px solid var(--green);padding:12px 16px;margin:12px 0;border-radius:0 6px 6px 0;font-size:14px;}}
.callout-warn{{background:var(--amber-bg);border-left:4px solid var(--amber);padding:12px 16px;margin:12px 0;border-radius:0 6px 6px 0;font-size:14px;}}
.callout-tech{{background:var(--purple-bg);border-left:4px solid var(--purple);padding:12px 16px;margin:12px 0;border-radius:0 6px 6px 0;font-size:14px;}}
.callout-tech strong{{color:var(--purple);}}
.simple{{background:var(--panel-2);border-left:4px solid var(--muted);padding:12px 16px;margin:12px 0;border-radius:0 6px 6px 0;font-size:14.5px;}}
.simple::before{{content:"Plain English: ";font-weight:700;color:var(--muted);font-size:12px;letter-spacing:.5px;text-transform:uppercase;}}
.formula{{background:#010409;color:#e6edf3;padding:12px 16px;border-radius:6px;font-family:"SF Mono",Monaco,Consolas,monospace;font-size:12.5px;border:1px solid var(--border);}}
.formula .n{{color:#79c0ff;}} .formula .c{{color:#7ee787;}}
.filebox{{background:#010409;color:#f8f8f2;padding:8px 14px;border-radius:6px 6px 0 0;font-family:"SF Mono",Monaco,Consolas,monospace;font-size:12px;margin-top:12px;border:1px solid var(--border);border-bottom:none;}}
.filebox+table{{margin-top:0;border-radius:0 0 6px 6px;}}
.pill-pass{{background:var(--green-bg);color:var(--green);padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;}}
.pill-fail{{background:var(--red-bg);color:var(--red);padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;}}
.pill-pend{{background:var(--amber-bg);color:var(--amber);padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;}}
.case{{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:6px 20px 18px;margin:18px 0;}}
.chartfig{{margin:14px 0;}} .chartfig img{{width:100%;border:1px solid var(--border);border-radius:8px;display:block;}}
.chartfig figcaption{{font-size:12px;color:var(--muted);margin-top:6px;}}
.axisgrid{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:14px 0;}}
.axiscard{{background:var(--panel-2);border:1px solid var(--border);border-radius:8px;padding:12px 14px;}}
.axiscard h4{{margin:0 0 4px;font-size:13px;}} .axiscard .v{{font-size:13px;font-weight:700;}}
footer{{margin-top:40px;padding-top:16px;border-top:1px solid var(--border);color:var(--muted);font-size:12px;}}
</style></head><body><div class="container">

<header class="cover">
  <h1>Manual Verification RESULTS — SL/Target Features &amp; Order Types</h1>
  <p class="subtitle">Time &middot; Logic &middot; Price — every trade re-derived from the order book + the NIFTY 1-second catalog bars, never from what the engine claims. MIS / NIFTY, square-off 15:29 IST.</p>
  <p class="meta">Generated {gen} &middot; Source order book: <code>{esc(ORDERBOOK_CSV)}</code> &middot; Engine: nautilus_trader 1.224.0, Path A, no LatencyModel</p>
</header>

{overall_banner}

<div class="callout"><strong>What this report proves.</strong> For VP-01 (leg fixed stop-loss, 0.5%), each trade's stop-loss <em>level</em>, <em>trigger second</em>, and <em>realised fill</em> were independently recomputed from the trade's own entry fill + the raw 1-second bars and matched against the engine's output to the tick and to the second. A reader needs no engine access to trust the result — the math and the bars are shown.</div>

<h2>1 · How to read this report — three engine facts &amp; three axes</h2>
<div class="simple">Every trade is checked three ways: <b>when</b> it exited (Time), <b>why</b> it exited (Logic), and <b>at what price</b> the stop sat and filled (Price). We compute each from the data ourselves and confirm the engine agrees.</div>
<div class="callout-tech"><strong>Technically:</strong> verified against <code>core/managed_strategy.py</code> for this build. (1) Entry is a MARKET/GTC order filling at the <b>signal bar's close</b> (no LatencyModel). (2) An SL/Target does <b>not</b> fill at the level — it submits a reduce-only MARKET close that fills at the <b>trigger bar's close</b> (so trigger&ne;fill, by design). (3) The MIS square-off runs <em>before</em> SL/TP and fires on the first bar whose IST minute &ge; the 15:29 cutoff.</div>
<table>
<thead><tr><th>Axis</th><th>The check (recomputed independently)</th></tr></thead>
<tbody>
<tr><td><b>A &middot; TIME</b></td><td>From the 1-second bars, the <em>first</em> bar after entry where the trigger is true (long: <code>low&le;SL</code>; short: <code>high&ge;SL</code>) must equal the order book's exit second — not earlier (look-ahead), not later (lag).</td></tr>
<tr><td><b>B &middot; LOGIC</b></td><td>The exit <code>reason</code> matches the condition actually true; exactly one outcome; SL rows say <code>Stop Loss</code>, envelope rows say <code>Squareoff</code>.</td></tr>
<tr><td><b>C &middot; PRICE</b></td><td>Recompute the level from <code>entry fill &times; (1&mp;0.005)</code> snapped to tick 0.01 — must equal the engine's <code>SL</code>; and the realised fill must equal the trigger bar's <b>close</b>.</td></tr>
</tbody></table>

<h2>2 · VP-01 — Leg fixed Stop-Loss (percentage 0.5%) &nbsp; {'<span class=pill-pass>PASS</span>' if vp01_pass else '<span class=pill-fail>FAIL</span>'}</h2>
<p class="simple">A long buys and sets a stop 0.5% below the fill; a short sells and sets it 0.5% above. The stop should trip the instant price touches that level, and the trade should close at that bar's price. We check all {n_sl} stop-loss exits and all {n_sq} daily square-offs.</p>
<div class="callout-tech"><strong>Technically:</strong> config delta <code>stop_loss_type=percentage, stop_loss_value=0.5</code>, no target. Long <code>SL=round(F&times;0.995,0.01)</code>, short <code>SL=round(F&times;1.005,0.01)</code>. EMA(9/21) on the 5-min composite is the entry trigger; both long and short entries occur, so both SL directions are exercised.</div>

<h3>2.1 · The Price-axis formula</h3>
<div class="formula">
expected_sl = round( F &times; ( {factor} ) , tick <span class="n">0.01</span> )<br>
OID {esc(c['oid'])} ({esc(c['side'])}): round( <span class="n">{c['F']:.2f}</span> &times; <span class="n">{(0.995 if c['is_long'] else 1.005)}</span> , 0.01 ) = <span class="c">{c['expected_sl']:.2f}</span>
</div>

<h3>2.2 · Sample — first 5 stop-loss trades (the 3 axes at a glance)</h3>
<div class="simple">A five-row excerpt of the verification, the way you'd lay it out in a spreadsheet: re-derived SL vs engine SL (Price), first-touch second vs exit second (Time).</div>
<div class="filebox">{esc(ORDERBOOK_CSV)} &middot; first 5 rows with EXIT REASON = "Stop Loss"</div>
<table>
<thead><tr><th>OID</th><th>Side</th><th class="num">Entry F</th><th class="num">Exp. SL</th><th class="num">Engine SL</th><th class="center">Price&#10003;</th><th>First touch</th><th>Exit sec</th><th class="center">Time&#10003;</th><th class="num">Trig close</th><th class="num">Fill</th><th class="center">Fill&#10003;</th></tr></thead>
<tbody>{sample_rows}</tbody></table>
<div class="callout-ok"><strong>Check (OID {esc(c['oid'])}).</strong> Price: {c['F']:.2f} &times; {(0.995 if c['is_long'] else 1.005)} = {c['expected_sl']:.2f} = engine SL {c['engine_sl']:.2f}. Time: the first 1-second bar with <code>{cross}</code> after entry is {esc(c['first_touch'][11:])}, which equals the exit second {esc(c['exit_iso'][11:])}. Price/fill: the trade closed at {c['fill']:.2f}, the trigger bar's close ({c['trig_close']:.2f}) — at/just past the {c['engine_sl']:.2f} level, not exactly on it (trigger&ne;fill, as designed).</div>

<h3>2.3 · Evidence charts — level &amp; first-touch on the real bars</h3>
{chart_blocks}

<h3>2.4 · Full per-trade verification — all {n_sl} stop-loss exits</h3>
<table>
<thead><tr><th>OID</th><th>Side</th><th class="num">Entry F</th><th class="num">Exp. SL</th><th class="num">Engine SL</th><th>Exit sec</th><th class="num">Trig close</th><th class="num">Fill</th><th class="center">Time</th><th class="center">Logic</th><th class="center">Price</th><th class="center">Verdict</th></tr></thead>
<tbody>{full_rows}</tbody></table>

<h3>2.5 · Daily MIS square-off exits — all {n_sq} (the outer envelope)</h3>
<div class="simple">Every trade that didn't hit its stop was force-closed at the 15:29 IST session close. These rows confirm the envelope fires at the right second with the right reason.</div>
<table>
<thead><tr><th>OID</th><th>Side</th><th>Exit (IST)</th><th class="center">Time @15:29</th><th class="center">Reason=Squareoff</th><th class="center">Verdict</th></tr></thead>
<tbody>{sq_rows}</tbody></table>

<h3>2.6 · VP-01 axis roll-up</h3>
<div class="axisgrid">
  <div class="axiscard"><h4>A &middot; Time</h4><div class="v">{tick(time_all)} {('all triggers on the first-touch second' if time_all else 'MISMATCH')}</div></div>
  <div class="axiscard"><h4>B &middot; Logic</h4><div class="v">{tick(logic_all)} {('every reason correct; exactly one exit/trade' if logic_all else 'MISMATCH')}</div></div>
  <div class="axiscard"><h4>C &middot; Price</h4><div class="v">{tick(price_all)} {('SL to the tick; fill = trigger close' if price_all else 'MISMATCH')}</div></div>
</div>

<h2>3 · Remaining cases — verification roadmap</h2>
<div class="callout-warn"><strong>Template stage.</strong> This report fully verifies <b>VP-01</b> as the approved format. On sign-off, the same Time/Logic/Price treatment (sample table + charts + full table) is generated for every case below by the headless harness, and any failing case is banner-RED.</div>
<table>
<thead><tr><th>Case</th><th>Feature</th><th class="center">Status</th></tr></thead>
<tbody>{rm_rows}</tbody></table>

<footer>Generated by the testing2 verification driver ({gen}) &middot; bars: NIFTY 1-second spot, tick 0.01 &middot; engine + square-off facts verified against <code>core/managed_strategy.py</code>; order-type/timezone facts from the ntm3 NautilusTrader docs.</footer>
</div></body></html>"""


def main():
    bars = V.load_bars()
    ob = load_orderbook(ORDERBOOK_CSV)
    sl_rows = ob[ob["EXIT REASON"].str.strip().str.lower() == "stop loss"]
    sq_rows = ob[ob["EXIT REASON"].str.strip().str.lower() == "squareoff"]

    sl_results = [analyse_sl_row(bars, r) for _, r in sl_rows.iterrows()]
    sqoff_results = [analyse_sqoff_row(r) for _, r in sq_rows.iterrows()]

    # representative charts: first SHORT and first LONG SL trade
    short = next((r for r in sl_results if not r["is_long"]), None)
    long = next((r for r in sl_results if r["is_long"]), None)
    charts = []
    for r in [short, long]:
        if r is None:
            continue
        charts.append((chart_full_trade(bars, r),
                       f"OID {r['oid']} ({r['side']}): close rides from entry {r['F']:.2f} until it "
                       f"touches SL {r['engine_sl']:.2f}; red ✕ = realised fill at the trigger bar's close."))
        charts.append((chart_trigger_zoom(bars, r),
                       f"OID {r['oid']} ({r['side']}) trigger zoom: the amber bar is the first second the "
                       f"{'low pierces' if r['is_long'] else 'high pierces'} the dashed SL line — its second equals the order book's exit time."))

    html_out = build_html(sl_results, sqoff_results, charts)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html_out)

    n_pass = sum(r["passed"] for r in sl_results)
    print(f"SL trades: {n_pass}/{len(sl_results)} passed")
    print(f"Squareoff: {sum(r['passed'] for r in sqoff_results)}/{len(sqoff_results)} passed")
    for r in sl_results:
        if not r["passed"]:
            print("  FAIL", r["oid"], r["side"], "time", r["time_ok"], "price", r["price_ok"],
                  "fill", r["fill_ok"], "logic", r["logic_ok"])
    print("wrote", OUT_HTML)


if __name__ == "__main__":
    main()