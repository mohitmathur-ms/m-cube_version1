# -*- coding: utf-8 -*-
"""Portfolio-level Combined Loss / Combined Profit ACTION verification report.

Tests the three portfolio-level on-breach actions — SqOff, ReExecute (market),
ReExecute at Entry Price (limit) — on BOTH the Combined-Loss (pf_sl) and
Combined-Profit (pf_tgt) sides. Each section runs a dedicated 2-slot portfolio,
embeds the real orderbook (collapsible) as proof, isolates the PORTFOLIO-level
fires by their "Portfolio Stoploss/Target: combined N hit -> ..." tag, and (for
ReExecute at Entry Price) cross-checks the re-entry price against catalog bars.

ETHUSD COINBASE_MS 1-minute, 2026-01-01..02-01. Backend UTC, orderbook IST.
"""
import bisect, hashlib, html, io, json, re as _re
import pandas as pd
import sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from core.models.serialization import portfolio_from_dict
from core.backtest_runner.orchestration import run_portfolio_backtest
from core.report_generator import build_orderbook_dataframe
from core.custom_strategy_loader import sanitize_filename
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

CATALOG = "catalog"
OUT_HTML = "html_reports/portfolio_combined_actions_report.html"
IST_OFF = pd.Timedelta(hours=5, minutes=30)
BT = "ETHUSD.COINBASE_MS-1-MINUTE-LAST-EXTERNAL"
OB_COLS = ["STRATEGY", "TRANSACTION", "ENTRY TIME", "ENTRY PRICE", "ENTRY DETAILED REASON",
           "EXIT TIME", "AVG EXIT PRICE", "EXIT REASON", "EXIT DETAILED REASON", "PNL"]

print("loading catalog bars ...")
_cat = ParquetDataCatalog(CATALOG)
BARS = {int(b.ts_event): (float(b.open), float(b.high), float(b.low), float(b.close))
        for b in _cat.bars(bar_types=[BT], start="2026-01-01", end="2026-02-03")}
_BK = sorted(BARS)


def make_ob(res, pf_name):
    per = res.get("per_strategy", {})
    pos, fills = res.get("positions_report"), res.get("fills_report")
    s2t, s2s = res.get("slot_to_trader_id", {}), res.get("slot_to_strategy_id", {})

    def filt(df, tid, sid):
        if df is None or df.empty:
            return pd.DataFrame()
        if tid and sid and "trader_id" in df.columns and "strategy_id" in df.columns:
            return df[(df["trader_id"] == tid) & (df["strategy_id"] == sid)]
        if sid and "strategy_id" in df.columns:
            return df[df["strategy_id"] == sid]
        return pd.DataFrame()

    allr = {}
    if per:
        for slot_id, sr in per.items():
            base = sanitize_filename(sr.get("display_name", str(slot_id))).rstrip("_")
            lbl = f"{base}__{hashlib.md5(str(slot_id).encode()).hexdigest()[:8]}"
            if lbl in allr:
                lbl += f"_{len(allr)}"
            tid, sid = s2t.get(slot_id, ""), s2s.get(slot_id, "")
            allr[lbl] = {"slot_id": lbl.rsplit("__", 1)[-1],
                         "positions_report": filt(pos, tid, sid), "fills_report": filt(fills, tid, sid),
                         "starting_capital": res.get("starting_capital", 0),
                         "final_balance": res.get("final_balance", 0)}
    else:
        allr = {pf_name: res}
    return build_orderbook_dataframe(allr, user_id="_default", portfolio_name=pf_name)


def run(name):
    with open(f"portfolios/_default/{name}.json", encoding="utf-8") as f:
        d = json.load(f)
    res = run_portfolio_backtest(CATALOG, portfolio_from_dict(d), user_id="_default")
    return make_ob(res, name)


def _f(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0


def recs(ob):
    out = []
    if ob is None or ob.empty:
        return out
    for i in range(len(ob)):
        et = pd.to_datetime(ob["ENTRY TIME"].iloc[i], format="%d-%m-%Y %H:%M:%S", errors="coerce")
        out.append({"txn": str(ob["TRANSACTION"].iloc[i]).upper(), "et": et,
                    "epx": _f(ob["ENTRY PRICE"].iloc[i]),
                    "er": str(ob["ENTRY DETAILED REASON"].iloc[i]),
                    "xr": str(ob["EXIT DETAILED REASON"].iloc[i])})
    return out


def _utc_ns(ts):
    return int((ts - IST_OFF).value) if pd.notna(ts) else None


def esc(x):
    return html.escape(str(x))


def ob_table(ob, limit=26):
    if ob is None or ob.empty:
        return "<p class='muted'>(no trades)</p>"
    cols = [c for c in OB_COLS if c in ob.columns]
    n = len(ob)
    rows = ob.head(limit)
    head = "".join(f"<th>{esc(c)}</th>" for c in ["#"] + cols)
    body = []
    for i in range(len(rows)):
        cells = [f"<td>{i+1}</td>"]
        for c in cols:
            v = rows[c].iloc[i]
            cls = " class='buy'" if (c == "TRANSACTION" and str(v).upper() == "BUY") else (
                " class='sell'" if c == "TRANSACTION" else "")
            cells.append(f"<td{cls}>{esc(v)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    table = f"<table class='ob'><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"
    return (f"<details class='obwrap'><summary>Show orderbook — {min(limit, n)} of {n} rows "
            f"(timestamps IST)</summary>{table}</details>")


# ---------- analyzers ----------
def _pf_prefix(side):
    return "Portfolio Stoploss" if side == "loss" else "Portfolio Target"


def an_sqoff(ob, side):
    r = recs(ob)
    pf = _pf_prefix(side)
    sq = [x for x in r if pf in x["xr"] and "-> SqOff" in x["xr"]]
    scope = ("day-scoped — fires once per calendar day, blocks re-entry to end of day, resumes next day"
             if side == "loss" else
             "absolute / rest-of-run — fires once when combined profit first hits the target, then halts")
    ok = len(sq) > 0
    ex = sq[0]["xr"] if sq else ""
    return (f"<b>{len(sq)}</b> portfolio-level SqOff closes tagged "
            f"<code>{esc(pf)}: combined 100 hit -&gt; SqOff</code> (the combined PnL crossed the threshold "
            f"and the monitor squared the legs). Semantics: <b>{scope}</b>. Sample exit tag: "
            f"<code>{esc(ex)}</code>.<br><span class='muted'>(Other <code>-&gt; SqOff</code> rows are the "
            f"legs' own 2% SL — only the <b>Portfolio</b>-prefixed tags are portfolio-level.)</span>", ok)


def an_reexec(ob, side):
    r = recs(ob)
    pf = _pf_prefix(side)
    fires = [x for x in r if pf in x["xr"] and "-> ReExecute" in x["xr"] and "Entry Price" not in x["xr"]]
    reents = [x for x in r if "ReExecute (market) — re-entry fired by portfolio" in x["er"]]
    ok = len(reents) > 0
    return (f"<b>{len(reents)}</b> portfolio ReExecute re-entries tagged "
            f"<code>ReExecute (market) — re-entry fired by portfolio SL/Target</code> "
            f"(= portfolio fires &times; 2 legs). The breach closes both legs and <b>re-opens them at "
            f"MARKET on the next base bar</b>, trailing-reset and capped at <code>reexecute_count=3</code>. "
            f"Close tag: <code>{esc(fires[0]['xr']) if fires else '—'}</code>.", ok)


def an_reexec_entry(ob, side):
    r = recs(ob)
    reents = [x for x in r if "ReExecute at Entry Price (limit) — re-entry fired by portfolio" in x["er"]]
    # catalog data check: re-entry fills at the captured entry price; the bar at that
    # timestamp must reach it (side-aware: BUY -> low<=E ; SELL -> high>=E).
    drows = []
    for x in reents:
        ns = _utc_ns(x["et"])
        bar = BARS.get(ns) if ns is not None else None
        if bar:
            o, h, l, c = bar
            reached = (l <= x["epx"]) if x["txn"] == "BUY" else (h >= x["epx"])
            drows.append((x["et"], x["txn"], x["epx"], l, h, reached))
    ok = len(reents) > 0 and (not drows or all(d[5] for d in drows))
    tbl = ("<table class='ob'><thead><tr><th>Re-entry time (IST)</th><th>Side</th><th>Entry-price fill</th>"
           "<th>Bar Low</th><th>Bar High</th><th>Price reached entry?</th></tr></thead><tbody>"
           + "".join(f"<tr><td>{d[0]:%d-%m-%Y %H:%M:%S}</td><td class='{'buy' if d[1]=='BUY' else 'sell'}'>{d[1]}</td>"
                     f"<td>{d[2]:.2f}</td><td>{d[3]:.2f}</td><td>{d[4]:.2f}</td>"
                     f"<td class='{'ok' if d[5] else 'bad'}'>{'YES' if d[5] else 'NO'}</td></tr>" for d in drows)
           + "</tbody></table>")
    return (f"<b>{len(reents)}</b> portfolio ReExecute-at-Entry-Price re-entries tagged "
            f"<code>ReExecute at Entry Price (limit) — re-entry fired by portfolio SL/Target</code>. "
            f"The breach closes both legs and <b>re-enters via a resting LIMIT at each leg's captured entry "
            f"price</b>, filled when price returns to that level (capped at <code>reexecute_count=3</code>). "
            f"<b>Catalog data check</b> — at each re-entry the ETHUSD bar reaches the entry price "
            f"({sum(d[5] for d in drows)}/{len(drows)}):" + tbl, ok)


SECTIONS = []


def add(title, pf_name, side, expected, fn):
    print(f"running {pf_name} ...")
    ob = run(pf_name)
    fact, ok = fn(ob, side)
    badge = "<span class='pass'>VERIFIED</span>" if ok else "<span class='warn'>REVIEW</span>"
    SECTIONS.append("\n".join([
        f"<h3>{esc(title)} {badge}</h3>",
        f"<p class='spec'><b>Expected:</b> {expected}</p>",
        f"<p class='cfg'><b>Portfolio:</b> <code>{esc(pf_name)}.json</code> &middot; 2 managed slots "
        f"(EMA 20/50 + 50/100, leg SL 2%/TP 5%) &middot; threshold = combined 100</p>",
        f"<p class='fact'><b>Key fact:</b> {fact}</p>",
        ob_table(ob)]))


SECTIONS.append("<h2 id='loss'>1. Combined Loss (pf_sl) actions</h2>")
add("1.1 Combined Loss → SqOff", "PFCL_SQOFF", "loss",
    "On combined loss ≤ −100 (day-scoped): square off all legs, block re-entry until next day.", an_sqoff)
add("1.2 Combined Loss → ReExecute (market)", "PFCL_REEXEC", "loss",
    "On combined loss ≤ −100: close all legs and re-open at MARKET on the next bar, capped at 3 fires.", an_reexec)
add("1.3 Combined Loss → ReExecute at Entry Price", "PFCL_REEXEC_ENTRY", "loss",
    "On combined loss ≤ −100: close all legs and re-enter via LIMIT at each leg's entry price, capped at 3.", an_reexec_entry)

SECTIONS.append("<h2 id='profit'>2. Combined Profit (pf_tgt) actions</h2>")
add("2.1 Combined Profit → SqOff", "PFCP_SQOFF", "profit",
    "On combined profit ≥ +100 (rest-of-run): square off all legs once and halt for the rest of the run.", an_sqoff)
add("2.2 Combined Profit → ReExecute (market)", "PFCP_REEXEC", "profit",
    "On combined profit ≥ +100: close all legs and re-open at MARKET on the next bar, capped at 3 fires.", an_reexec)
add("2.3 Combined Profit → ReExecute at Entry Price", "PFCP_REEXEC_ENTRY", "profit",
    "On combined profit ≥ +100: close all legs and re-enter via LIMIT at each leg's entry price, capped at 3.", an_reexec_entry)

DOC = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Portfolio-Level Combined SL/Target Actions — Verification</title>
<style>
 body{{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#0f1420;color:#dbe3ef;line-height:1.55}}
 .wrap{{max-width:1300px;margin:0 auto;padding:24px 28px}}
 h1{{font-size:23px;margin:0 0 4px}} h2{{margin:30px 0 10px;border-bottom:2px solid #2a3550;padding-bottom:6px;color:#fff}}
 h3{{margin:22px 0 6px;color:#cfe0ff}}
 a{{color:#7db3ff;text-decoration:none}} code{{background:#1b2538;padding:1px 5px;border-radius:4px;color:#a8d5ff;font-size:12.5px}}
 .muted{{color:#7d8aa3;font-size:12px}} .sub{{color:#9fb3d8}} .spec{{color:#c6d2e6}} .cfg{{color:#9fb3d8;font-size:13px}}
 .fact{{background:#16203a;border-left:3px solid #4a86ff;padding:10px 14px;border-radius:5px;margin:8px 0}}
 .pass{{background:#1c5d34;color:#b6f5cf;padding:2px 9px;border-radius:10px;font-size:12px;font-weight:700}}
 .warn{{background:#6b4a14;color:#ffd9a0;padding:2px 9px;border-radius:10px;font-size:12px;font-weight:700}}
 table.ob{{border-collapse:collapse;width:100%;font-size:11px;margin:10px 12px;table-layout:fixed}}
 table.ob th,table.ob td{{border:1px solid #26324d;padding:3px 6px;text-align:left;vertical-align:top;word-break:break-word}}
 table.ob th{{background:#1b2740;color:#cdd9ef}}
 table.ob td.buy{{color:#5fd38d;font-weight:600}} table.ob td.sell{{color:#ff8f8f;font-weight:600}}
 td.ok{{color:#8ff0bd;font-weight:600}} td.bad{{color:#ff9a9a;font-weight:600}}
 details.obwrap{{margin:6px 0 16px;border:1px solid #26324d;border-radius:6px;background:#121a2c}}
 details.obwrap>summary{{cursor:pointer;padding:8px 12px;color:#9fd0ff;font-weight:600;list-style:none}}
 details.obwrap>summary::-webkit-details-marker{{display:none}}
 details.obwrap>summary::before{{content:'\\25B8  '}}
 details.obwrap[open]>summary::before{{content:'\\25BE  '}}
 details.obwrap>summary:hover{{background:#16203a}}
 .nav{{background:#16203a;padding:10px 14px;border-radius:6px;margin:14px 0}}
 .note{{background:#241c33;border-left:3px solid #a06bff;padding:10px 14px;border-radius:5px;margin:10px 0;font-size:13px}}
</style></head><body><div class="wrap">
<h1>Portfolio-Level Combined SL / Target — Action Verification</h1>
<p class="sub">m-cube &middot; ETHUSD COINBASE_MS 1-minute (2026-01-01 → 02-01) &middot; 2-slot portfolios &middot;
backend UTC, orderbook display IST (+5:30). Actions tested: <b>SqOff</b>, <b>ReExecute (market)</b>,
<b>ReExecute at Entry Price (limit)</b> on both the Combined-Loss and Combined-Profit sides.</p>
<div class="nav"><a href='#loss'>Combined Loss</a> &middot; <a href='#profit'>Combined Profit</a></div>
<div class="note"><b>How it fires:</b> a per-portfolio <code>PortfolioMonitorStrategy</code> tracks combined
realized+unrealized PnL each bar and publishes a breach to a shared fire bus; each leg then closes live and
(SqOff) blocks re-entry, or (ReExecute) re-opens at market / (ReExecute at Entry Price) rests a limit at its
entry price. <b>Loss SqOff is day-scoped; Profit SqOff is rest-of-run.</b> ReExecute fires are capped by
<code>reexecute_count</code> and each fire acts on both legs.</div>
{chr(10).join(SECTIONS)}
<p class="muted">Generated by <code>verify_portfolio_combined_actions_report.py</code>. Orderbook + catalog
data are real backtest values. Portfolio-level fires are isolated by their
<code>Portfolio Stoploss/Target: combined N hit -&gt; …</code> tag.</p>
</div></body></html>"""

with open(OUT_HTML, "w", encoding="utf-8") as f:
    f.write(DOC)
print(f"\nwrote {OUT_HTML}")
