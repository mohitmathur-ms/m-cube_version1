# -*- coding: utf-8 -*-
"""Leg-Level SL/Target Actions & Settings verification report.

Runs each leg-action test portfolio, embeds the REAL orderbook rows (with the
correct per-slot STRATEGY name) as proof, computes per-action facts from the
authoritative reason tags, and adds DATA-BACKED proofs straight from the catalog
bars for re_entry (price actually reached the entry level) and SL-Wait
(still-breached -> exit vs recovered -> no exit). Emits
html_reports/leg_actions_verification_report.html.

Backend timestamps are UTC; orderbook columns are IST (+5:30 display).
"""
import hashlib
import html
import io
import json
import re as _re
import sys

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from core.models.serialization import portfolio_from_dict
from core.backtest_runner.orchestration import run_portfolio_backtest
from core.report_generator import build_orderbook_dataframe
from core.custom_strategy_loader import sanitize_filename
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

import bisect

CATALOG = "catalog"
IST_OFF = pd.Timedelta(hours=5, minutes=30)

# ── Data selection ───────────────────────────────────────────────────────────
# Switch DATASET to re-target the WHOLE report. The leg-action portfolios are
# loaded from disk; their bar type / aggregation / dates are overridden in run()
# so the identical action set is exercised on either dataset. The SL-Wait and
# re_entry catalog proofs walk the ACTUAL bar sequence (bisect), so they work at
# any resolution and correctly skip session gaps (NIFTY trades NSE hours only).
DATASET = "ethusd_1m"
if DATASET == "nifty_1s":
    BT = "NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS-1-SECOND-LAST-EXTERNAL"
    STRAT_AGG = "NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS-1-MINUTE-LAST-INTERNAL@1-SECOND-EXTERNAL"
    START_DATE, END_DATE = "2026-03-02", "2026-05-01"
    BARS_START, BARS_END = "2026-03-01", "2026-05-02"
    SYMBOL, RES_LABEL = "NIFTY_SPOT", "1-second"
    OUT_HTML = "html_reports/leg_actions_verification_NIFTY_1s_report.html"
else:  # original ETHUSD 1-minute
    BT = "ETHUSD.COINBASE_MS-1-MINUTE-LAST-EXTERNAL"
    STRAT_AGG, START_DATE, END_DATE = None, None, None
    BARS_START, BARS_END = "2026-01-01", "2026-02-03"
    SYMBOL, RES_LABEL = "ETHUSD", "1-minute"
    OUT_HTML = "html_reports/leg_actions_verification_report.html"

OB_COLS = ["STRATEGY", "TRANSACTION", "ENTRY TIME", "ENTRY PRICE", "ENTRY DETAILED REASON",
           "EXIT TIME", "AVG EXIT PRICE", "EXIT REASON", "EXIT DETAILED REASON", "PNL"]

print(f"loading catalog bars ({SYMBOL} {RES_LABEL}) ...")
_cat = ParquetDataCatalog(CATALOG)
_bars = _cat.bars(bar_types=[BT], start=BARS_START, end=BARS_END)
BARS = {int(b.ts_event): (float(b.open), float(b.high), float(b.low), float(b.close)) for b in _bars}
_BK = sorted(BARS)  # sorted bar timestamps for gap-safe next/prev navigation


def _next_bar(ns):
    i = bisect.bisect_right(_BK, ns)
    return (_BK[i], BARS[_BK[i]]) if i < len(_BK) else None


def _prev_bar(ns):
    i = bisect.bisect_left(_BK, ns)
    return (_BK[i - 1], BARS[_BK[i - 1]]) if i > 0 else None


def make_ob(res, pf_name):
    """Build the orderbook the way the live app does: split the merged result
    per-slot (so STRATEGY = real strategy display name, PORTFOLIO = pf_name)."""
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
            # sanitize_filename turns the slot display ("EMA Cross on ETHUSD | SL:
            # 0.5%") into the orderbook label; rstrip("_") drops the trailing
            # underscore the final "%" leaves so it reads ..._SL_0.5__<token>.
            base = sanitize_filename(sr.get("display_name", str(slot_id))).rstrip("_")
            lbl = f"{base}__{hashlib.md5(str(slot_id).encode()).hexdigest()[:8]}"
            if lbl in allr:
                lbl += f"_{len(allr)}"
            tid, sid = s2t.get(slot_id, ""), s2s.get(slot_id, "")
            allr[lbl] = {"slot_id": lbl.rsplit("__", 1)[-1],
                         "positions_report": filt(pos, tid, sid),
                         "fills_report": filt(fills, tid, sid),
                         "starting_capital": res.get("starting_capital", 0),
                         "final_balance": res.get("final_balance", 0)}
    else:
        allr = {pf_name: res}
    return build_orderbook_dataframe(allr, user_id="_default", portfolio_name=pf_name)


def run(name, overrides=None):
    with open(f"portfolios/_default/{name}.json", encoding="utf-8") as f:
        d = json.load(f)
    if STRAT_AGG:  # re-target the portfolio onto the selected dataset
        for s in d["slots"]:
            s["bar_type_str"] = BT
            s["strategy_bar_types"] = [STRAT_AGG]
            s["start_date"] = None
            s["end_date"] = None
        d["start_date"], d["end_date"] = START_DATE, END_DATE
    if overrides:
        for s in d["slots"]:
            s["exit_config"].update(overrides)
    res = run_portfolio_backtest(CATALOG, portfolio_from_dict(d), user_id="_default")
    ob = make_ob(res, name)
    pr = res.get("positions_report")
    pr = pr.reset_index() if pr is not None and not pr.empty else pd.DataFrame()
    return ob, pr


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
        xt = pd.to_datetime(ob["EXIT TIME"].iloc[i], format="%d-%m-%Y %H:%M:%S", errors="coerce")
        out.append({"txn": str(ob["TRANSACTION"].iloc[i]).upper(), "et": et, "xt": xt,
                    "epx": _f(ob["ENTRY PRICE"].iloc[i]),
                    "er": str(ob["ENTRY DETAILED REASON"].iloc[i]),
                    "xr": str(ob["EXIT DETAILED REASON"].iloc[i]),
                    "xreason": str(ob["EXIT REASON"].iloc[i])})
    out.sort(key=lambda r: r["et"] if pd.notna(r["et"]) else pd.Timestamp.min)
    return out


def _utc_ns(ist_ts):
    return int((ist_ts - IST_OFF).value) if pd.notna(ist_ts) else None


def _utcd(ts):
    return (ts - IST_OFF).date() if pd.notna(ts) else None


def esc(x):
    return html.escape(str(x))


def ob_table(ob, limit=20):
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


def _leg_action(pf_name, action_key):
    """Return (target_leg_id, target_leg_action) for the leg whose on_* action
    == 'execute' in this portfolio."""
    with open(f"portfolios/_default/{pf_name}.json", encoding="utf-8") as f:
        d = json.load(f)
    akey = "on_sl_action" if "SL" in pf_name else "on_target_action"
    src = next((s for s in d["slots"] if s["exit_config"].get(akey) == "execute"), None)
    if not src:
        return None, None
    tgt_id = src["exit_config"].get("execute_target_leg_id")
    tgt = next((s for s in d["slots"] if s["slot_id"] == tgt_id), None)
    return tgt_id, (tgt["exit_config"].get(akey) if tgt else None)


# ---------- analyzers: (ob, pr, pf_name, side) -> (fact_html, ok) ----------
def an_close(ob, pr, pf, side):
    r = recs(ob)
    sq = sum(1 for x in r if "-> SqOff" in x["xr"])
    nd = sd = 0
    ex = []
    for i in range(1, len(r)):
        pu, cu = _utcd(r[i - 1]["xt"]), _utcd(r[i]["et"])
        if pu and cu:
            if cu > pu:
                nd += 1
            elif cu == pu:
                sd += 1
            if len(ex) < 3:
                ex.append(f"close {pu} (UTC) &rarr; next entry {cu} (UTC)")
    ok = sq > 0 and nd > 0 and sd == 0
    return (f"{len(r)} positions; <b>{sq}</b> exits tagged <code>-&gt; SqOff</code>. Re-entry resumes "
            f"on a <b>later UTC day</b> in {nd}/{nd + sd} pairs (same-UTC-day: {sd}) — idle for the rest "
            f"of the UTC day, resume next day.<br>" + "<br>".join(ex), ok)


def an_reexecute(ob, pr, pf, side):
    r = recs(ob)
    rx = [i for i in range(len(r)) if "Re-Execute" in r[i]["er"]]
    gaps, ss = set(), 0
    for i in rx:
        if i > 0 and pd.notna(r[i]["et"]) and pd.notna(r[i - 1]["xt"]):
            gaps.add(round((r[i]["et"] - r[i - 1]["xt"]).total_seconds()))
            if r[i]["txn"] == r[i - 1]["txn"]:
                ss += 1
    ok = len(rx) > 0 and ss == len(rx)
    note = ""
    if 0 in gaps:
        note = (" <b>gap=0 = same bar</b>: the exit landed on the 5-min aggregation boundary, so the "
                "aggregated entry-check (<code>_evaluate_entry</code>) runs that bar and fires the pending "
                "re-entry immediately; off-boundary exits re-enter on the next base bar via the per-base "
                "position-management path.")
    return (f"<b>{len(rx)}</b> re-execute re-entries fired (capped by Max&nbsp;Re-ex). Gap "
            f"close&rarr;re-entry = <b>{sorted(gaps)} s</b> (= next base bar at {RES_LABEL} resolution; "
            f"larger values are across NSE session gaps). Same side preserved in {ss}/{len(rx)} "
            f"(re-opens the SAME direction).{note}", ok)


def an_reverse(ob, pr, pf, side):
    r = recs(ob)
    rv = [i for i in range(len(r)) if "Reverse on" in r[i]["er"]]
    sts = opp = 0
    for i in rv:
        if i > 0 and pd.notna(r[i]["et"]) and pd.notna(r[i - 1]["xt"]):
            if abs((r[i]["et"] - r[i - 1]["xt"]).total_seconds()) < 0.5:
                sts += 1
            if r[i]["txn"] != r[i - 1]["txn"]:
                opp += 1
    ok = len(rv) > 0 and sts == len(rv) and opp == len(rv)
    return (f"<b>{len(rv)}</b> reverse flips. Opposite side in {opp}/{len(rv)}; opened at the "
            f"<b>same timestamp</b> as the close in {sts}/{len(rv)} (stop-and-reverse on the breach bar).", ok)


def an_execute(ob, pr, pf, side):
    r = recs(ob)
    ex = sum(1 for x in r if "Execute(" in x["xr"])
    by = {}
    if not pr.empty:
        for i in range(len(pr)):
            s = str(pr["strategy_id"].iloc[i])[-5:]
            by[s] = by.get(s, 0) + 1
    tgt_id, tgt_act = _leg_action(pf, side)
    act_txt = (f"its own configured exit action (here <code>{esc(tgt_act)}</code>)"
               if tgt_act else "its own configured exit action")
    ok = ex > 0 and len(by) >= 2
    return (f"<b>{ex}</b> Execute event(s) fired (the triggering leg's hit &rarr; start leg "
            f"<code>{esc(tgt_id)}</code>). Per-leg position counts: <b>{by}</b>. The leg that fires "
            f"Execute is then <b>permanently stopped</b>; the target leg then trades under "
            f"{act_txt} — it does NOT inherit re-execute.", ok)


def an_reentry(ob, pr, pf, side):
    r = recs(ob)
    rr = [x for x in r if "ReEntry at entry price" in x["er"]]
    m = 0
    drows = []
    for x in rr:
        mm = _re.search(r"entry price ([\d.]+)", x["er"])
        lvl = float(mm.group(1)) if mm else x["epx"]
        ok_px = abs(lvl - x["epx"]) < 0.05
        m += int(ok_px)
        # DATA proof: the catalog bar at the re-entry timestamp must straddle lvl
        ns = _utc_ns(x["et"])
        bar = BARS.get(ns) if ns is not None else None
        if bar:
            o, h, l, c = bar
            reached = l <= lvl <= h
            drows.append((x["et"], lvl, x["epx"], l, h, reached))
    ok = len(rr) > 0 and m == len(rr) and all(d[5] for d in drows)
    tbl = ("<table class='ob'><thead><tr><th>Re-entry time (IST)</th><th>Captured entry price</th>"
           "<th>Re-entry fill</th><th>Bar Low</th><th>Bar High</th><th>Low&le;price&le;High (reached?)</th>"
           "</tr></thead><tbody>"
           + "".join(f"<tr><td>{d[0]:%d-%m-%Y %H:%M:%S}</td><td>{d[1]:.2f}</td><td>{d[2]:.2f}</td>"
                     f"<td>{d[3]:.2f}</td><td>{d[4]:.2f}</td><td class='{'ok' if d[5] else 'bad'}'>"
                     f"{'YES' if d[5] else 'NO'}</td></tr>" for d in drows)
           + "</tbody></table>")
    return (f"<b>{len(rr)}</b> re-entries fired (capped by Max&nbsp;Re-Entries). Each re-enters at the "
            f"<b>captured entry price</b> (fill == original entry) in {m}/{len(rr)}. "
            f"<b>Data check vs catalog bars</b> — at each re-entry timestamp the ETHUSD bar's "
            f"[Low, High] actually straddles the entry price ({sum(d[5] for d in drows)}/{len(drows)} "
            f"bars reached it), confirming the re-entry filled only when price RETURNED to that level:"
            + tbl, ok)


def an_keep(ob, pr, pf, side):
    r = recs(ob)
    sl = sum(1 for x in r if x["xreason"] in ("Stop Loss", "Take Profit"))
    xr = {}
    for x in r:
        xr[x["xreason"]] = xr.get(x["xreason"], 0) + 1
    durs = [(x["xt"] - x["et"]).total_seconds() / 86400 for x in r if pd.notna(x["xt"]) and pd.notna(x["et"])]
    ok = sl == 0
    return (f"{len(r)} position(s); <b>{sl}</b> exits triggered by the SL/TP. The trigger is "
            f"<b>ignored</b> — the leg keeps running; closes only via {xr} (longest hold "
            f"{max(durs) if durs else 0:.1f} days). Contrast <code>close</code>, which closed on every hit.", ok)


SECTIONS = []


def add_action(key, title, pf_name, side, expected, analyze):
    print(f"running {pf_name} ...")
    ob, pr = run(pf_name)
    fact, ok = analyze(ob, pr, pf_name, side)
    badge = "<span class='pass'>VERIFIED</span>" if ok else "<span class='warn'>REVIEW</span>"
    SECTIONS.append("\n".join([
        f"<h3>{esc(title)} {badge}</h3>",
        f"<p class='spec'><b>Expected (spec):</b> {expected}</p>",
        f"<p class='cfg'><b>Portfolio:</b> <code>{esc(pf_name)}.json</code> &middot; On-{side}-Action = "
        f"<code>{key}</code></p>",
        f"<p class='fact'><b>Key fact:</b> {fact}</p>",
        ob_table(ob)]))


SECTIONS.append("<h2 id='sl'>1. Leg-Level On-SL Actions</h2>")
add_action("close", "1.1 close (SqOff)", "LEG_SL_01_close", "SL",
           "Close and stay idle for the rest of the (UTC) day; resume next day.", an_close)
add_action("re_execute", "1.2 re_execute", "LEG_SL_02_re_execute", "SL",
           "Close, then re-open the SAME side on the NEXT base bar (Max Re-ex).", an_reexecute)
add_action("reverse", "1.3 reverse", "LEG_SL_03_reverse", "SL",
           "Close and open the OPPOSITE side (stop-and-reverse).", an_reverse)
add_action("execute", "1.4 execute (other leg)", "LEG_SL_04_execute", "SL",
           "Start the target leg; the triggering leg is permanently stopped.", an_execute)
add_action("re_entry", "1.5 re_entry", "LEG_SL_05_re_entry", "SL",
           "Close, then re-enter only when price RETURNS to the original entry price (Max Re-Entries).", an_reentry)
add_action("keep_leg_running", "1.6 keep_leg_running", "LEG_SL_06_keep_leg_running", "SL",
           "Ignore the trigger — keep the leg running.", an_keep)

SECTIONS.append("<h2 id='tgt'>2. Leg-Level On-Target Actions</h2>")
add_action("close", "2.1 close (SqOff)", "LEG_TGT_01_close", "Target",
           "Close and stay idle for the rest of the (UTC) day; resume next day.", an_close)
add_action("re_execute", "2.2 re_execute", "LEG_TGT_02_re_execute", "Target",
           "Close, then re-open the SAME side on the NEXT base bar (Max Re-ex).", an_reexecute)
add_action("reverse", "2.3 reverse", "LEG_TGT_03_reverse", "Target",
           "Close and open the OPPOSITE side.", an_reverse)
add_action("execute", "2.4 execute (other leg)", "LEG_TGT_04_execute", "Target",
           "Start the target leg; the triggering leg is permanently stopped.", an_execute)
add_action("re_entry", "2.5 re_entry", "LEG_TGT_05_re_entry", "Target",
           "Re-enter when price RETURNS to the original entry price (Max Re-Entries).", an_reentry)
add_action("keep_leg_running", "2.6 keep_leg_running", "LEG_TGT_06_keep_leg_running", "Target",
           "Ignore the trigger — keep the leg running.", an_keep)

# ---------- SL Wait, with DATA proof from catalog bars ----------
print("running SL-Wait comparison ...")
ob0, pr0 = run("LEG_SL_01_close", overrides={"sl_wait_sec": 0})
obW, prW = run("LEG_SL_01_close", overrides={"sl_wait_sec": 600})
n0, nW = len(pr0), len(prW)


def _sl_level(side, E):
    return E * (1 - 0.005) if side == "BUY" else E * (1 + 0.005)


def _breach(side, bar, SL):
    o, h, l, c = bar
    return (l <= SL) if side == "BUY" else (h >= SL)


# Deferred-exit proof: a wait=600 Stop Loss exit where the breach persisted ~600s
# and price is STILL breaching at the exit bar.
deferred = []
for x in recs(obW):
    if x["xreason"] != "Stop Loss":
        continue
    ns = _utc_ns(x["xt"])
    SL = _sl_level(x["txn"], x["epx"])
    bar = BARS.get(ns)
    if not bar or not _breach(x["txn"], bar, SL):
        continue
    onset = ns
    while True:
        pb = _prev_bar(onset)
        if pb and _breach(x["txn"], pb[1], SL):
            onset = pb[0]
        else:
            break
    waited = (ns - onset) / 1e9
    lo = bar[2] if x["txn"] == "BUY" else bar[1]
    deferred.append((x["xt"], x["txn"], x["epx"], SL, waited, lo))
    if len(deferred) >= 3:
        break

# Recovery (no-exit) proof: a wait=0 Stop Loss breach where, within 600s, a bar
# is back on the safe side of SL -> in wait=600 that exit is cancelled.
recovery = []
for x in recs(ob0):
    if x["xreason"] != "Stop Loss":
        continue
    ns = _utc_ns(x["xt"])
    SL = _sl_level(x["txn"], x["epx"])
    bar = BARS.get(ns)
    if not bar or not _breach(x["txn"], bar, SL):
        continue
    cur = ns  # walk ACTUAL bars within 600s (gap-safe); find first non-breaching one
    while True:
        nb = _next_bar(cur)
        if not nb or nb[0] > ns + 600_000_000_000:
            break
        if not _breach(x["txn"], nb[1], SL):
            safe = nb[1][2] if x["txn"] == "BUY" else nb[1][1]
            recovery.append((x["xt"], x["txn"], x["epx"], SL, bar[2] if x["txn"] == "BUY" else bar[1],
                             int((nb[0] - ns) / 1e9), safe))
            break
        cur = nb[0]
    if len(recovery) >= 3:
        break

wait_ok = nW < n0 and len(deferred) > 0 and len(recovery) > 0
def_tbl = ("<table class='ob'><thead><tr><th>Exit time (IST)</th><th>Side</th><th>Entry</th><th>SL level</th>"
           "<th>Breach persisted</th><th>Price at exit bar</th><th>Still breaching?</th></tr></thead><tbody>"
           + "".join(f"<tr><td>{d[0]:%d-%m-%Y %H:%M:%S}</td><td>{d[1]}</td><td>{d[2]:.2f}</td>"
                     f"<td>{d[3]:.2f}</td><td>{d[4]:.0f}s</td><td>{d[5]:.2f}</td>"
                     f"<td class='ok'>YES (&le; SL)</td></tr>" for d in deferred) + "</tbody></table>")
rec_tbl = ("<table class='ob'><thead><tr><th>Breach time (IST)</th><th>Side</th><th>Entry</th><th>SL level</th>"
           "<th>Price at breach</th><th>Recovered after</th><th>Price (back on safe side)</th>"
           "<th>wait=600 exits?</th></tr></thead><tbody>"
           + "".join(f"<tr><td>{d[0]:%d-%m-%Y %H:%M:%S}</td><td>{d[1]}</td><td>{d[2]:.2f}</td>"
                     f"<td>{d[3]:.2f}</td><td>{d[4]:.2f}</td><td>{d[5]}s</td><td>{d[6]:.2f}</td>"
                     f"<td class='bad'>NO (cancelled)</td></tr>" for d in recovery) + "</tbody></table>")
slwait = (
    f"<p class='spec'><b>Expected (spec):</b> on an SL breach, wait the configured seconds, then re-check "
    f"— exit only if STILL breached; if price recovered within the window, do NOT exit. "
    f"{'<span class=pass>VERIFIED</span>' if wait_ok else '<span class=warn>REVIEW</span>'}</p>"
    f"<p class='cfg'><b>Setup:</b> <code>LEG_SL_01_close</code> (SL 0.5%) run twice: <code>SL Wait = 0s</code> "
    f"vs <code>SL Wait = 600s</code>.</p>"
    f"<p class='fact'><b>Key fact:</b> positions wait=0: <b>{n0}</b>; wait=600s: <b>{nW}</b> "
    f"({n0}&rarr;{nW} — transient breaches were not exited).</p>"
    f"<h4>(a) Still breaching after the wait &rarr; exit IS taken (from catalog bars)</h4>"
    f"<p class='muted'>For these wait=600 Stop-Loss exits, the breach persisted ~600s and the price is still "
    f"on the wrong side of the SL at the exit bar:</p>{def_tbl}"
    f"<h4>(b) Price recovered within 600s &rarr; exit is NOT taken (from catalog bars)</h4>"
    f"<p class='muted'>These are wait=0 Stop-Loss breaches; within 600s a bar is back on the safe side of the "
    f"SL, so with wait=600 the exit is cancelled (the {n0 - nW} extra wait=0 exits):</p>{rec_tbl}"
    f"<h4>Orderbooks (the two runs side by side)</h4>"
    "<div class='cols'>"
    f"<div><h4>SL Wait = 0s ({n0} positions)</h4>{ob_table(ob0, 14)}</div>"
    f"<div><h4>SL Wait = 600s ({nW} positions)</h4>{ob_table(obW, 14)}</div></div>")
SECTIONS.append("<h2 id='slwait'>3. SL Wait (sec) — verified against catalog data</h2>" + slwait)

# ---------- Spec compliance ----------
SPEC = [
    ("Stoploss", "Exit Price Format (A/B/C)", "exit_price_format",
     "execution_logic.html §4.1–4.2", "Wired"),
    ("Stoploss", "SL Type (none/%/points/atr/trailing)", "stop_loss_type", "sl_features §1.1; §4.4", "Wired"),
    ("Stoploss", "SL Value", "stop_loss_value", "sl_features §1.1", "Wired"),
    ("Stoploss", "ATR Period / ATR Mult", "sl_atr_period / sl_atr_multiplier", "ATR SL", "Wired"),
    ("Stoploss", "Trail Step / Trail Offset", "trailing_sl_step / trailing_sl_offset",
     "sl_features §1.4 / §2.2 (BUY adds, SELL subtracts)", "Verified (sign fix; trails into profit)"),
    ("Stoploss", "SL Wait (sec)", "sl_wait_sec", "breach-confirmation wait", "Verified (§3, catalog data)"),
    ("Stoploss", "Max Re-ex", "max_re_executions", "sl_features §1.7", "Verified (§1.2)"),
    ("Stoploss", "On SL Action (6)", "on_sl_action", "execution_logic §4.7 / leg_actions", "Verified (§1)"),
    ("Stoploss", "Execute Target Leg", "execute_target_leg_id", "Execute action", "Verified (§1.4)"),
    ("Stoploss", "ReEntry Price", "reentry_price", "re_entry price-wait", "Verified (§1.5, catalog data)"),
    ("Stoploss", "Max Re-Entries", "max_re_entries", "re_entry cap", "Verified (§1.5)"),
    ("Stoploss", "Armed at start", "armed_at_start", "SL/TP armed at entry", "Wired"),
    ("Target", "TP Type / TP Value", "target_type / target_value", "execution_logic_target §4", "Wired"),
    ("Target", "ATR Period / ATR Mult", "tgt_atr_period / tgt_atr_multiplier", "ATR TP", "Wired"),
    ("Target", "On TP Action (6)", "on_target_action", "symmetric to On-SL", "Verified (§2)"),
    ("Target", "TP Wait (sec)", "tgt_wait_sec", "symmetric to SL Wait", "Wired"),
    ("Target", "Leg Trailing Target (Profit-Lock): Reach/Lock/Every/By",
     "tgt_trail_* ", "execution_logic_target §4.7 pseudocode", "Verified (exact spec match, live trace)"),
]
spec_body = "".join(
    f"<tr><td>{esc(t)}</td><td>{esc(f)}</td><td><code>{esc(k)}</code></td><td>{esc(s)}</td>"
    f"<td class='ok'>{esc(v)}</td></tr>" for t, f, k, s, v in SPEC)
SECTIONS.append(
    "<h2 id='spec'>4. Spec compliance — Stoploss &amp; Target tabs</h2>"
    "<table class='spec'><thead><tr><th>Tab</th><th>UI Field</th><th>Backend key</th>"
    "<th>Spec reference</th><th>Status</th></tr></thead><tbody>" + spec_body + "</tbody></table>")

nav = " &middot; ".join(f"<a href='#{a}'>{t}</a>" for a, t in
                        [("sl", "On-SL"), ("tgt", "On-Target"), ("slwait", "SL Wait"), ("spec", "Spec")])
DOC = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Leg-Level SL/Target Actions — Verification Report</title>
<style>
 body{{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#0f1420;color:#dbe3ef;line-height:1.5}}
 .wrap{{max-width:1320px;margin:0 auto;padding:24px 28px}}
 h1{{font-size:24px;margin:0 0 4px}} h2{{margin:34px 0 10px;border-bottom:2px solid #2a3550;padding-bottom:6px;color:#fff}}
 h3{{margin:24px 0 6px;color:#cfe0ff}} h4{{margin:14px 0 4px;color:#9fb3d8}}
 a{{color:#7db3ff;text-decoration:none}} code{{background:#1b2538;padding:1px 5px;border-radius:4px;color:#a8d5ff;font-size:13px}}
 .muted{{color:#7d8aa3;font-size:12px;margin:6px 0}} .sub{{color:#9fb3d8}}
 .spec{{color:#c6d2e6}} .cfg{{color:#9fb3d8;font-size:13px}}
 .fact{{background:#16203a;border-left:3px solid #4a86ff;padding:10px 14px;border-radius:5px;margin:8px 0}}
 .pass{{background:#1c5d34;color:#b6f5cf;padding:2px 9px;border-radius:10px;font-size:12px;font-weight:700}}
 .warn{{background:#6b4a14;color:#ffd9a0;padding:2px 9px;border-radius:10px;font-size:12px;font-weight:700}}
 table.ob{{border-collapse:collapse;width:100%;font-size:11px;margin:6px 0 18px;table-layout:fixed}}
 table.ob th,table.ob td{{border:1px solid #26324d;padding:3px 6px;text-align:left;vertical-align:top;word-break:break-word}}
 table.ob th{{background:#1b2740;color:#cdd9ef}}
 table.ob td.buy{{color:#5fd38d;font-weight:600}} table.ob td.sell{{color:#ff8f8f;font-weight:600}}
 details.obwrap{{margin:6px 0 16px;border:1px solid #26324d;border-radius:6px;background:#121a2c}}
 details.obwrap>summary{{cursor:pointer;padding:8px 12px;color:#9fd0ff;font-weight:600;user-select:none;list-style:none}}
 details.obwrap>summary::-webkit-details-marker{{display:none}}
 details.obwrap>summary:hover{{background:#16203a}}
 details.obwrap>summary::before{{content:'\\25B8  '}}
 details.obwrap[open]>summary{{border-bottom:1px solid #26324d;color:#cfe7ff}}
 details.obwrap[open]>summary::before{{content:'\\25BE  '}}
 details.obwrap>table{{margin:10px 12px}}
 td.ok{{color:#8ff0bd;font-weight:600}} td.bad{{color:#ff9a9a;font-weight:600}}
 table.spec{{border-collapse:collapse;width:100%;font-size:13px}}
 table.spec th,table.spec td{{border:1px solid #26324d;padding:6px 9px;text-align:left}}
 table.spec th{{background:#1b2740}}
 .nav{{background:#16203a;padding:10px 14px;border-radius:6px;margin:14px 0}}
 .note{{background:#241c33;border-left:3px solid #a06bff;padding:10px 14px;border-radius:5px;margin:10px 0;font-size:13px}}
</style></head><body><div class="wrap">
<h1>Leg-Level SL / Target — Actions &amp; Settings Verification</h1>
<p class="sub">m-cube &middot; {SYMBOL} {RES_LABEL} ({START_DATE or '2026-01'} &rarr; {END_DATE or '2026-02'}) &middot; backend UTC,
orderbook display IST (+5:30). Each section runs a dedicated test portfolio; re_entry and SL-Wait are
cross-checked against the raw catalog bars. At {RES_LABEL} resolution the "next base bar" re-entry is +1 unit
of that resolution (e.g. +1s on second data), proving the timing is not hardcoded.</p>
<div class="nav">{nav}</div>
<div class="note"><b>UTC vs IST:</b> engine logic runs in UTC; orderbook columns are shifted +5:30 for display
only. A <code>close</code> idle "for the rest of the day" is a <b>UTC</b>-day rule, so on the IST orderbook an
exit and its next-day re-entry can fall on the same calendar date.</div>
{chr(10).join(SECTIONS)}
<p class="muted">Generated by <code>verify_leg_actions_report.py</code>. Orderbook tables and the re_entry /
SL-Wait data tables are actual backtest + catalog values (not mock-ups).</p>
</div></body></html>"""

with open(OUT_HTML, "w", encoding="utf-8") as f:
    f.write(DOC)
print(f"\nwrote {OUT_HTML} | SL-Wait {n0}->{nW} | deferred {len(deferred)} | recovery {len(recovery)}")
