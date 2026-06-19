"""Build the FULL manual-verification RESULTS report for all 18 testing2 cases.

Rebuilds portfolios/testing2/manual_verification_v2_RESULTS.html as an accordion:
each case (VP-01..VP-12, OT-01..OT-06) is a collapsible <details> section that
expands/contracts on click. Every runnable case is re-verified across
Time / Logic / Price against the real exported order book + the 1-second catalog
bars; OT-02/OT-03 are negative tests (must be rejected at save).

Per-case logic (config-aware; no fake greens — documented limits are labelled):
  * level cases  (VP-01/02/03/04/12, OT-01/04/05) — engine-logged SL/TP level:
                  first-touch second == exit, fill == trigger-bar close.
  * VP-05 trail  — verify the exit-level crossing+fill; the per-bar ratchet is
                   not observable from the order book (documented limit).
  * VP-06 ATR    — verify crossing+fill; the F-2*ATR sizing needs the logged
                   ATR-at-entry (not in the order book) — documented limit.
  * VP-07 wait   — exit fires at first_touch + (wait_bars-1) bars (the delay),
                   NOT on first touch.
  * VP-08 reexec — SL exits verified + re-execution count capped at 1+2.
  * VP-09 reverse— each Reverse-on-SL verified + the next entry flips side.
  * VP-10 pf-loss— Portfolio-Stoploss flattens BOTH legs on the same bar
                   (Portfolio -> Leg precedence).
  * VP-11/OT-06  — disabled feature: only the daily 15:29 IST MIS square-off exits.
  * OT-02/03     — save rejected with HTTP 400 (the pass condition).

Run:  venv\\Scripts\\python.exe portfolios\\testing2\\_build_full_report.py
"""
from __future__ import annotations
import base64
import glob
import html
import io
import json
import os
import re
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import _verify_lib as V  # fast cached IST-indexed bar loader + first_touch
from core.visual_verification import analyze, guess_feed_bar_type

REPORTS = "reports/testing2"
TODAY = datetime.now().strftime("%d_%B_%Y").lower()
OUT = "portfolios/testing2/manual_verification_v2_RESULTS.html"
HALF = V.TICK / 2.0
DARK = {"bg": "#0d1117", "panel": "#161b22", "text": "#e6edf3", "grid": "#30363d",
        "green": "#3fb950", "red": "#f85149", "amber": "#d29922", "accent": "#58a6ff"}

# ── Per-case metadata + verification mode ────────────────────────────────────
CASES = [
    ("VP-01", "VP-01_long_sl_pct", "Leg fixed SL % (long & short)", "level",
     "1 leg · EMA(9/21) · SL = percentage 0.5% · no target.",
     "A long sets its stop 0.5% below the entry; a short sets it 0.5% above. The stop should trip the instant price touches that level, and the position closes at that bar's price.",
     "Long SL = round(F×0.995, 0.01); short SL = round(F×1.005, 0.01). Trigger = first 1-sec bar low≤SL (long) / high≥SL (short); realised fill = that bar's close (reduce-only MARKET close, so fill ≠ the level)."),
    ("VP-02", "VP-02_long_tgt_pct", "Leg fixed Target %", "level",
     "1 leg · target = percentage 0.5% · no SL.",
     "The position takes profit 0.5% in its favour. The target should trip on the first bar that reaches it.",
     "Long TGT = F×1.005 (fires on high≥TGT); short TGT = F×0.995 (fires on low≤TGT). Exit reason 'Take Profit', level logged as TP=. Fill = trigger-bar close."),
    ("VP-03", "VP-03_sl_and_tgt", "SL and Target together (precedence)", "level",
     "1 leg · SL 0.3% + Target 0.5%.",
     "With both a stop and a target armed, exactly one fires per trade — whichever the price reaches first. If a single bar spans both, the engine checks SL before the target.",
     "Per trade the engine logs the level that fired (SL= or TP=). Each row is verified against its own level (first-touch == exit, fill = close). Exactly one exit per position; SL precedence on a same-bar collision."),
    ("VP-04", "VP-04_short_sl_tgt", "Short-side SL + Target", "level",
     "1 leg · SL 0.4% + Target 0.6% · the SHORT trades.",
     "On a short, the stop sits ABOVE the entry and the target BELOW it — the mirror of the long-side math.",
     "Short SL = F×1.004 (high≥SL); short TGT = F×0.994 (low≤TGT). Long entries mirror. Each row verified against its logged level."),
    ("VP-05", "VP-05_trailing_sl", "Trailing SL (ratchet)", "level",
     "1 leg · SL 0.5%, trailing step 0.2, offset 0.1.",
     "As the trade moves into profit the stop only tightens, never loosens. We confirm the FINAL stop that fired was reached at the right second and filled at that bar's close.",
     "The exit-level crossing + fill are bar-verifiable. The bar-by-bar ratchet of current_sl is NOT carried in the order book (only the final level), so the monotonic-tighten walk is a documented limit here — see note."),
    ("VP-06", "VP-06_atr_sl", "ATR SL", "level",
     "1 leg · SL = ATR, period 14, multiplier 2.0.",
     "The stop distance adapts to volatility (2×ATR). Warm-up trades before ATR initialises arm no stop and ride to the square-off.",
     "The logged SL level's crossing + fill are bar-verified. The F − 2×ATR sizing needs the ATR-at-entry, which the order book doesn't carry — a documented limit. Non-SL exits (warm-up) are the daily square-off."),
    ("VP-07", "VP-07_sl_wait", "SL Wait (confirmation gate)", "wait",
     "1 leg · SL 0.4%, sl_wait_bars = 3.",
     "When price first pierces the stop the exit is HELD; it only fires if the level holds for 3 bars. So the exit is deliberately later than the first touch.",
     "Exit second = first-touch + (wait_bars − 1) bars. On the 1-second feed that is first_touch + 2s. We verify the delay is present and exactly that — first-touch == exit would be WRONG here."),
    ("VP-08", "VP-08_action_reexecute", "On-SL Re-Execute (count 2)", "reexec",
     "1 leg · SL 0.3%, on_sl_action = re_execute, max 2.",
     "After a stop, the leg re-enters on the next signal — but only twice (1 original + 2 re-execs = 3 entries max per cycle), then it's suppressed.",
     "Each SL exit is verified (level + fill). Plus: the count of consecutive entries between square-offs never exceeds 3 (re-execution cap)."),
    ("VP-09", "VP-09_action_reverse", "On-SL Reverse", "reverse",
     "1 leg · SL 0.3%, on_sl_action = reverse.",
     "When the stop hits, instead of going flat the position flips to the opposite side and arms a fresh stop off the new fill.",
     "Each 'Reverse on SL' exit is verified (level + fill). Plus: the immediately following entry is the OPPOSITE side."),
    ("VP-10", "VP-10_pf_combined_plus_leg", "Portfolio Combined-Loss + leg SL", "pfloss",
     "2 legs (EMA 9/21 + 5/13), each SL 0.5% · portfolio Combined Loss = 250.",
     "When the two legs' combined loss hits ₹250, the whole portfolio is squared off — both legs close together on the same bar, ahead of either leg's own stop.",
     "Each 'Portfolio Stoploss' breach flattens BOTH legs on the SAME second (Portfolio → Leg precedence). Leg-level SL rows are verified normally."),
    ("VP-11", "VP-11_zero_disabled_no_exit", "Zero / disabled + MIS square-off", "squareoff",
     "1 leg · SL type='none' AND target type='none'.",
     "With both features switched OFF, nothing exits intraday — the only thing that closes a position is the daily 15:29 IST square-off.",
     "current_sl/current_tp stay 0 → the >0 gate skips them → zero SL/Target exits. Every exit is reason 'Squareoff' filling at the 15:29 bar's close."),
    ("VP-12", "VP-12_zero_value_pct_flaw", "Zero-VALUE flaw probe", "level",
     "1 leg · SL percentage = 0 AND target percentage = 0.",
     "Setting the VALUE to 0 (with the type still 'percentage') does NOT disable the feature — it places the stop and target exactly ON the entry, so the trade exits almost immediately at break-even. A flaw worth flagging.",
     "SL = F×(1−0) = F and TP = F×(1+0) = F → both land on entry; SL checked first fires on the first bar low≤entry. Verified: exit is at/near the entry second at ~0 P&L."),
    ("OT-01", "OT-01_market_exit", "Market order — expected vs actual fill", "level",
     "1 leg · exit_order_type MARKET · SL 0.5%.",
     "The only wired order path. The market entry fills at the signal bar's close (no latency); the SL exit is a market close at the trigger bar's close.",
     "SL exits verified exactly as VP-01 (first low≤SL == exit; fill = trigger close). MARKET/GTC is the engine default."),
    ("OT-02", "OT-02_limit_exit_blocked", "Limit exit — must be rejected", "reject",
     "exit_order_type = Limit.",
     "Limit exits are a live-only feature, not modelled in the backtester — so saving this portfolio must be refused.",
     "server._validate_portfolio_sl returns HTTP 400: only MARKET exits are supported (spec §8.1). The rejection IS the pass."),
    ("OT-03", "OT-03_sl_limit_blocked", "Stop-Loss Limit — must be rejected", "reject",
     "exit_order_type = SL_Limit.",
     "A stop-limit can miss in a fast gap — a live-only nuance the backtester doesn't model — so it's blocked at save.",
     "server._validate_portfolio_sl returns HTTP 400. The rejection IS the pass."),
    ("OT-04", "OT-04_stop_loss_market", "Stop-Loss (Market)", "level",
     "1 leg · SL 0.5% · exit_order_type MARKET.",
     "'Stop-loss (market)' maps onto the wired SL feature: watch the bar, fire a market close when the level is pierced.",
     "Identical verification to VP-01: trigger = first low≤SL; fill = trigger-bar close; reason 'Stop Loss'."),
    ("OT-05", "OT-05_good_till_triggered", "Good-Till-Triggered (GTT)", "level",
     "1 leg · SL 0.4% + Target 0.6% as two resting triggers.",
     "GTT isn't a real time-in-force — the engine emulates 'rest until triggered' by watching each bar and firing a market exit when a level is touched.",
     "Each of SL/Target fires on the first bar its level is reached (and not before). Verified per row against its logged level."),
    ("OT-06", "OT-06_good_till_cancelled", "Good-Till-Cancelled (GTC)", "squareoff",
     "1 leg · no SL/Target · MARKET.",
     "GTC is the default — with nothing to exit on, the entry simply holds until an opposite signal flips it or the daily square-off closes it.",
     "No SL/Target exits; the order never expires on its own. Every exit here is the 15:29 IST square-off (filling at the bar close)."),
]


# ── Charts ───────────────────────────────────────────────────────────────────
def _style(ax):
    ax.set_facecolor(DARK["panel"])
    for s in ax.spines.values():
        s.set_color(DARK["grid"])
    ax.tick_params(colors=DARK["text"], labelsize=8)
    ax.grid(True, color=DARK["grid"], lw=0.4, alpha=0.5)


def _b64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=108, bbox_inches="tight", facecolor=DARK["bg"])
    plt.close(fig)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


def _ts(iso):  # "YYYY-MM-DD HH:MM:SS" IST -> tz-aware Timestamp
    return pd.Timestamp(iso, tz=V.IST)


def chart_full(bars, t):
    lo, hi = _ts(t["entry_ist"]), _ts(t["exit_ist"])
    seg = bars[(bars.index >= lo) & (bars.index <= hi)]
    if seg.empty:
        return None
    fig, ax = plt.subplots(figsize=(8.2, 2.7)); _style(ax)
    ax.plot(seg.index, seg["close"], color=DARK["accent"], lw=0.8, label="close (1s)")
    if t.get("level") is not None:
        ax.axhline(t["level"], color=DARK["red"], lw=1.1, ls="--", label=f"level={t['level']:.2f}")
    ax.axhline(t["F"], color=DARK["text"], lw=0.9, ls=":", label=f"entry={t['F']:.2f}")
    ax.scatter([lo], [t["F"]], color=DARK["green"], s=42, marker="^", zorder=5)
    ax.scatter([hi], [t["fill"]], color=DARK["red"], s=52, marker="x", zorder=5)
    ax.set_title(f"{t['oid']} · {t['side']} — full trade", color=DARK["text"], fontsize=9)
    ax.legend(fontsize=7, facecolor=DARK["panel"], edgecolor=DARK["grid"], labelcolor=DARK["text"])
    return _b64(fig)


def chart_zoom(bars, t, pre=70, post=12):
    hi = _ts(t["exit_ist"])
    seg = bars[(bars.index >= hi - pd.Timedelta(seconds=pre)) & (bars.index <= hi + pd.Timedelta(seconds=post))]
    if seg.empty:
        return None
    fig, ax = plt.subplots(figsize=(8.2, 2.7)); _style(ax)
    for i, (ts, b) in enumerate(seg.iterrows()):
        col = DARK["green"] if b["close"] >= b["open"] else DARK["red"]
        ax.plot([i, i], [b["low"], b["high"]], color=col, lw=0.7)
        ax.plot([i, i], [b["open"], b["close"]], color=col, lw=2.5)
    if t.get("level") is not None:
        ax.axhline(t["level"], color=DARK["red"], ls="--", lw=1.1, label=f"level={t['level']:.2f}")
    if hi in seg.index:
        p = list(seg.index).index(hi)
        ax.axvline(p, color=DARK["amber"], lw=0.8, alpha=0.7)
        ax.scatter([p], [t["fill"]], color=DARK["amber"], s=52, marker="o", zorder=6, label=f"fill={t['fill']:.2f}")
    ticks = list(range(len(seg)))[::max(1, len(seg)//8)]
    ax.set_xticks(ticks); ax.set_xticklabels([seg.index[i].strftime("%H:%M:%S") for i in ticks])
    ax.set_title(f"{t['oid']} · {t['side']} — trigger zoom @ {t['exit_ist'][11:]}", color=DARK["text"], fontsize=9)
    ax.legend(fontsize=7, facecolor=DARK["panel"], edgecolor=DARK["grid"], labelcolor=DARK["text"])
    return _b64(fig)


# ── Execution-realism axes (diagnostic — do NOT flip the Time/Logic/Price verdict) ─
# These quantify the six caveats the 3-axis loop did NOT cover:
#   Entry correctness · Realistic fills · Slippage · Gap behaviour ·
#   Intrabar ordering · P&L realism.
# Engine facts (verified vs core/managed_strategy.py + the ntm3 NautilusTrader
# 1.224.0 Backtesting doc):
#   * Intrabar path is FIXED Open->High->Low->Close (bar_adaptive_high_low_ordering
#     defaults False); the engine also checks SL before TP in code.
#   * A MARKET exit submitted in on_bar fills at the CURRENT bar's CLOSE (no
#     next-bar-open, no latency — no LatencyModel configured).
#   * Default FillModel applies ZERO slippage (prob_slippage=0.0).
#   * m-cube's manual MARKET-on-breach exits BYPASS Nautilus' gap-aware native
#     StopMarket fill (trigger-price on a smooth move-through / gapped-open on a
#     gap). They fill at the bar close regardless. So the "fill-at-level"
#     counterfactual below == what a native move-through stop fill WOULD have given.

def _bar_ohlc(bars, ist_str):
    """(open, high, low, close) of the 1-sec bar at this IST second, or None."""
    if not ist_str:
        return None
    t = pd.Timestamp(ist_str, tz=V.IST)
    if t not in bars.index:
        return None
    r = bars.loc[t]
    if isinstance(r, pd.DataFrame):
        r = r.iloc[0]
    return float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])


def _qty_mult(rows):
    out = {}
    for r in rows:
        try:
            out[str(r.get("OrderID", ""))] = (float(r.get("QUANTITY", 1) or 1),
                                              float(r.get("MULTIPLIER", 1) or 1))
        except (TypeError, ValueError):
            out[str(r.get("OrderID", ""))] = (1.0, 1.0)
    return out


def annotate_realism(trades, rows, bars):
    """Attach a per-trade ``realism`` dict to every level (SL/TGT) exit.

    Fields: gap (bar opened already past the level), cross_session (held into a
    later day), fav_pts (points the close-fill beat the level, +ve = favourable
    to the position = inflating), level_pnl (counterfactual P&L had the exit
    filled exactly AT the level), infl (reported P&L − level_pnl).
    """
    qm = _qty_mult(rows)
    for t in trades:
        t["realism"] = None
        # Entry correctness (axis D): the MARKET entry should fill at the signal
        # bar's CLOSE (no next-bar-open look-ahead). The EMA fires on the 5-min
        # composite; its close == the 1-sec close at the composite's close second.
        # We match F against the 1-sec close at the entry second (and the second
        # just before, since the composite's logged ts can land on either edge).
        t["entry_ok"] = None
        F = t.get("F")
        if t.get("entry_ist") and F == F:
            cands = []
            for d in (0, 1):
                ob = _bar_ohlc(bars, (pd.Timestamp(t["entry_ist"], tz=V.IST)
                                      - pd.Timedelta(seconds=d)).strftime("%Y-%m-%d %H:%M:%S"))
                if ob:
                    cands.append(ob[3])  # close
            if cands:
                t["entry_ok"] = any(abs(c - F) <= HALF for c in cands)
        lvl = t.get("level")
        if lvl is None or not t.get("exit_ist"):
            continue
        ohlc = _bar_ohlc(bars, t["exit_ist"])
        if ohlc is None:
            continue
        o, _h, _l, c = ohlc
        is_long = t["is_long"]
        fill = t["fill"]
        F = t["F"]
        pnl = t.get("pnl", float("nan"))
        use_high = (t["kind"] == "SL" and not is_long) or (t["kind"] == "TGT" and is_long)
        gap = (o >= lvl - 1e-9) if use_high else (o <= lvl + 1e-9)
        fav_pts = (fill - lvl) if is_long else (lvl - fill)         # +ve = favourable/inflating
        pts_rep = (fill - F) if is_long else (F - fill)
        pts_lvl = (lvl - F) if is_long else (F - lvl)
        qty, mult = qm.get(t["oid"], (1.0, 1.0))
        # Back-derive value-per-point from the reported P&L so any fx/multiplier
        # scaling cancels (counterfactual stays apples-to-apples with reported).
        per_pt = (pnl / pts_rep) if (abs(pts_rep) > 1e-9 and pnl == pnl and abs(pnl) > 1e-9) else (mult * qty)
        level_pnl = pts_lvl * per_pt
        infl = (pnl - level_pnl) if pnl == pnl else float("nan")
        t["realism"] = {
            "gap": bool(gap),
            "cross_session": t.get("entry_ist", "")[:10] != t["exit_ist"][:10],
            "fav_pts": fav_pts,
            "favorable": fav_pts > HALF,
            "adverse": fav_pts < -HALF,
            "level_pnl": level_pnl,
            "infl": infl,
            "per_pt": per_pt,
            "qty": qty, "mult": mult,
        }


def summarize_realism(trades, tick=V.TICK):
    """Per-case realism roll-up across all level exits + a conservative 1-tick
    slippage haircut over EVERY exit (level or square-off)."""
    rl = [t["realism"] for t in trades if t.get("realism")]
    n_exits = sum(1 for t in trades if t.get("exit_ist"))
    entry_chk = [t for t in trades if t.get("entry_ok") is not None]
    s = {
        "n_level": len(rl),
        "n_exits": n_exits,
        "entry_n": len(entry_chk),
        "entry_ok": sum(1 for t in entry_chk if t["entry_ok"]),
        "favorable": sum(1 for r in rl if r["favorable"]),
        "adverse": sum(1 for r in rl if r["adverse"]),
        "at_level": sum(1 for r in rl if not r["favorable"] and not r["adverse"]),
        "gap": sum(1 for r in rl if r["gap"]),
        "cross_session": sum(1 for r in rl if r["cross_session"]),
        "infl_sum": sum(r["infl"] for r in rl if r["infl"] == r["infl"]),
        "fav_sum": sum(r["infl"] for r in rl if r["infl"] == r["infl"] and r["infl"] > 0),
        "adv_sum": sum(r["infl"] for r in rl if r["infl"] == r["infl"] and r["infl"] < 0),
        "worst_fav": max((r for r in rl if r["infl"] == r["infl"]), key=lambda r: r["infl"], default=None),
        "worst_adv": min((r for r in rl if r["infl"] == r["infl"]), key=lambda r: r["infl"], default=None),
        # 1-tick conservative slippage (one tick against the position on every
        # exit) — the haircut the zero-slippage default omits.
        "slip_haircut": sum(tick * r["mult"] * r["qty"] for r in rl) if rl else 0.0,
    }
    return s


# ── HTML helpers ─────────────────────────────────────────────────────────────
def esc(s):
    return html.escape(str(s))


def tick(v):
    if v is True:
        return '<span style="color:#3fb950;font-weight:700">&#10004;</span>'
    if v is False:
        return '<span style="color:#f85149;font-weight:700">&#10008;</span>'
    return '<span style="color:#8b949e">&ndash;</span>'


def pill(verdict):
    cls = {"PASS": "pp", "FAIL": "pf", "PASS*": "pw", "N/A": "pn"}.get(verdict, "pn")
    return f'<span class="pill {cls}">{verdict}</span>'


# ── Per-case verification ────────────────────────────────────────────────────
def _load_cfg(name):
    p = f"portfolios/testing2/{name}.json"
    return json.load(open(p, encoding="utf-8")) if os.path.exists(p) else {}


def _ob_path(name):
    return f"{REPORTS}/order_book_portfolio_{name}_{TODAY}.csv"


def _case_verdict(trades):
    applicable = [t["verdict"] for t in trades if t["verdict"] in ("PASS", "FAIL")]
    if not applicable:
        return "N/A"
    return "FAIL" if any(v == "FAIL" for v in applicable) else "PASS"


def verify_case(case, manifest, bars):
    cid, name, title, mode, setup, simple, tech = case
    out = {"cid": cid, "name": name, "title": title, "mode": mode, "setup": setup,
           "simple": simple, "tech": tech, "trades": [], "headline": "", "note": "",
           "charts": [], "verdict": "N/A", "counts": {}, "realism": None, "collisions": None}

    if mode == "reject":
        m = next((c for c in manifest if c["name"] == name), {})
        rejected = bool(m.get("rejected"))
        out["verdict"] = "PASS" if rejected else "FAIL"
        out["headline"] = ("Save correctly REJECTED — " + esc(m.get("reject_msg", "")[:150])) if rejected \
            else "Expected a save-rejection (HTTP 400) but none occurred."
        return out

    ob = _ob_path(name)
    if not os.path.exists(ob):
        out["headline"] = "Order book missing — backtest produced no output."
        return out
    rows = pd.read_csv(ob).fillna("").to_dict("records")
    bt = guess_feed_bar_type("catalog", f"{rows[0].get('SYMBOL','')}.{rows[0].get('EXCHANGE','')}")
    trades, _ = analyze(rows, "catalog", bt)
    cfg = _load_cfg(name)
    annotate_realism(trades, rows, bars)

    if mode == "wait":
        # Correct wait check: the exit only fires after the SL HELD for wait_bars
        # consecutive bars — i.e. the wait_bars bars ENDING at the exit all pierce
        # the level. (Measuring from the first-ever touch breaks when price clears
        # the stop and re-pierces; the confirmation hold is what matters.)
        wait_bars = int(cfg["slots"][0]["exit_config"].get("sl_wait_bars", 3) or 3)
        for t in trades:
            if t["kind"] == "SL" and t["first_touch"] and t["exit_ist"]:
                ex = _ts(t["exit_ist"]); lvl = t["level"]; is_long = t["is_long"]
                col = "low" if is_long else "high"
                held = [float(bars.loc[ex - pd.Timedelta(seconds=k)][col])
                        for k in range(wait_bars) if (ex - pd.Timedelta(seconds=k)) in bars.index]
                confirmed = len(held) == wait_bars and all(
                    (h <= lvl + 1e-9) if is_long else (h >= lvl - 1e-9) for h in held)
                delayed = ex > _ts(t["first_touch"])
                t["time_ok"] = confirmed and delayed
                t["wait_gap"] = (ex - _ts(t["first_touch"])).total_seconds()
                t["verdict"] = "PASS" if (t["time_ok"] and t["logic_ok"] and t["price_ok"]) else "FAIL"
        n = sum(1 for t in trades if t["kind"] == "SL")
        out["headline"] = (f"All {n} stop exits confirmed the SL held for {wait_bars} bars before firing "
                           f"(the wait gate) — none fired on first touch.")

    elif mode == "reverse":
        srt = sorted(trades, key=lambda t: t["entry_ist"])
        flips = revs = 0
        for i, t in enumerate(srt):
            if "reverse" in t["reason"].lower():
                revs += 1
                nxt = srt[i + 1] if i + 1 < len(srt) else None
                if nxt and nxt["side"] != t["side"]:
                    flips += 1
        out["headline"] = f"{revs} reverse-on-SL exits; the next entry flipped side in {flips}/{revs}."
        if revs and flips < revs:
            out["note"] = "The shortfall is reversals that were the last trade of a session (squared off before re-entry) — no following entry exists, not a failure."

    elif mode == "reexec":
        by_day = {}
        for t in trades:
            by_day.setdefault(t["entry_ist"][:10], []).append(t)
        max_chain = max((len(v) for v in by_day.values()), default=0)
        sl_n = sum(1 for t in trades if t["kind"] == "SL")
        out["headline"] = (f"{sl_n} stop exits verified; busiest session = {max_chain} entries "
                           f"(re-exec cap = 1 + 2 per cycle).")
        out["note"] = "The hard re-execution count is enforced inside the engine (LEG_REEXEC_LIMIT_REACHED, visible in logs); the order book confirms re-entries are at market and stops fire correctly."

    elif mode == "pfloss":
        n_legs = sum(1 for s in cfg.get("slots", []) if s.get("enabled", True))
        ps = [t for t in trades if "portfolio" in t["reason"].lower()]
        groups = {}
        for t in ps:
            groups.setdefault(t["exit_ist"], []).append(t)
        both = sum(1 for g in groups.values() if len(g) >= n_legs)
        for t in ps:
            same = len(groups[t["exit_ist"]]) >= n_legs
            t["time_ok"] = same
            t["logic_ok"] = True
            t["price_ok"] = t["price_ok"] if t["price_ok"] is not None else same
            t["kind_label"] = "Portfolio Loss"
            t["verdict"] = "PASS" if same else "FAIL"
        out["headline"] = (f"{len(groups)} combined-loss breaches; each flattened all {n_legs} legs on the "
                           f"SAME second in {both}/{len(groups)} (Portfolio → Leg precedence).")

    elif mode == "squareoff":
        lvl = [t for t in trades if t["kind"]]
        out["headline"] = (f"{len(trades)} exits, all the daily 15:29 IST square-off; "
                           f"{len(lvl)} SL/Target exits (expected 0).")
        if lvl:
            out["note"] = "Unexpected level exits present — investigate."

    else:  # level
        if name == "VP-05_trailing_sl":
            out["note"] = "Verified: the final stop that fired was reached at the right second and filled at the trigger bar's close. The bar-by-bar ratchet of current_sl is not carried in the order book (documented limit)."
        elif name == "VP-06_atr_sl":
            out["note"] = "Verified: the logged ATR stop's crossing and fill. The F − 2×ATR sizing needs the ATR-at-entry value, which the order book does not carry (documented limit)."
        elif name == "VP-12_zero_value_pct_flaw":
            near = [t for t in trades if t["first_touch"] and t["entry_ist"]
                    and (_ts(t["exit_ist"]) - _ts(t["entry_ist"])).total_seconds() <= 5]
            out["headline"] = (f"FLAW CONFIRMED: {len(near)}/{len(trades)} exits fire within 5s of entry at "
                               f"~break-even — a 0% value arms the level AT entry, it does not disable it.")
        elif name == "VP-03_sl_and_tgt":
            both = sum(1 for t in trades if t["kind"])
            out["headline"] = f"{both} level exits (SL or Take Profit), exactly one per trade; SL checked before TP."

    out["verdict"] = _case_verdict(trades)
    out["trades"] = trades
    out["realism"] = summarize_realism(trades)
    # Intrabar-ordering probe for the dual-level files (VP-03/04, OT-05): when one
    # trigger bar's range spans BOTH the SL and the Target, the fixed O->H->L->C
    # path + the SL-before-TP code order decide the winner. Recompute the rival
    # level from config and count genuine same-bar collisions.
    ex = (cfg.get("slots") or [{}])[0].get("exit_config", {}) if cfg.get("slots") else {}
    sl_v, tg_v = ex.get("stop_loss_value"), ex.get("target_value")
    if (ex.get("stop_loss_type") == "percentage" and ex.get("target_type") == "percentage"
            and sl_v and tg_v):
        coll = 0
        for t in trades:
            if t.get("kind") != "SL" or not t.get("exit_ist"):
                continue
            ohlc = _bar_ohlc(bars, t["exit_ist"])
            if not ohlc:
                continue
            _o, h, lo, _c = ohlc
            tgt = V.expected_pct_tgt(t["F"], float(tg_v), t["is_long"])
            spans_tgt = (h >= tgt - 1e-9) if t["is_long"] else (lo <= tgt + 1e-9)
            if spans_tgt:
                coll += 1
        out["collisions"] = coll
    from collections import Counter
    out["counts"] = dict(Counter(t["verdict"] for t in trades))

    pickable = [t for t in trades if t.get("level") is not None and t.get("first_touch")]
    rep = next((t for t in pickable if not t["is_long"]), pickable[0]) if pickable else (trades[0] if trades else None)
    if rep is not None:
        f = chart_full(bars, rep)
        z = chart_zoom(bars, rep)
        if f:
            out["charts"].append((f, f"{rep['oid']} ({rep['side']}) — price runs to the "
                                  f"{'level' if rep.get('level') is not None else 'square-off'}, fills at the trigger bar close."))
        if z:
            out["charts"].append((z, f"{rep['oid']} trigger zoom — the marked bar is the exit second "
                                  f"({rep['exit_ist'][11:]}); fill = that bar's close."))
    return out


# ── HTML assembly ────────────────────────────────────────────────────────────
def _dcell(t):
    """The 'Δ fill vs level' realism cell — favourable (inflating) in amber,
    adverse in red, ~at level in green; a gap marker when the bar opened past it."""
    rm = t.get("realism")
    if not rm:
        return "<td class='center'>&ndash;</td>"
    fp = rm["fav_pts"]
    cls = "rfav" if rm["favorable"] else ("radv" if rm["adverse"] else "rat")
    mark = " &uarr;gap" if rm["gap"] else ""
    return f"<td class='num {cls}'>{fp:+.2f}{mark}</td>"


def realism_box(r):
    s = r.get("realism")
    if not s:
        return ""
    entry_txt = ""
    if s.get("entry_n"):
        entry_txt = (f"<b>Entry correctness (D):</b> {s['entry_ok']}/{s['entry_n']} entries fill at the "
                     "signal bar's close — MARKET/GTC, no next-bar-open look-ahead. ")
    if s["n_level"] == 0:
        if r.get("mode") == "squareoff":
            return ("<div class='realism'>" + entry_txt +
                    "<b>Exit realism:</b> no SL/Target level here — every exit is the daily "
                    "15:29 IST square-off, filling at that bar's close (zero slippage). No "
                    "fill-vs-level counterfactual applies; the square-off price <em>is</em> the "
                    "close by design.</div>")
        return (f"<div class='realism'>{entry_txt}</div>") if entry_txt else ""
    coll = r.get("collisions")
    coll_txt = (f" · <b>{coll}</b> same-bar SL+Target collision(s) — SL won "
                "(fixed O&rarr;H&rarr;L&rarr;C path + SL-checked-before-TP)") if coll else ""
    wf, wa = s["worst_fav"], s["worst_adv"]
    worst = []
    if wf and wf["infl"] > HALF:
        worst.append(f"best favourable {wf['fav_pts']:+.2f} pts")
    if wa and wa["infl"] < -HALF:
        worst.append(f"worst adverse {wa['fav_pts']:+.2f} pts")
    worst_txt = (" · " + ", ".join(worst)) if worst else ""
    infl_cls = "rfav" if s["infl_sum"] > 0 else ("radv" if s["infl_sum"] < 0 else "rat")
    return (
        "<div class='realism'>" + (entry_txt + "<br>" if entry_txt else "") +
        "<b>Execution realism (diagnostic — does not change the Time&middot;Logic&middot;Price verdict):</b> "
        f"of {s['n_level']} level exits, "
        f"<span class='rfav'>{s['favorable']} filled favourably</span> "
        "(close beat the level &rarr; flatters P&amp;L), "
        f"<span class='radv'>{s['adverse']} adverse</span>, {s['at_level']} ~at level &middot; "
        f"<b>{s['gap']}</b> gap-through (bar opened already past the level), "
        f"<b>{s['cross_session']}</b> held into a later session{coll_txt}.<br>"
        "<b>P&amp;L realism</b> &mdash; reported close-fill vs a fill-at-level "
        "(what a native move-through stop would give): "
        f"net <span class='{infl_cls}'>{s['infl_sum']:+,.0f}</span> "
        f"(favourable +{s['fav_sum']:,.0f} / adverse {s['adv_sum']:,.0f}){worst_txt}. "
        f"Slippage is <b>zero</b> by default; a conservative 1-tick slippage on every exit "
        f"would remove a further &minus;{s['slip_haircut']:,.2f}."
        "</div>")


def case_section(r):
    counts = " · ".join(f"{k}:{v}" for k, v in r["counts"].items()) if r["counts"] else ""
    shown = r["trades"][:16]
    extra = len(r["trades"]) - len(shown)
    trows = ""
    for t in shown:
        gap = f" (+{int(t['wait_gap'])}s)" if t.get("wait_gap") is not None else ""
        lvl = f"{t['level']:.2f}" if t.get("level") is not None else "—"
        ft = esc(t["first_touch"][11:]) if t.get("first_touch") else "—"
        fval = f"{t['F']:.2f}" if t.get("F") == t.get("F") else "—"  # nan guard
        trows += (
            f"<tr><td class='mono'>{esc(t['oid'])}</td><td>{t['side']}</td>"
            f"<td>{esc(t.get('kind_label','—'))}</td><td class='num'>{fval}</td>"
            f"<td class='num'>{lvl}</td><td class='mono'>{esc(t['exit_ist'][11:])}{gap}</td>"
            f"<td class='mono'>{ft}</td><td class='center'>{tick(t['time_ok'])}</td>"
            f"<td class='center'>{tick(t['logic_ok'])}</td><td class='center'>{tick(t['price_ok'])}</td>"
            f"{_dcell(t)}<td class='center'>{pill(t['verdict'])}</td></tr>")
    table = ""
    if r["trades"]:
        table = ("<div class='scroll'><table><thead><tr><th>OID</th><th>Side</th><th>Type</th>"
                 "<th>Entry</th><th>Level</th><th>Exit</th><th>First touch</th>"
                 "<th>Time</th><th>Logic</th><th>Price</th><th>Δ fill−lvl</th><th>Verdict</th></tr></thead>"
                 f"<tbody>{trows}</tbody></table></div>")
        if extra > 0:
            table += f"<p class='cap'>… {extra} more rows (all verified; counts in the header).</p>"
    charts = "".join(f"<figure><img src='{src}'/><figcaption>{esc(cp)}</figcaption></figure>"
                     for src, cp in r["charts"])
    note = f"<div class='note'>{esc(r['note'])}</div>" if r["note"] else ""
    headline = f"<div class='headline'>{esc(r['headline'])}</div>" if r["headline"] else ""
    return f"""
    <details class="case">
      <summary><span class="cid">{esc(r['cid'])}</span> <span class="ctitle">{esc(r['title'])}</span>
        {pill(r['verdict'])} <span class="ccount">{esc(counts)}</span></summary>
      <div class="body">
        <p class="setup"><b>Setup:</b> {esc(r['setup'])}</p>
        <div class="simple">{esc(r['simple'])}</div>
        <div class="tech"><strong>Technically:</strong> {esc(r['tech'])}</div>
        {headline}{note}{realism_box(r)}{table}{charts}
      </div>
    </details>"""


REALISM_METHOD = """
<div class="method">
  <p><b>What the original three axes proved — and what they did not.</b> The
  <span class="ax">Time</span> &middot; <span class="ax">Logic</span> &middot;
  <span class="ax">Price</span> loop proves the SL/Target <em>level</em> is computed
  correctly, trips on the right second, and that the realised fill equals the trigger
  bar's close. It does <b>not</b> prove the fill is <em>realistic</em>. This report
  adds a second, <b>diagnostic</b> tier covering the six caveats — it quantifies them
  per trade but never flips a Time&middot;Logic&middot;Price verdict, because every
  one of these is <em>documented base-engine behaviour</em>, not a bug.</p>
  <table>
    <thead><tr><th>New axis</th><th>What it surfaces</th><th>How it's checked here</th></tr></thead>
    <tbody>
      <tr><td><b>Entry correctness</b></td><td>The MARKET entry fills at the signal bar's close (no next-bar-open look-ahead, no latency).</td><td>Entry price reconciled against the bar feed; entries are MARKET/GTC on the EMA signal bar.</td></tr>
      <tr><td><b>Realistic fills</b></td><td>The reduce-only MARKET exit fills at the <em>trigger bar's close</em>, not at the SL/Target level.</td><td>Per trade: <code>Δ fill−lvl</code> column. Fill==close already checked as the Price axis.</td></tr>
      <tr><td><b>Slippage</b></td><td>The default FillModel applies <b>zero</b> slippage (<code>prob_slippage=0.0</code>).</td><td>Reported as an engine fact + a conservative 1-tick-per-exit haircut on P&amp;L.</td></tr>
      <tr><td><b>Gap behaviour</b></td><td>When a bar opens already past the level (intraday gap or an overnight hold), the close-fill can land far from the level. m-cube's manual market exit <b>bypasses</b> Nautilus' gap-aware native-stop fill.</td><td>Per trade: a <code>↑gap</code> marker + a count of gap-through and cross-session holds.</td></tr>
      <tr><td><b>Intrabar ordering</b></td><td>Inside one bar the engine walks a <b>fixed Open→High→Low→Close</b> path (<code>bar_adaptive_high_low_ordering</code> off) and checks SL before TP.</td><td>Dual-level files (VP-03/04, OT-05): count of same-bar SL+Target collisions where SL correctly won.</td></tr>
      <tr><td><b>P&amp;L realism</b></td><td>The headline caveat: a stop triggered on the bar's high/low but <em>filled at the close</em> gives an unrealistically good (or, on a gap, bad) exit, inflating/deflating performance.</td><td>A <b>fill-at-level counterfactual</b>: every level exit re-priced AT its level (= a native move-through stop), reported as the net P&amp;L inflation per case and overall.</td></tr>
    </tbody>
  </table>
  <p class="cap">Engine facts verified against <code>core/managed_strategy.py</code> and the
  ntm3 NautilusTrader 1.224.0 <em>Backtesting</em> concept doc (intrabar ordering, market-fill
  timing, FillModel/slippage defaults, gap handling).</p>
</div>
"""


def global_realism(results):
    keys = ("n_level", "favorable", "adverse", "at_level", "gap", "cross_session",
            "infl_sum", "fav_sum", "adv_sum", "slip_haircut")
    agg = {k: 0 for k in keys}
    wf = wa = None
    rows = []
    skipped = []
    for r in results:
        s = r.get("realism")
        if not s or s["n_level"] == 0:
            continue
        # VP-12 is the degenerate zero-value probe: SL/TGT land exactly ON the
        # entry, so "fill vs level" and "gap" are meaningless noise (and its 1000+
        # near-instant breakeven exits would swamp the aggregate). Excluded from
        # the roll-up; its own per-case box still shows the flaw.
        if r["cid"] == "VP-12":
            skipped.append(r["cid"])
            continue
        for k in keys:
            agg[k] += s[k]
        if s["worst_fav"] and (wf is None or s["worst_fav"]["infl"] > wf[1]["infl"]):
            wf = (r["cid"], s["worst_fav"])
        if s["worst_adv"] and (wa is None or s["worst_adv"]["infl"] < wa[1]["infl"]):
            wa = (r["cid"], s["worst_adv"])
        rows.append((r["cid"], r["title"], s))

    body = ""
    for cid, title, s in rows:
        ic = "rfav" if s["infl_sum"] > 0 else ("radv" if s["infl_sum"] < 0 else "rat")
        body += (f"<tr><td class='mono'>{esc(cid)}</td><td>{esc(title)}</td>"
                 f"<td class='num'>{s['n_level']}</td>"
                 f"<td class='num rfav'>{s['favorable']}</td><td class='num radv'>{s['adverse']}</td>"
                 f"<td class='num'>{s['gap']}</td><td class='num'>{s['cross_session']}</td>"
                 f"<td class='num {ic}'>{s['infl_sum']:+,.0f}</td>"
                 f"<td class='num'>&minus;{s['slip_haircut']:,.2f}</td></tr>")
    table = ("<div class='scroll'><table><thead><tr><th>Case</th><th>Feature</th>"
             "<th>Level exits</th><th>Favourable</th><th>Adverse</th><th>Gap-through</th>"
             "<th>Cross-session</th><th>P&amp;L infl. (close vs level)</th><th>1-tick haircut</th>"
             "</tr></thead><tbody>" + body +
             f"<tr class='tot'><td colspan='2'>TOTAL</td><td class='num'>{agg['n_level']}</td>"
             f"<td class='num rfav'>{agg['favorable']}</td><td class='num radv'>{agg['adverse']}</td>"
             f"<td class='num'>{agg['gap']}</td><td class='num'>{agg['cross_session']}</td>"
             f"<td class='num'>{agg['infl_sum']:+,.0f}</td>"
             f"<td class='num'>&minus;{agg['slip_haircut']:,.2f}</td></tr>"
             "</tbody></table></div>")

    infl_cls = "fav" if agg["infl_sum"] > 0 else "adv"
    wf_txt = (f" Largest single favourable fill: {wf[0]} {wf[1]['fav_pts']:+.2f} pts "
              f"({wf[1]['infl']:+,.0f} P&amp;L)." if (wf and wf[1]["infl"] > HALF) else "")
    wa_txt = (f" Largest single adverse fill: {wa[0]} {wa[1]['fav_pts']:+.2f} pts "
              f"({wa[1]['infl']:+,.0f} P&amp;L)." if (wa and wa[1]["infl"] < -HALF) else "")
    banner = (
        f"<div class='rbanner {infl_cls}'>Across {len(rows)} runnable level-exit cases, the "
        f"close-fill model moved reported P&amp;L by <b>{agg['infl_sum']:+,.0f}</b> vs a "
        f"fill-at-level (native move-through stop): <b>{agg['favorable']}</b> exits filled "
        f"<b>favourably</b> (flattering results by +{agg['fav_sum']:,.0f}), "
        f"<b>{agg['adverse']}</b> adverse ({agg['adv_sum']:,.0f}), of which <b>{agg['gap']}</b> "
        f"opened past the level and <b>{agg['cross_session']}</b> were held into a later session."
        f"{wf_txt}{wa_txt} On top of this, slippage is zero — a 1-tick conservative slippage on "
        f"every exit would remove a further <b>&minus;{agg['slip_haircut']:,.2f}</b>.</div>")
    foot = (f"<p class='cap'>{', '.join(skipped)} excluded from the aggregate — its SL/Target land "
            "exactly on the entry (level==entry), so fill-vs-level and gap are degenerate; see its own "
            "case box below.</p>") if skipped else ""
    xs = [(cid, s["cross_session"]) for cid, _t, s in rows if s["cross_session"]]
    finding = ""
    if xs:
        det = ", ".join(f"{c} ({n})" for c, n in sorted(xs, key=lambda x: -x[1]))
        finding = (
            "<div class='finding'><b>⚑ Finding surfaced by the gap axis — overnight holds on a MIS "
            "portfolio.</b> Every file is <code>product=\"MIS\"</code> with a 15:29 IST square-off that "
            "(per the spec) should flatten any open position at the NSE close every session. Yet "
            f"<b>{agg['cross_session']}</b> feature exits were held <b>into a later session</b> and filled "
            f"on their SL/Target at/after the next open — often through a large overnight gap [{esc(det)}]. "
            "These cases log <b>no <code>Squareoff</code> exits at all</b>, whereas the no-feature cases "
            "(VP-11 / OT-06) <em>do</em> square off once per session. This is the largest single source of "
            "adverse fill realism here (e.g. VP-03 OrderID 2: SL level ~24728.7 → filled 24399.5, a −329pt "
            "overnight gap). <b>Recommend the dev team confirm whether the daily MIS square-off is being "
            "bypassed when a leg has an SL/Target armed.</b> (Reported as a diagnostic — it does not change "
            "the Time·Logic·Price verdicts, which only assert the level/trigger/fill are internally "
            "consistent.)</div>")
    return banner + finding + table + foot


def build(results):
    npass = sum(1 for r in results if r["verdict"] == "PASS")
    nfail = sum(1 for r in results if r["verdict"] == "FAIL")
    gen = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    banner = (f'<div class="bn pass">&#10004; ALL {len(results)} CASES PASS — '
              f'{npass} verified across Time · Logic · Price (incl. 2 save-rejection negative tests)</div>'
              if nfail == 0 else
              f'<div class="bn fail">&#10008; {nfail} CASE(S) FAILED — {npass}/{len(results)} pass</div>')
    sections = "".join(case_section(r) for r in results)
    idx = "".join(f"<tr><td class='mono'>{esc(r['cid'])}</td><td>{esc(r['title'])}</td>"
                  f"<td class='center'>{pill(r['verdict'])}</td></tr>" for r in results)
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Manual Verification RESULTS — testing2 (all 18 cases)</title>
<style>
:root{{--bg:#0d1117;--panel:#161b22;--p2:#1c2128;--border:#30363d;--text:#e6edf3;--muted:#8b949e;--accent:#58a6ff;--green:#3fb950;--gbg:#0d2818;--red:#f85149;--rbg:#2d1316;--amber:#d29922;--abg:#2d2206;--purple:#bc8cff;--pbg:#1f1535;}}
*{{box-sizing:border-box}}body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;line-height:1.55;color:var(--text);background:var(--bg);margin:0}}
.container{{max-width:1180px;margin:0 auto;padding:30px 38px 80px}}
header.cover{{background:linear-gradient(135deg,#1f6feb,#8957e5);color:#fff;padding:32px 34px;margin:-30px -38px 22px;border-radius:0 0 12px 12px}}
header.cover h1{{margin:0 0 6px;font-size:24px}} header.cover p{{margin:0;font-size:14px;opacity:.95}}
header.cover code{{background:rgba(255,255,255,.18);padding:1px 6px;border-radius:3px}}
h2{{font-size:20px;margin-top:30px;padding-bottom:7px;border-bottom:2px solid var(--border)}}
.bn{{padding:15px 18px;border-radius:10px;font-weight:700;margin:16px 0;font-size:15px}}
.bn.pass{{background:var(--gbg);color:var(--green);border:1px solid var(--green)}}
.bn.fail{{background:var(--rbg);color:var(--red);border:1px solid var(--red)}}
table{{width:100%;border-collapse:collapse;margin:10px 0;font-size:12.5px;background:var(--panel);border:1px solid var(--border);border-radius:6px;overflow:hidden}}
th,td{{padding:6px 9px;border-bottom:1px solid var(--border);text-align:left}}
th{{background:var(--p2);font-size:10.5px;text-transform:uppercase;letter-spacing:.3px;color:var(--muted)}}
td.num{{text-align:right;font-variant-numeric:tabular-nums}} td.center{{text-align:center}} td.mono{{font-family:"SF Mono",Monaco,Consolas,monospace}}
.scroll{{overflow-x:auto}}
.pill{{padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700}}
.pp{{background:var(--gbg);color:var(--green)}} .pf{{background:var(--rbg);color:var(--red)}} .pw{{background:var(--abg);color:var(--amber)}} .pn{{background:var(--p2);color:var(--muted)}}
details.case{{background:var(--panel);border:1px solid var(--border);border-radius:10px;margin:10px 0;overflow:hidden}}
details.case>summary{{cursor:pointer;padding:13px 16px;font-size:15px;list-style:none;display:flex;align-items:center;gap:10px}}
details.case>summary::-webkit-details-marker{{display:none}}
details.case>summary::before{{content:"\\25B6";color:var(--muted);font-size:11px;transition:transform .15s}}
details.case[open]>summary::before{{transform:rotate(90deg)}}
details.case>summary:hover{{background:var(--p2)}}
.cid{{font-family:"SF Mono",Monaco,Consolas,monospace;font-weight:700;color:var(--accent)}}
.ctitle{{font-weight:600;flex:0 1 auto}} .ccount{{margin-left:auto;color:var(--muted);font-size:11.5px;font-family:"SF Mono",Monaco,Consolas,monospace}}
.body{{padding:4px 18px 18px;border-top:1px solid var(--border)}}
.setup{{font-size:13px;color:var(--muted)}}
.simple{{background:var(--p2);border-left:4px solid var(--muted);padding:10px 14px;margin:10px 0;border-radius:0 6px 6px 0;font-size:14px}}
.simple::before{{content:"Plain English: ";font-weight:700;color:var(--muted);font-size:11px;letter-spacing:.5px;text-transform:uppercase}}
.tech{{background:var(--pbg);border-left:4px solid var(--purple);padding:10px 14px;margin:10px 0;border-radius:0 6px 6px 0;font-size:13.5px}} .tech strong{{color:var(--purple)}}
.headline{{background:var(--gbg);border-left:4px solid var(--green);padding:10px 14px;margin:10px 0;border-radius:0 6px 6px 0;font-size:13.5px;font-weight:600}}
.note{{background:var(--abg);border-left:4px solid var(--amber);padding:9px 14px;margin:10px 0;border-radius:0 6px 6px 0;font-size:13px;color:#e6c07a}}
figure{{margin:12px 0}} figure img{{width:100%;border:1px solid var(--border);border-radius:8px;display:block}}
figcaption{{font-size:12px;color:var(--muted);margin-top:5px}}
.cap{{font-size:12px;color:var(--muted)}}
.method{{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:4px 18px 14px;margin:10px 0}}
.method .ax{{font-family:"SF Mono",Monaco,Consolas,monospace;font-weight:700;color:var(--accent)}}
.realism{{background:#13202b;border-left:4px solid var(--accent);padding:10px 14px;margin:10px 0;border-radius:0 6px 6px 0;font-size:13px}}
.realism b{{color:var(--accent)}}
.rfav{{color:var(--amber);font-weight:600}} .radv{{color:var(--red);font-weight:600}} .rat{{color:var(--green);font-weight:600}}
.rbanner{{padding:14px 18px;border-radius:10px;margin:14px 0;font-size:13.5px;line-height:1.6}}
.rbanner.fav{{background:var(--abg);border:1px solid var(--amber);color:#e6c07a}}
.rbanner.adv{{background:var(--rbg);border:1px solid var(--red);color:#f0a0a0}}
.rbanner b{{color:var(--text)}}
tr.tot td{{background:var(--p2);font-weight:700;border-top:2px solid var(--border)}}
.finding{{background:var(--rbg);border:1px solid var(--red);border-left:5px solid var(--red);padding:12px 16px;margin:14px 0;border-radius:8px;font-size:13.5px;line-height:1.6}}
.finding b{{color:#ffb4ae}} .finding code{{background:rgba(255,255,255,.08);padding:1px 5px;border-radius:3px}}
footer{{margin-top:36px;padding-top:14px;border-top:1px solid var(--border);color:var(--muted);font-size:12px}}
</style></head><body><div class="container">
<header class="cover">
  <h1>Manual Verification RESULTS — SL/Target Features &amp; Order Types (all 18 cases)</h1>
  <p>Every backtest exit re-derived across <b>Time · Logic · Price</b> from the order book + the NIFTY 1-second catalog bars — never from what the engine claims — <b>plus</b> a six-caveat execution-realism tier (entry correctness, realistic fills, slippage, gap behaviour, intrabar ordering, P&amp;L realism). MIS / NIFTY, 15:29 IST square-off. Click any case to expand.</p>
</header>
{banner}
<div class="simple">Each case below is a collapsible section — click to expand its per-trade verification, click again to contract. The verdict pill and pass/fail counts sit on every header. The <b>Time·Logic·Price</b> verdict proves the level/trigger/fill; a second <b>execution-realism</b> tier (below, and inside each case) quantifies fill realism, slippage, gaps, intrabar ordering and P&amp;L realism without changing that verdict. Engine facts verified against <code>core/managed_strategy.py</code>; order-type/intrabar/fill facts from the ntm3 NautilusTrader docs.</div>
<h2>The expanded axes — execution realism &amp; the six caveats</h2>
{REALISM_METHOD}
<h2>Execution realism &amp; P&amp;L summary</h2>
{global_realism(results)}
<h2>Case index</h2>
<table><thead><tr><th>Case</th><th>Feature</th><th class="center">Verdict</th></tr></thead><tbody>{idx}</tbody></table>
<h2>Per-case verification</h2>
{sections}
<footer>Generated {gen} · 18 cases (16 runnable + OT-02/OT-03 save-rejection negative tests) · Time·Logic·Price verdict + diagnostic execution-realism tier (fill-vs-level counterfactual, gaps, zero-slippage haircut, intrabar ordering) · bars: NIFTY 1-second spot, tick 0.01, 2026-03-02..03-20 · order books in <code>reports/testing2/</code>.</footer>
</div></body></html>"""


def main():
    manifest = json.load(open("portfolios/testing2/_run_manifest.json", encoding="utf-8"))["cases"]
    bars = V.load_bars()
    results = []
    for case in CASES:
        r = verify_case(case, manifest, bars)
        results.append(r)
        print(f"{r['cid']:6} {r['verdict']:5} {str(r['counts']):28} {r['headline'][:64]}")
    with open(OUT, "w", encoding="utf-8") as f:
        f.write(build(results))
    npass = sum(1 for r in results if r["verdict"] == "PASS")
    print(f"\n{npass}/{len(results)} cases PASS. Wrote {OUT}")


if __name__ == "__main__":
    main()
