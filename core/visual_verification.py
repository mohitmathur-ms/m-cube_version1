"""Bar-driven verification of backtest exits for the "Visual Verification" UI tab.

Given a backtest's order-book rows + the 1-second feed from the catalog, this
re-derives each exit across three axes and reports a per-trade verdict:

  * TIME  — the first bar after entry (<= exit) whose low<=level (long) /
            high>=level (short) must equal the order book's exit second.
  * LOGIC — the EXIT REASON is the claimed one; exactly one exit per trade.
  * PRICE — the trigger bar actually reaches the engine's logged level, and the
            realised fill (AVG EXIT PRICE) equals that bar's CLOSE (trigger != fill,
            by design — a reduce-only MARKET close fills at the trigger bar close).

The exit level is read from the engine's own `EXIT DETAILED REASON`
("... SL=24733.05 ..." / "... TGT=24812.50 ..."), so no portfolio config is
needed. Square-off / EOD rows (no level) are checked on reason + fill==bar-close.

This is the live, config-free cousin of portfolios/testing2/_verify_lib.py; it
reads bars through the same `nautilus_loader.load_catalog` path the
`/api/data/bars` route uses, rather than decoding the parquet directly.
"""
from __future__ import annotations

import re

import pandas as pd

from core.nautilus_loader import load_catalog

IST = "Asia/Kolkata"
TICK_DEFAULT = 0.01

# "SL=24733.05" / "TP=24812.5" / "TGT=..." / "TARGET=..." — the engine's logged
# exit level. SL exits log "SL="; Take-Profit exits log "TP=".
_LEVEL_RE = re.compile(r"\b(SL|TGT|TP|TARGET)\s*=\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)


def parse_exit_level(detailed_reason) -> tuple[str | None, float | None]:
    """Return ("SL"|"TGT", level) parsed from an EXIT DETAILED REASON, or (None, None)."""
    if not detailed_reason:
        return None, None
    m = _LEVEL_RE.search(str(detailed_reason))
    if not m:
        return None, None
    kind = "SL" if m.group(1).upper() == "SL" else "TGT"
    return kind, float(m.group(2))


def _parse_ist(s):
    """Order-book times are IST wall-clock 'DD-MM-YYYY HH:MM:SS' (also accepts ISO)."""
    if s in (None, "") or (isinstance(s, float) and pd.isna(s)):
        return pd.NaT
    t = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.isna(t):
        return pd.NaT
    return t.tz_localize(IST) if t.tzinfo is None else t.tz_convert(IST)


def load_bars_ist(catalog_path: str, bar_type: str, start_ist, end_ist) -> pd.DataFrame:
    """Decode the catalog feed over [start, end] into an IST-indexed OHLC frame.

    Returns an empty frame (never raises) when the feed/range is absent, so the
    caller can degrade to 'no charts / verdicts N/A' gracefully.
    """
    cols = ["open", "high", "low", "close"]
    if not bar_type:
        return pd.DataFrame(columns=cols)
    try:
        catalog = load_catalog(catalog_path)
        bars = catalog.bars(
            bar_types=[bar_type],
            start=start_ist.tz_convert("UTC"),
            end=end_ist.tz_convert("UTC"),
        )
    except Exception:
        return pd.DataFrame(columns=cols)
    if not bars:
        return pd.DataFrame(columns=cols)
    idx = pd.to_datetime([b.ts_event for b in bars], utc=True).tz_convert(IST)
    df = pd.DataFrame({
        "open": [float(b.open) for b in bars],
        "high": [float(b.high) for b in bars],
        "low": [float(b.low) for b in bars],
        "close": [float(b.close) for b in bars],
    }, index=idx)
    return df.sort_index()


def first_touch(bars: pd.DataFrame, entry, exit_, level: float, use_high: bool):
    """First bar in (entry, exit] where the trigger crosses ``level``, or None.

    ``use_high`` picks the side of the bar that must reach the level:
      * SL on a LONG  -> price falls to it below  -> low<=level   (use_high=False)
      * SL on a SHORT -> price rises to it above  -> high>=level  (use_high=True)
      * TGT on a LONG -> price rises to it above  -> high>=level  (use_high=True)
      * TGT on a SHORT-> price falls to it below  -> low<=level   (use_high=False)
    """
    window = bars[(bars.index > entry) & (bars.index <= exit_)]
    if window.empty:
        return None
    hit = window[window["high"] >= level - 1e-9] if use_high else window[window["low"] <= level + 1e-9]
    return hit.index[0] if not hit.empty else None


def _bar_at(bars: pd.DataFrame, ts):
    if ts is None or ts is pd.NaT or ts not in bars.index:
        return None
    r = bars.loc[ts]
    return r.iloc[0] if isinstance(r, pd.DataFrame) else r


def analyze(rows: list[dict], catalog_path: str, bar_type: str,
            tick: float = TICK_DEFAULT, config: dict | None = None) -> tuple[list[dict], bool]:
    """Verify every order-book row across Time/Logic/Price.

    Returns ``(verdicts, feed_in_catalog)``. Each verdict carries the fields the
    UI needs to render the table AND the per-trade charts (entry/exit/level/
    first_touch). When the feed isn't in the catalog, bar-dependent checks are
    ``None`` and the verdict is ``"N/A"``.

    Each trade also carries a DIAGNOSTIC execution-realism payload (``entry_ok`` +
    a ``realism`` dict: gap / cross_session / fav_pts / favorable / adverse /
    level_pnl / infl / slip) that quantifies fill realism, gaps and the
    fill-at-level P&L counterfactual. It NEVER changes ``verdict`` (which stays on
    Time/Logic/Price). See ``_attach_realism``.

    ``config`` (the portfolio JSON dict, when the order book came from a saved
    portfolio) enables per-feature verdict parity with the static RESULTS report:
    an SL-Wait gate (``sl_wait_bars``) deliberately fires the exit *later* than
    first-touch, so the time axis is re-checked as "the level held for
    ``sl_wait_bars`` bars ending at the exit" instead of "first-touch == exit".
    With ``config=None`` the generic (stricter) check is used — no behaviour change.
    """
    half = tick / 2.0
    # Per-feature parity: the common SL-Wait bar count across enabled legs (only
    # applied when unambiguous — uniform across legs — else fall back to generic).
    sl_wait_bars = 0
    if config:
        waits = {int((s.get("exit_config") or {}).get("sl_wait_bars", 0) or 0)
                 for s in (config.get("slots") or []) if s.get("enabled", True)}
        waits.discard(0)
        if len(waits) == 1:
            sl_wait_bars = waits.pop()
    parsed = [(r, _parse_ist(r.get("ENTRY TIME")), _parse_ist(r.get("EXIT TIME"))) for r in rows]
    entries = [e for _, e, _ in parsed if e is not pd.NaT]
    exits = [x for _, _, x in parsed if x is not pd.NaT]

    bars = pd.DataFrame(columns=["open", "high", "low", "close"])
    if entries and exits:
        start = min(entries).normalize()
        end = max(exits).normalize() + pd.Timedelta(days=1)
        bars = load_bars_ist(catalog_path, bar_type, start, end)
    have_bars = not bars.empty

    out = []
    for r, entry, exit_ in parsed:
        is_long = str(r.get("TRANSACTION", "")).strip().upper() == "BUY"
        reason = str(r.get("EXIT REASON", "")).strip()
        rl = reason.lower()
        kind, level = parse_exit_level(r.get("EXIT DETAILED REASON"))

        def _f(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return float("nan")

        fill = _f(r.get("AVG EXIT PRICE"))
        trig = _bar_at(bars, exit_) if have_bars else None
        trig_close = float(trig["close"]) if trig is not None else None
        fill_ok = trig_close is not None and abs(trig_close - fill) <= half

        rd = {
            "oid": str(r.get("OrderID", "")),
            "side": "LONG" if is_long else "SHORT",
            "is_long": is_long,
            "symbol": str(r.get("SYMBOL", "")),
            "F": _f(r.get("ENTRY PRICE")),
            "fill": fill,
            "pnl": _f(r.get("PNL")),
            "reason": reason,
            "entry_ist": entry.strftime("%Y-%m-%d %H:%M:%S") if entry is not pd.NaT else "",
            "exit_ist": exit_.strftime("%Y-%m-%d %H:%M:%S") if exit_ is not pd.NaT else "",
            "kind": kind, "level": level,
            "trig_close": trig_close,
            "first_touch": None,
            "time_ok": None, "logic_ok": None, "price_ok": None,
            "entry_ok": None, "realism": None,
        }

        if kind and level is not None:
            # Which side of the bar must reach the level (SL vs Target × side).
            use_high = (kind == "SL" and not is_long) or (kind == "TGT" and is_long)
            rd["kind_label"] = "Stop Loss" if kind == "SL" else "Take Profit"
            reached = None
            if have_bars and trig is not None:
                ft = first_touch(bars, entry, exit_, level, use_high)
                reached = (trig["high"] >= level - 1e-9) if use_high else (trig["low"] <= level + 1e-9)
                rd["first_touch"] = ft.strftime("%Y-%m-%d %H:%M:%S") if ft is not None else None
                rd["time_ok"] = ft is not None and ft == exit_
                rd["price_ok"] = bool(reached) and fill_ok
                # SL-Wait parity (config-driven): the exit is HELD until the level
                # has stayed pierced for sl_wait_bars bars, so it fires LATER than
                # first-touch by design. Re-check the time axis as "the sl_wait_bars
                # bars ENDING at the exit are all pierced AND the exit is delayed".
                if sl_wait_bars and kind == "SL":
                    col = "low" if is_long else "high"
                    pos = bars.index.get_indexer([exit_])[0] if exit_ is not pd.NaT else -1
                    held = (bars.iloc[max(0, pos - sl_wait_bars + 1):pos + 1][col].tolist()
                            if pos >= 0 else [])
                    confirmed = len(held) == sl_wait_bars and all(
                        (v <= level + 1e-9) if is_long else (v >= level - 1e-9) for v in held)
                    delayed = ft is not None and exit_ > ft
                    rd["time_ok"] = bool(confirmed and delayed)
                    rd["wait_bars"] = sl_wait_bars
            # A labeled level-hit (incl. "Reverse on SL", "Portfolio Stoploss") is
            # logically valid when the trigger bar genuinely reached the level.
            rd["logic_ok"] = rl in ("stop loss", "target", "take profit") or bool(reached)
            rd["verdict"] = _verdict([rd["time_ok"], rd["logic_ok"], rd["price_ok"]])
        elif (rl in ("squareoff", "square off", "end of day", "time square off")
              or "portfolio" in rl):
            # Envelope close (daily MIS square-off OR a portfolio-level SL/Target
            # flatten): no per-leg level, but it still fills at the bar's close.
            rd["kind_label"] = "Portfolio exit" if "portfolio" in rl else "Squareoff"
            rd["logic_ok"] = True
            if have_bars:
                rd["time_ok"] = trig is not None          # fired on a real bar
                rd["price_ok"] = fill_ok                    # fill == that bar's close
            rd["verdict"] = _verdict([rd["time_ok"], rd["logic_ok"], rd["price_ok"]])
        else:
            rd["kind_label"] = reason or "—"
            rd["logic_ok"] = bool(reason)
            rd["verdict"] = "N/A"

        _attach_realism(rd, r, entry, bars, kind, level, fill, is_long, trig,
                        have_bars, half, tick, _f)
        out.append(rd)

    return out, have_bars


def _attach_realism(rd, r, entry, bars, kind, level, fill, is_long, trig,
                    have_bars, half, tick, _f):
    """Execution-realism tier — DIAGNOSTIC ONLY, never changes ``verdict``.

    Mirrors portfolios/testing2/_build_full_report.py: entry correctness, the
    fill-vs-level gap, intraday/overnight gaps, and the fill-at-level P&L
    counterfactual (what a native move-through stop would have given). All fields
    are additive; existing consumers ignore them.
    """
    F = rd["F"]
    # Entry correctness (D): the MARKET entry fills at the signal bar's CLOSE
    # (no next-bar-open look-ahead). Match F to the bar close at the entry second
    # or the one just before (the composite's ts can land on either edge).
    if have_bars and F == F and entry is not pd.NaT:
        cands = []
        for d in (0, 1):
            b = _bar_at(bars, entry - pd.Timedelta(seconds=d))
            if b is not None:
                cands.append(float(b["close"]))
        if cands:
            rd["entry_ok"] = any(abs(c - F) <= half for c in cands)

    if not (have_bars and kind and level is not None and trig is not None
            and fill == fill and F == F):
        return
    o = float(trig["open"])
    use_high = (kind == "SL" and not is_long) or (kind == "TGT" and is_long)
    gap = (o >= level - 1e-9) if use_high else (o <= level + 1e-9)
    fav_pts = (fill - level) if is_long else (level - fill)   # +ve = favourable/inflating
    pts_rep = (fill - F) if is_long else (F - fill)
    pts_lvl = (level - F) if is_long else (F - level)
    qty, mult = _f(r.get("QUANTITY")), _f(r.get("MULTIPLIER"))
    qty = qty if (qty == qty and qty) else 1.0
    mult = mult if (mult == mult and mult) else 1.0
    pnl = rd["pnl"]
    # Back-derive value-per-point from the reported P&L so fx/multiplier scaling
    # cancels and the counterfactual stays apples-to-apples.
    per_pt = (pnl / pts_rep) if (abs(pts_rep) > 1e-9 and pnl == pnl and abs(pnl) > 1e-9) else (mult * qty)
    level_pnl = pts_lvl * per_pt
    infl = (pnl - level_pnl) if pnl == pnl else None
    cross = bool(rd["entry_ist"] and rd["exit_ist"] and rd["entry_ist"][:10] != rd["exit_ist"][:10])
    rd["realism"] = {
        "gap": bool(gap),
        "cross_session": cross,
        "fav_pts": fav_pts,
        "favorable": fav_pts > half,
        "adverse": fav_pts < -half,
        "level_pnl": level_pnl,
        "infl": infl,
        "slip": tick * mult * qty,
    }


def _verdict(checks: list) -> str:
    """PASS only if every APPLICABLE (non-None) check is True; FAIL if any is
    False; N/A when nothing could be checked (no bars)."""
    applicable = [c for c in checks if c is not None]
    if not applicable:
        return "N/A"
    return "PASS" if all(applicable) else "FAIL"


def guess_feed_bar_type(catalog_path: str, instrument: str) -> str:
    """Best feed bar_type for an instrument id (e.g. 'NIFTY_SPOT_YYYYINR.NIFTY_FUTURES_MS').

    Lists the catalog's bar dirs (their names ARE the bar_type strings), filters
    to this instrument, and prefers the finest resolution (SECOND > MINUTE >
    HOUR > DAY). Returns "" if none match.
    """
    from pathlib import Path
    bar_dir = Path(catalog_path) / "data" / "bar"
    if not bar_dir.exists() or not instrument:
        return ""
    prefix = f"{instrument}-"
    cands = [d.name for d in bar_dir.iterdir() if d.is_dir() and d.name.startswith(prefix)]
    if not cands:
        return ""
    order = {"SECOND": 0, "MINUTE": 1, "HOUR": 2, "DAY": 3, "WEEK": 4, "MONTH": 5}

    def rank(bt: str) -> int:
        for unit, score in order.items():
            if f"-{unit}-" in bt:
                return score
        return 9
    return sorted(cands, key=rank)[0]


def orderbook_rows_from_html(html: str) -> list[dict]:
    """Extract the embedded ORDERBOOK_DATA array from a portfolio HTML report."""
    import json
    m = re.search(r"const\s+ORDERBOOK_DATA\s*=\s*(\[.*?\])\s*;", html, re.DOTALL)
    if not m:
        return []
    try:
        return json.loads(m.group(1))
    except Exception:
        return []
