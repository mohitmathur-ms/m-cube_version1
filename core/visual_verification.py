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

import hashlib
import re

import pandas as pd

from core.nautilus_loader import load_catalog

IST = "Asia/Kolkata"
TICK_DEFAULT = 0.01

# "SL=24733.05" / "TP=24812.5" / "TGT=..." / "TARGET=..." — the engine's logged
# exit level. SL exits log "SL="; Take-Profit exits log "TP=".
_LEVEL_RE = re.compile(r"\b(SL|TGT|TP|TARGET)\s*=\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
# "... price=24864.65 ..." — the bar extreme that the engine detected the breach on
# (managed_strategy logs the trigger price, e.g. bar HIGH for a short SL).
_PRICE_RE = re.compile(r"\bprice\s*=\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)


def parse_reason_price(detailed_reason):
    """Return the float logged as ``price=`` in an EXIT DETAILED REASON, or None."""
    if not detailed_reason:
        return None
    m = _PRICE_RE.search(str(detailed_reason))
    return float(m.group(1)) if m else None


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


def _slot_ec_index(config: dict | None) -> dict:
    """Map the order-book ``SLOT_ID`` token -> that slot's ``exit_config`` dict.

    The saved order book stamps each row with ``SLOT_ID = md5(slot_id)[:8]``
    (server.py builds the report labels this way), so we re-derive the same token
    from each config slot to recover its per-leg SL settings. Returns ``{}`` when
    there's no config (uploaded CSV / non-portfolio order book) -> generic checks.
    """
    out: dict[str, dict] = {}
    if not config:
        return out
    for s in (config.get("slots") or []):
        sid = s.get("slot_id")
        if sid is None:
            continue
        token = hashlib.md5(str(sid).encode("utf-8")).hexdigest()[:8]
        out[token] = s.get("exit_config") or {}
    return out


def _snap(price: float, tick: float) -> float:
    """Snap to the instrument tick grid, mirroring ManagedExitStrategy._snap_to_tick."""
    return round(price / tick) * tick if (tick and tick > 0 and price > 0) else price


def _initial_sl(entry_px: float, is_long: bool, ec: dict, tick: float) -> float:
    """Initial leg SL at fill, mirroring managed_strategy._calculate_sl_tp.

    percentage/trailing -> entry × (1 ∓ value/100); points -> entry ∓ value;
    none/atr -> 0.0 (not statically reconstructable -> caller falls back).
    """
    t = ec.get("stop_loss_type", "none")
    v = float(ec.get("stop_loss_value", 0) or 0)
    if t in ("percentage", "trailing"):
        return _snap(entry_px * (1 - v / 100) if is_long else entry_px * (1 + v / 100), tick)
    if t == "points":
        return _snap(entry_px - v if is_long else entry_px + v, tick)
    return 0.0


def _initial_tp(entry_px: float, is_long: bool, ec: dict, tick: float) -> float:
    """Initial leg Target at fill, mirroring managed_strategy (`current_tp`).

    percentage -> entry × (1 ± value/100); points -> entry ± value (LONG above /
    SHORT below entry); none/atr -> 0.0 (not statically reconstructable).
    """
    t = ec.get("target_type", "none")
    v = float(ec.get("target_value", 0) or 0)
    if t == "percentage":
        return _snap(entry_px * (1 + v / 100) if is_long else entry_px * (1 - v / 100), tick)
    if t == "points":
        return _snap(entry_px + v if is_long else entry_px - v, tick)
    return 0.0


def _build_recipe(kind: str, is_long: bool, entry_px: float, level: float,
                  ec: dict | None, tick: float, half: float,
                  trig, fill: float, reason_price) -> dict:
    """Per-row, two-step SL/TP verification narrative (the manual recipe on screen).

    ``check1`` (Config -> level): recompute the leg's level from its JSON config
    (percentage/points) and compare to the engine's logged ``SL=``/``TP=``. Only
    decidable for STATIC legs — trailing/atr/move-SL/target-trail levels move
    per-bar, so ``ok`` is ``None`` with an explanatory note (matching the recipe's
    own footnote). ``check2`` (level -> bar): the trigger bar's adverse extreme
    breached the level, the reason's ``price=`` equals that extreme, and the fill
    (AVG EXIT PRICE) equals the bar CLOSE.
    """
    is_sl = kind == "SL"
    # ── Check 1: Config -> level ────────────────────────────────────────────
    if not ec:
        c1 = {"ok": None, "logged": level,
              "note": "no portfolio config — cannot recompute the level from JSON "
                      "(upload a saved portfolio order book for Check 1)"}
    else:
        if is_sl:
            t = ec.get("stop_loss_type", "none")
            v = float(ec.get("stop_loss_value", 0) or 0)
            moves = bool(ec.get("move_sl_enabled") or ec.get("move_sl_trail_after"))
            moves_lbl = "Move-SL-to-Cost"
        else:
            t = ec.get("target_type", "none")
            v = float(ec.get("target_value", 0) or 0)
            moves = bool(ec.get("tgt_trail_enabled"))
            moves_lbl = "Target-trailing"
        if t == "trailing":
            c1 = {"ok": None, "logged": level, "type": t,
                  "note": "trailing stop ratchets per bar — Check 1 needs the bar series, not just entry price"}
        elif t not in ("percentage", "points"):
            c1 = {"ok": None, "logged": level, "type": t,
                  "note": f"{t or 'none'} {kind} — Check 1 needs the bar series (ATR computed at entry / no level)"}
        elif moves:
            c1 = {"ok": None, "logged": level, "type": t,
                  "note": f"{moves_lbl} active — the level moves per bar; Check 1 needs the bar series"}
        else:
            computed = _initial_sl(entry_px, is_long, ec, tick) if is_sl else _initial_tp(entry_px, is_long, ec, tick)
            use_minus = (is_sl == is_long)   # SL-long / TP-short subtract; SL-short / TP-long add
            if t == "percentage":
                op = "−" if use_minus else "+"
                formula = f"{entry_px:.2f} × (1 {op} {v:g}/100) = {computed:.2f}"
            else:  # points
                op = "−" if use_minus else "+"
                formula = f"{entry_px:.2f} {op} {v:g} = {computed:.2f}"
            ok = level is not None and abs(computed - level) <= max(half, 5e-3)
            c1 = {"ok": bool(ok), "type": t, "value": v, "formula": formula,
                  "computed": computed, "logged": level,
                  "label": "SL" if is_sl else "TP"}

    # ── Check 2: level -> actual bar ────────────────────────────────────────
    use_high = (is_sl and not is_long) or (not is_sl and is_long)
    c2: dict = {"side_rule": ("high ≥ " if use_high else "low ≤ ") + "level",
                "extreme_label": "high" if use_high else "low",
                "level": level, "reason_price": reason_price, "fill": fill}
    if trig is not None:
        o, h, low_, close = float(trig["open"]), float(trig["high"]), float(trig["low"]), float(trig["close"])
        extreme = h if use_high else low_
        breach = (extreme >= level - 1e-9) if use_high else (extreme <= level + 1e-9)
        c2.update({
            "ohlc": {"open": o, "high": h, "low": low_, "close": close},
            "extreme_val": extreme,
            "breach": bool(breach),
            "bar_close": close,
            "fill_ok": fill == fill and abs(close - fill) <= half,
            "reason_price_ok": (reason_price is not None and abs(reason_price - extreme) <= half)
                               if reason_price is not None else None,
        })
    else:
        c2.update({"ohlc": None, "extreme_val": None, "breach": None,
                   "bar_close": None, "fill_ok": None, "reason_price_ok": None})
    return {"check1": c1, "check2": c2}


def _reconstructable(ec: dict | None) -> bool:
    """True when we can faithfully replay this leg's SL state machine.

    We model the initial SL (percentage/points/trailing) plus the pure
    profit-%-driven trailing ratchet and the SL-Wait gate. We deliberately do
    NOT model Move-SL-to-Cost or Target-Lock (extra timing/flag state); legs
    using those fall back to the static first-touch check.
    """
    if not ec:
        return False
    if ec.get("stop_loss_type") not in ("percentage", "points", "trailing"):
        return False
    if ec.get("move_sl_enabled") or ec.get("move_sl_trail_after"):
        return False
    if (ec.get("target_lock_trigger") or 0) and (ec.get("target_lock_minimum") or 0):
        return False
    return True


def _reconstruct_sl_hit(bars: pd.DataFrame, entry, exit_, entry_px: float,
                        is_long: bool, ec: dict, tick: float):
    """Replay the engine's per-bar SL (initial + trailing ratchet + SL-Wait) and
    return the timestamp of the first CONFIRMED hit in ``(entry, exit]``, or None.

    Faithful to core/managed_strategy.py: profit_pct uses each bar's CLOSE;
    ``highest_profit`` ratchets the trailing SL onto the profit side by
    ``trailing_sl_offset`` per ``trailing_sl_step`` of gain (tighten-only); the
    SL fires intrabar (long: low<=SL; short: high>=SL); SL-Wait (sec preferred,
    else bars) delays the fire until the level has held. A fixed (non-trailing)
    leg reduces to a constant SL == the logged level, so this matches the static
    check when no wait/trail is in play.
    """
    if entry_px != entry_px or entry_px <= 0:          # NaN / unusable entry
        return None
    cur = _initial_sl(entry_px, is_long, ec, tick)
    window = bars[(bars.index > entry) & (bars.index <= exit_)]
    if window.empty:
        return None
    is_trailing = ec.get("stop_loss_type") == "trailing"
    step = float(ec.get("trailing_sl_step", 0) or 0)
    off = float(ec.get("trailing_sl_offset", 0) or 0)
    wait_sec = int(ec.get("sl_wait_sec", 0) or 0)
    wait_bars = int(ec.get("sl_wait_bars", 0) or 0)
    hp = 0.0
    wait_count = 0
    wait_start = None
    for ts, row in window.iterrows():
        close, low, high = float(row["close"]), float(row["low"]), float(row["high"])
        profit = ((close - entry_px) / entry_px * 100) if is_long else ((entry_px - close) / entry_px * 100)
        if profit > hp:
            hp = profit
        if is_trailing and step > 0:
            steps = int(hp / step)
            if steps > 0:
                # Negative offset -> SL onto the profit side (matches engine).
                trail = (_snap(entry_px * (1 + steps * off / 100), tick) if is_long
                         else _snap(entry_px * (1 - steps * off / 100), tick))
                if is_long and trail > cur:
                    cur = trail
                elif (not is_long) and (cur == 0 or trail < cur):
                    cur = trail
        if cur <= 0:
            wait_count, wait_start = 0, None
            continue
        hit = (low <= cur + 1e-9) if is_long else (high >= cur - 1e-9)
        if hit:
            if wait_sec > 0:
                if wait_start is None:
                    wait_start = ts
                if (ts - wait_start).total_seconds() < wait_sec:
                    continue
            elif wait_bars > 0:
                wait_count += 1
                if wait_count < wait_bars:
                    continue
            return ts
        wait_count, wait_start = 0, None
    return None


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
    # Per-feature parity: recover each leg's exit_config (keyed by the order
    # book's SLOT_ID token) so the TIME axis can replay a MOVING stop loss
    # (trailing ratchet + SL-Wait) instead of testing the single final logged
    # level against the whole hold — which false-fails trailing exits whose stop
    # ratcheted onto the profit side. Empty for uploaded/non-portfolio books.
    ec_index = _slot_ec_index(config)
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
            "config_ok": None, "recipe": None,
            "entry_ok": None, "realism": None,
        }

        if kind and level is not None:
            # Which side of the bar must reach the level (SL vs Target × side).
            use_high = (kind == "SL" and not is_long) or (kind == "TGT" and is_long)
            rd["kind_label"] = "Stop Loss" if kind == "SL" else "Take Profit"
            # Recover this leg's exit_config (keyed by the SLOT_ID token) up front:
            # the per-row recipe's Check 1 (Config -> level) needs only entry+config,
            # and the SL time-reconstruction below reuses it.
            ec = ec_index.get(str(r.get("SLOT_ID", ""))) if ec_index else None
            reason_price = parse_reason_price(r.get("EXIT DETAILED REASON"))
            reached = None
            if have_bars and trig is not None:
                ft = first_touch(bars, entry, exit_, level, use_high)
                reached = (trig["high"] >= level - 1e-9) if use_high else (trig["low"] <= level + 1e-9)
                rd["first_touch"] = ft.strftime("%Y-%m-%d %H:%M:%S") if ft is not None else None
                rd["time_ok"] = ft is not None and ft == exit_
                rd["price_ok"] = bool(reached) and fill_ok
                # Config-driven TIME parity for MOVING stops. The static check
                # above tests the single final logged level against the whole
                # hold, which false-fails a trailing SL whose level ratcheted
                # onto the profit side (every early bar already "touches" it) and
                # any SL-Wait gate. When the leg's exit_config is recoverable and
                # faithfully replayable, re-derive the true first CONFIRMED hit by
                # replaying the per-bar SL state machine instead.
                if kind == "SL" and _reconstructable(ec):
                    ft_dyn = _reconstruct_sl_hit(bars, entry, exit_, rd["F"],
                                                 is_long, ec, tick)
                    if ft_dyn is not None:   # else: keep the static result
                        rd["first_touch"] = ft_dyn.strftime("%Y-%m-%d %H:%M:%S")
                        rd["time_ok"] = ft_dyn == exit_
                        if int(ec.get("sl_wait_bars", 0) or 0) or int(ec.get("sl_wait_sec", 0) or 0):
                            rd["wait_bars"] = int(ec.get("sl_wait_bars", 0) or 0)
                # Sub-second round trip: entry and exit land on the SAME 1-second
                # feed bar (e.g. a ReEntry LIMIT that fills and is stopped within
                # one bar), so the touch bar can't be strictly after entry and the
                # TIME axis is unresolvable on this feed. Mark it not-checkable
                # rather than failing it; LOGIC/PRICE still apply.
                if exit_ is not pd.NaT and entry is not pd.NaT and exit_ <= entry:
                    rd["time_ok"] = None
                    rd["first_touch"] = exit_.strftime("%Y-%m-%d %H:%M:%S")
            # A labeled level-hit (incl. "Reverse on SL", "Portfolio Stoploss") is
            # logically valid when the trigger bar genuinely reached the level.
            rd["logic_ok"] = rl in ("stop loss", "target", "take profit") or bool(reached)
            # Two-step recipe (Check 1 Config->level, Check 2 level->bar). Check 1's
            # verdict gates the trade only when statically recomputable (else None,
            # ignored by _verdict) — so uploaded CSVs / trailing-ATR legs are unaffected.
            rd["recipe"] = _build_recipe(kind, is_long, rd["F"], level, ec, tick, half,
                                         trig if have_bars else None, fill, reason_price)
            rd["config_ok"] = rd["recipe"]["check1"]["ok"]
            rd["verdict"] = _verdict([rd["time_ok"], rd["logic_ok"], rd["price_ok"], rd["config_ok"]])
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
