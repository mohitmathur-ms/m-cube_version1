"""Conservative exit-fill models: session-cumulative VWAP re-pricing and the
directional bid/ask close-fill model for SL/TP leg exits."""

from __future__ import annotations

import pandas as pd
from core.fx_rates import parse_money_string
from core.venue_config import load_adapter_config_for_bar_type

from core.backtest_runner.report_utils import _pick_col


# ─── VWAP fill model (spec §4.2 / §3 / 5. Logics/sl_tgt.html) ────────────────
#
# Enabled per-portfolio (vwap_exit_fill) or via the _USE_VWAP_FILL env flag.
# When on, SL/Target exit fills are repriced to the conservative VWAP fill
# described in "5. Logics/sl_tgt.html" — exit_price = vwap if vwap > hit_price
# else hit_price. ``vwap`` is the spec §3 session-cumulative volume-weighted
# VWAP, Σ((H+L+C)/3·volume)/Σ(volume) over the trading session (reset at the
# venue's session_start_time, UTC), computed in _build_vwap_lookup from each
# bar's volume. Where a session has no volume yet (zero/absent volume) it
# degrades to the bar's typical price (H+L+C)/3. NOTE: per the spec volume
# caveat, FX/commodity volume is broker quote-size, so the VWAP weights quote
# depth there; crypto/index-futures feeds carry true traded volume. All of this
# is runner-side post-run report surgery (same idiom as _apply_portfolio_clip)
# — the engine and ManagedExitStrategy are untouched.

# Close-order tag prefixes that mark an SL / Target exit (set by
# ManagedExitStrategy._handle_exit). Squareoff and entry fills are excluded.
_VWAP_SL_PREFIXES = ("Stop Loss", "Trailing SL", "Reverse on SL")
_VWAP_TP_PREFIXES = ("Take Profit", "Reverse on TP")


def _session_start_minute(bar_type_str: str) -> int:
    """Minute-of-day (UTC) the venue's trading session starts — the VWAP reset
    boundary. Read from the venue's adapter config ``session_start_time``
    ("HH:MM[:SS]" UTC, ``adapter_admin/adapters_config/<venue>.json``). Defaults
    to 0 (midnight-UTC daily reset) when no config / unparseable.
    """
    try:
        cfg = load_adapter_config_for_bar_type(bar_type_str) or {}
        s = str(cfg.get("session_start_time") or "").strip()
        if not s:
            return 0
        parts = s.split(":")
        hh = int(parts[0])
        mm = int(parts[1]) if len(parts) > 1 else 0
        return (hh % 24) * 60 + (mm % 60)
    except Exception:
        return 0


def _vwap_session_bucket(ts_ns: int, session_start_min: int) -> int:
    """Trading-session id for ``ts_ns`` (UTC), rolling at ``session_start_min``.

    Bars in the same session share a bucket; the VWAP cumulation resets when the
    bucket changes. ``session_start_min`` shifts the daily boundary off midnight
    (e.g. an FX week starting 22:00 UTC → session_start_min = 1320).
    """
    total_min = ts_ns // 60_000_000_000  # ns → whole UTC minutes since epoch
    return (total_min - session_start_min) // 1440


def _build_vwap_lookup(bars, session_start_min: int = 0) -> dict | None:
    """Index bars by ts_event with the spec session-cumulative VWAP.

    Returns ``{"ask": {ts_ns: (vwap, high, low)}, "bid": {...}, "single": {...}}``.
    ``vwap`` is the running volume-weighted typical price over the trading
    session, per spec §3:

        VWAP_N = Σ((H+L+C)/3 · volume) / Σ(volume)      (session start … bar N)

    i.e. the same ``Σ(typical·vol)/Σ(vol)`` cumulation as ``aggregator._vwap``,
    reset at the venue's ``session_start_min`` (UTC). Where no volume has yet
    accumulated in the session (zero/absent volume) it falls back to the bar's
    typical price.

    ``ask``/``bid`` index the FX bid/ask trigger series (Format A); ``single``
    indexes the slot's own primary series (MID / LAST / OHLCV — any non-bid/ask
    price type) for the Format B fill, so crypto / OHLCV-only slots (no bid/ask)
    still get a spec §4.2 Format B fill rather than nothing. Returns ``None``
    only when there are no usable bars at all.
    """
    sides: dict[str, list[tuple]] = {"ask": [], "bid": [], "single": []}
    for bar in bars:
        bt = str(bar.bar_type)
        if "-ASK-" in bt:
            key = "ask"
        elif "-BID-" in bt:
            key = "bid"
        else:
            key = "single"  # MID / LAST / OHLCV primary series → Format B
        try:
            h = float(bar.high)
            l = float(bar.low)
            c = float(bar.close)
            v = float(getattr(bar, "volume", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        sides[key].append((int(bar.ts_event), h, l, c, v))
    if not sides["ask"] and not sides["bid"] and not sides["single"]:
        return None

    def _running_vwap(rows: list[tuple]) -> dict[int, tuple]:
        out: dict[int, tuple] = {}
        rows.sort(key=lambda r: r[0])  # chronological — cumulation needs order
        cum_pv = 0.0
        cum_v = 0.0
        cur_bucket: int | None = None
        for ts, h, l, c, v in rows:
            bucket = _vwap_session_bucket(ts, session_start_min)
            if bucket != cur_bucket:  # new session — reset the cumulation
                cur_bucket = bucket
                cum_pv = 0.0
                cum_v = 0.0
            typical = (h + l + c) / 3.0
            cum_pv += typical * v
            cum_v += v
            vwap = (cum_pv / cum_v) if cum_v > 0 else typical
            out[ts] = (vwap, h, l)
        return out

    return {
        "ask": _running_vwap(sides["ask"]),
        "bid": _running_vwap(sides["bid"]),
        "single": _running_vwap(sides["single"]),
    }


def _vwap_normalize_tag(tg) -> str:
    """Unwrap a fills_report ``tags`` cell to its verbatim string.

    Nautilus stores tags as ``['Stop Loss: …']``; mirror report_generator's
    _normalize_tags so the prefix match sees the raw reason.
    """
    if isinstance(tg, (list, tuple)):
        return str(tg[0]) if tg else ""
    if tg is None:
        return ""
    return str(tg)


def _vwap_ts_to_ns(raw) -> int:
    """Best-effort conversion of a report timestamp cell to UTC nanoseconds.

    Handles nanosecond ints, pandas.Timestamp and ISO strings identically so
    a positions_report ``ts_closed`` matches a _build_vwap_lookup key (which
    is a raw ``bar.ts_event`` int)."""
    if raw is None:
        return 0
    try:
        if isinstance(raw, float) and pd.isna(raw):
            return 0
    except Exception:
        pass
    try:
        ts = pd.Timestamp(raw)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return int(ts.value)
    except Exception:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0


def _vwap_row_is_long(report, idx, sqty_col, entry_col, side_col):
    """Return True/False for a positions_report row's direction, or None.

    Prefers ``signed_qty`` (positive = long), then the ``entry`` order side,
    then the ``side`` position-side column."""
    if sqty_col:
        try:
            v = float(report.at[idx, sqty_col])
            if v > 0:
                return True
            if v < 0:
                return False
        except (TypeError, ValueError):
            pass
    if entry_col:
        e = str(report.at[idx, entry_col]).upper()
        if "BUY" in e:
            return True
        if "SELL" in e:
            return False
    if side_col:
        s = str(report.at[idx, side_col]).upper()
        if "LONG" in s:
            return True
        if "SHORT" in s:
            return False
    return None


def _vwap_adjust_pnl_cell(report, idx, pnl_col, delta: float) -> None:
    """Add ``delta`` (quote currency) to a realized_pnl cell, preserving its
    ``"<amount> <ccy>"`` Money-string shape so downstream parsing is unchanged."""
    amount, ccy = parse_money_string(report.at[idx, pnl_col])
    new_amount = amount + delta
    report.at[idx, pnl_col] = f"{new_amount} {ccy}" if ccy else new_amount


def _apply_vwap_fill(positions_report, fills_report, vwap_lookup) -> int:
    """Reprice SL/Target exit fills to the conservative VWAP price.

    Spec §4.2: SELL legs use ``MAX(vwap, hit)`` (conservative, pay more); BUY
    legs use ``MIN(vwap, hit)`` (conservative, receive less). ``vwap`` is the
    **session-cumulative volume-weighted VWAP** (spec §3, ``Σ(typical·vol)/Σ(vol)``
    reset per session — see ``_build_vwap_lookup``).

    Format A (Bid/Ask) when the bar has paired bid+ask:
      * BUY  leg (close long, sell at BID): ``MIN(bid_vwap, hit)``,
        hit = ask_low (SL) / ask_high (Target)
      * SELL leg (close short, buy at ASK): ``MAX(ask_vwap, hit)``,
        hit = bid_high (SL) / bid_low (Target)

    Format B (single OHLCV/LAST/MID series) when there is no bid/ask
    (crypto / OHLCV-only slots) — vwap and hit both from the slot's own series:
      * BUY  leg: ``MIN(vwap, hit)``, hit = low (SL) / high (Target)
      * SELL leg: ``MAX(vwap, hit)``, hit = high (SL) / low (Target)

    Format C (LTP): no bid/ask and no volume series → no-op (fill stays the
    engine's last price). Mutates ``positions_report``'s realized_pnl column in
    place (quote currency); the caller's FX conversion / metric code picks the
    change up. Returns the number of positions adjusted.
    """
    if not vwap_lookup or positions_report is None or fills_report is None:
        return 0
    if positions_report.empty or fills_report.empty:
        return 0
    pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"])
    ts_col = _pick_col(positions_report, ["ts_closed", "ts_last"])
    close_col = _pick_col(positions_report, ["avg_px_close", "AvgPxClose", "avg_close"])
    if not pnl_col or not ts_col or not close_col:
        return 0

    # Map exit timestamp -> "sl" | "tp" from the close fills' structured tags.
    fill_ts_col = _pick_col(fills_report, ["ts_init", "ts_last", "ts_event"])
    tags_col = _pick_col(fills_report, ["tags", "Tags"])
    if not fill_ts_col or not tags_col:
        return 0
    exit_kind: dict[int, str] = {}
    for ts_raw, tg in zip(fills_report[fill_ts_col].tolist(),
                          fills_report[tags_col].tolist()):
        reason = _vwap_normalize_tag(tg).strip()
        if not reason:
            continue
        if reason.startswith(_VWAP_SL_PREFIXES):
            kind = "sl"
        elif reason.startswith(_VWAP_TP_PREFIXES):
            kind = "tp"
        else:
            continue
        ts_ns = _vwap_ts_to_ns(ts_raw)
        if ts_ns:
            exit_kind[ts_ns] = kind
    if not exit_kind:
        return 0

    ask = vwap_lookup.get("ask", {})
    bid = vwap_lookup.get("bid", {})
    single = vwap_lookup.get("single", {})
    qty_col = _pick_col(positions_report, ["peak_qty", "quantity", "Quantity"])
    sqty_col = _pick_col(positions_report, ["signed_qty", "SignedQty"])
    entry_col = _pick_col(positions_report, ["entry", "Entry"])
    side_col = _pick_col(positions_report, ["side", "Side"])

    adjusted = 0
    for idx in positions_report.index:
        ts_ns = _vwap_ts_to_ns(positions_report.at[idx, ts_col])
        kind = exit_kind.get(ts_ns)
        if kind is None:
            continue
        was_long = _vwap_row_is_long(positions_report, idx, sqty_col, entry_col, side_col)
        if was_long is None:
            continue
        try:
            actual_px = float(positions_report.at[idx, close_col])
            qty = abs(float(positions_report.at[idx, qty_col])) if qty_col else 0.0
        except (TypeError, ValueError):
            continue
        if qty <= 0 or actual_px <= 0:
            continue

        a = ask.get(ts_ns)
        b = bid.get(ts_ns)
        if a is not None and b is not None:
            # Format A (Bid/Ask) — spec §4.2.
            ask_vwap, ask_hi, ask_lo = a
            bid_vwap, bid_hi, bid_lo = b
            if was_long:
                # BUY leg: close a long by SELLING at the BID → MIN(bid_vwap, hit),
                # conservative (receive less). hit = ask_low (SL) / ask_high (TGT).
                vwap = bid_vwap
                hit = ask_lo if kind == "sl" else ask_hi
                exit_px = vwap if vwap < hit else hit          # MIN
            else:
                # SELL leg: close a short by BUYING at the ASK → MAX(ask_vwap, hit),
                # conservative (pay more). hit = bid_high (SL) / bid_low (TGT).
                vwap = ask_vwap
                hit = bid_hi if kind == "sl" else bid_lo
                exit_px = vwap if vwap > hit else hit          # MAX
        else:
            # Format B (single OHLCV/LAST/MID series) — spec §4.2. Used when the
            # slot has no paired bid/ask (crypto / OHLCV-only). vwap and the
            # hit (own bar high/low) come from the same single series.
            s = single.get(ts_ns)
            if s is None:
                continue  # no series for this bar — leave the engine fill
            s_vwap, s_hi, s_lo = s
            if was_long:
                # BUY leg: MIN(vwap, hit). hit = low (SL) / high (TGT).
                hit = s_lo if kind == "sl" else s_hi
                exit_px = s_vwap if s_vwap < hit else hit      # MIN
            else:
                # SELL leg: MAX(vwap, hit). hit = high (SL) / low (TGT).
                hit = s_hi if kind == "sl" else s_lo
                exit_px = s_vwap if s_vwap > hit else hit      # MAX
        # Long pnl rises with the exit price; short pnl falls with it.
        delta = (exit_px - actual_px) * qty if was_long else (actual_px - exit_px) * qty
        if delta == 0.0:
            continue
        _vwap_adjust_pnl_cell(positions_report, idx, pnl_col, delta)
        adjusted += 1
    return adjusted


# ─── Directional-close exit-fill model (spec execution_logic.html §8.1) ──────
#
# Gated by the per-portfolio ``directional_close_fill`` toggle (or the
# ``_USE_DIRECTIONAL_FILL`` env override). Under MARKET exits the spec uses the
# *directional close* as the fill base price, with a fallback chain:
#
#   SELL leg (close short → buy back):  ask_close → close → last_mtm → entry
#   BUY  leg (close long  → sell out):  bid_close → close → last_mtm → entry
#
# Synth-MID FX slots otherwise fill at the MID close, which understates the
# half-spread paid on exit. This post-run surgery reprices each closed
# position's exit to the directional bid/ask close at the exit bar, modelling
# that spread. When the bid/ask close is missing for a bar the engine's own
# fill (the MID ``close`` rung of the chain) is kept — the deeper ``last_mtm``
# / ``entry`` rungs are degenerate in a bar backtest and collapse to "leave
# as-is". Mutually exclusive with the VWAP fill at the orchestrator level
# (VWAP is the richer SL/Target model and wins when both are configured).


def _build_close_lookup(bars) -> dict | None:
    """Index ASK/BID bar *closes* by ts_event for the directional-close fill.

    Returns ``{"ask": {ts_ns: ask_close}, "bid": {ts_ns: bid_close}}`` or
    ``None`` when no ASK/BID bars are present (LAST / crypto-only slots), in
    which case the directional-close fill is not applicable.
    """
    ask: dict[int, float] = {}
    bid: dict[int, float] = {}
    for bar in bars:
        bt = str(bar.bar_type)
        if "-ASK-" in bt:
            side = ask
        elif "-BID-" in bt:
            side = bid
        else:
            continue
        try:
            side[int(bar.ts_event)] = float(bar.close)
        except (TypeError, ValueError):
            continue
    if not ask and not bid:
        return None
    return {"ask": ask, "bid": bid}


def _apply_directional_close_fill(
    positions_report, close_lookup, fills_report=None, vwap_active: bool = False,
) -> int:
    """Reprice exit fills to the spec §8.1 directional close base price.

    Per §8.1 the directional close is the base price for **every** MARKET exit
    (Format A: a LONG leg closes on the BID close, a SHORT leg on the ASK
    close; Format B/C: the bare close/ltp, which is what the engine already
    filled — a no-op here, so only Format A repricing happens). When the
    quote-side close is unavailable for the bar the engine's fill is left
    untouched (the ``close`` rung of the spec fallback chain; the deeper
    ``last_mtm``/``entry`` rungs are degenerate in a bar backtest).

    This **composes** with the VWAP fill rather than replacing it. The spec
    layers the VWAP enhancement on top of the directional close for leg
    SL/Target hits only (§4.2 anchors that on the trigger extreme; §8.1's
    Portfolio-SqOff enhancement anchors on the directional close). In this
    per-slot engine SL/Target exits are leg-level, so they are owned by
    ``_apply_vwap_fill`` (§4.2). Therefore when ``vwap_active`` is True this
    function **skips** SL/Target-tagged exits (VWAP already repriced them) and
    applies the directional close only to the remaining exits — squareoff,
    end-of-day and other time-based closes, which §8.1 fills at the directional
    close with no VWAP enhancement. When ``vwap_active`` is False the
    directional close is the base for all exits including SL/Target.

    Mutates ``positions_report``'s realized_pnl column in place (quote
    currency); returns the number of positions repriced.
    """
    if not close_lookup or positions_report is None or positions_report.empty:
        return 0
    pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"])
    ts_col = _pick_col(positions_report, ["ts_closed", "ts_last"])
    close_col = _pick_col(positions_report, ["avg_px_close", "AvgPxClose", "avg_close"])
    if not pnl_col or not ts_col or not close_col:
        return 0

    # When the VWAP fill is active it owns the leg SL/Target fills (§4.2);
    # collect their exit timestamps so the directional close skips them and the
    # two models stay on disjoint exit sets (no double repricing).
    sl_tp_ts: set[int] = set()
    if vwap_active and fills_report is not None and not fills_report.empty:
        fill_ts_col = _pick_col(fills_report, ["ts_init", "ts_last", "ts_event"])
        tags_col = _pick_col(fills_report, ["tags", "Tags"])
        if fill_ts_col and tags_col:
            for ts_raw, tg in zip(fills_report[fill_ts_col].tolist(),
                                  fills_report[tags_col].tolist()):
                reason = _vwap_normalize_tag(tg).strip()
                if reason.startswith(_VWAP_SL_PREFIXES) or reason.startswith(_VWAP_TP_PREFIXES):
                    ts_ns = _vwap_ts_to_ns(ts_raw)
                    if ts_ns:
                        sl_tp_ts.add(ts_ns)

    ask = close_lookup.get("ask", {})
    bid = close_lookup.get("bid", {})
    qty_col = _pick_col(positions_report, ["peak_qty", "quantity", "Quantity"])
    sqty_col = _pick_col(positions_report, ["signed_qty", "SignedQty"])
    entry_col = _pick_col(positions_report, ["entry", "Entry"])
    side_col = _pick_col(positions_report, ["side", "Side"])

    adjusted = 0
    for idx in positions_report.index:
        ts_ns = _vwap_ts_to_ns(positions_report.at[idx, ts_col])
        if not ts_ns:
            continue
        if ts_ns in sl_tp_ts:
            continue  # VWAP fill (§4.2) owns this leg SL/Target exit
        was_long = _vwap_row_is_long(positions_report, idx, sqty_col, entry_col, side_col)
        if was_long is None:
            continue
        # Directional close: long closes on the BID, short on the ASK.
        dir_close = bid.get(ts_ns) if was_long else ask.get(ts_ns)
        if dir_close is None:
            continue  # quote-side close missing — keep the engine's MID-close fill
        try:
            actual_px = float(positions_report.at[idx, close_col])
            qty = abs(float(positions_report.at[idx, qty_col])) if qty_col else 0.0
        except (TypeError, ValueError):
            continue
        if qty <= 0 or actual_px <= 0 or dir_close <= 0:
            continue
        # Long pnl rises with the exit price; short pnl falls with it.
        delta = (dir_close - actual_px) * qty if was_long else (actual_px - dir_close) * qty
        if delta == 0.0:
            continue
        _vwap_adjust_pnl_cell(positions_report, idx, pnl_col, delta)
        adjusted += 1
    return adjusted
