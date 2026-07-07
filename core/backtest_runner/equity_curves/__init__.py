"""Equity-curve primitives shared across the result layer: merge per-slot
curves into a combined-P&L timeline, build a curve from account snapshots,
and ensure a final end-of-run point."""

from __future__ import annotations

import pandas as pd


def _merge_equity_curves(curves: list[list[dict]]) -> list[dict]:
    """Merge multiple timestamped equity curves by summing balances at each timestamp.

    Each curve is a list of ``{"timestamp": iso_str, "balance": float}`` points.
    Output: one point per unique timestamp across all curves; the balance at
    each timestamp is the sum of every curve's most-recent balance at-or-before
    that timestamp (curves contribute 0.0 before their first point).
    """
    if not curves:
        return []
    if len(curves) == 1:
        return curves[0]

    # The dict-walk path here outperformed a pandas concat+ffill+sum
    # equivalent across every realistic input size (9-100 curves × 1k-10k
    # points): the vectorised version paid heavy concat/groupby/ffill
    # overhead that the small-N inner loop never recovered. Kept simple.
    all_timestamps = set()
    for curve in curves:
        for pt in curve:
            if pt.get("timestamp"):
                all_timestamps.add(pt["timestamp"])

    if not all_timestamps:
        return curves[0]

    sorted_ts = sorted(all_timestamps)

    # For each curve, build timestamp -> balance map with forward-fill
    curve_maps = []
    for curve in curves:
        ts_map = {}
        for pt in curve:
            if pt.get("timestamp"):
                ts_map[pt["timestamp"]] = pt["balance"]
        curve_maps.append(ts_map)

    # Merge: at each timestamp, sum the latest known balance from each curve
    merged = []
    last_balances = [0.0] * len(curves)
    for ts in sorted_ts:
        for i, ts_map in enumerate(curve_maps):
            if ts in ts_map:
                last_balances[i] = ts_map[ts]
        merged.append({"timestamp": ts, "balance": sum(last_balances)})

    return merged


def _build_equity_curve_from_account(accounts: list, starting_capital: float) -> list[dict]:
    """Build a timestamped equity curve from account state events.

    Returns list of {"timestamp": iso_str, "balance": float} dicts.
    """
    equity_curve_ts = [{"timestamp": None, "balance": starting_capital}]

    if not accounts:
        return equity_curve_ts

    account = accounts[0]
    try:
        events = account.events
    except Exception:
        return equity_curve_ts

    if not events:
        return equity_curve_ts

    # ts_iso -> index in `curve`, so dedup replacement is O(1) instead of an
    # O(n) reverse scan per duplicate.
    ts_to_idx: dict[str, int] = {}
    curve: list[dict] = []
    for event in events:
        ts_iso = pd.Timestamp(event.ts_event, unit="ns", tz="UTC").isoformat()
        # Hoisted try/except: failing `balances` iteration skips the event
        # without paying Python's per-iteration try-setup cost inside a sum.
        try:
            total = sum(float(bal.total) for bal in event.balances)
        except Exception:
            continue
        existing = ts_to_idx.get(ts_iso)
        if existing is not None:
            curve[existing]["balance"] = total
        else:
            ts_to_idx[ts_iso] = len(curve)
            curve.append({"timestamp": ts_iso, "balance": total})

    if curve:
        # Prepend starting point with the first event's timestamp if different
        if curve[0]["balance"] != starting_capital:
            first_ts = curve[0]["timestamp"]
            curve.insert(0, {"timestamp": first_ts, "balance": starting_capital})
        return curve

    return equity_curve_ts


def _ensure_final_equity_point(
    equity_curve_ts: list[dict],
    final_balance: float,
) -> None:
    """Append (or update) a final curve point matching `final_balance`.

    The engine-emitted equity curve reflects realized cash flow on the account,
    which misses FX-converted unrealized PnL. Stitch the corrected endpoint so
    downstream drawdown/return math sees the right terminal value.
    """
    if not equity_curve_ts:
        equity_curve_ts.append({"timestamp": None, "balance": float(final_balance)})
        return
    last = equity_curve_ts[-1]
    if abs(float(last.get("balance") or 0.0) - final_balance) < 1e-6:
        return
    equity_curve_ts.append({
        "timestamp": last.get("timestamp"),
        "balance": float(final_balance),
    })


def _build_mtm_equity_curve(
    positions_report,
    capital: float,
    bar_closes: list,
    base_pnl_fn,
) -> list[dict]:
    """Per-bar MARK-TO-MARKET equity curve for one slot (spec: portfolio SL/Target
    should monitor the *live* combined P&L every bar, not only realized P&L at
    position closes).

    For each bar, ``balance = capital + realized-so-far + unrealized-of-open-pos``.
    Each position's $/price-point is calibrated from its OWN realized P&L:
    ``ppp = realized / (avg_px_close - avg_px_open)`` — so the marked value equals
    realized exactly at the close (no discontinuity) and the multiplier / contract
    size / FX conversion are inherited automatically.

    Args:
      positions_report: per-position rows (avg_px_open/close, ts_opened/closed).
      capital: slot starting capital.
      bar_closes: sorted ``[(ts_ns:int, close:float)]`` for the slot's EXECUTION bars.
      base_pnl_fn: ``callable(row)->float`` giving a position's realized P&L in the
        account base currency.

    A point is emitted only while a position is OPEN (the curve is flat between
    trades — the merge carries the last value forward), which bounds the size and
    still captures every intraday breach. Returns ``[{"timestamp", "balance"}]``.
    Defensive: returns just the start point on any issue.
    """
    from core.backtest_runner.report_utils import _pick_col
    from core.backtest_runner.exit_fill import _vwap_ts_to_ns
    import pandas as _pd

    start = [{"timestamp": None, "balance": float(capital)}]
    try:
        if positions_report is None or positions_report.empty or not bar_closes:
            return start
        open_col = _pick_col(positions_report, ["ts_opened", "ts_init"])
        close_col = _pick_col(positions_report, ["ts_closed", "ts_last"])
        po_col = _pick_col(positions_report, ["avg_px_open", "AvgPxOpen", "avg_open"])
        pc_col = _pick_col(positions_report, ["avg_px_close", "AvgPxClose", "avg_close"])
        if not open_col or not close_col or not po_col or not pc_col:
            return start
        recs = []
        for idx in positions_report.index:
            o_ns = _vwap_ts_to_ns(positions_report.at[idx, open_col])
            c_ns = _vwap_ts_to_ns(positions_report.at[idx, close_col])
            try:
                avg_open = float(positions_report.at[idx, po_col])
                avg_close = float(positions_report.at[idx, pc_col])
            except (TypeError, ValueError):
                continue
            base_pnl = float(base_pnl_fn(positions_report.loc[idx]))
            denom = avg_close - avg_open
            ppp = (base_pnl / denom) if abs(denom) > 1e-12 else 0.0
            recs.append({"o": o_ns, "c": c_ns, "open": avg_open, "pnl": base_pnl, "ppp": ppp})
        if not recs:
            return start
        recs.sort(key=lambda r: r["o"])
        n = len(recs)
        ri = 0
        realized = 0.0
        out = list(start)
        for ts, close in bar_closes:
            # Book realized P&L of every position already closed at/before this bar.
            while ri < n and recs[ri]["c"] and recs[ri]["c"] <= ts:
                realized += recs[ri]["pnl"]
                ri += 1
            # Mark the open position (if any) to this bar's close.
            if ri < n:
                r = recs[ri]
                if r["o"] and r["o"] <= ts and (not r["c"] or ts < r["c"]):
                    unreal = (close - r["open"]) * r["ppp"]
                    out.append({
                        "timestamp": _pd.Timestamp(ts, unit="ns", tz="UTC").isoformat(),
                        "balance": float(capital + realized + unreal),
                    })
        return out
    except Exception:
        return start
