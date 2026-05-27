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
