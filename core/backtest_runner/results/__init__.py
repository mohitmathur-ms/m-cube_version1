"""Single-engine result extraction with FX-to-base-currency conversion of
realized/unrealized PnL, and the positions_report_with_base export."""

from __future__ import annotations

import pandas as pd
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.model.identifiers import Venue
from core.fx_rates import FxRateResolver
from core.fx_rates import parse_money_string

from core.backtest_runner.equity_curves import (
    _build_equity_curve_from_account,
    _ensure_final_equity_point,
)
from core.backtest_runner.exit_fill import (
    _apply_directional_close_fill,
    _apply_vwap_fill,
)
from core.backtest_runner.report_utils import (
    _pick_col,
    _to_utc_ts,
)


def _extract_results(
    engine: BacktestEngine,
    starting_capital: float,
    fx_resolver: FxRateResolver | None = None,
    vwap_lookup: dict | None = None,
    close_lookup: dict | None = None,
) -> dict:
    """Extract backtest results from the engine, converting per-position PnL
    into the account base currency via the supplied FX resolver.

    Without a resolver, results use engine-native numbers (identical to the
    pre-FX-aware behavior) — safe default for USD-only catalogs.

    ``vwap_lookup`` (from ``_build_vwap_lookup``) enables the VWAP proxy-fill
    model: SL/Target exit fills are repriced before any metric is computed.
    ``None`` (the default) leaves fills as the engine produced them.
    """
    trader = engine.trader

    # Generate reports (safe)
    fills_report = None
    try:
        fills_report = trader.generate_order_fills_report()
    except Exception:
        pass

    positions_report = None
    try:
        positions_report = trader.generate_positions_report()
    except Exception:
        pass

    # VWAP proxy-fill (spec §4.2): reprice SL/Target exits before any metric
    # is derived from positions_report, so win/loss, daily PnL, totals and the
    # base-currency column all reflect the adjusted fills.
    vwap_fill_adjustments = 0
    if vwap_lookup:
        try:
            vwap_fill_adjustments = _apply_vwap_fill(
                positions_report, fills_report, vwap_lookup,
            )
        except Exception:
            vwap_fill_adjustments = 0

    # Directional-close exit fill (spec §8.1) — base price for every exit;
    # composes with the VWAP fill, which owns the SL/Target leg exits when
    # active (vwap_active skips them so the two stay on disjoint exit sets).
    directional_fill_adjustments = 0
    if close_lookup:
        try:
            directional_fill_adjustments = _apply_directional_close_fill(
                positions_report, close_lookup, fills_report,
                vwap_active=bool(vwap_lookup),
            )
        except Exception:
            directional_fill_adjustments = 0

    account_report = None
    try:
        # Try to get account report using the venue from cached accounts
        accounts = list(engine.kernel.cache.accounts())
        if accounts:
            venue = accounts[0].id.get_issuer()
            account_report = trader.generate_account_report(Venue(str(venue)))
    except Exception:
        pass

    # Realized cash on the account balance is unreliable here: when the engine
    # has no xrate source, JPY PnL from USDJPY never converts back to USD and
    # the USD balance stays frozen at the starting capital. Instead we rebuild
    # the total from per-position PnL, each converted via the FX resolver.
    accounts = list(engine.kernel.cache.accounts())

    # Include unrealized P&L from open positions, converted to base currency.
    # pos.unrealized_pnl returns a Money in the position's quote_currency — for
    # USDJPY that's JPY. Without conversion we'd be adding JPY to USD.
    all_positions = engine.kernel.cache.positions()
    open_positions = [p for p in all_positions if p.is_open]
    closed_positions = [p for p in all_positions if p.is_closed]

    unrealized_pnl = 0.0
    for pos in open_positions:
        unrealized_pnl += _position_unrealized_in_base(pos, fx_resolver)

    # Sum realized PnL across closed positions *in base currency*. Note: this
    # replaces the prior approach of reading `final_balance` from the USD
    # account, because the USD balance never moves when the engine has no
    # xrate source — the only trustworthy realized-PnL total is the sum of
    # per-position PnLs, each converted individually.
    realized_pnl_base = 0.0
    for pos in closed_positions:
        realized_pnl_base += _position_realized_in_base(pos, fx_resolver)

    # Prefer the positions_report for accurate trade counts (NETTING mode
    # collapses cache.positions() to 1 per instrument).
    report_realized_base = None
    if positions_report is not None and not positions_report.empty:
        report_realized_base = _positions_report_realized_in_base(
            positions_report, fx_resolver,
        )

    # Use the report-derived total when available (it captures every round
    # trip, not just the single NETTING position). Fall back to the cache
    # sum otherwise.
    total_realized_base = (
        report_realized_base if report_realized_base is not None
        else realized_pnl_base
    )

    total_pnl = total_realized_base + unrealized_pnl
    total_return_pct = (total_pnl / starting_capital) * 100 if starting_capital > 0 else 0

    # Count trades from order fills (round-trip analysis)
    orders = engine.kernel.cache.orders()
    filled_orders = [o for o in orders if o.is_closed]
    total_orders = len(filled_orders)

    wins = 0
    losses = 0
    total_trades = 0

    # In NETTING mode, cache.positions() returns only 1 position per instrument
    # (it gets reused for every open/close cycle). The positions_report has the
    # actual round-trip trades, so prefer that for accurate trade counting.
    if positions_report is not None and not positions_report.empty:
        pnl_col = None
        for col_name in ["realized_pnl", "RealizedPnl", "pnl"]:
            if col_name in positions_report.columns:
                pnl_col = col_name
                break
        ts_col = _pick_col(positions_report, ["ts_closed", "ts_last", "ts_init"])

        if pnl_col:
            for _, row in positions_report.iterrows():
                pnl_val = _row_pnl_to_base(row, pnl_col, ts_col, fx_resolver)
                total_trades += 1
                if pnl_val > 0:
                    wins += 1
                elif pnl_val < 0:
                    losses += 1
        else:
            total_trades = len(positions_report)

    # Fallback to cache positions if positions_report was empty
    if total_trades == 0:
        for pos in closed_positions:
            pnl = _position_realized_in_base(pos, fx_resolver)
            total_trades += 1
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1

    # Count open positions
    for pos in open_positions:
        pnl = _position_unrealized_in_base(pos, fx_resolver)
        total_trades += 1
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1

    # Last resort fallback from fills count
    if total_trades == 0 and fills_report is not None and not fills_report.empty:
        total_trades = max(total_orders // 2, 1)
        if total_pnl > 0:
            wins = 1
        elif total_pnl < 0:
            losses = 1

    # ``flat_trades`` are closed positions whose realized P&L rounds to zero in
    # the account base currency. They are real trades — entry + exit both
    # filled — but the price moved by less than the sub-cent precision can
    # represent (or not at all). Surfacing this separately prevents the
    # "12,052 trades, 339 wins, 180 losses" confusion where the simple win
    # rate (wins / total) penalises the strategy for trades that didn't lose.
    flat_trades = max(total_trades - wins - losses, 0)
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    # ``decisive_win_rate`` excludes flat trades from the denominator. Useful
    # when most trades are flat (e.g. a too-small trade_size relative to bar
    # noise) — gives the meaningful "of the trades that produced P&L, what
    # fraction were wins?" figure.
    _decisive_n = wins + losses
    decisive_win_rate = (wins / _decisive_n * 100) if _decisive_n > 0 else None

    # Day-based win percentage for single-strategy backtest
    # Use entry time (ts_init) for daily grouping to match the HTML report
    _daily_pnl: dict[str, float] = {}
    _daily_ts_col = _pick_col(positions_report, ["ts_init", "ts_closed", "ts_last"]) if positions_report is not None and not positions_report.empty else None
    _pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"]) if positions_report is not None and not positions_report.empty else None
    # _row_pnl_to_base needs the close-time column for FX conversion
    _close_ts_col = _pick_col(positions_report, ["ts_closed", "ts_last", "ts_init"]) if positions_report is not None and not positions_report.empty else None
    if _daily_ts_col and _pnl_col and positions_report is not None and not positions_report.empty:
        for _, row in positions_report.iterrows():
            pnl_val = _row_pnl_to_base(row, _pnl_col, _close_ts_col, fx_resolver)
            ts_raw = row.get(_daily_ts_col)
            dt = _to_utc_ts(ts_raw)
            day_key = dt.strftime("%Y-%m-%d") if dt is not None else "unknown"
            _daily_pnl[day_key] = _daily_pnl.get(day_key, 0.0) + pnl_val
    _total_days = len(_daily_pnl)
    _winning_days = sum(1 for v in _daily_pnl.values() if v > 0)
    _losing_days = sum(1 for v in _daily_pnl.values() if v < 0)
    _win_pct_days = (_winning_days / _total_days * 100) if _total_days > 0 else 0.0
    _loss_pct_days = (_losing_days / _total_days * 100) if _total_days > 0 else 0.0

    # Build timestamped equity curve from account events. The engine emits
    # these in the account's base currency, so no per-event conversion is
    # needed — but the JPY-native unrealized PnL never hit the account, so
    # we stitch a final point reflecting the converted total.
    equity_curve_ts = _build_equity_curve_from_account(accounts, starting_capital)
    _ensure_final_equity_point(equity_curve_ts, starting_capital + total_pnl)

    return {
        "starting_capital": starting_capital,
        "final_balance": starting_capital + total_pnl,
        "total_pnl": total_pnl,
        "total_return_pct": total_return_pct,
        "total_orders": total_orders,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "flat_trades": flat_trades,
        "win_rate": win_rate,
        "decisive_win_rate": decisive_win_rate,
        "total_days": _total_days,
        "winning_days": _winning_days,
        "losing_days": _losing_days,
        "win_pct_days": _win_pct_days,
        "loss_pct_days": _loss_pct_days,
        "daily_pnl": _daily_pnl,
        "equity_curve_ts": equity_curve_ts,
        "fills_report": fills_report,
        "positions_report": positions_report_with_base(positions_report, fx_resolver),
        "account_report": account_report,
        "vwap_fill_applied": bool(vwap_fill_adjustments),
        "vwap_fill_adjustments": vwap_fill_adjustments,
        "directional_fill_applied": bool(directional_fill_adjustments),
        "directional_fill_adjustments": directional_fill_adjustments,
    }


def _position_realized_in_base(pos, fx_resolver: FxRateResolver | None) -> float:
    """Convert a closed position's realized PnL into the account base currency.

    Uses `pos.ts_closed` as the rate timestamp so 2015 fills convert at 2015
    rates and 2024 fills convert at 2024 rates.
    """
    try:
        money = pos.realized_pnl
    except Exception:
        return 0.0
    if money is None:
        return 0.0
    try:
        amount = float(money)
    except (TypeError, ValueError):
        return 0.0
    if fx_resolver is None or amount == 0:
        return amount
    ccy = getattr(money, "currency", None)
    ccy_code = str(getattr(ccy, "code", "") or "").upper()
    ts_ns = getattr(pos, "ts_closed", None) or getattr(pos, "ts_last", None)
    at = pd.Timestamp(ts_ns, unit="ns", tz="UTC") if ts_ns else None
    return fx_resolver.convert(amount, ccy_code, at)


def _position_unrealized_in_base(pos, fx_resolver: FxRateResolver | None) -> float:
    """Convert an open position's unrealized PnL into the account base currency.

    Uses `pos.ts_last` (last price timestamp) as the rate timestamp.
    """
    try:
        money = pos.unrealized_pnl(pos.last_price)
    except Exception:
        return 0.0
    if money is None:
        return 0.0
    try:
        amount = float(money)
    except (TypeError, ValueError):
        return 0.0
    if fx_resolver is None or amount == 0:
        return amount
    ccy = getattr(money, "currency", None)
    ccy_code = str(getattr(ccy, "code", "") or "").upper()
    ts_ns = getattr(pos, "ts_last", None) or getattr(pos, "ts_closed", None)
    at = pd.Timestamp(ts_ns, unit="ns", tz="UTC") if ts_ns else None
    return fx_resolver.convert(amount, ccy_code, at)


def _row_pnl_to_base(
    row,
    pnl_col: str,
    ts_col: str | None,
    fx_resolver: FxRateResolver | None,
) -> float:
    """Parse a positions_report PnL cell (e.g. '0 JPY') and convert to base."""
    amount, ccy = parse_money_string(row[pnl_col])
    if fx_resolver is None:
        return amount
    at = _to_utc_ts(row[ts_col]) if ts_col else None
    return fx_resolver.convert(amount, ccy, at)


def _base_values_from_report(
    positions_report: pd.DataFrame,
    pnl_col: str,
    ts_col: str | None,
    fx_resolver: FxRateResolver | None,
) -> list[float]:
    """Per-row base-currency PnL without paying iterrows' Series-per-row cost.

    Pulls the two underlying columns out as Python lists once, then walks them
    in a tight loop. Semantics identical to iterating with _row_pnl_to_base.
    """
    pnl_values = positions_report[pnl_col].tolist()
    ts_values = positions_report[ts_col].tolist() if ts_col else None
    out: list[float] = []
    if fx_resolver is None:
        for pnl_raw in pnl_values:
            amount, _ = parse_money_string(pnl_raw)
            out.append(amount)
        return out
    for i, pnl_raw in enumerate(pnl_values):
        amount, ccy = parse_money_string(pnl_raw)
        at = _to_utc_ts(ts_values[i]) if ts_values is not None else None
        out.append(fx_resolver.convert(amount, ccy, at))
    return out


def _positions_report_realized_in_base(
    positions_report: pd.DataFrame,
    fx_resolver: FxRateResolver | None,
) -> float | None:
    """Sum realized PnL from every row of a positions_report, in base currency.

    Returns None if the report has no recognizable PnL column — caller should
    fall back to the cache-positions sum.
    """
    pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"])
    if pnl_col is None:
        return None
    ts_col = _pick_col(positions_report, ["ts_closed", "ts_last", "ts_init"])
    return float(sum(_base_values_from_report(positions_report, pnl_col, ts_col, fx_resolver)))


def positions_report_with_base(
    positions_report: pd.DataFrame | None,
    fx_resolver: FxRateResolver | None,
) -> pd.DataFrame | None:
    """Return a copy of `positions_report` with an added `realized_pnl_base`
    column expressed in the account's base currency. No-op if no resolver or
    no conversion rules (report already in base currency).
    """
    if positions_report is None or positions_report.empty:
        return positions_report
    if fx_resolver is None:
        return positions_report
    pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"])
    if pnl_col is None:
        return positions_report
    ts_col = _pick_col(positions_report, ["ts_closed", "ts_last", "ts_init"])
    base_ccy = fx_resolver.base_currency
    base_values = _base_values_from_report(positions_report, pnl_col, ts_col, fx_resolver)
    out = positions_report.copy()
    out[f"realized_pnl_{base_ccy.lower()}"] = base_values
    return out
