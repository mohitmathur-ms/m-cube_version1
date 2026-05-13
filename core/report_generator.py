"""
HTML report generator for backtest results.

Reads the template from docs/report_template.html, maps NautilusTrader
backtest results to the template's expected JSON format, and produces
a self-contained HTML report file.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from core._pandas_utils import iter_columns


def _parse_nautilus_value(value) -> float:
    """Parse a NautilusTrader Money/Quantity value to float.

    Handles strings like "150.00 USD", Decimal, int, float.
    """
    if value is None:
        return 0.0
    # partition() is one-pass and avoids allocating a list for the common
    # "<amount> <ccy>" case. Empty values fall through to ValueError.
    amount_str, _, _ = str(value).strip().partition(" ")
    try:
        return float(amount_str)
    except ValueError:
        return 0.0


def _format_timestamp(ts) -> str:
    """Convert a NautilusTrader timestamp to DD-MM-YYYY HH:MM:SS format."""
    if ts is None:
        return ""
    try:
        dt = pd.to_datetime(ts, utc=True)
        return dt.strftime("%d-%m-%Y %H:%M:%S")
    except Exception:
        return str(ts)


def _format_timestamp_series(series: pd.Series) -> list[str]:
    """Vectorized equivalent of applying _format_timestamp to every cell.

    Falls back to per-cell _format_timestamp for entries that vectorized
    parsing can't handle, so string-like fallbacks ("str(ts)" for unparseable
    values) are preserved exactly.
    """
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    formatted = parsed.dt.strftime("%d-%m-%Y %H:%M:%S")
    # NaN entries in formatted line up with NaT in parsed; fill them from the
    # original slow path so we keep the "return str(ts)" fallback verbatim.
    nat_mask = parsed.isna()
    if nat_mask.any():
        raw_values = series.tolist()
        result = formatted.tolist()
        for i, is_bad in enumerate(nat_mask.tolist()):
            if is_bad:
                result[i] = _format_timestamp(raw_values[i])
        return result
    return formatted.tolist()


def _resolve_column(df: pd.DataFrame, candidates: list[str], default=None):
    """Find the first matching column name from candidates."""
    for col in candidates:
        if col in df.columns:
            return col
    return default


def _determine_reason(
    order_type: str,
    is_reduce: bool,
    contingency_type: str = "",
    tags: str = "",
) -> str:
    """Derive a human-readable entry/exit reason from NautilusTrader order fields.

    Priority: tags > order type / contingency type > fallback.
    """
    tags = (tags or "").strip()
    if tags:
        return tags

    order_type = (order_type or "").strip().upper()
    contingency_type = (contingency_type or "").strip().upper()

    if is_reduce:
        if contingency_type == "ONE_CANCELS_OTHER":
            return "OCO Exit"
        if order_type in ("STOP_MARKET", "STOP"):
            return "Stop Loss"
        if order_type == "STOP_LIMIT":
            return "Stop Limit"
        if order_type == "LIMIT":
            return "Take Profit"
        if order_type == "MARKET":
            return "Market Exit"
        return "Strategy Signal"
    else:
        if order_type == "MARKET":
            return "Market Order"
        if order_type == "LIMIT":
            return "Limit Order"
        return "Strategy Signal"


def _build_fills_lookup(fills_report) -> dict:
    """Build lookups mapping a fill to its reason fields.

    Returns a dict with three views:

    - ``"by_oid"`` — keyed by venue_order_id (the existing path; works when
      the positions_report's opening_order_id happens to match the venue id,
      which is rare under NautilusTrader's default reports because positions
      use the *client* order id format).
    - ``"by_pos_ts_open"`` / ``"by_pos_ts_close"`` — keyed by
      ``(trader_id, strategy_id, instrument_id, ts_int_ns)`` where
      ``ts_int_ns`` is the fill's ``ts_init`` rounded to the nearest second.
      Splitting open vs. close fills (by ``is_reduce_only``) is required
      because ``on_sl_action="reverse"`` / ``on_target_action="reverse"``
      emits a close fill (with the structured tag) and immediately
      submits an opposite-side open fill on the *same bar* — the two
      fills share an identical timestamp, so a single dict would let the
      tag-less open fill overwrite the tagged close fill, and the
      orderbook EXIT REASON would lose the "Reverse on SL"/"Reverse on
      TP" label.
    - ``"by_pos_ts"`` — combined fallback (whichever fill landed last)
      for fills_reports that don't carry an ``is_reduce_only`` column.

    Each entry contains ``{type, contingency_type, tags}``.
    """
    out = {"by_oid": {}, "by_pos_ts": {},
           "by_pos_ts_open": {}, "by_pos_ts_close": {}}
    if fills_report is None or fills_report.empty:
        return out

    df = fills_report.reset_index()
    col_order_id = _resolve_column(df, ["venue_order_id", "VenueOrderId", "client_order_id", "order_id"])
    col_type = _resolve_column(df, ["type", "order_type", "OrderType"])
    col_contingency = _resolve_column(df, ["contingency_type", "ContingencyType"])
    col_tags = _resolve_column(df, ["tags", "Tags"])
    col_trader = _resolve_column(df, ["trader_id", "TraderId"])
    col_strategy = _resolve_column(df, ["strategy_id", "StrategyId"])
    col_instrument = _resolve_column(df, ["instrument_id", "InstrumentId"])
    col_ts = _resolve_column(df, ["ts_init", "TsInit", "ts_event", "ts_last"])
    col_reduce = _resolve_column(df, ["is_reduce_only", "IsReduceOnly", "reduce_only"])

    def _normalize_tags(tg):
        # Nautilus stores tags as ``['EMA Cross BUY: …']``; unwrap to the
        # first element's verbatim string, not the bracketed repr.
        if isinstance(tg, (list, tuple)):
            return str(tg[0]) if tg else ""
        if tg is None:
            return ""
        return str(tg)

    def _ts_to_seconds(ts):
        # Normalize timestamp -> int seconds since epoch for stable lookup.
        # Handles pandas.Timestamp, nanosecond ints, and ISO strings.
        if ts is None:
            return None
        try:
            t = pd.Timestamp(ts)
            if t.tzinfo is None:
                t = t.tz_localize("UTC")
            return int(t.timestamp())
        except Exception:
            try:
                # nanosecond integer fallback
                return int(int(ts) // 1_000_000_000)
            except Exception:
                return None

    if col_order_id is None and col_ts is None:
        return out

    for row in df.itertuples(index=False):
        rec = row._asdict() if hasattr(row, "_asdict") else dict(zip(df.columns, row))
        tg = rec.get(col_tags) if col_tags else None
        info = {
            "type": str(rec.get(col_type) or "") if col_type else "",
            "contingency_type": str(rec.get(col_contingency) or "") if col_contingency else "",
            "tags": _normalize_tags(tg),
        }
        if col_order_id:
            oid = str(rec.get(col_order_id) or "")
            if oid:
                out["by_oid"][oid] = info
        if col_trader and col_strategy and col_instrument and col_ts:
            sec = _ts_to_seconds(rec.get(col_ts))
            if sec is not None:
                key = (str(rec.get(col_trader)),
                       str(rec.get(col_strategy)),
                       str(rec.get(col_instrument)),
                       sec)
                # Combined view kept for back-compat / no-reduce-col fallback.
                out["by_pos_ts"][key] = info
                # Role-split views — disambiguate close from open when
                # both fire on the same bar (reverse-on-SL/TP path).
                if col_reduce:
                    is_close = bool(rec.get(col_reduce))
                    if is_close:
                        out["by_pos_ts_close"][key] = info
                    else:
                        out["by_pos_ts_open"][key] = info
    return out


def _build_orderbook(all_results: dict, user_id: str | None = None) -> list[dict]:
    """Build the ORDERBOOK trade list from all strategy results.

    Each closed position becomes one trade record in the template's format.
    When ``user_id`` is provided, the USERID, MULTIPLIER, and LOTS columns
    reflect the user's per-trade multiplier (from ``config/users.json``);
    otherwise legacy defaults are used (USERID="UID001", MULTIPLIER=1.0).
    """
    if user_id:
        # Local import — avoid coupling report generation to the user
        # registry when no user_id is supplied (legacy/test path).
        from core.users import get_multiplier as _get_multiplier
        resolved_userid = user_id
        multiplier = float(_get_multiplier(user_id))
    else:
        resolved_userid = "UID001"
        multiplier = 1.0

    trades = []

    for strategy_name, results in all_results.items():
        positions_report = results.get("positions_report")
        if positions_report is None or positions_report.empty:
            continue

        fills_lookup = _build_fills_lookup(results.get("fills_report"))

        df = positions_report.reset_index()

        # Resolve column names (NautilusTrader may use different naming)
        col_instrument = _resolve_column(df, ["instrument_id", "InstrumentId", "instrument"])
        col_side = _resolve_column(df, ["entry", "side", "Side"])
        col_qty = _resolve_column(df, ["peak_qty", "quantity", "Quantity", "qty"])
        col_avg_open = _resolve_column(df, ["avg_px_open", "AvgPxOpen", "avg_open"])
        col_avg_close = _resolve_column(df, ["avg_px_close", "AvgPxClose", "avg_close"])
        # Prefer the base-currency PnL column (e.g. realized_pnl_usd) so that
        # daily aggregation in the HTML report doesn't mix currencies.
        _base_pnl_col = next(
            (c for c in df.columns if c.startswith("realized_pnl_")
             and c not in ("realized_pnl_",)),
            None,
        )
        col_pnl = _base_pnl_col or _resolve_column(df, ["realized_pnl", "RealizedPnl", "realized_return", "pnl"])
        col_ts_open = _resolve_column(df, ["ts_opened", "TsOpened", "opened_time", "ts_init"])
        col_ts_close = _resolve_column(df, ["ts_closed", "TsClosed", "closed_time", "ts_last"])
        col_id = _resolve_column(df, ["id", "Id", "position_id", "index"])
        col_opening_order = _resolve_column(df, ["opening_order_id", "OpeningOrderId"])
        col_closing_order = _resolve_column(df, ["closing_order_id", "ClosingOrderId"])
        # Identity columns used for the timestamp-fallback fill lookup. The
        # primary order-id linkage often fails because NautilusTrader's
        # positions report uses the *client* order id format while the fills
        # report uses the *venue* order id format. Falling back to
        # (trader_id, strategy_id, instrument_id, ts_opened) gives a stable
        # bridge using fields both reports share.
        col_trader = _resolve_column(df, ["trader_id", "TraderId"])
        col_strategy = _resolve_column(df, ["strategy_id", "StrategyId"])

        # Pre-format timestamp columns once per strategy (vectorized) instead
        # of calling pd.to_datetime per row inside the iterrows loop.
        entry_times = _format_timestamp_series(df[col_ts_open]) if col_ts_open else None
        exit_times = _format_timestamp_series(df[col_ts_close]) if col_ts_close else None

        # Numeric (epoch-seconds) versions of ts_opened / ts_closed for the
        # ts-keyed fills_lookup fallback. Computed once per strategy.
        def _ts_seconds_series(s):
            try:
                t = pd.to_datetime(s, utc=True)
                return [int(v.timestamp()) if not pd.isna(v) else None for v in t]
            except Exception:
                return [None] * len(s)

        entry_ts_secs = _ts_seconds_series(df[col_ts_open]) if col_ts_open else [None] * len(df)
        exit_ts_secs = _ts_seconds_series(df[col_ts_close]) if col_ts_close else [None] * len(df)

        # iter_columns walks all columns positionally as a single zip — ~5×
        # faster than iterrows' Series-per-row allocation. Missing columns
        # yield None for the corresponding tuple slot; the `if col_X` guards
        # still substitute the right defaults.
        rows = iter_columns(df, col_instrument, col_side, col_qty,
                            col_avg_open, col_avg_close, col_pnl,
                            col_id, col_opening_order, col_closing_order,
                            col_trader, col_strategy)

        for i, (instr_v, side_v, qty_v, avg_open_v, avg_close_v,
                pnl_v, id_v, opening_oid_v, closing_oid_v,
                trader_v, strat_v) in enumerate(rows):
            raw_instrument = str(instr_v) if col_instrument else strategy_name
            # Clean up instrument id (e.g. "BTCUSD.CRYPTO" -> symbol="BTCUSD", exchange="CRYPTO")
            parts = raw_instrument.split(".")
            symbol = parts[0] if parts else strategy_name
            exchange = parts[1] if len(parts) > 1 else ""

            pnl = _parse_nautilus_value(pnl_v) if col_pnl else 0.0
            qty = _parse_nautilus_value(qty_v) if col_qty else 1.0
            entry_time = entry_times[i] if entry_times is not None else ""
            exit_time = exit_times[i] if exit_times is not None else ""

            opening_oid = str(opening_oid_v) if col_opening_order else ""
            closing_oid = str(closing_oid_v) if col_closing_order else ""

            # Try venue_order_id linkage first (legacy path; usually empty
            # under default reports because positions use client-order ids).
            entry_fill = fills_lookup["by_oid"].get(opening_oid, {})
            exit_fill = fills_lookup["by_oid"].get(closing_oid, {})
            # Fall back to (trader, strategy, instrument, ts_opened/closed).
            # Use the role-split lookups (open vs close) so a reverse-on-SL/TP
            # close fill on the same bar as its follow-up open fill keeps
            # its structured tag (e.g. "Reverse on SL: …") instead of
            # being overwritten by the tag-less open fill. Combined view
            # remains the fallback when fills_report has no reduce-only column.
            if not entry_fill and col_trader and col_strategy and col_instrument:
                key = (str(trader_v), str(strat_v), raw_instrument, entry_ts_secs[i])
                entry_fill = (fills_lookup["by_pos_ts_open"].get(key)
                              or fills_lookup["by_pos_ts"].get(key, {}))
            if not exit_fill and col_trader and col_strategy and col_instrument:
                key = (str(trader_v), str(strat_v), raw_instrument, exit_ts_secs[i])
                exit_fill = (fills_lookup["by_pos_ts_close"].get(key)
                             or fills_lookup["by_pos_ts"].get(key, {}))

            # ENTRY REASON keeps the order-type-derived taxonomy
            # (Market Order / Limit Order). Indicator string lives in
            # ENTRY DETAILED REASON via the entry_tags below.
            entry_reason = _determine_reason(
                order_type=entry_fill.get("type", ""),
                is_reduce=False,
                contingency_type=entry_fill.get("contingency_type", ""),
                tags="",
            )

            # EXIT REASON: when the closing order has a structured tag like
            # ``"Stop Loss: price=149.95 ≤ SL=149.96 (entry 150.00, -0.05%)"``,
            # use the prefix before the first ``":"`` as the column value
            # ("Stop Loss"). When the tag has no colon, use it verbatim. When
            # there's no tag at all (legacy paths), fall back to
            # _determine_reason's order-type taxonomy ("Market Exit" / "Stop
            # Loss" via order_type / "OCO Exit" / "Take Profit" / etc.).
            exit_tags_raw = (exit_fill.get("tags") or "").strip()
            if exit_tags_raw:
                exit_reason = exit_tags_raw.split(":", 1)[0].strip()
            else:
                exit_reason = _determine_reason(
                    order_type=exit_fill.get("type", ""),
                    is_reduce=True,
                    contingency_type=exit_fill.get("contingency_type", ""),
                    tags="",
                )
            entry_detailed_reason = (entry_fill.get("tags") or "").strip()
            exit_detailed_reason = exit_tags_raw

            # qty from the positions report is post-multiplier (the user's
            # multiplier is applied at order placement in backtest_runner).
            # LOTS exposes the pre-multiplier base size so that
            # LOTS * MULTIPLIER == QUANTITY reads coherently in the orderbook.
            base_lots = (qty / multiplier) if multiplier else qty
            trade = {
                "USERID": resolved_userid,
                "SYMBOL": symbol,
                "EXCHANGE": exchange,
                "TRANSACTION": str(side_v) if col_side else "BUY",
                "LOTS": base_lots,
                "MULTIPLIER": multiplier,
                "QUANTITY": qty,
                "OrderID": str(id_v) if col_id else str(i),
                "ENTRY TIME": entry_time,
                "ENTRY PRICE": _parse_nautilus_value(avg_open_v) if col_avg_open else 0.0,
                "ENTRY REASON": entry_reason,
                "ENTRY DETAILED REASON": entry_detailed_reason,
                "OPTION TYPE": "",
                "STRIKE": "",
                "PORTFOLIO NAME": strategy_name,
                "STRATEGY": strategy_name,
                "EXIT TIME": exit_time,
                "AVG EXIT PRICE": _parse_nautilus_value(avg_close_v) if col_avg_close else 0.0,
                "EXIT REASON": exit_reason,
                "EXIT DETAILED REASON": exit_detailed_reason,
                "PNL": pnl,
                "_IS_HEDGE": False,
                "_PARENT_ID": "",
            }
            trades.append(trade)

    # Sort by entry time
    trades.sort(key=lambda t: t["ENTRY TIME"])
    return trades


def build_orderbook_dataframe(all_results: dict, user_id: str | None = None) -> pd.DataFrame:
    """Build an order book DataFrame matching the full CSV schema.

    Parameters
    ----------
    all_results : dict
        Strategy name -> results dict (from run_backtest).
    user_id : str, optional
        Identity that produced the run. Used to populate USERID and to
        scale the MULTIPLIER/LOTS columns from ``config/users.json``.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns matching the order book CSV spec.
    """
    trades = _build_orderbook(all_results, user_id=user_id)
    if not trades:
        return pd.DataFrame()

    column_order = [
        "USERID", "SYMBOL", "EXCHANGE", "TRANSACTION", "QUANTITY", "LOTS",
        "MULTIPLIER", "OrderID", "ENTRY TIME", "ENTRY PRICE",
        "ENTRY REASON", "ENTRY DETAILED REASON",
        "OPTION TYPE", "STRIKE", "PORTFOLIO NAME", "STRATEGY",
        "EXIT TIME", "AVG EXIT PRICE",
        "EXIT REASON", "EXIT DETAILED REASON",
        "PNL",
        "_IS_HEDGE", "_PARENT_ID",
    ]
    return pd.DataFrame(trades).reindex(columns=column_order, fill_value="")


def build_logs_dataframe(
    all_results: dict,
    run_timestamp: str = "",
    user_id: str = "UID001",
) -> pd.DataFrame:
    """Build a trading logs DataFrame from fills and positions reports.

    Generates ENTRY and EXIT log rows from the backtest results, matching
    the log CSV schema: Timestamp, Backtest_Timestamp, Log Type, Message,
    UserID, Strategy Tag, Option Portfolio, Strike.

    Parameters
    ----------
    all_results : dict
        Strategy name -> results dict (from run_backtest).
    run_timestamp : str
        The wall-clock time when the backtest was executed (for the Timestamp column).
    user_id : str
        Default user ID for the logs.

    Returns
    -------
    pd.DataFrame
        DataFrame with log columns.
    """
    from datetime import datetime as _dt

    if not run_timestamp:
        run_timestamp = _dt.now().strftime("%Y-%m-%d %H:%M:%S")

    logs: list[dict] = []

    for strategy_name, results in all_results.items():
        fills = results.get("fills_report")
        if fills is None or fills.empty:
            continue

        df = fills.reset_index()

        col_instrument = _resolve_column(df, ["instrument_id", "InstrumentId", "instrument"])
        col_side = _resolve_column(df, ["side", "Side"])
        col_qty = _resolve_column(df, ["filled_qty", "quantity", "Quantity", "qty"])
        col_price = _resolve_column(df, ["avg_px", "AvgPx", "price"])
        col_ts = _resolve_column(df, ["ts_last", "ts_init", "TsLast"])
        col_reduce = _resolve_column(df, ["is_reduce_only", "IsReduceOnly"])
        col_order_id = _resolve_column(df, ["venue_order_id", "VenueOrderId", "order_id"])
        col_position_id = _resolve_column(df, ["position_id", "PositionId"])
        col_type = _resolve_column(df, ["type", "order_type", "OrderType"])
        col_contingency = _resolve_column(df, ["contingency_type", "ContingencyType"])
        col_tags = _resolve_column(df, ["tags", "Tags"])

        # Pre-format timestamp column once per strategy (vectorized).
        bt_times = _format_timestamp_series(df[col_ts]) if col_ts else None

        # iter_columns walks all columns positionally as a single zip — ~3-5×
        # faster than iterrows' Series-per-row allocation. Missing columns
        # yield None for the corresponding tuple slot; the `if col_X` guards
        # still substitute the right defaults.
        rows = iter_columns(df, col_instrument, col_side, col_qty, col_price,
                            col_reduce, col_order_id, col_position_id,
                            col_type, col_contingency, col_tags)

        for i, (instr_v, side_v, qty_v, price_v, reduce_v,
                order_id_v, position_id_v, type_v, contingency_v,
                tags_v) in enumerate(rows):
            raw_instrument = str(instr_v) if col_instrument else ""
            parts = raw_instrument.split(".")
            symbol = parts[0] if parts else ""

            bt_time = bt_times[i] if bt_times is not None else ""
            side = str(side_v) if col_side else ""
            price = _parse_nautilus_value(price_v) if col_price else 0.0
            qty = _parse_nautilus_value(qty_v) if col_qty else 0.0
            is_reduce = bool(reduce_v) if col_reduce else False
            order_id = str(order_id_v) if col_order_id else ""
            position_id = str(position_id_v) if col_position_id else ""

            reason = _determine_reason(
                order_type=str(type_v) if col_type else "",
                is_reduce=is_reduce,
                contingency_type=str(contingency_v) if col_contingency else "",
                tags=str(tags_v) if col_tags else "",
            )

            action = "EXIT" if is_reduce else "ENTRY"
            log_type = "TRADING"
            msg = (
                f"{action} | {reason} | {side} {qty} {symbol} @ {price:.2f}"
                f" | OrderID={order_id} | PositionID={position_id}"
            )

            logs.append({
                "Timestamp": run_timestamp,
                "Backtest_Timestamp": bt_time,
                "Log Type": log_type,
                "Message": msg,
                "UserID": user_id,
                "Strategy Tag": strategy_name,
                "Option Portfolio": strategy_name,
                "Strike": "",
            })

    if not logs:
        return pd.DataFrame()

    result = pd.DataFrame(logs)
    result.sort_values("Backtest_Timestamp", inplace=True, ignore_index=True)
    return result


def _build_summary(orderbook: list[dict]) -> dict:
    """Build per-date summary from the orderbook trades.

    Returns a dict keyed by DD-MM-YYYY date strings with max/min PNL info.
    """
    if not orderbook:
        return {}

    # Group trades by date
    date_groups: dict[str, list[dict]] = {}
    for trade in orderbook:
        date_str = trade["ENTRY TIME"].split(" ")[0] if trade["ENTRY TIME"] else ""
        if not date_str:
            continue
        date_groups.setdefault(date_str, []).append(trade)

    summary = {}
    for date_str, day_trades in date_groups.items():
        pnl_values = [float(t["PNL"]) for t in day_trades]
        cumulative = []
        running = 0.0
        for pnl in pnl_values:
            running += pnl
            cumulative.append(running)

        max_cum = max(cumulative) if cumulative else 0.0
        min_cum = min(cumulative) if cumulative else 0.0
        max_idx = cumulative.index(max_cum)
        min_idx = cumulative.index(min_cum)

        # Build per-portfolio leg stats for Min/Max PNL column
        portfolio_stats: dict = {}
        for trade in day_trades:
            pf = trade.get("PORTFOLIO NAME", "")
            if pf not in portfolio_stats:
                portfolio_stats[pf] = {"max_pnl": 0.0, "leg_stats": {}}
            pnl = float(trade["PNL"])
            order_id = trade.get("OrderID", "")
            entry_time = trade.get("ENTRY TIME", "")
            if order_id:
                portfolio_stats[pf]["leg_stats"][order_id] = {
                    "min_pnl": pnl,
                    "max_pnl": pnl,
                    "min_pnl_time": entry_time,
                    "max_pnl_time": entry_time,
                }
            pf_pnls = [float(t["PNL"]) for t in day_trades if t.get("PORTFOLIO NAME") == pf]
            portfolio_stats[pf]["max_pnl"] = sum(pf_pnls)

        summary[date_str] = {
            "max_pnl": max_cum,
            "min_pnl": min_cum,
            "max_pnl_time": day_trades[max_idx]["ENTRY TIME"],
            "min_pnl_time": day_trades[min_idx]["ENTRY TIME"],
            "portfolio_stats": portfolio_stats,
        }

    return summary


def generate_report(
    all_results: dict,
    backtest_name: str = "Backtest",
    template_path: str | None = None,
    user_id: str | None = None,
    date_range: dict | None = None,
) -> str:
    """Generate a self-contained HTML backtest report.

    Parameters
    ----------
    all_results : dict
        Strategy name -> results dict (from run_backtest).
    backtest_name : str
        Title for the report.
    template_path : str, optional
        Path to the HTML template. Defaults to docs/report_template.html.
    user_id : str, optional
        Identity that produced the run; populates USERID/MULTIPLIER columns
        in the embedded orderbook (see ``_build_orderbook``).
    date_range : dict, optional
        Configured engine range as ``{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}``.
        Drives the report's "Date range:" header and the initial values of the
        date-range filter inputs. Falls back to trade-derived min/max in the
        template when omitted or when either field is empty.

    Returns
    -------
    str
        Complete HTML string ready to be saved or served.
    """
    if template_path is None:
        template_path = str(Path(__file__).resolve().parent.parent / "docs" / "report_template.html")

    template = Path(template_path).read_text(encoding="utf-8")

    orderbook = _build_orderbook(all_results, user_id=user_id)
    summary = _build_summary(orderbook)
    logs: list = []

    html = template.replace("{{ ORDERBOOK_JSON }}", json.dumps(orderbook, default=str))
    html = html.replace("{{ SUMMARY_JSON }}", json.dumps(summary, default=str))
    html = html.replace("{{ LOGS_JSON }}", json.dumps(logs, default=str))
    html = html.replace("{{ DATE_RANGE_JSON }}", json.dumps(date_range or {}, default=str))
    html = html.replace("{{BACKTEST_NAME}}", backtest_name)

    return html
