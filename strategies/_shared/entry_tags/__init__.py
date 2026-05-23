"""Order-tag ("ENTRY DETAILED REASON") string builders.

Each strategy attaches a human-readable reason string to its entry order's
``tags=[...]``. That string lands in ``fills_report["tags"]`` and surfaces in
the orderbook page's *ENTRY DETAILED REASON* column. The format is a shared,
cross-cutting concern, so the per-strategy f-strings live here as pure
functions instead of being scattered inline in each ``on_bar`` / ``_submit_order``.

The strings are reproduced **byte-for-byte** from the original inline versions
(same ``≥``/``≤``/``σ`` glyphs and ``:.4f``/``:.2f`` precision); changing them
would alter the orderbook column and any downstream consumers of the tag.
"""

from __future__ import annotations

from nautilus_trader.model.enums import OrderSide


def ema_reason(
    side: OrderSide, fast_p: int, slow_p: int, fast_v: float, slow_v: float
) -> str:
    if side == OrderSide.BUY:
        return f"EMA Cross BUY: fast({fast_p})={fast_v:.4f} ≥ slow({slow_p})={slow_v:.4f}"
    return f"EMA Cross SELL: fast({fast_p})={fast_v:.4f} < slow({slow_p})={slow_v:.4f}"


def rsi_reason(
    side: OrderSide, period: int, value: float, oversold: float, overbought: float
) -> str:
    if side == OrderSide.BUY:
        return f"RSI({period})={value:.2f} ≤ oversold({oversold})"
    return f"RSI({period})={value:.2f} ≥ overbought({overbought})"


def bollinger_reason(
    side: OrderSide, close: float, band_val: float, std: float, period: int
) -> str:
    if side == OrderSide.BUY:
        return f"Bollinger BUY: close={close:.4f} ≤ lower({std}σ,p{period})={band_val:.4f}"
    return f"Bollinger SELL: close={close:.4f} ≥ upper({std}σ,p{period})={band_val:.4f}"


def four_ma_reason(
    side: OrderSide, periods: tuple[int, int, int, int], values: tuple[float, float, float, float]
) -> str:
    p1, p2, p3, p4 = periods
    v1, v2, v3, v4 = values
    if side == OrderSide.BUY:
        return (
            f"4MA BUY: ma{p1}>ma{p2}>ma{p3}>ma{p4} "
            f"({v1:.4f}/{v2:.4f}/{v3:.4f}/{v4:.4f})"
        )
    return (
        f"4MA SELL: ma{p1}<ma{p2}<ma{p3}<ma{p4} "
        f"({v1:.4f}/{v2:.4f}/{v3:.4f}/{v4:.4f})"
    )


def rbo_reason(
    side: OrderSide,
    leg_no: int,
    price: float,
    range_high: float | None,
    range_low: float | None,
) -> str:
    """Range-breakout entry tag. Falls back to a price-only form when the
    leg's range bound is unknown (mirrors the original inline behaviour)."""
    if side == OrderSide.BUY:
        return (
            f"RBO leg{leg_no} BUY: price={price:.4f} > range_high={range_high:.4f}"
            if range_high is not None
            else f"RBO leg{leg_no} BUY @ {price:.4f}"
        )
    return (
        f"RBO leg{leg_no} SELL: price={price:.4f} < range_low={range_low:.4f}"
        if range_low is not None
        else f"RBO leg{leg_no} SELL @ {price:.4f}"
    )
