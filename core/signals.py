"""
Signal functions for the ManagedExitStrategy.

Each signal function is a pure function that takes indicator values, indicator
periods, and position state. It returns a 2-tuple
``(OrderSide | None, reason | None)`` where ``reason`` is a one-line string
describing which indicator and which threshold caused the signal — used
forensically by the orderbook's ``ENTRY DETAILED REASON`` column. ``None`` for
both means no signal.
"""

from __future__ import annotations

from nautilus_trader.indicators import (
    BollingerBands,
    ExponentialMovingAverage,
    RelativeStrengthIndex,
    SimpleMovingAverage,
)
from nautilus_trader.model.enums import OrderSide


def ema_cross_signal(fast_ema_value, slow_ema_value,
                     fast_period, slow_period,
                     is_flat, is_long, is_short):
    """Buy when fast EMA >= slow EMA, sell when fast EMA < slow EMA."""
    if fast_ema_value >= slow_ema_value:
        if is_flat or is_short:
            return (
                OrderSide.BUY,
                f"EMA Cross BUY: fast({fast_period})={fast_ema_value:.4f} ≥ "
                f"slow({slow_period})={slow_ema_value:.4f}",
            )
    elif fast_ema_value < slow_ema_value:
        if is_flat or is_long:
            return (
                OrderSide.SELL,
                f"EMA Cross SELL: fast({fast_period})={fast_ema_value:.4f} < "
                f"slow({slow_period})={slow_ema_value:.4f}",
            )
    return (None, None)


def rsi_signal(rsi_value, overbought, oversold, period,
               is_flat, is_long, is_short):
    """Buy when RSI oversold, sell when overbought."""
    if rsi_value <= oversold:
        if is_flat or is_short:
            return (
                OrderSide.BUY,
                f"RSI({period})={rsi_value:.2f} ≤ oversold({oversold})",
            )
    elif rsi_value >= overbought:
        if is_flat or is_long:
            return (
                OrderSide.SELL,
                f"RSI({period})={rsi_value:.2f} ≥ overbought({overbought})",
            )
    return (None, None)


def bollinger_signal(close, bb_lower, bb_upper, period, std_dev,
                     is_flat, is_long, is_short):
    """Buy at lower band, sell at upper band."""
    if close <= bb_lower:
        if is_flat or is_short:
            return (
                OrderSide.BUY,
                f"Bollinger BUY: close={close:.4f} ≤ "
                f"lower({std_dev}σ,p{period})={bb_lower:.4f}",
            )
    elif close >= bb_upper:
        if is_flat or is_long:
            return (
                OrderSide.SELL,
                f"Bollinger SELL: close={close:.4f} ≥ "
                f"upper({std_dev}σ,p{period})={bb_upper:.4f}",
            )
    return (None, None)


def four_ma_signal(v1, v2, v3, v4, p1, p2, p3, p4,
                   is_flat, is_long, is_short):
    """Buy on bullish MA alignment, sell on bearish."""
    if v1 > v2 > v3 > v4:
        if is_flat or is_short:
            return (
                OrderSide.BUY,
                f"4MA BUY: ma{p1}>ma{p2}>ma{p3}>ma{p4} "
                f"({v1:.4f}/{v2:.4f}/{v3:.4f}/{v4:.4f})",
            )
    elif v1 < v2 < v3 < v4:
        if is_flat or is_long:
            return (
                OrderSide.SELL,
                f"4MA SELL: ma{p1}<ma{p2}<ma{p3}<ma{p4} "
                f"({v1:.4f}/{v2:.4f}/{v3:.4f}/{v4:.4f})",
            )
    return (None, None)


SIGNAL_REGISTRY = {
    "EMA Cross": {
        "signal_fn": ema_cross_signal,
        "indicators": {
            "fast_ema": {"class": ExponentialMovingAverage, "param_key": "fast_ema_period", "default": 10},
            "slow_ema": {"class": ExponentialMovingAverage, "param_key": "slow_ema_period", "default": 20},
        },
        "extract_args": lambda indicators, params, close: {
            "fast_ema_value": indicators["fast_ema"].value,
            "slow_ema_value": indicators["slow_ema"].value,
            "fast_period": params.get("fast_ema_period", 10),
            "slow_period": params.get("slow_ema_period", 20),
        },
    },
    "RSI Mean Reversion": {
        "signal_fn": rsi_signal,
        "indicators": {
            "rsi": {"class": RelativeStrengthIndex, "param_key": "rsi_period", "default": 14},
        },
        "extract_args": lambda indicators, params, close: {
            "rsi_value": indicators["rsi"].value,
            "overbought": params.get("overbought", 70.0),
            "oversold": params.get("oversold", 30.0),
            "period": params.get("rsi_period", 14),
        },
    },
    "Bollinger Bands": {
        "signal_fn": bollinger_signal,
        "indicators": {
            "bb": {"class": BollingerBands, "param_key": "bb_period", "default": 20, "extra_param_key": "bb_std", "extra_default": 2.0},
        },
        "extract_args": lambda indicators, params, close: {
            "close": close,
            "bb_lower": indicators["bb"].lower,
            "bb_upper": indicators["bb"].upper,
            "period": params.get("bb_period", 20),
            "std_dev": params.get("bb_std", 2.0),
        },
    },
    "4 Moving Averages": {
        "signal_fn": four_ma_signal,
        "indicators": {
            "ma1": {"class": None, "param_key": "ma1_period", "default": 5, "use_ema_key": "use_ema"},
            "ma2": {"class": None, "param_key": "ma2_period", "default": 10, "use_ema_key": "use_ema"},
            "ma3": {"class": None, "param_key": "ma3_period", "default": 20, "use_ema_key": "use_ema"},
            "ma4": {"class": None, "param_key": "ma4_period", "default": 50, "use_ema_key": "use_ema"},
        },
        "extract_args": lambda indicators, params, close: {
            "v1": indicators["ma1"].value,
            "v2": indicators["ma2"].value,
            "v3": indicators["ma3"].value,
            "v4": indicators["ma4"].value,
            "p1": params.get("ma1_period", 5),
            "p2": params.get("ma2_period", 10),
            "p3": params.get("ma3_period", 20),
            "p4": params.get("ma4_period", 50),
        },
    },
}
