"""
Signal functions for the ManagedExitStrategy.

Each signal function is a pure function that takes indicator values and position state,
and returns an OrderSide (BUY/SELL) or None.
"""

from __future__ import annotations

from nautilus_trader.indicators import (
    BollingerBands,
    ExponentialMovingAverage,
    RelativeStrengthIndex,
    SimpleMovingAverage,
)
from nautilus_trader.model.enums import OrderSide


def ema_cross_signal(fast_ema_value, slow_ema_value, is_flat, is_long, is_short):
    """Buy when fast EMA > slow EMA, sell when fast EMA < slow EMA."""
    if fast_ema_value >= slow_ema_value:
        if is_flat or is_short:
            return OrderSide.BUY
    elif fast_ema_value < slow_ema_value:
        if is_flat or is_long:
            return OrderSide.SELL
    return None


def rsi_signal(rsi_value, overbought, oversold, is_flat, is_long, is_short):
    """Buy when RSI oversold, sell when overbought."""
    if rsi_value <= oversold:
        if is_flat or is_short:
            return OrderSide.BUY
    elif rsi_value >= overbought:
        if is_flat or is_long:
            return OrderSide.SELL
    return None


def bollinger_signal(close, bb_lower, bb_upper, is_flat, is_long, is_short):
    """Buy at lower band, sell at upper band."""
    if close <= bb_lower:
        if is_flat or is_short:
            return OrderSide.BUY
    elif close >= bb_upper:
        if is_flat or is_long:
            return OrderSide.SELL
    return None


def four_ma_signal(v1, v2, v3, v4, is_flat, is_long, is_short):
    """Buy on bullish MA alignment, sell on bearish."""
    if v1 > v2 > v3 > v4:
        if is_flat or is_short:
            return OrderSide.BUY
    elif v1 < v2 < v3 < v4:
        if is_flat or is_long:
            return OrderSide.SELL
    return None


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
        },
    },
}
