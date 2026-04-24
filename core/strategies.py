"""
Strategy registry for the dashboard.

Re-exports STRATEGY_REGISTRY from the strategies package where each strategy
lives in its own file. This module exists for backward compatibility.
"""

from strategies import STRATEGY_REGISTRY

# Re-export individual classes for any direct imports
from strategies.ema_cross import EMACrossStrategy, EMACrossConfig
from strategies.rsi_mean_reversion import RSIMeanReversionStrategy, RSIConfig
from strategies.bollinger_bands import BollingerBandsStrategy, BollingerConfig
from strategies.four_ma import FourMAStrategy, FourMAConfig

__all__ = [
    "STRATEGY_REGISTRY",
    "EMACrossStrategy", "EMACrossConfig",
    "RSIMeanReversionStrategy", "RSIConfig",
    "BollingerBandsStrategy", "BollingerConfig",
    "FourMAStrategy", "FourMAConfig",
]
