"""
Predefined portfolio templates for quick start.
"""

from core.models import PortfolioConfig, StrategySlotConfig, ExitConfig


def get_templates() -> dict[str, dict]:
    """Return available portfolio templates with descriptions."""
    return {
        "Trend Following": {
            "description": "EMA Cross strategies with trailing SL on multiple instruments.",
            "build": _build_trend_following,
        },
        "Mean Reversion": {
            "description": "RSI + Bollinger with tight SL/TP for range-bound markets.",
            "build": _build_mean_reversion,
        },
        "Diversified": {
            "description": "One of each strategy type with balanced SL/TP.",
            "build": _build_diversified,
        },
        "Conservative": {
            "description": "Single EMA Cross with wide SL, TP, and target locking.",
            "build": _build_conservative,
        },
    }


def build_template(template_name: str, bar_types: list[str]) -> PortfolioConfig:
    """Build a portfolio config from a template name and available bar types."""
    templates = get_templates()
    if template_name not in templates:
        raise ValueError(f"Unknown template: {template_name}")
    return templates[template_name]["build"](bar_types)


def _build_trend_following(bar_types: list[str]) -> PortfolioConfig:
    slots = []
    for i, bt in enumerate(bar_types[:3]):
        slots.append(StrategySlotConfig(
            strategy_name="EMA Cross",
            strategy_params={"fast_ema_period": 10, "slow_ema_period": 30},
            bar_type_str=bt,
            trade_size=1.0,
            exit_config=ExitConfig(
                stop_loss_type="trailing",
                stop_loss_value=5.0,
                trailing_sl_step=3.0,
                trailing_sl_offset=1.5,
                target_type="percentage",
                target_value=15.0,
            ),
        ))
    return PortfolioConfig(
        name="Trend Following",
        description="EMA Cross with trailing SL on up to 3 instruments.",
        starting_capital=200_000.0,
        slots=slots,
    )


def _build_mean_reversion(bar_types: list[str]) -> PortfolioConfig:
    slots = []
    if len(bar_types) >= 1:
        slots.append(StrategySlotConfig(
            strategy_name="RSI Mean Reversion",
            strategy_params={"rsi_period": 14, "overbought": 70.0, "oversold": 30.0},
            bar_type_str=bar_types[0],
            trade_size=1.0,
            exit_config=ExitConfig(
                stop_loss_type="percentage", stop_loss_value=3.0,
                target_type="percentage", target_value=5.0,
            ),
        ))
    if len(bar_types) >= 2:
        slots.append(StrategySlotConfig(
            strategy_name="Bollinger Bands",
            strategy_params={"bb_period": 20, "bb_std": 2.0},
            bar_type_str=bar_types[1],
            trade_size=1.0,
            exit_config=ExitConfig(
                stop_loss_type="percentage", stop_loss_value=3.0,
                target_type="percentage", target_value=5.0,
            ),
        ))
    return PortfolioConfig(
        name="Mean Reversion",
        description="RSI + Bollinger with tight SL 3% / TP 5%.",
        starting_capital=150_000.0,
        slots=slots,
    )


def _build_diversified(bar_types: list[str]) -> PortfolioConfig:
    strats = [
        ("EMA Cross", {"fast_ema_period": 10, "slow_ema_period": 20}),
        ("RSI Mean Reversion", {"rsi_period": 14, "overbought": 70.0, "oversold": 30.0}),
        ("Bollinger Bands", {"bb_period": 20, "bb_std": 2.0}),
    ]
    slots = []
    for i, (name, params) in enumerate(strats):
        bt = bar_types[i % len(bar_types)] if bar_types else ""
        slots.append(StrategySlotConfig(
            strategy_name=name,
            strategy_params=params,
            bar_type_str=bt,
            trade_size=1.0,
            exit_config=ExitConfig(
                stop_loss_type="percentage", stop_loss_value=5.0,
                target_type="percentage", target_value=10.0,
            ),
        ))
    return PortfolioConfig(
        name="Diversified",
        description="One of each strategy type, SL 5% / TP 10%.",
        starting_capital=200_000.0,
        slots=slots,
    )


def _build_conservative(bar_types: list[str]) -> PortfolioConfig:
    bt = bar_types[0] if bar_types else ""
    return PortfolioConfig(
        name="Conservative",
        description="Single EMA Cross with wide SL 8%, TP 12%, target locking.",
        starting_capital=100_000.0,
        slots=[StrategySlotConfig(
            strategy_name="EMA Cross",
            strategy_params={"fast_ema_period": 10, "slow_ema_period": 30},
            bar_type_str=bt,
            trade_size=1.0,
            exit_config=ExitConfig(
                stop_loss_type="percentage", stop_loss_value=8.0,
                target_type="percentage", target_value=12.0,
                target_lock_trigger=500.0,
                target_lock_minimum=200.0,
            ),
        )],
    )
