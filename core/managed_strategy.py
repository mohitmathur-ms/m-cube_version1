"""
ManagedExitStrategy - Wraps any signal logic with SL/TP/trailing/target locking.

Used by the portfolio system to add exit management to any strategy from the signal registry.
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import ExponentialMovingAverage, SimpleMovingAverage
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy

from core.models import ExitConfig
from core.signals import SIGNAL_REGISTRY


class ManagedExitConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")

    # Signal
    signal_name: str = "EMA Cross"
    signal_params: dict = {}

    # Exit management
    stop_loss_type: str = "none"
    stop_loss_value: float = 0.0
    trailing_sl_step: float = 0.0
    trailing_sl_offset: float = 0.0
    target_type: str = "none"
    target_value: float = 0.0
    target_lock_trigger: float = 0.0
    target_lock_minimum: float = 0.0
    sl_wait_bars: int = 0
    on_sl_action: str = "close"
    on_target_action: str = "close"
    max_re_executions: int = 0


class ManagedExitStrategy(Strategy):
    """On each bar: check exits first (SL, TP, trailing, target lock, SL wait), then entries."""

    def __init__(self, config: ManagedExitConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument = None
        self.indicators = {}
        self.entry_price = 0.0
        self.highest_profit = 0.0
        self.current_sl = 0.0
        self.current_tp = 0.0
        self.sl_wait_count = 0
        self.re_execution_count = 0
        self.position_side = None  # "LONG" or "SHORT" or None

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        signal_entry = SIGNAL_REGISTRY.get(self.config.signal_name)
        if not signal_entry:
            self.log.error(f"Unknown signal: {self.config.signal_name}")
            self.stop()
            return

        params = dict(self.config.signal_params) if self.config.signal_params else {}

        for ind_name, ind_spec in signal_entry["indicators"].items():
            period = params.get(ind_spec["param_key"], ind_spec["default"])

            # Determine indicator class
            ind_class = ind_spec["class"]
            if ind_class is None and "use_ema_key" in ind_spec:
                use_ema = params.get(ind_spec["use_ema_key"], False)
                ind_class = ExponentialMovingAverage if use_ema else SimpleMovingAverage

            # Create indicator
            if "extra_param_key" in ind_spec:
                extra_val = params.get(ind_spec["extra_param_key"], ind_spec.get("extra_default", 2.0))
                indicator = ind_class(int(period), float(extra_val))
            else:
                indicator = ind_class(int(period))

            self.indicators[ind_name] = indicator
            self.register_indicator_for_bars(self.config.bar_type, indicator)

        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        if not self.indicators_initialized():
            return

        close = float(bar.close)
        is_flat = self.portfolio.is_flat(self.config.instrument_id)
        is_long = self.portfolio.is_net_long(self.config.instrument_id)
        is_short = self.portfolio.is_net_short(self.config.instrument_id)

        if not is_flat:
            self._check_exits(close, is_long, is_short)
        else:
            self._check_entries(close, is_flat, is_long, is_short)

    def _check_exits(self, close: float, is_long: bool, is_short: bool) -> None:
        if self.entry_price == 0:
            return

        # Calculate current profit
        if is_long:
            profit_pct = ((close - self.entry_price) / self.entry_price) * 100
        else:
            profit_pct = ((self.entry_price - close) / self.entry_price) * 100

        # Update highest profit
        if profit_pct > self.highest_profit:
            self.highest_profit = profit_pct

        # Target locking
        if self.config.target_lock_trigger > 0 and self.config.target_lock_minimum > 0:
            if self.highest_profit >= self.config.target_lock_trigger:
                lock_sl = self._compute_sl_price(is_long, self.config.target_lock_minimum)
                if is_long and lock_sl > self.current_sl:
                    self.current_sl = lock_sl
                elif is_short and (self.current_sl == 0 or lock_sl < self.current_sl):
                    self.current_sl = lock_sl

        # Trailing SL
        if self.config.stop_loss_type == "trailing" and self.config.trailing_sl_step > 0:
            steps = int(self.highest_profit / self.config.trailing_sl_step)
            if steps > 0:
                trail_offset = steps * self.config.trailing_sl_offset
                trail_sl = self._compute_sl_price(is_long, trail_offset)
                if is_long and trail_sl > self.current_sl:
                    self.current_sl = trail_sl
                elif is_short and (self.current_sl == 0 or trail_sl < self.current_sl):
                    self.current_sl = trail_sl

        # Check SL hit
        sl_hit = False
        if self.current_sl > 0:
            if is_long and close <= self.current_sl:
                sl_hit = True
            elif is_short and close >= self.current_sl:
                sl_hit = True

        if sl_hit:
            if self.config.sl_wait_bars > 0:
                self.sl_wait_count += 1
                if self.sl_wait_count < self.config.sl_wait_bars:
                    sl_hit = False
            if sl_hit:
                self._handle_exit("sl", is_long)
                return
        else:
            self.sl_wait_count = 0

        # Check TP hit
        if self.current_tp > 0:
            tp_hit = False
            if is_long and close >= self.current_tp:
                tp_hit = True
            elif is_short and close <= self.current_tp:
                tp_hit = True

            if tp_hit:
                self._handle_exit("tp", is_long)

    def _handle_exit(self, exit_type: str, was_long: bool) -> None:
        action = self.config.on_sl_action if exit_type == "sl" else self.config.on_target_action

        self.close_all_positions(self.config.instrument_id)
        self._reset_exit_state()

        if action == "re_execute":
            if self.re_execution_count < self.config.max_re_executions:
                self.re_execution_count += 1
                # Allow re-entry on next signal
        elif action == "reverse":
            side = OrderSide.SELL if was_long else OrderSide.BUY
            self._submit_order(side)
            self._set_exit_levels(side)

    def _check_entries(self, close: float, is_flat: bool, is_long: bool, is_short: bool) -> None:
        signal_entry = SIGNAL_REGISTRY.get(self.config.signal_name)
        if not signal_entry:
            return

        params = dict(self.config.signal_params) if self.config.signal_params else {}
        args = signal_entry["extract_args"](self.indicators, params, close)
        args["is_flat"] = is_flat
        args["is_long"] = is_long
        args["is_short"] = is_short

        side = signal_entry["signal_fn"](**args)
        if side is not None:
            self._submit_order(side)
            self._set_exit_levels(side)

    def _set_exit_levels(self, side: OrderSide) -> None:
        # Will be set on next bar when we know the fill price
        # For simplicity, use current close as proxy
        pass

    def on_order_filled(self, event) -> None:
        """Set exit levels when an order fills."""
        self.entry_price = float(event.last_px)
        self.highest_profit = 0.0
        self.sl_wait_count = 0

        is_buy = event.order_side == OrderSide.BUY
        self.position_side = "LONG" if is_buy else "SHORT"

        # Compute SL
        if self.config.stop_loss_type in ("percentage", "trailing"):
            self.current_sl = self._compute_sl_price(is_buy, self.config.stop_loss_value)
        elif self.config.stop_loss_type == "points":
            if is_buy:
                self.current_sl = self.entry_price - self.config.stop_loss_value
            else:
                self.current_sl = self.entry_price + self.config.stop_loss_value
        else:
            self.current_sl = 0.0

        # Compute TP
        if self.config.target_type == "percentage":
            if is_buy:
                self.current_tp = self.entry_price * (1 + self.config.target_value / 100)
            else:
                self.current_tp = self.entry_price * (1 - self.config.target_value / 100)
        elif self.config.target_type == "points":
            if is_buy:
                self.current_tp = self.entry_price + self.config.target_value
            else:
                self.current_tp = self.entry_price - self.config.target_value
        else:
            self.current_tp = 0.0

    def _compute_sl_price(self, is_long: bool, pct: float) -> float:
        if is_long:
            return self.entry_price * (1 - pct / 100)
        else:
            return self.entry_price * (1 + pct / 100)

    def _reset_exit_state(self) -> None:
        self.entry_price = 0.0
        self.highest_profit = 0.0
        self.current_sl = 0.0
        self.current_tp = 0.0
        self.sl_wait_count = 0
        self.position_side = None

    def _submit_order(self, side: OrderSide) -> None:
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


def config_from_exit(exit_config: ExitConfig, signal_name: str, signal_params: dict,
                     instrument_id, bar_type, trade_size) -> ManagedExitConfig:
    """Build a ManagedExitConfig from an ExitConfig dataclass."""
    return ManagedExitConfig(
        instrument_id=instrument_id,
        bar_type=bar_type,
        trade_size=Decimal(str(trade_size)),
        signal_name=signal_name,
        signal_params=signal_params,
        stop_loss_type=exit_config.stop_loss_type,
        stop_loss_value=exit_config.stop_loss_value,
        trailing_sl_step=exit_config.trailing_sl_step,
        trailing_sl_offset=exit_config.trailing_sl_offset,
        target_type=exit_config.target_type,
        target_value=exit_config.target_value,
        target_lock_trigger=exit_config.target_lock_trigger or 0.0,
        target_lock_minimum=exit_config.target_lock_minimum or 0.0,
        sl_wait_bars=exit_config.sl_wait_bars,
        on_sl_action=exit_config.on_sl_action,
        on_target_action=exit_config.on_target_action,
        max_re_executions=exit_config.max_re_executions,
    )
