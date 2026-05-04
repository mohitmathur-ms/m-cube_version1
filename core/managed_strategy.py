"""
ManagedExitStrategy - Wraps any signal logic with SL/TP/trailing/target locking.

Used by the portfolio system to add exit management to any strategy from the signal registry.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import ExponentialMovingAverage, SimpleMovingAverage
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy

from core.models import ExitConfig
from core.signals import SIGNAL_REGISTRY


# -1 = squareoff disabled. Storing the parsed minute-of-day (0..1439) avoids
# re-parsing the HH:MM string on every bar.
_SQUAREOFF_DISABLED = -1


def _parse_squareoff_minute(squareoff_time: str | None) -> int:
    """Convert "HH:MM" → minute-of-day, or -1 when disabled.

    Tolerates ``None`` and an empty string. Raises ``ValueError`` for malformed
    inputs so a typo in a portfolio JSON fails loudly at engine build instead
    of silently disabling the squareoff.
    """
    if not squareoff_time:
        return _SQUAREOFF_DISABLED
    h, _, m = squareoff_time.partition(":")
    return int(h) * 60 + int(m)


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

    # Square-off (resolved by core.models.resolve_squareoff before engine build).
    # squareoff_minute = -1 → disabled. Otherwise daily force-close at this
    # local-time minute-of-day, no re-entry until next session.
    squareoff_minute: int = _SQUAREOFF_DISABLED
    squareoff_tz: str = "UTC"

    # Range Breakout (RBO). All in seconds-of-day UTC; 0 / -1 / "" → disabled.
    # When ``rbo_enabled``, fresh entries are gated by a per-day breakout
    # state machine (see _rbo_step). Spec: 5. Logics/rbo_logics.html.
    # Re-entries (re_execution_count > 0) bypass the gate per spec.
    rbo_enabled: bool = False
    rbo_monitoring_start_sec: int = 0
    rbo_monitoring_end_sec: int = 0
    rbo_entry_start_sec: int = 0
    rbo_entry_end_sec: int = 0
    rbo_range_buffer_sec: int = 0
    rbo_entry_at: str = "Any"  # "Any" / "RangeHigh" / "RangeLow"
    rbo_cancel_other_side: bool = False


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
        self._expecting_close_fill = False  # next on_order_filled is a close, not an open

        # Squareoff state. Resolve the tz once at init — ZoneInfo lookups are
        # cached but the conversion still costs a hash; storing the object lets
        # on_bar do a single astimezone() call. Bars are UTC-stamped, so we keep
        # a UTC tzinfo too rather than rebuilding it per bar.
        self._squareoff_min: int = int(config.squareoff_minute)
        self._utc_tz = timezone.utc
        try:
            self._squareoff_tz = ZoneInfo(config.squareoff_tz) if self._squareoff_min >= 0 else self._utc_tz
        except ZoneInfoNotFoundError:
            # Fall back to UTC rather than crash the run; will be visible in
            # any squareoff log because times won't shift for DST.
            self._squareoff_tz = self._utc_tz
        # Date (in squareoff_tz) on which we've already squared off. Blocks
        # re-entries until the calendar flips. None until first squareoff fires.
        self._squareoff_done_date: date | None = None

        # RBO state machine. Spec: 5. Logics/rbo_logics.html.
        # All times are seconds-of-day UTC; resets every UTC day.
        self._rbo_enabled = bool(config.rbo_enabled)
        self._rbo_phase: str = "IDLE"  # IDLE / MONITORING / ENTRY / DONE
        self._rbo_range_high: float | None = None
        self._rbo_range_low: float | None = None
        self._rbo_triggered_sides: set[str] = set()  # subset of {"HIGH","LOW"}
        self._rbo_last_day_ns: int | None = None
        # Per spec P9: when cancel_other_side fires, phase moves to DONE
        # *after* the breakout has executed — i.e. legs still get to enter on
        # the breakout bar itself. We defer the transition by one bar via
        # this flag so _check_entries (which runs after _rbo_step on the same
        # bar) still sees phase==ENTRY and allows the entry through.
        self._rbo_pending_done: bool = False

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
        # RBO state runs every bar, even before indicators warm up — the
        # range-monitoring window can start before the indicator gets enough
        # bars, and we still need to track high/low through that period.
        if self._rbo_enabled:
            self._rbo_step(bar)

        if not self.indicators_initialized():
            return

        close = float(bar.close)
        # Use this strategy's OWN state rather than ``self.portfolio.is_flat(...)``,
        # which aggregates across every strategy trading the same (venue, instrument).
        # Aggregation is wrong when multiple strategies share an engine (Direction B
        # shared-engine grouping) — Strategy A's position would make Strategy B's
        # view non-flat, blocking B's entries. In single-strategy engines these are
        # equivalent (the strategy's tracked state mirrors the portfolio's net).
        is_flat = self.position_side is None
        is_long = self.position_side == "LONG"
        is_short = self.position_side == "SHORT"

        # Squareoff check runs FIRST so a bar at-or-past the configured local
        # time always exits, even when SL/TP would fire on the same bar. This
        # makes squareoff the deterministic outer envelope.
        if self._squareoff_min >= 0:
            local_dt = datetime.fromtimestamp(bar.ts_event / 1e9, tz=self._utc_tz).astimezone(self._squareoff_tz)
            local_min = local_dt.hour * 60 + local_dt.minute
            local_date = local_dt.date()

            # New session ⇒ release the re-entry lock so the next signal can fire.
            if self._squareoff_done_date is not None and local_date != self._squareoff_done_date:
                self._squareoff_done_date = None

            if local_min >= self._squareoff_min and self._squareoff_done_date != local_date:
                if not is_flat:
                    # Plain close — bypass on_sl/on_target action wiring so
                    # squareoff doesn't accidentally re_execute or reverse.
                    self._force_squareoff()
                self._squareoff_done_date = local_date
                return  # No entries on the squareoff bar itself.

            # Already squared off today — skip both exit and entry logic.
            if self._squareoff_done_date == local_date:
                return

        if not is_flat:
            self._check_exits(close, is_long, is_short)
        else:
            self._check_entries(close, is_flat, is_long, is_short)

    # ─────────────────────────────────────────────────────────────────────
    # RBO (Range Breakout) — per-day state machine.
    # Spec: 5. Logics/rbo_logics.html. Phases IDLE → MONITORING → ENTRY →
    # DONE, reset every UTC day. Range built from the slot's own bar OHLC
    # (spec P8 "Underlying"). Fresh entries are blocked outside an active
    # breakout side; re-entries bypass entirely (handled in _check_entries).
    # ─────────────────────────────────────────────────────────────────────

    _NANOS_PER_DAY = 86_400_000_000_000

    def _rbo_step(self, bar: Bar) -> None:
        """Advance the RBO day state machine by one bar.

        Cheap path: a few integer comparisons and at most two float updates.
        Called every bar when ``rbo_enabled``, regardless of indicator warmup
        (we still need to track the monitoring window during warmup).
        """
        # Day rollover. ts_event is UTC nanoseconds; the floor-divide trick
        # avoids any datetime construction in the hot path.
        bar_day_ns = bar.ts_event - (bar.ts_event % self._NANOS_PER_DAY)
        if self._rbo_last_day_ns != bar_day_ns:
            self._rbo_phase = "IDLE"
            self._rbo_range_high = None
            self._rbo_range_low = None
            self._rbo_triggered_sides = set()
            self._rbo_pending_done = False
            self._rbo_last_day_ns = bar_day_ns

        # Pending DONE transition from a previous bar's cancel_other_side fire.
        # Apply at the *start* of this bar so the phase has already advanced
        # before the rest of the state machine runs.
        if self._rbo_pending_done:
            self._rbo_phase = "DONE"
            self._rbo_pending_done = False

        tod_sec = (bar.ts_event % self._NANOS_PER_DAY) // 1_000_000_000

        cfg = self.config

        # IDLE → MONITORING when monitoring window opens.
        if self._rbo_phase == "IDLE":
            if tod_sec >= cfg.rbo_monitoring_start_sec:
                self._rbo_phase = "MONITORING"

        # MONITORING: roll the high/low. Transition to ENTRY when window closes.
        # The transition check happens BEFORE returning so a single bar that
        # straddles monitoring_end still contributes to the range, then the
        # state advances — matching the spec's "at range_monitoring_end the
        # values are frozen" semantics.
        if self._rbo_phase == "MONITORING":
            high = float(bar.high)
            low = float(bar.low)
            self._rbo_range_high = high if self._rbo_range_high is None else max(self._rbo_range_high, high)
            self._rbo_range_low = low if self._rbo_range_low is None else min(self._rbo_range_low, low)
            if tod_sec >= cfg.rbo_monitoring_end_sec:
                self._rbo_phase = "ENTRY"

        # ENTRY: detect breakouts. Buffer collapses to entry_end the moment
        # the first side fires (spec P6).
        if self._rbo_phase == "ENTRY":
            effective_end = cfg.rbo_entry_end_sec + (
                cfg.rbo_range_buffer_sec if not self._rbo_triggered_sides else 0
            )
            if tod_sec > effective_end:
                self._rbo_phase = "DONE"
                return

            if tod_sec < cfg.rbo_entry_start_sec:
                return  # quiet gap between range freeze and entry-start

            high = float(bar.high)
            low = float(bar.low)

            # HIGH breakout
            if (
                "HIGH" not in self._rbo_triggered_sides
                and self._rbo_range_high is not None
                and high > self._rbo_range_high
                and cfg.rbo_entry_at in ("Any", "RangeHigh")
            ):
                self._rbo_triggered_sides.add("HIGH")
                if cfg.rbo_cancel_other_side:
                    # Per spec P9: phase moves to DONE *after* the breakout
                    # executes — defer to the next bar so this bar's
                    # _check_entries can still let the strategy enter.
                    self._rbo_pending_done = True
                    return

            # LOW breakout
            if (
                "LOW" not in self._rbo_triggered_sides
                and self._rbo_range_low is not None
                and low < self._rbo_range_low
                and cfg.rbo_entry_at in ("Any", "RangeLow")
            ):
                self._rbo_triggered_sides.add("LOW")
                if cfg.rbo_cancel_other_side:
                    self._rbo_pending_done = True
                    return

    def _rbo_allows_entry(self) -> bool:
        """Per-bar gate for fresh entries.

        True only during ENTRY phase with at least one breakout side fired.
        DONE phase blocks all fresh entries (cancel_other_side or past the
        effective deadline). IDLE/MONITORING phases obviously block.
        """
        if self._rbo_phase != "ENTRY":
            return False
        return bool(self._rbo_triggered_sides)

    def _force_squareoff(self) -> None:
        """Squareoff exit: close position without triggering re_execute/reverse."""
        # Same close-fill flag as _handle_exit; without it on_order_filled would
        # mis-classify the closing fill as a new entry.
        self._expecting_close_fill = True
        self.close_all_positions(self.config.instrument_id)
        self._reset_exit_state()

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

        # Flag the upcoming fill as a close — otherwise on_order_filled would
        # set position_side to the opposite side (SELL closing a LONG would
        # incorrectly mark us as SHORT) and get us stuck in an impossible state.
        self._expecting_close_fill = True
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
        # RBO entry gate. Spec rbo_logics.html: re-entries (execute_trigger,
        # i.e. our re_execution_count > 0) bypass the gate so they can fire
        # past entry_end up to portfolio squareoff_time. Fresh entries
        # (re_execution_count == 0) require an active breakout side.
        if (
            self._rbo_enabled
            and self.re_execution_count == 0
            and not self._rbo_allows_entry()
        ):
            return

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
        """Set exit levels when an order fills.

        If this fill is the CLOSE of an existing position (flagged by
        ``_expecting_close_fill`` in ``_handle_exit``), do not treat it as
        an entry — skip state updates so the position stays flat.
        """
        if self._expecting_close_fill:
            self._expecting_close_fill = False
            return

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
                     instrument_id, bar_type, trade_size,
                     order_id_tag: str | None = None,
                     squareoff_time: str | None = None,
                     squareoff_tz: str | None = None,
                     rbo_settings=None) -> ManagedExitConfig:
    """Build a ManagedExitConfig from an ExitConfig dataclass.

    ``order_id_tag`` is optional and passes through to ``StrategyConfig``; when
    multiple strategy instances of the same class coexist in a single engine
    (Direction B shared-engine grouping) it must be unique per instance so
    Nautilus assigns each its own ``strategy_id``.

    ``squareoff_time`` / ``squareoff_tz`` carry the *already-resolved*
    portfolio→slot→leg priority result (see core.models.resolve_squareoff).
    Resolution stays out of this function so the runner can audit/log the
    effective value before engine build.

    ``rbo_settings`` is the ``_RBOSettings`` dataclass (or ``None``) returned
    by ``core.backtest_runner._resolve_rbo``; when provided it switches on the
    per-day RBO state machine inside the strategy. Spec: rbo_logics.html.
    """
    kwargs = dict(
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
        squareoff_minute=_parse_squareoff_minute(squareoff_time),
        squareoff_tz=squareoff_tz or "UTC",
    )
    if order_id_tag is not None:
        kwargs["order_id_tag"] = order_id_tag
    if rbo_settings is not None:
        kwargs.update(
            rbo_enabled=True,
            rbo_monitoring_start_sec=rbo_settings.monitoring_start_sec,
            rbo_monitoring_end_sec=rbo_settings.monitoring_end_sec,
            rbo_entry_start_sec=rbo_settings.entry_start_sec,
            rbo_entry_end_sec=rbo_settings.entry_end_sec,
            rbo_range_buffer_sec=rbo_settings.range_buffer_sec,
            rbo_entry_at=rbo_settings.entry_at,
            rbo_cancel_other_side=rbo_settings.cancel_other_side,
        )
    return ManagedExitConfig(**kwargs)
