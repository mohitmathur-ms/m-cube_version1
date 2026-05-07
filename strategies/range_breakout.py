"""Range Breakout (Opening Range Breakout) Strategy with N-leg pyramid sizing.

Per trading day:
  1. During [range_start_hhmm, leg_range_end_hhmm[i]) each leg i records its
     own high/low range. When no per-leg end is set, all legs share
     ``range_end_hhmm`` and accumulate identical ranges.
  2. After leg i's range window closes (and after leg i-1 has hit its target
     at least once) leg i is eligible to enter when the current bar breaks its
     own upper/lower trigger (range +/- buffer).
  3. After ``breakout_end_hhmm`` no new entries; open legs keep running their
     target/stop logic.
  4. At ``squareoff_hhmm``, force-close all open legs and reset daily state.

Leg pyramid (per side; long and short run independently):
  - Leg 1 fires on the first qualifying breakout bar after its own range window.
  - Leg i (i >= 2) fires only after Leg i-1 has hit its target at least once
    *and* Leg i's own range window has closed.
  - Stop hits close that leg but do NOT promote the next leg.
  - Each leg may re-enter up to ``max_reentries_per_leg`` times per day.

Target/stop basis is user-selectable:
  - 0 = percent of entry price
  - 1 = percent of *that leg's own* range size (range_high - range_low)

Entry price used for target/stop modeling: the bar's open, clamped to the
breakout trigger. Concretely for longs: ``max(upper_trigger, bar.open)``. If
the bar gapped past the trigger we fill at the open; otherwise we fill at the
trigger. This is the modeling entry — the engine still reports realized P&L
from its own fill model.

Intra-bar exit convention: ``pessimistic_intra_bar_exits`` controls what
happens when a bar touches both target and stop. When False (default) the
target resolves first — historical behavior, optimistic. When True the stop
resolves first — conservative; also means target-promotion of the next leg is
blocked on ambiguous bars.

Degenerate range (``range_high == range_low`` with ``target_stop_basis=1``)
disables entries for that leg that day — the target/stop distance would be
zero so every leg would round-trip on its entry bar.

Additional toggles:
  - ``breakout_buffer_mode``: 0 = percent (``breakout_buffer_pct``), 1 = points
    (``breakout_buffer_pts``). Points mode is useful for instruments where a
    fixed tick offset filters noise better than a percentage.
  - ``range_monitoring_type``: 0 = Realtime (wick touches trigger = breakout),
    1 = MinuteClose (bar-close must cross the trigger). MinuteClose kills
    intrabar false breakouts that wick-reverse within the bar.
  - ``one_side_entry_only``: when True, the first side to enter for the day
    locks out the other side until squareoff.
  - ``opposite_side_sl``: when True, stop loss is the opposite end of *that
    leg's* range (low for longs, high for shorts), ignoring ``stop_pct``.
  - ``legN_range_end_hhmm``: per-leg range end time; 0 means inherit the
    shared ``range_end_hhmm``. Single-instrument analog of the classic
    portfolio "leg range breakout" feature — lets later legs use progressively
    wider ranges (larger target/stop under basis=1) without changing instrument.
  - ``first_entry_cutoff_minutes``: if >0, leg 1's very first entry is skipped
    when more than N minutes have passed since leg 1's trigger first fired for
    the day. 0 = disabled (squareoff is the effective cutoff).
  - ``reexecute_cutoff_minutes``: if >0, any re-execution (leg 1 re-entry, or
    any leg 2+ entry) is skipped when more than N minutes have passed since
    the side's initial breakout. 0 = disabled.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy


MAX_LEGS = 10


class RangeBreakoutConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    extra_bar_types: list[BarType] | None = None

    range_start_hhmm: int = 930
    range_end_hhmm: int = 1030
    breakout_end_hhmm: int = 1130
    squareoff_hhmm: int = 1515
    timezone_offset_min: int = 0

    breakout_buffer_pct: float = 0.1
    breakout_buffer_mode: int = 0  # 0 = percent, 1 = points
    breakout_buffer_pts: float = 0.0
    range_monitoring_type: int = 0  # 0 = realtime (wick), 1 = minute close
    target_pct: float = 10.0
    stop_pct: float = 40.0
    target_stop_basis: int = 1  # 0 = entry price, 1 = range size
    opposite_side_sl: bool = False

    num_legs: PositiveInt = 3
    leg1_lots: int = 1
    leg2_lots: int = 1
    leg3_lots: int = 1
    leg4_lots: int = 1
    leg5_lots: int = 1
    leg6_lots: int = 1
    leg7_lots: int = 1
    leg8_lots: int = 1
    leg9_lots: int = 1
    leg10_lots: int = 1
    # 0 = inherit shared range_end_hhmm; HHMM otherwise (e.g., 945 = 09:45).
    leg1_range_end_hhmm: int = 0
    leg2_range_end_hhmm: int = 0
    leg3_range_end_hhmm: int = 0
    leg4_range_end_hhmm: int = 0
    leg5_range_end_hhmm: int = 0
    leg6_range_end_hhmm: int = 0
    leg7_range_end_hhmm: int = 0
    leg8_range_end_hhmm: int = 0
    leg9_range_end_hhmm: int = 0
    leg10_range_end_hhmm: int = 0

    max_reentries_per_leg: int = 3
    enable_long: bool = True
    enable_short: bool = True
    pessimistic_intra_bar_exits: bool = False
    one_side_entry_only: bool = False
    first_entry_cutoff_minutes: int = 0   # 0 = disabled (squareoff is the de-facto cutoff)
    reexecute_cutoff_minutes: int = 0     # 0 = disabled


@dataclass
class LegState:
    active: bool = False
    entry_price: float = 0.0
    ever_hit_target: bool = False
    entry_count: int = 0

    def reset(self) -> None:
        self.active = False
        self.entry_price = 0.0
        self.ever_hit_target = False
        self.entry_count = 0


def _hhmm_to_min(hhmm: int) -> int:
    return (hhmm // 100) * 60 + (hhmm % 100)


class RangeBreakoutStrategy(Strategy):
    """Opening Range Breakout with N-leg pyramiding (target promotes next leg)."""

    def __init__(self, config: RangeBreakoutConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument = None

        n = int(config.num_legs)
        if n < 1:
            n = 1
        if n > MAX_LEGS:
            n = MAX_LEGS
        self._n_legs = n
        self._leg_lots: list[int] = [
            int(getattr(config, f"leg{i}_lots")) for i in range(1, n + 1)
        ]

        # Precompute per-leg quantity as Decimal; Quantity objects are built in
        # on_start once the instrument is available from the cache.
        self._leg_qty_decimals: list[Decimal] = [
            config.trade_size * Decimal(lots) for lots in self._leg_lots
        ]
        self._leg_qty: list[Quantity] = []

        # Hot-path config values cached as primitives.
        self._buf_pct = float(config.breakout_buffer_pct)
        self._buf_mode = int(config.breakout_buffer_mode)
        self._buf_pts = float(config.breakout_buffer_pts)
        self._monitoring = int(config.range_monitoring_type)
        self._tgt_pct = float(config.target_pct)
        self._stop_pct = float(config.stop_pct)
        self._basis = int(config.target_stop_basis)
        self._opposite_range_sl = bool(config.opposite_side_sl)
        self._max_reentries = int(config.max_reentries_per_leg)
        self._enable_long = bool(config.enable_long)
        self._enable_short = bool(config.enable_short)
        self._pessimistic_exits = bool(config.pessimistic_intra_bar_exits)
        self._one_side_only = bool(config.one_side_entry_only)
        self._first_entry_cutoff = int(config.first_entry_cutoff_minutes)
        self._reexecute_cutoff = int(config.reexecute_cutoff_minutes)

        self._range_start_min = _hhmm_to_min(config.range_start_hhmm)
        self._range_end_min = _hhmm_to_min(config.range_end_hhmm)
        self._breakout_end_min = _hhmm_to_min(config.breakout_end_hhmm)
        self._squareoff_min = _hhmm_to_min(config.squareoff_hhmm)
        self._tz_delta = timedelta(minutes=int(config.timezone_offset_min))

        # Per-leg range end (in minutes). 0 in config ⇒ inherit shared.
        self._leg_range_end_min: list[int] = []
        for i in range(1, n + 1):
            override = int(getattr(config, f"leg{i}_range_end_hhmm"))
            self._leg_range_end_min.append(
                _hhmm_to_min(override) if override > 0 else self._range_end_min
            )

        self._current_day: date | None = None
        # Per-leg range accumulation + derived values (reset daily).
        self._leg_range_high: list[float | None] = [None] * n
        self._leg_range_low: list[float | None] = [None] * n
        self._leg_upper_trigger: list[float | None] = [None] * n
        self._leg_lower_trigger: list[float | None] = [None] * n
        self._leg_tgt_delta: list[float] = [0.0] * n
        self._leg_stop_delta: list[float] = [0.0] * n
        self._side_locked: OrderSide | None = None
        # First bar per side where leg 1's trigger fires — reference for
        # first_entry_cutoff_minutes / reexecute_cutoff_minutes.
        self._first_breakout_min: dict[OrderSide, int | None] = {
            OrderSide.BUY: None,
            OrderSide.SELL: None,
        }
        self._long_legs: list[LegState] = [LegState() for _ in range(n)]
        self._short_legs: list[LegState] = [LegState() for _ in range(n)]

    # ── Nautilus lifecycle ────────────────────────────────────────────────
    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return
        self._leg_qty = [self.instrument.make_qty(q) for q in self._leg_qty_decimals]
        self.subscribe_bars(self.config.bar_type)
        if self.config.extra_bar_types:
            for bt in self.config.extra_bar_types:
                self.subscribe_bars(bt)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)

    def on_bar(self, bar: Bar) -> None:
        if bar.bar_type != self.config.bar_type:
            return

        local_dt = datetime.fromtimestamp(bar.ts_event / 1e9, tz=timezone.utc) + self._tz_delta
        local_min = local_dt.hour * 60 + local_dt.minute
        local_date = local_dt.date()

        if self._current_day != local_date:
            if self._current_day is not None:
                self._force_close_all()
            self._reset_day_state()
            self._current_day = local_date

        if local_min >= self._squareoff_min:
            self._force_close_all()
            return

        if local_min < self._range_start_min:
            return

        bar_high = float(bar.high)
        bar_low = float(bar.low)

        # Per-leg range accumulation + lazy trigger finalization.
        any_leg_ready = False
        for i in range(self._n_legs):
            end_min = self._leg_range_end_min[i]
            if local_min < end_min:
                # Still inside leg i's range window — accumulate.
                rh = self._leg_range_high[i]
                rl = self._leg_range_low[i]
                self._leg_range_high[i] = bar_high if rh is None else (bar_high if bar_high > rh else rh)
                self._leg_range_low[i] = bar_low if rl is None else (bar_low if bar_low < rl else rl)
            elif self._leg_upper_trigger[i] is None:
                # Leg i's window has just closed — finalize once.
                if self._leg_range_high[i] is not None:
                    self._finalize_leg_triggers(i)
                    any_leg_ready = True
            else:
                any_leg_ready = True

        if not any_leg_ready:
            return

        in_entry_window = local_min < self._breakout_end_min
        bar_open = float(bar.open)
        bar_close = float(bar.close) if self._monitoring == 1 else 0.0

        long_blocked = self._one_side_only and self._side_locked == OrderSide.SELL
        short_blocked = self._one_side_only and self._side_locked == OrderSide.BUY

        if self._enable_long and not long_blocked:
            self._process_side(
                side=OrderSide.BUY,
                legs=self._long_legs,
                bar_open=bar_open,
                bar_high=bar_high,
                bar_low=bar_low,
                bar_close=bar_close,
                in_entry_window=in_entry_window,
                local_min=local_min,
            )
        if self._enable_short and not short_blocked:
            self._process_side(
                side=OrderSide.SELL,
                legs=self._short_legs,
                bar_open=bar_open,
                bar_high=bar_high,
                bar_low=bar_low,
                bar_close=bar_close,
                in_entry_window=in_entry_window,
                local_min=local_min,
            )

        # Latch side lock after processing; long is checked first so it wins
        # on bars where both sides would have qualified.
        if self._one_side_only and self._side_locked is None:
            if any(lg.active for lg in self._long_legs):
                self._side_locked = OrderSide.BUY
            elif any(lg.active for lg in self._short_legs):
                self._side_locked = OrderSide.SELL

    def _finalize_leg_triggers(self, i: int) -> None:
        rh = self._leg_range_high[i]
        rl = self._leg_range_low[i]
        if self._buf_mode == 1:
            up_trig = rh + self._buf_pts
            dn_trig = rl - self._buf_pts
        else:
            buf = self._buf_pct / 100.0
            up_trig = rh * (1.0 + buf)
            dn_trig = rl * (1.0 - buf)
        if self._basis == 1:
            rng = rh - rl
            if rng <= 0.0:
                # Degenerate range: block entries on this leg for the day.
                self._leg_upper_trigger[i] = float("inf")
                self._leg_lower_trigger[i] = float("-inf")
            else:
                self._leg_upper_trigger[i] = up_trig
                self._leg_lower_trigger[i] = dn_trig
            self._leg_tgt_delta[i] = rng * (self._tgt_pct / 100.0)
            self._leg_stop_delta[i] = rng * (self._stop_pct / 100.0)
        else:
            self._leg_upper_trigger[i] = up_trig
            self._leg_lower_trigger[i] = dn_trig

    # ── Core per-side bar handler ─────────────────────────────────────────
    def _process_side(
        self,
        side: OrderSide,
        legs: list[LegState],
        bar_open: float,
        bar_high: float,
        bar_low: float,
        bar_close: float,
        in_entry_window: bool,
        local_min: int,
    ) -> None:
        pessimistic = self._pessimistic_exits
        minute_close = self._monitoring == 1
        is_buy = side == OrderSide.BUY

        # 1. Exit pass: check target/stop on each active leg.
        for idx, leg in enumerate(legs):
            if not leg.active:
                continue
            tgt = self._target_price(leg.entry_price, side, idx)
            stp = self._stop_price(leg.entry_price, side, idx)
            if is_buy:
                tgt_hit = bar_high >= tgt
                stp_hit = bar_low <= stp
            else:
                tgt_hit = bar_low <= tgt
                stp_hit = bar_high >= stp
            if pessimistic:
                if stp_hit:
                    self._exit_leg(side, legs, idx)
                elif tgt_hit:
                    self._exit_leg(side, legs, idx)
                    leg.ever_hit_target = True
            else:
                if tgt_hit:
                    self._exit_leg(side, legs, idx)
                    leg.ever_hit_target = True
                elif stp_hit:
                    self._exit_leg(side, legs, idx)

        # 2. Entry pass: eligible legs may (re-)enter when their own breakout
        # trigger is active.
        if not in_entry_window:
            return
        for idx, leg in enumerate(legs):
            if leg.active:
                continue
            if idx > 0 and not legs[idx - 1].ever_hit_target:
                break  # leg i-1 has not yet hit target; no leg >= i can fire
            if leg.entry_count > self._max_reentries:
                continue
            up_trig = self._leg_upper_trigger[idx]
            dn_trig = self._leg_lower_trigger[idx]
            if up_trig is None:
                # Leg's range window hasn't closed yet — wait.
                continue
            if is_buy:
                trig_src = bar_close if minute_close else bar_high
                if trig_src < up_trig:
                    continue
                entry_fill = up_trig if bar_open < up_trig else bar_open
            else:
                trig_src = bar_close if minute_close else bar_low
                if trig_src > dn_trig:
                    continue
                entry_fill = dn_trig if bar_open > dn_trig else bar_open
            # Record the side's initial breakout moment on leg 1's first-ever
            # qualifying bar, then gate by elapsed-minutes-since-breakout.
            if idx == 0 and self._first_breakout_min[side] is None:
                self._first_breakout_min[side] = local_min
            ref = self._first_breakout_min[side]
            if ref is not None:
                elapsed = local_min - ref
                is_first_ever = idx == 0 and leg.entry_count == 0
                cutoff = self._first_entry_cutoff if is_first_ever else self._reexecute_cutoff
                if cutoff > 0 and elapsed > cutoff:
                    continue
            self._enter_leg(side, legs, idx, entry_fill)

    # ── Pricing helpers ───────────────────────────────────────────────────
    def _target_price(self, entry: float, side: OrderSide, idx: int) -> float:
        delta = (
            entry * (self._tgt_pct / 100.0)
            if self._basis == 0
            else self._leg_tgt_delta[idx]
        )
        return entry + delta if side == OrderSide.BUY else entry - delta

    def _stop_price(self, entry: float, side: OrderSide, idx: int) -> float:
        if self._opposite_range_sl:
            # Longs stop at leg's range_low, shorts at leg's range_high.
            rl = self._leg_range_low[idx]
            rh = self._leg_range_high[idx]
            return rl if side == OrderSide.BUY else rh
        delta = (
            entry * (self._stop_pct / 100.0)
            if self._basis == 0
            else self._leg_stop_delta[idx]
        )
        return entry - delta if side == OrderSide.BUY else entry + delta

    # ── Order plumbing ────────────────────────────────────────────────────
    def _submit_market(self, side: OrderSide, qty: Quantity, reason: str | None = None) -> None:
        kwargs = dict(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=qty,
            time_in_force=TimeInForce.GTC,
        )
        if reason:
            kwargs["tags"] = [reason]
        order = self.order_factory.market(**kwargs)
        self.submit_order(order)

    def _enter_leg(self, side: OrderSide, legs: list[LegState], idx: int, price: float) -> None:
        if self._leg_qty_decimals[idx] <= 0:
            return
        # Forensic tag: which leg, which side broke out, the range it broke,
        # and the current price. Lands in fills_report["tags"] -> orderbook's
        # "ENTRY DETAILED REASON" column.
        leg_no = idx + 1
        rh = self._leg_range_high[idx] if idx < len(self._leg_range_high) else None
        rl = self._leg_range_low[idx] if idx < len(self._leg_range_low) else None
        if side == OrderSide.BUY:
            reason = f"RBO leg{leg_no} BUY: price={price:.4f} > range_high={rh:.4f}" if rh is not None else f"RBO leg{leg_no} BUY @ {price:.4f}"
        else:
            reason = f"RBO leg{leg_no} SELL: price={price:.4f} < range_low={rl:.4f}" if rl is not None else f"RBO leg{leg_no} SELL @ {price:.4f}"
        self._submit_market(side, self._leg_qty[idx], reason)
        leg = legs[idx]
        leg.active = True
        leg.entry_price = price
        leg.entry_count += 1

    def _exit_leg(self, side: OrderSide, legs: list[LegState], idx: int) -> None:
        leg = legs[idx]
        if not leg.active:
            return
        if self._leg_qty_decimals[idx] <= 0:
            leg.active = False
            return
        opposite = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
        self._submit_market(opposite, self._leg_qty[idx])
        leg.active = False

    def _force_close_all(self) -> None:
        any_active = any(lg.active for lg in self._long_legs) or any(
            lg.active for lg in self._short_legs
        )
        if not any_active:
            return
        for idx, leg in enumerate(self._long_legs):
            if leg.active:
                self._exit_leg(OrderSide.BUY, self._long_legs, idx)
        for idx, leg in enumerate(self._short_legs):
            if leg.active:
                self._exit_leg(OrderSide.SELL, self._short_legs, idx)
        # Belt-and-suspenders: flatten any residual net position.
        self.close_all_positions(self.config.instrument_id)

    def _reset_day_state(self) -> None:
        n = self._n_legs
        for i in range(n):
            self._leg_range_high[i] = None
            self._leg_range_low[i] = None
            self._leg_upper_trigger[i] = None
            self._leg_lower_trigger[i] = None
            self._leg_tgt_delta[i] = 0.0
            self._leg_stop_delta[i] = 0.0
        self._side_locked = None
        self._first_breakout_min[OrderSide.BUY] = None
        self._first_breakout_min[OrderSide.SELL] = None
        for leg in self._long_legs:
            leg.reset()
        for leg in self._short_legs:
            leg.reset()


# ── Registry exports ──
STRATEGY_NAME = "Range Breakout"
STRATEGY_CLASS = RangeBreakoutStrategy
CONFIG_CLASS = RangeBreakoutConfig
DESCRIPTION = (
    "Opening Range Breakout with N-leg pyramid (target promotes next leg, stops don't). "
    "Each leg may use its own range end time for wider-range pyramiding on the same instrument."
)
PARAMS = {
    "range_start_hhmm": {"label": "Range Start", "min": 0, "max": 2359, "default": 930, "type": "time"},
    "range_end_hhmm": {"label": "Range End", "min": 0, "max": 2359, "default": 1030, "type": "time"},
    "breakout_end_hhmm": {"label": "Last New Entry", "min": 0, "max": 2359, "default": 1130, "type": "time"},
    "squareoff_hhmm": {"label": "Squareoff", "min": 0, "max": 2359, "default": 1515, "type": "time"},
    "timezone_offset_min": {"label": "TZ Offset (min, UTC=0, IST=330)", "min": -720, "max": 720, "default": 0},
    "breakout_buffer_pct": {"label": "Breakout Buffer %", "min": 0.0, "max": 5.0, "default": 0.1},
    "breakout_buffer_mode": {"label": "Buffer Mode (0=percent, 1=points)", "min": 0, "max": 1, "default": 0},
    "breakout_buffer_pts": {"label": "Breakout Buffer (points)", "min": 0.0, "max": 1_000_000.0, "default": 0.0},
    "range_monitoring_type": {"label": "Range Monitoring (0=Realtime, 1=MinuteClose)", "min": 0, "max": 1, "default": 0},
    "target_pct": {"label": "Per-Leg Target %", "min": 0.1, "max": 100.0, "default": 10.0},
    "stop_pct": {"label": "Per-Leg Stop %", "min": 0.1, "max": 100.0, "default": 40.0},
    "target_stop_basis": {"label": "Target/Stop Basis (0=entry price, 1=range size)", "min": 0, "max": 1, "default": 1},
    "opposite_side_sl": {"label": "SL = Opposite Side of Range", "default": False},
    "num_legs": {"label": "Number of Legs", "min": 1, "max": MAX_LEGS, "default": 3},
}
for _i in range(1, MAX_LEGS + 1):
    PARAMS[f"leg{_i}_lots"] = {"label": f"Leg {_i} Lots", "min": 0, "max": 1000, "default": 1}
for _i in range(1, MAX_LEGS + 1):
    PARAMS[f"leg{_i}_range_end_hhmm"] = {
        "label": f"Leg {_i} Range End",
        "min": 0, "max": 2359, "default": 0,
        "type": "time",
        "inherit_zero": True,
        "placeholder": "(inherit)",
    }
PARAMS.update({
    "max_reentries_per_leg": {"label": "Max Re-entries / Leg", "min": 0, "max": 10, "default": 3},
    "enable_long": {"label": "Enable Long Side", "default": True},
    "enable_short": {"label": "Enable Short Side", "default": True},
    "pessimistic_intra_bar_exits": {
        "label": "Pessimistic Intra-Bar Exits (stop wins when both hit)",
        "default": False,
    },
    "one_side_entry_only": {
        "label": "One Side Entry Only (first breakout wins, other side locked)",
        "default": False,
    },
    "first_entry_cutoff_minutes": {
        "label": "First-Entry Cutoff Min (0=disabled, squareoff acts as default)",
        "min": 0, "max": 720, "default": 0,
    },
    "reexecute_cutoff_minutes": {
        "label": "Re-Execute Cutoff Min (0=disabled)",
        "min": 0, "max": 720, "default": 0,
    },
})
