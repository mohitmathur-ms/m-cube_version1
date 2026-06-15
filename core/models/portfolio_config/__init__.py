"""Portfolio-level configuration (the outermost schema dataclass).

``PortfolioConfig`` groups multiple ``StrategySlotConfig`` slots with
portfolio-wide settings: capital, max-loss/profit, allocation mode, date range,
square-off (incl. MIS/NRML product handling), day-of-week & intra-day entry
filters, winter-time adjustment, Range Breakout (RBO), Other-Settings,
portfolio-level SL/Target (+ trailing, Move-SL-to-Cost), ReExecute / Exit /
Monitoring tabs, and the slot list itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from core.models.strategy_slot_config import StrategySlotConfig


@dataclass
class PortfolioConfig:
    """Groups multiple strategy slots with portfolio-level settings."""

    name: str = "New Portfolio"
    description: str = ""
    # Portfolio Tag (spec §11). Groups this portfolio with others sharing the
    # same tag so a tag-level SL/Target (defined in config/tags.json) can clip
    # the whole group — a risk tier BETWEEN portfolio and user. None / "" = no
    # tag (this portfolio is only subject to its own and the user-level caps).
    # Distinct from the live-only broker ``strategy_tag`` UI field.
    portfolio_tag: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    starting_capital: float = 100_000.0
    max_loss: Optional[float] = None
    max_profit: Optional[float] = None
    allocation_mode: str = "equal"  # "equal" or "percentage"
    start_date: Optional[str] = None  # ISO date applied to all slots unless a slot overrides it
    end_date: Optional[str] = None
    # Portfolio-level default square-off time. Slot or leg level can override.
    squareoff_time: Optional[str] = None  # "HH:MM"
    squareoff_tz: Optional[str] = None    # IANA name, e.g. "America/New_York"
    # Product type (Live trading: MIS/NRML control broker margin & auto-squareoff.
    # Backtest: MIS supplies a default squareoff_time when none is set on
    # portfolio/slot/leg via effective_portfolio_squareoff(). NRML is a no-op
    # for backtest. Leverage/margin are NOT modeled — default_leverage stays 1.)
    product: Optional[str] = None                   # "MIS" | "NRML" | None
    mis_squareoff_time: Optional[str] = None        # "HH:MM", used only when product=="MIS"
    mis_squareoff_tz: Optional[str] = None          # IANA name, e.g. "Asia/Kolkata"
    # Day-of-week filter for backtest. None / empty list means all 7 days allowed.
    # Values: subset of ["MON","TUE","WED","THU","FRI","SAT","SUN"] (case-insensitive,
    # full names like "Monday" also accepted). When set, bars on excluded weekdays
    # are dropped before the engine sees them, so trades cannot fire on those days.
    # Weekday is computed from each bar's UTC ts_event.
    run_on_days: Optional[list[str]] = None
    # Intra-day entry window. Both endpoints in HH:MM format, UTC.
    # When either is set, bars outside [entry_start_time, entry_end_time] are
    # dropped before the engine sees them. Caveat: bars dropped at the tail
    # mean the strategy can't process exits past entry_end_time, so set
    # ``squareoff_time`` to the same time as ``entry_end_time`` if you need
    # forced closes at end-of-window.
    entry_start_time: Optional[str] = None  # "HH:MM" UTC, e.g. "09:30"
    entry_end_time: Optional[str] = None    # "HH:MM" UTC, e.g. "16:00"

    # Winter Time Adjustment (spec execution_logic_target.html §9). For
    # US-listed instruments whose data/config straddle a DST boundary, the
    # engine shifts every configured local time (entry window, square-off,
    # RBO windows, slot/leg square-off overrides) by +1 hour via
    # ``add_one_hour`` before applying them. India/NSE instruments don't
    # observe DST, so leave this off for them. User-controlled because
    # reliable per-instrument DST detection needs timezone metadata the
    # catalog doesn't carry. Applied once at run start in
    # ``backtest_runner._apply_winter_time``. Default off → no shift.
    winter_time_adjust: bool = False
    # Auto US-DST detection (opt-in). When True, the engine derives the winter
    # shift from the portfolio's primary US venue (CME/NYMEX/COMEX/CBOT/NYSE/
    # NASDAQ/ARCA → America/Chicago|New_York) and the run START date via
    # ``zoneinfo``: if that date is in US STANDARD time (winter), the +1h shift
    # is applied automatically — no manual toggle. No-op for non-US venues
    # (FX/crypto/NSE never observe US DST). CAVEAT: this is a whole-run
    # determination keyed on start_date; a run spanning a DST boundary should
    # be split. The explicit ``winter_time_adjust`` flag still forces the shift.
    winter_time_auto: bool = False

    # Range Breakout (RBO). When rbo_enabled, the entry timing for all enabled
    # slots is gated by a per-day state machine: range is built during
    # [range_monitoring_start, range_monitoring_end], frozen at the end of
    # that window, then breakouts above/below the range during
    # [rbo_entry_start, rbo_entry_end] arm strategy entries. Spec:
    # 5. Logics/rbo_logics.html. All times are HH:MM:SS UTC. range_buffer is
    # an integer number of MINUTES (NOT a price buffer) — extends entry_end
    # only until the first side fires, then collapses back per spec P6.
    rbo_enabled: bool = False
    range_monitoring_start: Optional[str] = None  # P2
    range_monitoring_end: Optional[str] = None    # P3 (UI auto-syncs rbo_entry_start to this)
    rbo_entry_start: Optional[str] = None         # P4
    rbo_entry_end: Optional[str] = None           # P5
    rbo_range_buffer: int = 0                     # P6 — minutes
    # P7. Backend-validated values: "Any" / "RangeHigh" / "RangeLow".
    # Options-only values "C_OnHigh_P_OnLow" / "P_OnHigh_C_OnLow" are silently
    # downgraded to "Any" with a warning log when used on FX/crypto.
    rbo_entry_at: str = "Any"
    # P8. Only "Underlying" is implemented; other values disable RBO with an
    # error log per spec validation rules.
    rbo_monitoring: str = "Underlying"
    rbo_cancel_other_side: bool = False           # P9

    # Other Settings tab. Spec: 5. Logics/Other_Settings_Logic.html.
    # delay_between_legs_sec: post-re-execution delay. Spec defines this at
    #   PORTFOLIO level (after a portfolio SL/TP triggers ReExecute). Our
    #   slot-independent architecture has no portfolio-level SL/TP that fires
    #   re-execution, so we apply this at the SLOT level: after a slot's own
    #   SL/TP triggers re_execute (per ExitConfig.on_sl_action / .on_target_action
    #   = "re_execute"), block the next entry on that slot until N seconds of
    #   bar-time have elapsed. Default 0 = no delay (matches spec).
    delay_between_legs_sec: int = 0
    # on_sl_action_on / on_target_action_on: filter that decides whether the
    # configured exit action fires when the SL/TP hit was the FIXED level vs
    # a TRAILING level that was moved by trailing_sl_step / target_lock.
    # Spec semantics defined for portfolio-level SL/TP; we adapt at slot
    # level using a was_trailed flag in ManagedExitStrategy.
    #   Values: "OnSL_N_Trailing_Both" (default — fire on any SL hit),
    #           "OnSL_Only" (suppress action when SL was trailed),
    #           "OnSL_Trailing_Only" (suppress action when SL was the fixed
    #            initial value). Same shape for on_target_action_on with
    #           "OnTarget_*" prefixes.
    # Note: OnTarget_Trailing_Only is effectively suppress-all on FX/crypto
    # because we have no trailing-target concept distinct from fixed TP
    # (target_lock exits route through SL path, not TP path).
    on_sl_action_on: str = "OnSL_N_Trailing_Both"
    on_target_action_on: str = "OnTarget_N_Trailing_Both"
    # Stored but UNUSED for FX/crypto — both are options-specific (Straddle)
    # or spec-missing (Trail Wait Trade). Kept on the schema so user-saved
    # values survive round-trips for when these features land.
    straddle_width_multiplier: float = 0.0
    trail_wait_trade: bool = False

    # Portfolio-level Stoploss & Target. Spec: 5. Logics/portfolio_sl_tgt.html.
    # Applied post-hoc in _merge_portfolio_results via _apply_portfolio_clip:
    # the unified equity curve is walked in time order, fixed/trailing SL & TP
    # are evaluated each tick (spec §8 evaluation order), and trades after the
    # clip timestamp are dropped from the merged outputs. All values are raw
    # PnL amounts (positive numbers; SL fires at PnL ≤ −value, TP at PnL ≥
    # value). Defaults all "off" — feature is opt-in via the *_enabled flags.
    # Type fields accept only the universal value for FX/crypto; options-only
    # values (Combined Premium, Underlying Movement, etc.) are silently
    # downgraded with a warning by _resolve_pf_stoploss / _resolve_pf_target.

    # ── Stoploss Settings (spec §1) ──
    pf_sl_enabled: bool = False
    # pf_sl_type accepts (FX/crypto): "Combined Loss", "Underlying Movement",
    # "Loss and Underlying Range". Underlying-based types default the underlying
    # to the portfolio's primary slot instrument (D5 "underlying = self").
    pf_sl_type: str = "Combined Loss"
    pf_sl_value: float = 0.0
    # Stoploss Value input mode (spec §2.1 — "accepts absolute or % input").
    # When True and the SL type is PnL-based (Combined Loss), pf_sl_value is a
    # PERCENT of starting_capital, converted to a currency amount at resolve
    # time. Ignored for underlying-price types (where the value is a price
    # level). Default False = absolute currency (unchanged behavior).
    pf_sl_value_is_pct: bool = False
    # Underlying price bounds for the "Underlying Movement" / "Loss and
    # Underlying Range" SL types. The SL fires when the primary instrument's
    # price crosses out of [below, above]. 0.0 on a bound disables that side.
    pf_sl_underlying_below: float = 0.0
    pf_sl_underlying_above: float = 0.0
    pf_sl_action: str = "SqOff"        # SqOff | ReExecute (others options-only)
    pf_sl_delay_sec: int = 0
    pf_sl_reexecute_count: int = 0     # 0 = unlimited per spec §1.7
    pf_sl_sqoff_only_loss_legs: bool = False    # spec §1.9
    pf_sl_sqoff_only_profit_legs: bool = False  # spec §1.10 (mutually exclusive with above)

    # ── Trailing SL Settings (spec §2) ──
    pf_sl_trail_enabled: bool = False
    pf_sl_trail_every: float = 0.0  # ratchet step in PnL
    pf_sl_trail_by: float = 0.0     # SL tightens by this per step

    # ── Move SL to Cost (spec §3) — applied per-slot via ManagedExitStrategy ──
    move_sl_enabled: bool = False
    move_sl_safety_sec: int = 0
    move_sl_action: str = "Move Only for Profitable Legs"
    move_sl_trail_after: bool = False
    move_sl_no_buy_legs: bool = False  # adapted: skip move-to-cost on LONG positions
    move_sl_hit_on_leg_sl: bool = False     # cross-slot trigger applied post-hoc
    move_sl_hit_on_leg_target: bool = False # same shape, on any slot's target
    move_sl_ltp_buffer: float = 0.0  # spec §3 action v3 — LTP + Buffer for loss legs
    # Portfolio-aggregate Move SL trigger (spec §2.3). When enabled, the
    # two-pass runner (gated by _USE_PF_AGG_MOVE_SL) finds the first time the
    # whole portfolio's combined P&L crosses ``move_sl_agg_pnl_threshold`` and
    # snaps every open leg's SL to its entry price from that point on.
    # ``direction`` = "loss" → trigger when combined PnL ≤ -threshold;
    # "profit" → trigger when combined PnL ≥ +threshold.
    move_sl_agg_pnl_enabled: bool = False
    move_sl_agg_pnl_threshold: float = 0.0
    move_sl_agg_pnl_direction: str = "loss"

    # ── Cross-portfolio targets (spec §2.1(m) / §2.4(d)) ──
    # Name of the other portfolio to act on when on_sl_action / on_target_action
    # is one of: SqOff Other Portfolio, Execute Other Portfolio, Start Other
    # Portfolio. Empty string disables cross-portfolio dispatch.
    pf_sl_target_portfolio: str = ""
    pf_tgt_target_portfolio: str = ""

    # ── Target Settings (spec §4) ──
    pf_tgt_enabled: bool = False
    pf_tgt_type: str = "Combined Profit"  # only universal value for FX/crypto
    pf_tgt_value: float = 0.0
    # Target Value input mode (spec §2.4). When True and the type is PnL-based
    # (Combined Profit), pf_tgt_value is a PERCENT of starting_capital. Ignored
    # for underlying-price types. Default False = absolute (unchanged).
    pf_tgt_value_is_pct: bool = False
    pf_tgt_action: str = "SqOff"
    pf_tgt_delay_sec: int = 0
    pf_tgt_reexecute_count: int = 0  # 0 = unlimited
    # Selective partial-close on a portfolio Target hit (sl_features.html §2.4 —
    # Target table lists both as supported). Mirror of the SL pair (§1.9/§1.10),
    # mutually exclusive. Default False = full SqOff (unchanged behavior).
    pf_tgt_sqoff_only_loss_legs: bool = False
    pf_tgt_sqoff_only_profit_legs: bool = False

    # ── Trailing Target Settings (spec §5) ──
    pf_tgt_trail_enabled: bool = False
    pf_tgt_trail_lock_min_profit: float = 0.0
    pf_tgt_trail_when_profit_reach: float = 0.0
    pf_tgt_trail_every: float = 0.0
    pf_tgt_trail_by: float = 0.0

    # ── ReExecute Tab (spec: 5. Logics/ReExecute_Logics.html) ──
    # P1 (no_reexec_sl_cost): WIRED for FX/crypto. When True, suppresses the
    #   re_execute action when the slot's SL was raised to entry via Move SL to
    #   Cost (per-position or per-trading-day; ManagedExitStrategy gate).
    # P2 (no_wait_trade_reexec): WIRED — skips the slot re-execution delay
    #   (delay_between_legs_sec) on re-executions. Effect only when that delay
    #   is > 0 (default 0), so it is inert for the common FX case. The literal
    #   options "Wait & Trade" pre-entry concept does not exist on FX; this is
    #   the FX adaptation (the delay it gates is our nearest analogue).
    # P3 (no_strike_change_reexec): options-only — strikes (ATM, ATM±N)
    #   don't exist for FX/crypto. Stored, marked gray in UI.
    # P4 (no_reentry_after_end): WIRED — blocks ReExecute / ReEntry past the
    #   intraday entry_end_time (post-window bars are kept and gated inside the
    #   strategy, NOT pre-filtered). Effect only when an intraday entry window
    #   is configured; inert for 24/5 FX portfolios with no entry window.
    # P5 (no_reentry_sl_cost): WIRED — blocks the re_entry action when SL was
    #   moved to cost. Default ON (spec).
    no_reexec_sl_cost: bool = False
    no_wait_trade_reexec: bool = False
    no_strike_change_reexec: bool = False
    no_reentry_after_end: bool = False
    no_reentry_sl_cost: bool = True  # spec default ON

    # ── Exit Settings Tab (spec: 5. Logics/Exit_Settings_Logics.html) ──
    # exit_order_type: only "MARKET" is implemented (spec confirms even for
    #   options). "Limit" / "SL_Limit" are not implemented; spec blocks them
    #   at portfolio save in the original engine.
    # exit_sell_first: spec §8.2 — within a batch exit (Squareoff / Portfolio
    #   SqOff), log SELL-leg closes before BUY-leg closes. Read by the orderbook
    #   builder (report_generator._build_orderbook). Log order ONLY — the engine
    #   has no intra-bar leg-exit delay, so fills/PnL are unchanged; a no-op for
    #   independent (different-entry-time) FX/crypto legs, visible for multi-leg
    #   same-entry groups (options-style straddles).
    # on_portfolio_complete: only "None" is wired. The 3 cross-portfolio
    #   actions need cross-portfolio infrastructure that doesn't exist.
    exit_order_type: str = "MARKET"
    exit_sell_first: bool = True
    on_portfolio_complete: str = "None"
    # Conservative VWAP exit-fill model (spec execution_logic.html §4.2 / §8.1).
    # When True, leg SL/Target exit fills are repriced post-run to the
    # direction-aware conservative price: SELL exits = max(vwap, hit),
    # BUY exits = min(vwap, hit), where vwap is the opposite-quote-side per-bar
    # typical price and hit is the trigger-side bar extreme (see
    # backtest_runner._apply_vwap_fill). Requires paired ASK/BID bars in the
    # catalog (Format A); on a single-series slot (Format B) it uses that
    # series' own VWAP + high/low; a no-op only where no usable series exists.
    # Default ON so leg SL/Target fills follow spec §4.2 out of the box; a
    # portfolio JSON may set it false to opt out. Also force-enabled by the
    # _USE_VWAP_FILL env flag (dev/parity tooling).
    vwap_exit_fill: bool = True
    # Directional-close exit fill (spec execution_logic.html §8.1). Base price
    # for the non-SL/Target exits (squareoff/EOD): SELL→ask_close, BUY→bid_close,
    # modelling the half-spread paid on exit. Composes with vwap_exit_fill (VWAP
    # owns the §4.2 leg SL/Target exits; this owns the rest). Default ON; a
    # portfolio JSON may set it false. Also force-enabled by _USE_DIRECTIONAL_FILL.
    directional_close_fill: bool = True

    # ── Monitoring Tab (no spec doc found in 5. Logics/) ──
    # All 6 fields are evaluation-frequency settings (Realtime / MinuteClose
    # / Interval) for live-trading monitoring; backtest processes every bar
    # in order so there's no analogue. Stored for round-trip only.
    leg_target_monitoring: str = "Realtime"
    leg_trailing_monitoring: str = "Realtime"
    leg_sl_monitoring: str = "Realtime"
    leg_sl_trailing_monitoring: str = "Realtime"
    combined_target_monitoring: str = "Realtime"
    combined_sl_monitoring: str = "Realtime"

    slots: list[StrategySlotConfig] = field(default_factory=list)

    def add_slot(self, slot: StrategySlotConfig) -> None:
        self.slots.append(slot)
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def remove_slot(self, slot_id: str) -> None:
        self.slots = [s for s in self.slots if s.slot_id != slot_id]
        self.updated_at = datetime.now(timezone.utc).isoformat()

    @property
    def enabled_slots(self) -> list[StrategySlotConfig]:
        return [s for s in self.slots if s.enabled]
