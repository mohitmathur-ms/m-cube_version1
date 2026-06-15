"""
ManagedExitStrategy - Wraps any signal logic with SL/TP/trailing/target locking.

Used by the portfolio system to add exit management to any strategy from the signal registry.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import (
    AverageTrueRange,
    ExponentialMovingAverage,
    SimpleMovingAverage,
)
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy

from core.aggregator import (
    BarAggregator,
    external_from_composite,
    timeframe_of_bar_type,
)
from core.models import ExitConfig, parse_leg_actions
from core.signals import SIGNAL_REGISTRY


# -1 = squareoff disabled. Storing the parsed minute-of-day (0..1439) avoids
# re-parsing the HH:MM string on every bar.
_SQUAREOFF_DISABLED = -1

# UTC weekday from ts_event nanoseconds. 1970-01-01 (UNIX epoch) was a Thursday
# → Python weekday() = 3. Mirrors core/backtest_runner.py constants of the same
# name — used by the per-bar day-of-week gate.
_NANOS_PER_DAY_MOD = 86_400_000_000_000
_EPOCH_WEEKDAY = 3


# Cross-slot event registry, scoped per portfolio. Strategies in the same
# portfolio share one dict via `get_cross_slot_bus(portfolio_id)`. Used by
# Move SL to Cost's "Hit On Leg SL/Target" feature (spec §3) to raise a
# leg's SL to entry when any sibling leg fires SL or target. Layout:
#   _CROSS_SLOT_EVENT_BUSES[portfolio_id][slot_id] = {"sl_ns": ts, "tgt_ns": ts}
_CROSS_SLOT_EVENT_BUSES: dict[str, dict[str, dict[str, int]]] = {}


def get_cross_slot_bus(portfolio_id: str) -> dict[str, dict[str, int]]:
    """Return the shared cross-slot event bus for a portfolio (idempotent)."""
    return _CROSS_SLOT_EVENT_BUSES.setdefault(portfolio_id, {})


def clear_cross_slot_bus(portfolio_id: str) -> None:
    """Reset the cross-slot event bus between portfolio runs."""
    _CROSS_SLOT_EVENT_BUSES.pop(portfolio_id, None)


# ── Portfolio-SL "fired" bus (unified-engine live enforcement) ──────────────
# The portfolio-monitor strategy appends a breach ts here on a combined-loss
# breach; each leg (when ``pf_monitor_enforced``) reads it in on_bar and closes
# its own position live. Separate from the cross-slot Move-SL bus so it never
# perturbs that {slot_id: {...}} structure. Keyed by portfolio_id; a plain list
# of UTC-ns breach timestamps.
_PF_SL_FIRED_BUSES: dict[str, list] = {}


def get_pf_sl_bus(portfolio_id: str) -> list:
    """Return the shared portfolio-SL fire bus for a portfolio (idempotent)."""
    return _PF_SL_FIRED_BUSES.setdefault(portfolio_id or "_standalone_", [])


def clear_pf_sl_bus(portfolio_id: str) -> None:
    """Reset the portfolio-SL fire bus between portfolio runs."""
    _PF_SL_FIRED_BUSES.pop(portfolio_id or "_standalone_", None)


# ── Portfolio aggregate Move-SL trigger bus (unified-engine live Move-SL) ────
# The monitor appends the bar ts at which the COMBINED P&L first crossed the
# aggregate Move-SL threshold; each leg reads bus[-1] as a live
# `move_sl_agg_trigger_ns` (replaces the two-pass discovery's static config).
# Keyed by portfolio_id; a plain list of one trigger ts (append-once).
_PF_AGG_FIRED_BUSES: dict[str, list] = {}


def get_pf_agg_bus(portfolio_id: str) -> list:
    """Return the shared aggregate-Move-SL trigger bus for a portfolio."""
    return _PF_AGG_FIRED_BUSES.setdefault(portfolio_id or "_standalone_", [])


def clear_pf_agg_bus(portfolio_id: str) -> None:
    """Reset the aggregate-Move-SL trigger bus between portfolio runs."""
    _PF_AGG_FIRED_BUSES.pop(portfolio_id or "_standalone_", None)


# ── Conservative-fill adjustment bus (live monitor consistency) ──────────────
# Each leg, when an SL/TP close fills, adds its conservative-vs-engine fill delta
# (the same §4.2 adjustment the POST-RUN reprice applies) here. The portfolio
# monitor adds this CUMULATIVE adjustment to its live combined P&L so its
# SqOff/Target/ReExecute decisions use the conservative exit prices — consistent
# with the final reported P&L — instead of the raw engine fills. A 1-element list
# [float] holder keyed by portfolio_id (mutable so the monitor sees updates).
_PF_CONS_ADJ_BUSES: dict[str, list] = {}


def get_pf_cons_bus(portfolio_id: str) -> list:
    """Return the shared conservative-adjustment accumulator ([cum_delta])."""
    return _PF_CONS_ADJ_BUSES.setdefault(portfolio_id or "_standalone_", [0.0])


def clear_pf_cons_bus(portfolio_id: str) -> None:
    """Reset the conservative-adjustment bus between portfolio runs."""
    _PF_CONS_ADJ_BUSES.pop(portfolio_id or "_standalone_", None)


# ── Live conservative-FILL price bus (custom FillModel) ──────────────────────
# When `_USE_LIVE_FILL_MODEL` is on, a leg writes the conservative §4.2 exit price
# here keyed by instrument id BEFORE submitting its SL/TP close; the venue's
# ConservativeFillModel reads it and hands the matching engine a synthetic book at
# that price so the close fills AT the conservative price DURING the run (no
# post-run reprice). `{instrument_id_str: price}` per portfolio_id. Cleared by the
# leg on the close fill.
_PF_FILL_PX_BUSES: dict[str, dict] = {}


def get_pf_fill_px_bus(portfolio_id: str) -> dict:
    """Return the shared pending-conservative-fill-price map for a portfolio."""
    return _PF_FILL_PX_BUSES.setdefault(portfolio_id or "_standalone_", {})


def clear_pf_fill_px_bus(portfolio_id: str) -> None:
    """Reset the conservative-fill-price bus between portfolio runs."""
    _PF_FILL_PX_BUSES.pop(portfolio_id or "_standalone_", None)


# ── Per-leg live P&L bus (multi-portfolio scoped monitor) ────────────────────
# The backtest cache PURGES closed positions, so a scoped monitor cannot rebuild a
# strategy's cumulative realized from cache.positions/_closed when an instrument is
# shared by >1 portfolio. Instead each leg, which sees its OWN fills, accumulates its
# realized and publishes ``realized + open-unrealized`` here every bar; the scoped
# monitor sums its scope's legs. ``{strategy_id_str: pnl_base_ccy}`` per portfolio_id.
_PF_LEGPNL_BUSES: dict[str, dict] = {}


def get_pf_legpnl_bus(portfolio_id: str) -> dict:
    """Return the shared per-leg live-P&L map for a portfolio."""
    return _PF_LEGPNL_BUSES.setdefault(portfolio_id or "_standalone_", {})


def clear_pf_legpnl_bus(portfolio_id: str) -> None:
    """Reset the per-leg P&L bus between portfolio runs."""
    _PF_LEGPNL_BUSES.pop(portfolio_id or "_standalone_", None)


# ── Live backtest-PROGRESS bus (UI day-wise progress) ────────────────────────
# The PortfolioMonitorStrategy sees every bar and already detects day boundaries.
# On each new simulated day it publishes the latest processed timestamp here so the
# streaming endpoint (which runs in the request thread while the engine runs in a
# background thread, same process) can surface "Processed up to <date>" on the UI.
# A 1-element holder [{"data_ts": int_ns, "day": "YYYY-MM-DD"}] keyed by portfolio_id;
# last-writer-wins, so a multi-year run emits at most a few hundred tiny updates.
_PF_PROGRESS_BUSES: dict[str, list] = {}


def get_pf_progress_bus(portfolio_id: str) -> list:
    """Return the shared latest-processed-day holder for a portfolio."""
    return _PF_PROGRESS_BUSES.setdefault(portfolio_id or "_standalone_", [])


def clear_pf_progress_bus(portfolio_id: str) -> None:
    """Reset the progress bus between portfolio runs."""
    _PF_PROGRESS_BUSES.pop(portfolio_id or "_standalone_", None)


def advance_trailing_target(
    active: bool,
    stop: float,
    anchor: float,
    profit_pct: float,
    when_reach: float,
    lock_min: float,
    every: float,
    by: float,
) -> tuple[bool, float, float, bool]:
    """Pure one-bar step of the leg-level Trailing Target / Profit-Lock.

    Spec execution_logic_target.html §4.7. Works in profit-% units.

    * Before activation: the lock arms the first time ``profit_pct`` reaches
      ``when_reach``; the floor starts at ``lock_min`` and the ratchet anchor
      at ``when_reach``.
    * Once armed: if profit falls back to (or below) the locked floor the leg
      must exit (``hit=True``); otherwise the floor ratchets up by ``by`` for
      every ``every`` of further profit gained past the anchor.

    Returns the updated ``(active, stop, anchor, hit)`` tuple. Pure — no engine
    state — so it is unit-testable in isolation.
    """
    if not active:
        if when_reach > 0 and profit_pct >= when_reach:
            active = True
            stop = lock_min
            anchor = when_reach
    hit = False
    if active:
        if profit_pct <= stop:
            hit = True
        elif every > 0:
            gain = profit_pct - anchor
            if gain >= every:
                steps = int(gain / every)
                stop += steps * by
                anchor += steps * every
    return active, stop, anchor, hit


# Exit-type name aliases (spec §4.4 / §4.4 target). The spec names SL/Target
# types "Premium" (% of entry), "PremiumBased" and "AbsolutePremium" (absolute
# distance); the engine's canonical values are "percentage" / "points". Accept
# the spec names as input and normalise to canonical so portfolios authored
# against either vocabulary run identically. Keys are lower-cased and stripped
# of spaces/underscores before lookup.
_SL_TYPE_CANON = {
    "none": "none",
    "percentage": "percentage", "premium": "percentage", "premiumbased": "percentage",
    "points": "points", "absolutepremium": "points", "absolute": "points",
    "trailing": "trailing",
    "atr": "atr",
}
_TGT_TYPE_CANON = {
    "none": "none",
    "percentage": "percentage", "premium": "percentage", "premiumbased": "percentage",
    "points": "points", "absolutepremium": "points", "absolute": "points",
    "atr": "atr",
}


# Crypto venues for the Mark Price gate (spec §3 "mark_price (crypto only)").
# Mark Price is a crypto-exchange fair-value reference, so the engine only
# honours the ``mark`` exit format on these venues; other instruments fall
# back to OHLCV with a log. Venue name = the token after "." in the bar type
# (e.g. "BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL" → "CRYPTO").
_CRYPTO_VENUES = frozenset({
    "CRYPTO", "BINANCE", "BYBIT", "OKX", "OKEX", "COINBASE", "KRAKEN",
    "DERIBIT", "BITMEX", "BITSTAMP", "KUCOIN", "HUOBI", "GATEIO",
})


def _canon_exit_type(value: str | None, table: dict[str, str]) -> str:
    """Normalise an SL/Target type string to the engine's canonical value.

    Accepts the spec vocabulary ("Premium", "AbsolutePremium", …) and the
    engine vocabulary ("percentage", "points", …), case- and spacing-
    insensitively. Unknown values pass through unchanged so a typo fails
    loudly downstream rather than being silently coerced.
    """
    if not value:
        return "none"
    key = str(value).strip().lower().replace(" ", "").replace("_", "")
    return table.get(key, str(value).strip())


def _apply_value_mode(canon_type: str, is_absolute) -> str:
    """Honour the spec §4.4 abs/% input-mode flag.

    Spec §4.4: "Premium" is one type whose value is read as EITHER a percentage
    OR an absolute distance, chosen by a separate input-mode flag — not by the
    type name. This overrides the percentage/points choice that ``_canon_exit_type``
    inferred from the name, so a ``Premium`` leg explicitly flagged absolute
    computes ``entry ∓ value`` instead of silently running as a percentage.

        is_absolute is True  → force "points"     (sl/tp = entry ∓ value)
        is_absolute is False → force "percentage"  (sl/tp = entry × (1 ∓ v/100))
        is_absolute is None  → keep the name-inferred canonical type (default).

    Only the Premium family (percentage ↔ points) is remapped; none / atr /
    trailing pass through untouched.
    """
    if is_absolute is None or canon_type not in ("percentage", "points"):
        return canon_type
    return "points" if is_absolute else "percentage"


def resolve_trigger_hl(
    exit_fmt: str,
    is_long: bool,
    is_short: bool,
    close: float,
    bar_high: float,
    bar_low: float,
    bid_high: float | None = None,
    bid_low: float | None = None,
    ask_high: float | None = None,
    ask_low: float | None = None,
    mark_price: float | None = None,
) -> tuple[float, float]:
    """Resolve the (high, low) an SL/Target trigger should consult.

    Three-format engine, spec §3 / §4.1:

    * **Format B** (``ohlcv``) — the slot's own bar high/low.
    * **Format C** (``ltp``) — a single last price; high and low both
      collapse to ``close``.
    * **Format A** (``bidask``) — a LONG leg consults the ASK series
      (SL on ``ask_low``, TP on ``ask_high``); a SHORT leg consults the
      BID series (SL on ``bid_high``, TP on ``bid_low``). Falls back to
      OHLCV when the paired bid/ask values are missing (data gap).
    * **Mark Price** (``mark``) — spec §3, crypto-only fair-value reference.
      Proxied by the **previous bar's close** (passed as ``mark_price``) — the
      last settled price ahead of the current bar; high and low both collapse
      to it, so intra-bar wicks (the bar high/low) do NOT fire the SL/Target.
      On the first bar (no previous close yet) ``mark_price`` is None/0 and it
      falls back to the current ``close``. The crypto-only gate is enforced
      upstream (``on_start`` downgrades to OHLCV on non-crypto venues); by the
      time this runs ``mark`` is already crypto-validated.

    Pure function — unit-testable without an engine.
    """
    if exit_fmt == "ltp":
        # LTP collapses to the bar's own last price.
        return close, close
    if exit_fmt == "mark":
        # Mark Price (proxy) = the PREVIOUS bar's close; collapse high/low to it
        # so intra-bar wicks are ignored. Fall back to the current close before
        # any previous close exists (first bar).
        m = mark_price if (mark_price is not None and mark_price > 0) else close
        return m, m
    if exit_fmt == "bidask" and None not in (bid_high, bid_low, ask_high, ask_low):
        if is_long:
            return ask_high, ask_low
        if is_short:
            return bid_high, bid_low
    return bar_high, bar_low


def _derive_bid_ask_bar_types(primary_bar_type_str: str) -> tuple[str, str]:
    """Derive the BID and ASK bar-type strings from a primary bar type.

    Used by Format A (Bid/Ask) so the strategy can subscribe to the paired
    series. Bar types are Nautilus-formatted
    ``<sym>.<venue>-<tf>-<price>-EXTERNAL``; we swap the price-type token.
    Returns ``("", "")`` when the primary isn't an FX-style ASK/BID/MID bar
    (e.g. crypto ``LAST`` bars have no bid/ask pair).
    """
    s = str(primary_bar_type_str or "")
    if "-MID-" in s:
        return s.replace("-MID-", "-BID-", 1), s.replace("-MID-", "-ASK-", 1)
    if "-BID-" in s:
        return s, s.replace("-BID-", "-ASK-", 1)
    if "-ASK-" in s:
        return s.replace("-ASK-", "-BID-", 1), s
    return "", ""


def _parse_squareoff_minute(squareoff_time: str | None) -> int:
    """Convert "HH:MM" or "HH:MM:SS" → minute-of-day, or -1 when disabled.

    Tolerates ``None`` and an empty string. Seconds are accepted (and dropped —
    minute-of-day granularity is sufficient for entry-window / squareoff
    triggers). Raises ``ValueError`` for malformed inputs so a typo in a
    portfolio JSON fails loudly at engine build instead of silently disabling.
    """
    if not squareoff_time:
        return _SQUAREOFF_DISABLED
    parts = squareoff_time.split(":")
    return int(parts[0]) * 60 + int(parts[1])


class ManagedExitConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")

    # Signal
    signal_name: str = "EMA Cross"
    signal_params: dict = {}

    # Exit-trigger data format (spec §3 — three-format engine).
    #   "ohlcv"  — Format B (default): trigger on this slot's bar high/low.
    #   "ltp"    — Format C: trigger collapses to the bar close (single price).
    #   "bidask" — Format A: SELL exits trigger on bid_high, BUY on ask_low.
    # For "bidask" the strategy also subscribes to bid_bar_type / ask_bar_type
    # and buffers them per-timestamp; empty strings → fall back to "ohlcv".
    exit_price_format: str = "ohlcv"
    bid_bar_type: str = ""
    ask_bar_type: str = ""

    # Extra strategy-subscribe bar types — composite bar types beyond the
    # primary one. When a leg selects strategy timeframe(s), ``bar_type``
    # above is set to the FIRST composite (the stream the signal / SL / TP
    # run on); this list holds any further composites the strategy also
    # subscribes to (received and available, but the single-signal logic
    # uses the primary). Empty in the common case.
    subscribe_bar_types: list = []

    # Custom streaming aggregation. When a leg selects a strategy timeframe
    # coarser than the base, this holds the TARGET EXTERNAL bar type the base
    # stream is aggregated up to (e.g. "EURUSD.FOREX_MS-5-MINUTE-MID-EXTERNAL").
    # ``bar_type`` stays the BASE stream the strategy subscribes to; each base
    # bar is fed to a core.aggregator.BarAggregator and the signal / indicators
    # / SL / TP run on the emitted aggregated bar. Empty ⇒ no aggregation (runs
    # on the base bar type, the original unchanged behaviour). This replaces the
    # old Nautilus-internal composite (``INTERNAL@``) aggregation.
    aggregate_to_bar_type: str = ""

    # Exit management
    stop_loss_type: str = "none"
    stop_loss_value: float = 0.0
    trailing_sl_step: float = 0.0
    trailing_sl_offset: float = 0.0
    # ATR-based SL sizing (spec sl_features.html §1.1 fn.4). Active when
    # stop_loss_type == "atr": SL distance = sl_atr_multiplier × ATR(period),
    # computed from the bar series at entry.
    sl_atr_period: int = 0
    sl_atr_multiplier: float = 0.0
    target_type: str = "none"
    target_value: float = 0.0
    # ATR-based Target sizing (spec sl_features.html §1.1 fn.4). Active when
    # target_type == "atr": TP distance = tgt_atr_multiplier × ATR(period),
    # computed from the bar series at entry. Independent of the SL ATR knobs.
    tgt_atr_period: int = 0
    tgt_atr_multiplier: float = 0.0
    target_lock_trigger: float = 0.0
    target_lock_minimum: float = 0.0
    # Leg-Level Trailing Target / Profit-Lock (spec execution_logic_target.html
    # §4.7). Ratcheting profit-lock; all thresholds in profit-% units.
    tgt_trail_enabled: bool = False
    tgt_trail_when_profit_reach: float = 0.0
    tgt_trail_lock_min_profit: float = 0.0
    tgt_trail_every: float = 0.0
    tgt_trail_by: float = 0.0
    sl_wait_sec: int = 0  # Spec name "SL Wait (sec)". Wins over sl_wait_bars if > 0.
    sl_wait_bars: int = 0
    # Target Wait (spec execution_logic_target.html §4.3). Mirror of SL Wait.
    # tgt_wait_sec (wall-clock) wins over tgt_wait_bars when both > 0.
    tgt_wait_sec: int = 0
    tgt_wait_bars: int = 0
    # Valid actions: close | re_execute | reverse | execute | re_entry | keep_leg_running
    on_sl_action: str = "close"
    on_target_action: str = "close"
    max_re_executions: int = 0
    # Spec §1.2 1.2(c) Execute (other leg by leg_id). Target slot to arm.
    execute_target_leg_id: str = ""
    # Spec §1.2 1.2(d) ReEntry (price-wait re-entry).
    reentry_price: float = 0.0
    max_re_entries: int = 0
    # Portfolio "ReExecute at Entry Price" / "ReExecute Same Contract at Entry
    # Price" (spec §5.2 / §2.1). RUNTIME-injected during the portfolio ReExecute
    # replay: the price the slot's pre-clip position was opened at, and its
    # direction. When > 0 the strategy arms a one-shot price-wait at start so its
    # FIRST re-entry waits until the instrument returns to that original entry
    # price (reusing the §1.2(d) re-entry gate) instead of entering at the next
    # signal's market price. 0.0 = no pin (plain ReExecute). Not user-saved.
    reexec_entry_price: float = 0.0
    reexec_entry_was_long: bool = True
    # Portfolio plain "ReExecute" (spec portfolio_sl_tgt.html: "Closes all legs
    # and immediately re-opens them"). When True, the replay re-enters the leg
    # with an immediate MARKET order on the original side on the first bar after
    # the breach — no signal, no price-wait. Reuses ``reexec_entry_was_long``
    # for the side; ``reexec_entry_price`` carries the captured price only to
    # flag "a re-entry is pending" (the gate is ``reexec_entry_price > 0``).
    # False → entry-price-limit (when price pinned) or legacy signal re-entry.
    reexec_market_mode: bool = False
    # Spec §1.2 1.2(c): when False, this leg ignores its own signals until
    # a sibling's "execute" action arms it via the cross-slot bus.
    armed_at_start: bool = True

    # Intraday entry window (spec §9). Minute-of-day UTC; -1 = disabled.
    # Fresh entries are blocked outside [entry_start_minute, entry_end_minute],
    # but — unlike a bar pre-filter — bars PAST entry_end_minute are still
    # delivered so open positions keep being monitored for SL/Target until
    # squareoff. ReExecute / ReEntry past End Time are blocked only when
    # no_reentry_after_end is set (spec §7 P3 / §9).
    entry_start_minute: int = -1
    entry_end_minute: int = -1
    no_reentry_after_end: bool = False

    # Day-of-week filter (portfolio.run_on_days). UTC weekday ints (0=Mon..6=Sun)
    # for which fresh entries are allowed. ``None`` = no filter (all days
    # allowed — default). ``[]`` (empty list) = explicit "no weekdays allowed",
    # blocks every fresh entry. Like entry_start_minute/entry_end_minute, this
    # gates fresh entries only — bars on excluded weekdays are still delivered
    # so open positions remain monitored for SL/Target/squareoff. The runner
    # used to pre-filter bars by weekday before engine.add_data(); that
    # approach broke internal bar aggregation (composite bar types received
    # discontinuous input and Nautilus's TimeBarAggregator emitted synthetic
    # stale-close bars across the gap, on which the strategy could fire
    # phantom signals). Gating at the strategy level keeps the input stream
    # continuous so the aggregator never sees gaps.
    allowed_weekdays: list | None = None

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

    # Other Settings (slot-level adaptation of portfolio-level spec).
    # Spec: 5. Logics/Other_Settings_Logic.html.
    delay_between_legs_sec: int = 0
    on_sl_action_on: str = "OnSL_N_Trailing_Both"
    on_target_action_on: str = "OnTarget_N_Trailing_Both"

    # Move SL to Cost (per-slot adaptation of spec §3).
    # Spec: 5. Logics/portfolio_sl_tgt.html.
    # When move_sl_enabled, after move_sl_safety_sec seconds in-position and
    # the position is in profit, raise current_sl to entry_price. Optionally
    # skip on long positions (no_buy_legs adaptation). When move_sl_trail_after
    # is set, the existing trailing-SL ratchet is suppressed until move-to-cost
    # has fired at least once for the current position.
    move_sl_enabled: bool = False
    move_sl_safety_sec: int = 0
    move_sl_action: str = "Move Only for Profitable Legs"
    move_sl_trail_after: bool = False
    move_sl_no_buy_legs: bool = False
    # Action v3 "Move SL to LTP + Buffer for Loss Making Legs": on a losing
    # leg, slide SL toward current price by this buffer (in price units).
    move_sl_ltp_buffer: float = 0.0
    # Hit-On-Leg cross-slot triggers (spec §3): raise this leg's SL to entry
    # when ANY sibling slot in the same portfolio fires its SL / target.
    # Consumed via the module-level _CROSS_SLOT_EVENT_BUSES registry.
    move_sl_hit_on_leg_sl: bool = False
    move_sl_hit_on_leg_target: bool = False
    # Portfolio-aggregate Move SL trigger (spec §2.3). The two-pass runner
    # (gated by _USE_PF_AGG_MOVE_SL) computes a single trigger timestamp from
    # pass 1's merged combined-P&L curve and injects it here for pass 2; any
    # leg open at/after this UTC-ns timestamp snaps its SL to entry. 0 = no
    # aggregate trigger (pass 1, or feature off).
    move_sl_agg_trigger_ns: int = 0
    # Pre-seeded cross-slot event bus for pass 2: {slot_id: {"sl_ns","tgt_ns"}}.
    # The module-level _CROSS_SLOT_EVENT_BUSES dict does not survive a process
    # boundary, so the runner threads pass-1's SL/target hit timestamps through
    # the (picklable) config instead. Merged into _sibling_bus in __init__.
    move_sl_preseeded_bus: dict = {}
    # Portfolio + slot identifiers for the cross-slot event bus. Empty strings
    # disable the bus (single-leg or unscoped strategies). Set by config_from_exit.
    portfolio_id: str = ""
    slot_id: str = ""
    # Conservative-fill bus id. A multi-portfolio SESSION sets this to the SESSION id
    # so the one per-venue ConservativeFillModel serves every portfolio (the per-leg
    # {instrument}|{strategy} pin keeps fills unique). Empty → falls back to
    # portfolio_id, so single-portfolio is unchanged.
    fill_bus_id: str = ""
    # Multi-portfolio SESSION: True when this leg runs in a session engine whose
    # shared bar stream was NOT entry-window pre-filtered (because co-resident
    # portfolios may have different windows). The leg then self-filters its own
    # pre-entry_start bars in on_bar, reproducing the single-portfolio stream
    # pre-filter per leg. False (default) → single-portfolio path, stream already
    # filtered, leg does not self-filter → byte-identical.
    session_window_self_filter: bool = False
    # Unified-engine live enforcement: when True, this leg watches the portfolio-SL
    # fire bus and closes its own position live on a combined-loss breach (the
    # one-pass replacement for the post-run clip). Default False → inert (per-slot
    # and default Path B are untouched).
    pf_monitor_enforced: bool = False
    # Live-enforcement portfolio action: "sqoff" (close + block rest of day) or
    # "reexecute" (close + re-arm a re-entry at the captured entry price, or market
    # for plain ReExecute). pf_monitor_market_mode selects market vs entry-price.
    pf_monitor_action: str = "sqoff"
    pf_monitor_market_mode: bool = False
    # True when the post-run conservative §4.2 fill (vwap_exit_fill) is active for
    # this run → the leg publishes a live conservative-vs-engine delta so the monitor
    # decides on conservative P&L (consistent with the final report).
    cons_fill_active: bool = False
    # True when the §8.1 directional close fill (directional_close_fill) is active →
    # the leg pins the directional close (bid_close/ask_close, Format A) for MARKET
    # exits so they fill there in-engine.
    dir_fill_active: bool = False

    # ReExecute Tab P1 (spec: 5. Logics/ReExecute_Logics.html).
    # When True, suppresses the configured re_execute action when the SL
    # that just fired was previously raised to entry_price by Move SL to
    # Cost (i.e. _move_sl_fired_this_position is True at exit time). The
    # action downgrades to plain "close" — position stays flat; no re-entry.
    no_reexec_sl_cost: bool = False
    # ReExecute Tab P2: when True, the slot re-execution delay
    # (delay_between_legs_sec) is skipped for re-executions (exec_count >= 1).
    no_wait_trade_reexec: bool = False
    # ReExecute Tab P5: when True, the re_entry action is suppressed (→ "close")
    # if the SL that fired had been raised to entry by Move SL to Cost.
    # Spec default ON.
    no_reentry_sl_cost: bool = True


class ManagedExitStrategy(Strategy):
    """On each bar: check exits first (SL, TP, trailing, target lock, SL wait), then entries."""

    def __init__(self, config: ManagedExitConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument = None
        self.indicators = {}
        # ATR indicator for stop_loss_type == "atr" (spec §1.1 fn.4). Registered
        # for bars in on_start; read once at entry-fill time to size the SL.
        self._atr: AverageTrueRange | None = None
        # Separate ATR indicator for target_type == "atr" (spec §1.1 fn.4) so
        # SL and Target can each have their own lookback period.
        self._tgt_atr: AverageTrueRange | None = None
        self.entry_price = 0.0
        self.highest_profit = 0.0
        self.current_sl = 0.0
        self.current_tp = 0.0
        # ATR value used to size an atr-type SL at entry. Surfaced in the SL
        # exit reason tag (atr@entry=…) so orderbook-only verification can
        # check expected_sl = entry ∓ mult × ATR without engine internals.
        self._atr_at_entry: float = 0.0
        self.sl_wait_count = 0
        self._sl_wait_started_ns: int = 0  # First-breach timestamp for sl_wait_sec
        # Target Wait (spec §4.3) — mirror of the SL-wait state.
        self.tp_wait_count = 0
        self._tp_wait_started_ns: int = 0
        # Leg-Level Trailing Target (spec §4.7) — per-position profit-lock state.
        # _tgt_trail_active flips True once profit first reaches the activation
        # threshold; _tgt_trail_stop is the locked profit-% floor; _tgt_trail_anchor
        # tracks the profit level the next ratchet step measures from.
        self._tgt_trail_active: bool = False
        self._tgt_trail_stop: float = 0.0
        self._tgt_trail_anchor: float = 0.0
        # True when the TP exit currently being dispatched came from the
        # trailing-target floor (drives the on_target_action_on filter and the
        # exit-reason label). Reset on each new entry.
        self._tp_was_trail: bool = False
        self.re_execution_count = 0
        # Cross-slot bus reference (shared dict). Empty portfolio_id → standalone bus.
        self._sibling_bus: dict[str, dict[str, int]] = get_cross_slot_bus(
            getattr(config, "portfolio_id", "") or "_standalone_"
        )
        # Unified-engine live portfolio-SL enforcement (gated; inert by default).
        self._pf_monitor_enforced: bool = bool(getattr(config, "pf_monitor_enforced", False))
        self._pf_monitor_action: str = str(getattr(config, "pf_monitor_action", "sqoff") or "sqoff").lower()
        self._pf_monitor_market_mode: bool = bool(getattr(config, "pf_monitor_market_mode", False))
        self._pf_sl_bus: list = get_pf_sl_bus(getattr(config, "portfolio_id", "") or "_standalone_")
        self._pf_sl_handled_ns: int = 0
        self._pf_sl_blocked_date = None  # UTC date entries are blocked (rest-of-day SqOff)
        self._pf_sl_blocked_forever = False  # rest-of-run block (absolute SqOff, e.g. pf_tgt)
        # Live aggregate Move-SL trigger bus (monitor publishes the cross ts).
        self._pf_agg_bus: list = get_pf_agg_bus(getattr(config, "portfolio_id", "") or "_standalone_")
        # Live conservative-fill adjustment: track running session VWAP(s) so an
        # SL/TP exit's conservative §4.2 fill (MIN/MAX(vwap, hit)) is fed to the
        # monitor's combined P&L — making its SqOff/Target/ReExecute decisions
        # consistent with the post-run conservative exits. Formats: A=bid/ask vwap,
        # B=single vwap, C=ltp (no vwap → no adjustment). DEFAULT ON when enforcing
        # AND the conservative reprice is active (cons_fill_active = vwap_exit_fill);
        # escape hatch _USE_LIVE_CONS_ADJ=0.
        self._cons_fill_active: bool = bool(getattr(config, "cons_fill_active", False))
        # §8.1 directional close (directional_close_fill): MARKET exits (squareoff /
        # portfolio) fill at the directional close — Format A bid_close/ask_close;
        # B/C already fill at close/ltp (no pin needed).
        self._dir_fill_active: bool = bool(getattr(config, "dir_fill_active", False))
        # LIVE-FILL mode (_USE_LIVE_FILL_MODEL): the exit close fills AT the pinned
        # price in-engine (via ConservativeFillModel) — single pass, no post-run
        # reprice. On when enforcing and EITHER the §4.2 conservative (vwap) or the
        # §8.1 directional fill is active. CONS-BUS mode (default escape): engine fills
        # as usual, the leg publishes a conservative delta the monitor consumes
        # (post-run reprices the reports). Mutually exclusive; both need the live VWAP.
        self._live_fill_on: bool = (self._pf_monitor_enforced
                                    and (self._cons_fill_active or self._dir_fill_active)
                                    and os.environ.get("_USE_LIVE_FILL_MODEL", "1") != "0")
        self._cons_adj_on: bool = (self._pf_monitor_enforced and self._cons_fill_active
                                   and not self._live_fill_on
                                   and os.environ.get("_USE_LIVE_CONS_ADJ", "1") != "0")
        self._cons_vwap_on: bool = self._cons_adj_on or self._live_fill_on
        self._cons_bus: list = get_pf_cons_bus(getattr(config, "portfolio_id", "") or "_standalone_")
        self._fill_px_bus: dict = get_pf_fill_px_bus(
            getattr(config, "fill_bus_id", "") or getattr(config, "portfolio_id", "") or "_standalone_")
        # Per-leg live-P&L publish (multi-portfolio scoped monitor). Active only in a
        # SESSION (fill_bus_id is the session id, distinct from this leg's portfolio_id);
        # in single-portfolio it's inert (no publish, no per-bar cost).
        _fbid = getattr(config, "fill_bus_id", "") or ""
        self._in_session: bool = bool(_fbid and _fbid != (getattr(config, "portfolio_id", "") or ""))
        self._legpnl_bus: dict = get_pf_legpnl_bus(getattr(config, "portfolio_id", "") or "_standalone_")
        self._cum_realized: float = 0.0          # accumulated realized across closed cycles
        self._realized_seen: set = set()         # position ids already counted (dedup)
        try:
            from core.backtest_runner.exit_fill import _session_start_minute as _ssm
            self._cons_session_min: int = _ssm(str(getattr(config, "bar_type", "")))
        except Exception:  # noqa: BLE001
            self._cons_session_min = 0
        # Per-side running VWAP + latest bar extremes (single / bid / ask).
        self._cons_state: dict = {
            s: {"pv": 0.0, "v": 0.0, "bucket": None, "vwap": 0.0, "hi": 0.0, "lo": 0.0, "close": 0.0}
            for s in ("single", "bid", "ask")
        }
        self._pending_cons_px: float = 0.0   # conservative exit price awaiting the close fill
        self._pending_cons_qty: float = 0.0
        self._pending_cons_long: bool = True
        # Pass-2 pre-seed (spec §2.3): the module-level bus is per-process, so
        # the runner injects pass-1's cross-slot SL/target hit timestamps via
        # config.move_sl_preseeded_bus. Merge them in (keep the latest ns per
        # key) so the existing Hit-On-Leg branch in _check_exits reacts to
        # siblings that ran in *other* worker processes.
        for sid, ev in (getattr(config, "move_sl_preseeded_bus", {}) or {}).items():
            cur = self._sibling_bus.setdefault(sid, {})
            for k, v in (ev or {}).items():
                try:
                    iv = int(v)
                except (TypeError, ValueError):
                    continue
                if iv > cur.get(k, 0):
                    cur[k] = iv
        # This leg's own SL/target hit timestamps, surfaced in the result dict
        # so the two-pass runner can build pass 2's pre-seeded bus. {} until a
        # SL or target fires.
        self._exit_events_self: dict[str, int] = {}
        # 1.2(c) Execute (other leg): per-slot ARM flag. Slots configured with
        # armed_at_start=False start dormant and only fire entries after a
        # sibling slot's "execute" action arms them via the bus.
        self._armed_for_entry: bool = bool(getattr(config, "armed_at_start", True))
        self._cross_pf_seen: set = set()  # consumed cross-portfolio event idems (once each)
        # 1.2(d) ReEntry (price-wait): when set, blocks signal entries until
        # the live price crosses the configured re-entry trigger.
        self._reentry_armed: bool = False
        self._reentry_target_price: float = 0.0
        self._reentry_was_long: bool = True
        # Portfolio "ReExecute at Entry Price" (spec ✅ in sl_features.html): when
        # the replay injects a pre-clip entry price, submit a resting LIMIT at
        # that price on the original side so the re-entry fills AT the entry
        # price on a bar touch — no signal needed. One-shot, isolated to this
        # action; the leg-level ReEntry price-wait (§1.2(d), _reentry_armed
        # above) is left untouched. Submitted in _check_entries, cleared on fill
        # (on_order_filled), abandoned at the daily squareoff if still unfilled.
        _reexec_px = float(getattr(config, "reexec_entry_price", 0.0) or 0.0)
        self._reexec_limit_px: float = _reexec_px if _reexec_px > 0 else 0.0
        self._reexec_limit_is_long: bool = bool(getattr(config, "reexec_entry_was_long", True))
        # Plain ReExecute → immediate MARKET re-entry instead of an entry-price
        # limit (spec portfolio_sl_tgt.html "immediately re-opens them").
        self._reexec_market_mode: bool = bool(getattr(config, "reexec_market_mode", False))
        self._reexec_limit_submitted: bool = False
        self._reexec_limit_client_id = None
        self.re_entry_count: int = 0
        self.position_side = None  # "LONG" or "SHORT" or None
        self._expecting_close_fill = False  # next on_order_filled is a close, not an open
        # Forensic reason string set by the signal function in ``_check_entries``
        # right before calling ``_submit_order``. Attached to the Nautilus
        # order's ``tags`` so it lands in fills_report["tags"], where
        # report_generator surfaces it via the orderbook's
        # "ENTRY DETAILED REASON" column. Cleared after each submit so a
        # subsequent reverse-on-SL or close-and-flip doesn't reuse it.
        self._pending_entry_reason: str | None = None

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

        # Other Settings state.
        # _was_trailed: True once current_sl has been moved by trailing or
        # target-lock logic. Drives on_sl_action_on filter classification.
        # Resets when a new entry fills (so each trade's was_trailed is fresh).
        self._was_trailed: bool = False
        # _reentry_blocked_until_ns: re-execution delay timestamp (UTC ns).
        # When set, _check_entries skips fresh entries until bar.ts_event > this.
        # Cleared after the next entry actually fires.
        self._reentry_blocked_until_ns: int = 0
        # Updated at the top of on_bar so _handle_exit can stamp delay timers
        # without threading bar through every call.
        self._current_bar_ts_ns: int = 0

        # Move SL to Cost state (spec §3, per-slot adaptation).
        # _entry_filled_at_ns: bar.ts_event of the bar when the entry filled.
        # safety_sec is measured against this. Reset on each new entry.
        # _move_sl_fired_this_position: tracks whether move-to-cost has fired
        # for the current position; gates the trail_after suppression.
        self._entry_filled_at_ns: int = 0
        self._move_sl_fired_this_position: bool = False
        # Spec §7 (sl_moved_to_cost persistence): the moved-to-cost mark is
        # sticky for the whole TRADING DAY (never cleared within a day; a new
        # day starts fresh) and carries across re-execution cycles.
        # _move_sl_fired_this_position above is per-position (reset on each entry
        # fill); this companion is per-day, so the ReExecute / ReEntry gates keep
        # blocking a leg that already flattened at cost earlier today even after
        # it re-executed into a fresh position. _move_sl_day_key tracks the
        # current trading day (session-tz ordinal) to detect the rollover reset.
        self._move_sl_fired_today: bool = False
        self._move_sl_day_key: int | None = None
        # Profit anchor for the dedicated post-move trail (spec §4.6); set to
        # profit_pct on the bar Move SL to Cost fires.
        self._move_sl_trail_anchor: float = 0.0

        # Three-format engine state (spec §3).
        #   _exit_fmt: "ohlcv" (B) / "ltp" (C) / "bidask" (A).
        #   Format A subscribes to the BID and ASK bar types and buffers all
        #   three streams per ts_event in _fa_pending; the bar logic runs once
        #   per timestamp when the (primary, bid, ask) trio is complete.
        self._exit_fmt: str = str(getattr(config, "exit_price_format", "ohlcv") or "ohlcv")
        self._bid_bt_str: str = str(getattr(config, "bid_bar_type", "") or "")
        self._ask_bt_str: str = str(getattr(config, "ask_bar_type", "") or "")
        self._fa_bid_bt: BarType | None = None
        self._fa_ask_bt: BarType | None = None
        self._fa_pending: dict[int, dict] = {}
        # Mark Price (spec §3, approach B): rolling previous-bar close. The
        # "mark" exit format triggers off this (the last settled price ahead of
        # the current bar), not the current close — updated every bar in
        # _on_primary_bar. 0.0 until the first bar is seen.
        self._prev_close: float = 0.0

        # Extra strategy-subscribe bar types — composite bar types beyond the
        # primary signal one (``config.bar_type``). Parsed to BarType objects
        # in on_start; received by on_bar but ignored by the signal / SL / TP
        # logic (the primary bar type drives those).
        self._extra_sub_bt_strs: list[str] = [
            str(s) for s in (getattr(config, "subscribe_bar_types", []) or []) if s
        ]
        self._extra_sub_bts: set = set()

        # Custom streaming aggregation state. ``_agg_to_str`` is the target
        # EXTERNAL bar type (empty ⇒ no aggregation). The aggregators and the
        # ``_aggregating`` flag are built in on_start (need the instrument).
        # ``_agg_indicators`` is the list of indicators fed manually from the
        # emitted aggregated bar (signal indicators + ATR(s)). ``_agg_pending``
        # pairs primary/bid/ask aggregated bars by window-close ts for Format A.
        self._agg_to_str: str = str(getattr(config, "aggregate_to_bar_type", "") or "")
        self._aggregating: bool = False
        self._primary_agg: BarAggregator | None = None
        self._bid_agg: BarAggregator | None = None
        self._ask_agg: BarAggregator | None = None
        self._agg_indicators: list = []
        self._agg_pending: dict[int, dict] = {}

        # Intraday entry window (spec §9). UTC minute-of-day; -1 = disabled.
        # Gates fresh entries in _check_entries WITHOUT dropping post-window
        # bars, so SL/Target keep being monitored until squareoff.
        self._entry_start_min: int = int(getattr(config, "entry_start_minute", -1))
        self._entry_end_min: int = int(getattr(config, "entry_end_minute", -1))
        self._no_reentry_after_end: bool = bool(getattr(config, "no_reentry_after_end", False))
        # Multi-portfolio session: self-filter pre-entry_start bars (the shared
        # stream wasn't pre-filtered). Gated on this flag — NOT _in_session — so a
        # 1-portfolio session (fill_bus_id == portfolio_id → _in_session False) also
        # self-filters and stays at single-portfolio parity.
        self._window_self_filter: bool = bool(getattr(config, "session_window_self_filter", False))
        # Multi-portfolio session + NON-aggregating leg + entry window: feed the
        # signal/ATR indicators MANUALLY (gated by the on_bar self-filter) instead of
        # register_indicator_for_bars, which auto-feeds from EVERY engine bar of this
        # bar type — including pre-window base bars another portfolio pulled in — and
        # so can't be window-gated. Manual feed (in _on_primary_bar, after the
        # self-filter) warms the indicator only on in-window bars, matching the single
        # path. Populated in on_start; empty (auto-register) otherwise.
        self._manual_feed_inds: list = []

        # Day-of-week entry filter — UTC weekday ints (0=Mon..6=Sun). Tri-state:
        #   None    → no filter (all days allowed; default)
        #   set()   → explicit empty: every fresh entry blocked
        #   {0,1,3} → only Mon/Tue/Thu fresh entries allowed
        # Resolved once here so the per-bar gate in _on_primary_bar is a single
        # set-membership check.
        _aw_raw = getattr(config, "allowed_weekdays", None)
        if _aw_raw is None:
            self._allowed_weekdays: set[int] | None = None
        else:
            self._allowed_weekdays = {int(d) for d in _aw_raw if isinstance(d, (int, bool))}

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

        # Resolve the custom streaming aggregation target (replaces Nautilus'
        # internal composite aggregation). When aggregating, indicators are NOT
        # registered with the engine — they would otherwise be fed on every BASE
        # bar — and are instead driven manually from the emitted aggregated bar
        # in on_bar. ``config.bar_type`` is always the BASE stream.
        self._aggregating = False
        if self._agg_to_str:
            try:
                _tgt = external_from_composite(self._agg_to_str) or self._agg_to_str
                _target_bt = BarType.from_str(_tgt)
                if _target_bt != self.config.bar_type:
                    self._primary_agg = BarAggregator(
                        self.instrument, _target_bt, timeframe_of_bar_type(str(_target_bt))
                    )
                    self._aggregating = True
            except Exception as e:  # noqa: BLE001 — degrade to no-aggregation
                self.log.warning(
                    f"Custom aggregation disabled for {self._agg_to_str!r} ({e}); "
                    f"running on base bar type"
                )
                self._aggregating = False
                self._primary_agg = None

        # Non-agg leg in a session WITH an entry window → feed indicators manually
        # (window-gated) instead of auto-registering (see _manual_feed_inds comment).
        _manual_ind = (self._window_self_filter and not self._aggregating
                       and self._entry_start_min >= 0)

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
            if not self._aggregating:
                if _manual_ind:
                    self._manual_feed_inds.append(indicator)
                else:
                    self.register_indicator_for_bars(self.config.bar_type, indicator)

        # ATR-based SL: register a dedicated ATR indicator so it warms up
        # alongside the signal indicators. indicators_initialized() also waits
        # on it, so the leg won't enter until ATR has a valid value.
        if self.config.stop_loss_type == "atr" and self.config.sl_atr_period > 0:
            self._atr = AverageTrueRange(int(self.config.sl_atr_period))
            if not self._aggregating:
                if _manual_ind:
                    self._manual_feed_inds.append(self._atr)
                else:
                    self.register_indicator_for_bars(self.config.bar_type, self._atr)

        # ATR-based Target: dedicated indicator (spec §1.1 fn.4). Registered so
        # indicators_initialized() also waits on it before the leg may enter.
        if self.config.target_type == "atr" and self.config.tgt_atr_period > 0:
            self._tgt_atr = AverageTrueRange(int(self.config.tgt_atr_period))
            if not self._aggregating:
                if _manual_ind:
                    self._manual_feed_inds.append(self._tgt_atr)
                else:
                    self.register_indicator_for_bars(self.config.bar_type, self._tgt_atr)

        # Indicators fed manually from the aggregated bar (aggregating mode) and
        # used for the readiness gate in both modes.
        self._agg_indicators = list(self.indicators.values())
        if self._atr is not None:
            self._agg_indicators.append(self._atr)
        if self._tgt_atr is not None:
            self._agg_indicators.append(self._tgt_atr)

        self.subscribe_bars(self.config.bar_type)

        # Format A (Bid/Ask): also subscribe to the paired BID/ASK series so
        # SL/Target triggers can consult them. Indicators stay registered on the
        # primary bar type only, so entry signals are unaffected. SL/Target
        # triggers consult these BASE bid/ask bars at base-data resolution — even
        # when the signal is aggregated to a higher timeframe, exits run on the
        # base bar (see _on_bar_aggregating_bidask), so no bid/ask aggregators
        # are needed.
        if self._exit_fmt == "bidask" and self._bid_bt_str and self._ask_bt_str:
            try:
                self._fa_bid_bt = BarType.from_str(self._bid_bt_str)
                self._fa_ask_bt = BarType.from_str(self._ask_bt_str)
                # Don't double-subscribe when the slot's own series already IS
                # one side of the pair (primary == ASK or BID bar type).
                if self._fa_bid_bt != self.config.bar_type:
                    self.subscribe_bars(self._fa_bid_bt)
                if self._fa_ask_bt != self.config.bar_type:
                    self.subscribe_bars(self._fa_ask_bt)
            except Exception as e:  # noqa: BLE001 — degrade, don't crash the run
                self.log.warning(
                    f"Format A: could not subscribe bid/ask bars ({e}); "
                    f"falling back to OHLCV trigger"
                )
                self._exit_fmt = "ohlcv"

        # Mark Price (spec §3) is crypto-only. The trigger reference is proxied
        # by the bar close (resolve_trigger_hl), but the spec restricts it to
        # crypto feeds — downgrade to OHLCV on non-crypto venues with a log.
        if self._exit_fmt == "mark":
            try:
                _venue = str(self.config.bar_type.instrument_id.venue.value).upper()
            except Exception:  # noqa: BLE001
                _venue = ""
            if _venue not in _CRYPTO_VENUES:
                self.log.warning(
                    f"Mark Price trigger is crypto-only; venue {_venue!r} is not "
                    f"crypto — falling back to OHLCV trigger"
                )
                self._exit_fmt = "ohlcv"

        # Extra strategy-subscribe bar types — composite bar types beyond the
        # primary signal one. Subscribed so the strategy receives them and
        # they are available; the signal / SL / TP logic runs on the primary
        # bar type (on_bar ignores these). Indicators are NOT registered on
        # them. Each subscription is best-effort.
        _already = {str(self.config.bar_type), self._bid_bt_str, self._ask_bt_str}
        for s in self._extra_sub_bt_strs:
            if not s or s in _already:
                continue
            try:
                bt = BarType.from_str(s)
            except Exception as e:  # noqa: BLE001
                self.log.warning(f"Strategy-subscribe bar type {s!r} invalid ({e}); skipped")
                continue
            if bt in (self.config.bar_type, self._fa_bid_bt, self._fa_ask_bt):
                continue
            self._extra_sub_bts.add(bt)
            try:
                self.subscribe_bars(bt)
            except Exception as e:  # noqa: BLE001 — degrade, don't crash the run
                self.log.warning(f"Could not subscribe strategy bar type {s!r} ({e})")
                self._extra_sub_bts.discard(bt)

    def on_bar(self, bar: Bar) -> None:
        # Multi-portfolio SESSION only: the shared per-bar-type stream is NOT
        # entry-window pre-filtered (different portfolios on the same instrument
        # may have different windows, so a single global pre-filter would corrupt
        # the others' warmup). Each leg therefore drops its OWN pre-entry_start
        # bars here, reproducing the single-portfolio start-side stream pre-filter
        # (`_filter_bars_by_time_of_day(bars, start, None)`) per leg. START side
        # only — post-end bars still run so exits keep processing. No-op in the
        # single-portfolio path (_in_session False; its stream is already filtered),
        # so that proven path stays byte-identical.
        if self._window_self_filter and self._entry_start_min >= 0:
            if (int(bar.ts_event) % 86_400_000_000_000) // 60_000_000_000 < self._entry_start_min:
                return

        # Extra strategy-subscribe bar types are received so they are
        # available, but the single-signal logic runs on the primary bar
        # type (``config.bar_type``) only — ignore the extras here.
        if bar.bar_type in self._extra_sub_bts:
            return

        # Live conservative-fill VWAP: accumulate the running session VWAP — same
        # cumulation the post-run `_running_vwap` does — so an SL/TP exit can be
        # repriced conservatively live for the monitor. Format A tracks bid/ask
        # independently; Format B tracks the single primary stream. Inert unless
        # gated on.
        if self._cons_vwap_on:
            if self._exit_fmt == "bidask":
                if bar.bar_type == self._fa_bid_bt:
                    self._update_cons_vwap(bar, "bid")
                elif bar.bar_type == self._fa_ask_bt:
                    self._update_cons_vwap(bar, "ask")
            elif self._exit_fmt == "ohlcv" and bar.bar_type == self.config.bar_type:
                self._update_cons_vwap(bar, "single")

        # Unified-engine live portfolio-SL enforcement (gated; inert by default).
        # The monitor publishes a combined-loss breach ts to the shared fire bus;
        # we close our OWN position live this bar (one-pass SqOff) and block
        # re-entry for the rest of the day. handled_ns dedups the per-day fire
        # across the primary + bid/ask bars sharing a timestamp.
        if self._pf_monitor_enforced and self._pf_sl_bus:
            _fire = self._pf_sl_bus[-1]
            # Bus entries are action-aware dicts {ts, action, market} (the monitor
            # carries each fire's own action so pf_sl and pf_tgt can differ);
            # tolerate a bare int for forward/backward safety.
            if isinstance(_fire, dict):
                _fts = int(_fire["ts"])
                _fire_action = str(_fire.get("action", self._pf_monitor_action))
                _fire_market = bool(_fire.get("market", self._pf_monitor_market_mode))
                _fire_reason = _fire.get("reason")
            else:
                _fts = int(_fire)
                _fire_action = self._pf_monitor_action
                _fire_market = self._pf_monitor_market_mode
                _fire_reason = None
            if _fts > self._pf_sl_handled_ns:
                self._pf_sl_handled_ns = _fts
                self._current_bar_ts_ns = int(bar.ts_event)
                if _fire_action == "reexecute":
                    # ReExecute: close our position and RE-ARM a re-entry at the
                    # captured entry price (or market for plain ReExecute) — reusing
                    # the existing `_try_reexec_reentry` machinery. Capture entry +
                    # side BEFORE the close wipes them; arm AFTER (the close's
                    # _reset_exit_state clears _reexec_limit_px). Do NOT block. The
                    # close carries the monitor's reason (so it reads "-> ReExecute
                    # at Entry Price", not "-> SqOff").
                    if self.entry_price != 0:
                        _e = self.entry_price
                        _long = (self.position_side == "LONG")
                        self._force_portfolio_sl_close(_fire_reason)
                        self._reexec_limit_px = _e
                        self._reexec_limit_is_long = _long
                        self._reexec_market_mode = _fire_market
                        self._reexec_limit_submitted = False
                else:
                    # SqOff: close (only when holding — a flat leg whose sibling
                    # triggered the breach must not set _expecting_close_fill, which
                    # no fill would clear) then block re-entry. "sqoff" = rest of day
                    # (day-scoped pf_sl); "sqoff_run" = rest of run (absolute pf_tgt).
                    if self.entry_price != 0:
                        self._force_portfolio_sl_close(_fire_reason)
                    if _fire_action == "sqoff_run":
                        self._pf_sl_blocked_forever = True
                    else:
                        self._pf_sl_blocked_date = datetime.fromtimestamp(
                            int(bar.ts_event) / 1e9, tz=self._utc_tz
                        ).date()

        # ── Cross-portfolio action consume (multi-portfolio session, SAME bar) ──
        # Another portfolio's monitor published a verb targeting THIS leg's portfolio;
        # act on it live this bar (spec §2.1(h)/(i)/(j)). Idempotent per event. Inert
        # for single-portfolio (the bus is empty/cleared). A TARGET portfolio may have
        # no enforcement of its own, so gate on portfolio_id only — not _pf_monitor_enforced.
        if self.config.portfolio_id:
            try:
                from core.backtest_runner.cross_portfolio import peek_cross_pf_live
                _xevts = peek_cross_pf_live(self.config.portfolio_id, int(bar.ts_event))
            except Exception:  # noqa: BLE001
                _xevts = []
            for _xe in _xevts:
                _idem = _xe.get("idem")
                if _idem in self._cross_pf_seen:
                    continue
                self._cross_pf_seen.add(_idem)
                _verb = _xe.get("verb"); _src = _xe.get("source")
                self._current_bar_ts_ns = int(bar.ts_event)
                if _verb == "sqoff":
                    if self.entry_price != 0:
                        self._force_portfolio_sl_close(
                            f"Portfolio Stoploss: cross-portfolio SqOff from {_src}")
                    self._pf_sl_blocked_date = datetime.fromtimestamp(
                        int(bar.ts_event) / 1e9, tz=self._utc_tz).date()
                elif _verb in ("execute", "start"):
                    # Activate the target leg's entries (and, for Start, clear any block).
                    self._armed_for_entry = True
                    if _verb == "start":
                        self._pf_sl_blocked_forever = False
                        self._pf_sl_blocked_date = None

        # Custom streaming aggregation: convert the BASE bar(s) into the
        # higher-timeframe aggregated bar(s) and run the strategy logic on
        # those, instead of the base bars (replaces Nautilus' internal
        # aggregation). Returns None mid-window.
        if self._aggregating:
            if self._exit_fmt != "bidask":
                # Exits (SL/TP/trailing/square-off) run on EVERY base bar so they
                # trigger at base-data resolution, not at the aggregated window.
                self._manage_open_position(bar)
                # Signal/entry runs only when the aggregated window closes.
                agg = self._primary_agg.on_bar(bar)
                if agg is not None:
                    self._feed_indicators(agg)
                    self._evaluate_entry(agg)
            else:
                self._on_bar_aggregating_bidask(bar)
            return

        # ── No aggregation: original base-bar dispatch ──
        # Format B / C: the slot has a single bar stream — process directly.
        if self._exit_fmt != "bidask":
            self._on_primary_bar(bar)
            return

        # Format A: route bid/ask/primary bars into a per-timestamp buffer and
        # run the bar logic once the (primary, bid, ask) trio for a ts_event is
        # complete. Bars arrive in ts order, so when a bar at ts_event T
        # arrives, any buffered ts < T is final and is flushed first (with an
        # OHLCV fallback for whichever side a data gap left missing).
        ts = bar.ts_event
        stale = sorted(t for t in self._fa_pending if t < ts)
        for t in stale:
            g = self._fa_pending.pop(t)
            if "primary" in g:
                self._on_primary_bar(g["primary"], g.get("bid"), g.get("ask"))
        slot = self._fa_pending.setdefault(ts, {})
        bt = bar.bar_type
        if bt == self.config.bar_type:
            slot["primary"] = bar
            # When the slot's own series IS one side of the pair it doubles
            # as that side (e.g. a slot configured directly on ASK bars).
            if self._fa_bid_bt is not None and bt == self._fa_bid_bt:
                slot["bid"] = bar
            if self._fa_ask_bt is not None and bt == self._fa_ask_bt:
                slot["ask"] = bar
        elif self._fa_bid_bt is not None and bt == self._fa_bid_bt:
            slot["bid"] = bar
        elif self._fa_ask_bt is not None and bt == self._fa_ask_bt:
            slot["ask"] = bar
        if "primary" in slot and "bid" in slot and "ask" in slot:
            self._fa_pending.pop(ts)
            self._on_primary_bar(slot["primary"], slot["bid"], slot["ask"])

    def _feed_indicators(self, agg_bar: Bar) -> None:
        """Drive the (unregistered) indicators from an emitted aggregated bar."""
        for ind in self._agg_indicators:
            ind.handle_bar(agg_bar)

    def _indicators_ready(self) -> bool:
        """Readiness gate that works in aggregating, manual-feed, and base-bar modes."""
        # Aggregating OR manual-feed (non-agg session leg with a window): the
        # indicators are NOT registered with the strategy, so indicators_initialized()
        # (which checks REGISTERED indicators) is wrong — check the manually-fed set
        # directly (``_agg_indicators`` holds the same indicator objects in both modes).
        if self._aggregating or self._manual_feed_inds:
            return all(ind.initialized for ind in self._agg_indicators)
        return self.indicators_initialized()

    def _on_bar_aggregating_bidask(self, bar: Bar) -> None:
        """Format A under aggregation: exits run on the BASE (primary, bid, ask)
        trio per ts_event (base-data resolution); signal/entry runs on the
        aggregated PRIMARY window. Mirrors the no-aggregation Format A buffering in
        on_bar, but routes exits to _manage_open_position and entries to the
        aggregated bar."""
        # ── Exits on the base trio (same buffering as the no-aggregation path) ──
        ts = bar.ts_event
        stale = sorted(t for t in self._fa_pending if t < ts)
        for t in stale:
            g = self._fa_pending.pop(t)
            if "primary" in g:
                self._manage_open_position(g["primary"], g.get("bid"), g.get("ask"))
        slot = self._fa_pending.setdefault(ts, {})
        bt = bar.bar_type
        if bt == self.config.bar_type:
            slot["primary"] = bar
            if self._fa_bid_bt is not None and bt == self._fa_bid_bt:
                slot["bid"] = bar
            if self._fa_ask_bt is not None and bt == self._fa_ask_bt:
                slot["ask"] = bar
        elif self._fa_bid_bt is not None and bt == self._fa_bid_bt:
            slot["bid"] = bar
        elif self._fa_ask_bt is not None and bt == self._fa_ask_bt:
            slot["ask"] = bar
        if "primary" in slot and "bid" in slot and "ask" in slot:
            self._fa_pending.pop(ts)
            self._manage_open_position(slot["primary"], slot["bid"], slot["ask"])

        # ── Signal/entry on the aggregated primary window close ──
        if bt == self.config.bar_type:
            agg = self._primary_agg.on_bar(bar)
            if agg is not None:
                self._feed_indicators(agg)
                self._evaluate_entry(agg)

    def _on_primary_bar(self, bar: Bar, bid_bar: Bar | None = None,
                        ask_bar: Bar | None = None) -> None:
        # Cache the current bar's timestamp so _handle_exit can stamp
        # _reentry_blocked_until_ns without us having to thread `bar` through
        # every call site. on_order_filled also reads it for the re-entry
        # delay starting point.
        self._current_bar_ts_ns = bar.ts_event

        # Manual indicator feed (non-agg session leg with a window): _on_primary_bar
        # runs only AFTER on_bar's self-filter has dropped pre-window bars, so feeding
        # here warms the indicators on in-window bars only — matching the single path's
        # filtered-stream auto-feed. Fed before _indicators_ready()/signal eval so the
        # current bar is included, exactly like register_indicator_for_bars would.
        for _ind in self._manual_feed_inds:
            _ind.handle_bar(bar)

        # Mark Price (spec §3, approach B): capture the PREVIOUS bar's close for
        # this bar's trigger, then roll the rolling prev-close forward. Done
        # before any early return so the series stays continuous across warm-up
        # and squareoff bars.
        _mark_prev_close = self._prev_close
        self._prev_close = float(bar.close)

        # RBO state runs every bar, even before indicators warm up — the
        # range-monitoring window can start before the indicator gets enough
        # bars, and we still need to track high/low through that period.
        if self._rbo_enabled:
            self._rbo_step(bar)

        if not self._indicators_ready():
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
                # Abandon any unfilled ReExecute-at-Entry-Price limit at the
                # cutoff (re-entries are blocked past squareoff; one-shot).
                self._cancel_reexec_limit()
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
            # Three-format trigger reference (spec §3 / §4.1) — see
            # resolve_trigger_hl. Format A passes the paired bid/ask OHLC.
            bh = float(bid_bar.high) if bid_bar is not None else None
            bl = float(bid_bar.low) if bid_bar is not None else None
            ah = float(ask_bar.high) if ask_bar is not None else None
            al = float(ask_bar.low) if ask_bar is not None else None
            eff_high, eff_low = resolve_trigger_hl(
                self._exit_fmt, is_long, is_short, close,
                float(bar.high), float(bar.low),
                bid_high=bh, bid_low=bl, ask_high=ah, ask_low=al,
                mark_price=_mark_prev_close,
            )
            self._check_exits(close, is_long, is_short, eff_high, eff_low)
        else:
            self._check_entries(close, is_flat, is_long, is_short)

    # ── Aggregating mode: exits on the BASE bar, signal on the aggregated bar ──
    # These two helpers split _on_primary_bar's responsibilities so the exit
    # engine (SL/TP/trailing/target-lock/move-to-cost/square-off) is evaluated at
    # base-data resolution, while the strategy signal/entry keeps operating on the
    # subscribed (aggregated) timeframe. The exit/entry PARAMETER logic itself
    # (_check_exits / _force_squareoff / _check_entries) is unchanged — only which
    # bar drives it, and when. Used only by the aggregating branch of on_bar; the
    # non-aggregating path stays on the combined _on_primary_bar above.
    def _manage_open_position(self, bar: Bar, bid_bar: Bar | None = None,
                              ask_bar: Bar | None = None) -> None:
        """Exit/envelope side — runs on every BASE bar. Mark-price roll, square-off
        and SL/TP/trailing via _check_exits. No indicators, no entries."""
        self._current_bar_ts_ns = bar.ts_event

        # Mark Price (spec §3, approach B): previous (base) bar's close.
        _mark_prev_close = self._prev_close
        self._prev_close = float(bar.close)

        is_flat = self.position_side is None
        is_long = self.position_side == "LONG"
        is_short = self.position_side == "SHORT"

        # Square-off is the deterministic outer envelope (identical logic to
        # _on_primary_bar) — now evaluated per base bar so it fires at the exact
        # base-resolution timestamp rather than the aggregated window close.
        if self._squareoff_min >= 0:
            local_dt = datetime.fromtimestamp(bar.ts_event / 1e9, tz=self._utc_tz).astimezone(self._squareoff_tz)
            local_min = local_dt.hour * 60 + local_dt.minute
            local_date = local_dt.date()

            if self._squareoff_done_date is not None and local_date != self._squareoff_done_date:
                self._squareoff_done_date = None

            if local_min >= self._squareoff_min and self._squareoff_done_date != local_date:
                # Abandon any unfilled ReExecute-at-Entry-Price limit at the
                # cutoff (re-entries are blocked past squareoff; one-shot) — same
                # as the non-aggregating _on_primary_bar path. Without this, a
                # leg still waiting for price to return to its entry price would
                # keep waiting from the next session instead of falling through
                # to its own signal logic.
                self._cancel_reexec_limit()
                if not is_flat:
                    self._force_squareoff()
                self._squareoff_done_date = local_date
                return
            if self._squareoff_done_date == local_date:
                return

        if not is_flat:
            close = float(bar.close)
            bh = float(bid_bar.high) if bid_bar is not None else None
            bl = float(bid_bar.low) if bid_bar is not None else None
            ah = float(ask_bar.high) if ask_bar is not None else None
            al = float(ask_bar.low) if ask_bar is not None else None
            eff_high, eff_low = resolve_trigger_hl(
                self._exit_fmt, is_long, is_short, close,
                float(bar.high), float(bar.low),
                bid_high=bh, bid_low=bl, ask_high=ah, ask_low=al,
                mark_price=_mark_prev_close,
            )
            self._check_exits(close, is_long, is_short, eff_high, eff_low)
        elif self._reexec_limit_px > 0:
            # Flat with a pending portfolio-ReExecute re-entry: process it here on
            # the BASE bar so it fires at base-data resolution — the next base bar
            # (plain ReExecute) or the instant price returns to the entry price
            # (ReExecute at Entry Price) — NOT delayed to the aggregated window.
            self._try_reexec_reentry(float(bar.close), is_flat)

    def _evaluate_entry(self, bar: Bar) -> None:
        """Signal/entry side — runs only when a new AGGREGATED bar closes. RBO step,
        readiness gate, and _check_entries when flat. A square-off that already
        fired today (set on the base-bar path) still blocks fresh entries."""
        if self._rbo_enabled:
            self._rbo_step(bar)

        if not self._indicators_ready():
            return

        if self._squareoff_min >= 0 and self._squareoff_done_date is not None:
            local_date = datetime.fromtimestamp(
                bar.ts_event / 1e9, tz=self._utc_tz
            ).astimezone(self._squareoff_tz).date()
            if self._squareoff_done_date == local_date:
                return

        if self.position_side is None:
            close = float(bar.close)
            self._check_entries(close, True, False, False)

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
        hh = self._squareoff_min // 60
        mm = self._squareoff_min % 60
        # Reports are displayed in IST. The squareoff time is configured/stored in
        # self._squareoff_tz (the UI sends it to the backend as UTC); express it
        # in IST here so the reason label matches the IST time columns rather than
        # showing e.g. "10:45 UTC".
        try:
            _ref = (datetime.fromtimestamp(self._current_bar_ts_ns / 1e9, tz=self._utc_tz)
                    .astimezone(self._squareoff_tz)
                    .replace(hour=hh, minute=mm, second=0, microsecond=0))
            _ist = _ref.astimezone(ZoneInfo("Asia/Kolkata"))
            reason = f"Squareoff: daily close @ {_ist:%H:%M} IST"
        except Exception:  # noqa: BLE001 — fall back to the configured tz label
            tz_name = getattr(self._squareoff_tz, "key", None) or str(self._squareoff_tz)
            reason = f"Squareoff: daily close @ {hh:02d}:{mm:02d} {tz_name}"
        self._pin_directional_close()
        self._close_with_reason(reason)
        self._reset_exit_state()

    def _pin_directional_close(self, vwap_enhance: bool = False) -> None:
        """Live §8.1 fill for a MARKET exit, pinned for the ConservativeFillModel.

        Spec §8.1 SPLIT (by who triggers the close):
        - TIME squareoff / End Time / EOD (``_force_squareoff``, vwap_enhance=False):
          the **directional close only** — Format A LONG→bid_close, SHORT→ask_close.
          (Format B/C need no pin — the engine already fills at close/ltp, which IS
          the directional close.)
        - PORTFOLIO SqOff = portfolio SL/Target hit / force-sqoff (``_force_portfolio_sl_close``,
          vwap_enhance=True): the directional close with the **§8.1 VWAP enhancement**
          layered on — LONG→MIN(vwap, close), SHORT→MAX(vwap, close) (conservative:
          receive less / pay more). Format A uses bid/ask vwap+close; Format B the
          single vwap+close; Format C (ltp) has no vwap → no pin.

        Inert off live-fill / directional-fill / when flat."""
        if not (self._live_fill_on and self._dir_fill_active and self.position_side):
            return
        long = self.position_side == "LONG"
        px = 0.0
        if self._exit_fmt == "bidask":
            st = self._cons_state["bid" if long else "ask"]
            close = st.get("close", 0.0)
            if not (close and close > 0):
                return
            px = close
            if vwap_enhance:  # §8.1 VWAP enhancement (Portfolio SqOff only)
                vw = st.get("vwap", 0.0)
                if vw and vw > 0:
                    px = min(vw, close) if long else max(vw, close)
        elif self._exit_fmt == "ohlcv":
            # Format B directional close = close = engine fill → a pin is only needed
            # for the Portfolio-SqOff VWAP enhancement.
            if not vwap_enhance:
                return
            st = self._cons_state["single"]
            close = st.get("close", 0.0)
            vw = st.get("vwap", 0.0)
            if not (close and close > 0 and vw and vw > 0):
                return
            px = min(vw, close) if long else max(vw, close)
        else:
            return  # Format C (ltp): no VWAP → engine fill = ltp
        if px and px > 0:
            # Key per (instrument, strategy) so two portfolios sharing a venue can't
            # collide on the pin. order.strategy_id == self.id at fill time, so this
            # is byte-identical for single portfolio (the key always matches).
            self._fill_px_bus[f"{self.config.instrument_id}|{self.id}"] = px

    def _force_portfolio_sl_close(self, reason: str | None = None) -> None:
        """Unified-engine live portfolio close on a combined SL/Target breach from
        the monitor. Like ``_force_squareoff`` (no re_execute/reverse). ``reason``
        is the monitor's action-aware tag (e.g. "Portfolio Stoploss: combined 60
        hit -> ReExecute at Entry Price" or "Portfolio Target: ... -> SqOff"); the
        re-entry (for ReExecute) is armed by the caller AFTER this. No-op when flat."""
        self._expecting_close_fill = True
        # §8.1: Portfolio SqOff (SL/Target hit) → directional close + VWAP enhancement.
        self._pin_directional_close(vwap_enhance=True)
        self._close_with_reason(reason or "Portfolio Stoploss: combined SL hit -> SqOff")
        self._reset_exit_state()

    def _update_cons_vwap(self, bar, side: str) -> None:
        """Running session VWAP for one side ("single"/"bid"/"ask"), replicating the
        post-run `_running_vwap`: cumulative (H+L+C)/3 × volume, reset at the session
        boundary; also stores the bar's high/low (the §4.2 "hit"). Drives the live
        conservative-fill adjustment fed to the monitor."""
        st = self._cons_state[side]
        total_min = int(bar.ts_event) // 60_000_000_000
        bucket = (total_min - self._cons_session_min) // 1440
        if bucket != st["bucket"]:
            st["bucket"] = bucket
            st["pv"] = 0.0
            st["v"] = 0.0
        h = float(bar.high); l = float(bar.low); c = float(bar.close); v = float(bar.volume)
        typical = (h + l + c) / 3.0
        st["pv"] += typical * v
        st["v"] += v
        st["vwap"] = (st["pv"] / st["v"]) if st["v"] > 0 else typical
        st["hi"] = h
        st["lo"] = l
        st["close"] = c  # §8.1 directional close (bid_close / ask_close)

    def _close_with_reason(self, reason: str | None) -> None:
        """Close all open positions on this slot's instrument with a tag.

        Thin wrapper around ``self.close_all_positions(...)`` that adds a
        structured ``tags=[reason]``. The closing fill's ``tags`` column
        flows into ``fills_report``, where the orderbook builder splits on
        ``":"`` — prefix becomes EXIT REASON ("Stop Loss"), full string
        becomes EXIT DETAILED REASON. ``reason=None`` is the legacy
        untagged path.
        """
        instrument_id = self.config.instrument_id
        kwargs = {}
        if reason:
            kwargs["tags"] = [reason]
        self.close_all_positions(instrument_id, **kwargs)

    def _current_trading_day_key(self) -> int:
        """Day index of the current bar in the squareoff/session timezone, used
        to reset the per-trading-day sl_moved_to_cost flag at session rollover
        (spec §7). When no non-UTC squareoff tz is configured (``_squareoff_tz``
        falls back to ``_utc_tz``) this is a cheap integer UTC-day bucket — no
        per-bar datetime conversion; otherwise it's the local-date ordinal."""
        if self._squareoff_tz is self._utc_tz:
            return self._current_bar_ts_ns // 86_400_000_000_000
        local_dt = datetime.fromtimestamp(
            self._current_bar_ts_ns / 1e9, tz=self._utc_tz
        ).astimezone(self._squareoff_tz)
        return local_dt.toordinal()

    def _check_exits(
        self,
        close: float,
        is_long: bool,
        is_short: bool,
        high: float | None = None,
        low: float | None = None,
    ) -> None:
        if self.entry_price == 0:
            return

        # Per-day reset (spec §7): the sticky moved-to-cost-today flag is cleared
        # only at each TRADING-DAY rollover, so a new day starts fresh while the
        # mark persists across re-execution cycles within the same day.
        _day = self._current_trading_day_key()
        if self._move_sl_day_key != _day:
            self._move_sl_day_key = _day
            self._move_sl_fired_today = False

        # Intrabar SL/TP triggering (spec execution_logic.html §4.1). A bar
        # whose range straddles the SL/TP level fires even if the bar CLOSE
        # did not cross it — Format-B "high/low" semantics. Falls back to
        # close when high/low aren't supplied (defensive; on_bar always does).
        bar_high = close if high is None else high
        bar_low = close if low is None else low

        # Calculate current profit
        if is_long:
            profit_pct = ((close - self.entry_price) / self.entry_price) * 100
        else:
            profit_pct = ((self.entry_price - close) / self.entry_price) * 100

        # Update highest profit
        if profit_pct > self.highest_profit:
            self.highest_profit = profit_pct

        # Move SL to Cost (spec §3, per-slot adaptation). When enabled and
        # the position has been open for at least safety_sec AND is currently
        # in profit, raise current_sl to entry_price (locking in breakeven).
        # Skipped on long positions when no_buy_legs is set. Action variant
        # "Move SL for All Legs Despite Loss/Profit" raises SL even when not
        # in profit (which immediately closes the trade — same as spec).
        _move_fired_before = self._move_sl_fired_this_position
        if self.config.move_sl_enabled and not self._move_sl_fired_this_position:
            # Hit-On-Leg cross-slot trigger (spec §3 1.3(f)/(g)): if any sibling
            # leg in this portfolio fired SL or target AFTER this leg entered,
            # snap our SL up to entry. Independent of safety_sec; explicit user
            # request that's typically a "protect surviving legs" reflex.
            if (self.config.move_sl_hit_on_leg_sl or self.config.move_sl_hit_on_leg_target) \
                    and self.config.slot_id and self._entry_filled_at_ns > 0:
                sibling_event = False
                for sid, events in self._sibling_bus.items():
                    if sid == self.config.slot_id:
                        continue
                    if self.config.move_sl_hit_on_leg_sl and events.get("sl_ns", 0) > self._entry_filled_at_ns:
                        sibling_event = True
                        break
                    if self.config.move_sl_hit_on_leg_target and events.get("tgt_ns", 0) > self._entry_filled_at_ns:
                        sibling_event = True
                        break
                if sibling_event:
                    new_sl = self.entry_price
                    if is_long and new_sl > self.current_sl:
                        self.current_sl = new_sl
                        self._was_trailed = True
                        self._move_sl_fired_this_position = True
                    elif is_short and (self.current_sl == 0 or new_sl < self.current_sl):
                        self.current_sl = new_sl
                        self._was_trailed = True
                        self._move_sl_fired_this_position = True

            # Safety Seconds (TBD-1, resolved): anchored to position entry time,
            # not market open. Uniform across IF/FX/CF/XF — FX/crypto trade
            # nearly continuously and have no clean "open" to anchor against.
            # Behavior: ignore the first N seconds after this leg entered.
            elapsed_ns = self._current_bar_ts_ns - self._entry_filled_at_ns
            safety_ns = int(self.config.move_sl_safety_sec) * 1_000_000_000
            if elapsed_ns >= safety_ns:
                # no_buy_legs adapted: skip move-to-cost on LONG positions.
                skip = self.config.move_sl_no_buy_legs and is_long
                if not skip:
                    in_profit = profit_pct > 0
                    # Match the Move-SL variant tolerantly. The UI emits
                    # "Move SL for All Legs Despite Loss / Profit" (spaces around
                    # the slash), older configs/tests use "…Loss/Profit", and the
                    # spec label is just "Move SL for All Legs" — an exact-string
                    # compare silently missed the UI spelling and degraded to
                    # profitable-only. Normalise (lower + strip spaces) and key on
                    # stable tokens so every spelling resolves to the same variant.
                    _act = (self.config.move_sl_action or "").lower().replace(" ", "")
                    ltp_buffer_action = "ltp" in _act and "buffer" in _act
                    move_all = "alllegs" in _act and not ltp_buffer_action
                    if ltp_buffer_action and not in_profit:
                        # Action v3 (spec execution_logic.html §4.5): on a losing
                        # leg slide SL toward LTP by a MULTIPLICATIVE buffer —
                        # Loss BUY → sl = ltp × (1 − buf); Loss SELL → sl = ltp ×
                        # (1 + buf). move_sl_ltp_buffer is a fraction (0.0005 ≈
                        # the spec's ×0.9995 / ×1.0005).
                        buf = max(0.0, float(self.config.move_sl_ltp_buffer))
                        if is_long:
                            new_sl = self._snap_to_tick(close * (1.0 - buf))
                            if new_sl > self.current_sl:
                                self.current_sl = new_sl
                                self._was_trailed = True
                                self._move_sl_fired_this_position = True
                        else:
                            new_sl = self._snap_to_tick(close * (1.0 + buf))
                            if self.current_sl == 0 or new_sl < self.current_sl:
                                self.current_sl = new_sl
                                self._was_trailed = True
                                self._move_sl_fired_this_position = True
                    elif in_profit or move_all:
                        new_sl = self.entry_price
                        # Only raise (long) / lower (short) — never relax.
                        if is_long and new_sl > self.current_sl:
                            self.current_sl = new_sl
                            self._was_trailed = True
                            self._move_sl_fired_this_position = True
                        elif is_short and (self.current_sl == 0 or new_sl < self.current_sl):
                            self.current_sl = new_sl
                            self._was_trailed = True
                            self._move_sl_fired_this_position = True

        # Portfolio-aggregate Move SL trigger (spec §2.3). The two-pass runner
        # injects move_sl_agg_trigger_ns — the bar timestamp at which the whole
        # portfolio's combined P&L first crossed the configured threshold.
        # Every leg open when that fired (entry at/before the trigger, still in
        # position) snaps its SL to entry on the first bar at/after it.
        # Independent of move_sl_enabled — it is a portfolio-level trigger, not
        # a sub-option of per-slot Move SL to Cost.
        agg_ns = self.config.move_sl_agg_trigger_ns
        # Unified-engine live aggregate Move-SL: the monitor publishes the combined-
        # P&L cross ts to the shared agg bus; use it as the trigger (replacing the
        # two-pass discovery's static config value). Inert unless enforcing + a
        # trigger has been published.
        if self._pf_monitor_enforced and self._pf_agg_bus:
            agg_ns = max(int(agg_ns), int(self._pf_agg_bus[-1]))
        if (agg_ns > 0 and not self._move_sl_fired_this_position
                and self._entry_filled_at_ns > 0
                and self._current_bar_ts_ns >= agg_ns
                and agg_ns >= self._entry_filled_at_ns):
            new_sl = self.entry_price
            if is_long and new_sl > self.current_sl:
                self.current_sl = new_sl
                self._was_trailed = True
                self._move_sl_fired_this_position = True
            elif is_short and (self.current_sl == 0 or new_sl < self.current_sl):
                self.current_sl = new_sl
                self._was_trailed = True
                self._move_sl_fired_this_position = True

        # When Move SL to Cost just fired this bar, anchor the dedicated
        # post-move trail (spec §4.6) at the current profit so subsequent
        # ratchet steps measure gain from the move point, not from entry. Also
        # stamp the per-day sticky flag (spec §7) so the ReExecute/ReEntry gates
        # keep blocking this leg for the rest of the trading day.
        if not _move_fired_before and self._move_sl_fired_this_position:
            self._move_sl_trail_anchor = profit_pct
            self._move_sl_fired_today = True

        # Target locking. When triggered, raises (long) or lowers (short)
        # current_sl to the target_lock_minimum. Sets _was_trailed for the
        # on_sl_action_on filter — a hit on the locked level is classified as
        # trailing SL per Other_Settings_Logic.html spec.
        if self.config.target_lock_trigger > 0 and self.config.target_lock_minimum > 0:
            if self.highest_profit >= self.config.target_lock_trigger:
                lock_sl = self._compute_sl_price(is_long, self.config.target_lock_minimum)
                if is_long and lock_sl > self.current_sl:
                    self.current_sl = lock_sl
                    self._was_trailed = True
                elif is_short and (self.current_sl == 0 or lock_sl < self.current_sl):
                    self.current_sl = lock_sl
                    self._was_trailed = True

        # Trailing SL — same was_trailed semantics as target lock.
        if self.config.move_sl_trail_after and self.config.move_sl_enabled:
            # Spec §4.6 — dedicated "Trail After Move SL". Trailing stays
            # suppressed until Move SL to Cost fires; afterwards a dedicated
            # ratchet, anchored at the move-fire profit, tightens the SL
            # further into profit by trailing_sl_offset for every
            # trailing_sl_step of profit gained past that anchor.
            if self._move_sl_fired_this_position and self.config.trailing_sl_step > 0:
                gain = profit_pct - self._move_sl_trail_anchor
                if gain >= self.config.trailing_sl_step:
                    steps = int(gain / self.config.trailing_sl_step)
                    # Negative pct in _compute_sl_price places the SL on the
                    # profit side of entry (above for long, below for short).
                    locked = self._compute_sl_price(
                        is_long, -steps * self.config.trailing_sl_offset
                    )
                    if is_long and locked > self.current_sl:
                        self.current_sl = locked
                        self._was_trailed = True
                        self.log.info(  # spec §4.6 observability token
                            f"LEG_TRAIL_SL_UPDATED | steps={steps} | sl={self.current_sl} "
                            f"| profit_pct={profit_pct:.4f}"
                        )
                    elif is_short and (self.current_sl == 0 or locked < self.current_sl):
                        self.current_sl = locked
                        self._was_trailed = True
                        self.log.info(  # spec §4.6 observability token
                            f"LEG_TRAIL_SL_UPDATED | steps={steps} | sl={self.current_sl} "
                            f"| profit_pct={profit_pct:.4f}"
                        )
            # else: move not fired yet → trailing suppressed (spec §3.4).
        elif self.config.stop_loss_type == "trailing" and self.config.trailing_sl_step > 0:
            steps = int(self.highest_profit / self.config.trailing_sl_step)
            if steps > 0:
                trail_offset = steps * self.config.trailing_sl_offset
                trail_sl = self._compute_sl_price(is_long, trail_offset)
                if is_long and trail_sl > self.current_sl:
                    self.current_sl = trail_sl
                    self._was_trailed = True
                elif is_short and (self.current_sl == 0 or trail_sl < self.current_sl):
                    self.current_sl = trail_sl
                    self._was_trailed = True

        # Check SL hit — intrabar (spec §4.1): a LONG leg's SL fires when the
        # bar LOW pierces it; a SHORT leg's SL fires when the bar HIGH does.
        sl_hit = False
        if self.current_sl > 0:
            if is_long and bar_low <= self.current_sl:
                sl_hit = True
            elif is_short and bar_high >= self.current_sl:
                sl_hit = True

        if sl_hit:
            # Prefer wall-clock seconds (spec name "SL Wait (sec)") when configured.
            # Falls back to legacy bar-count gate when only sl_wait_bars is set.
            if self.config.sl_wait_sec > 0:
                if self._sl_wait_started_ns == 0:
                    self._sl_wait_started_ns = self._current_bar_ts_ns
                elapsed_ns = self._current_bar_ts_ns - self._sl_wait_started_ns
                wait_ns = int(self.config.sl_wait_sec) * 1_000_000_000
                if elapsed_ns < wait_ns:
                    sl_hit = False
            elif self.config.sl_wait_bars > 0:
                self.sl_wait_count += 1
                if self.sl_wait_count < self.config.sl_wait_bars:
                    sl_hit = False
            if sl_hit:
                # Trigger extreme that pierced the SL: LONG fires on bar_low,
                # SHORT on bar_high (eff_high/eff_low = bid/ask under Format A).
                self._handle_exit("sl", is_long, close=close,
                                  trigger_px=(bar_low if is_long else bar_high))
                return
        else:
            self.sl_wait_count = 0
            self._sl_wait_started_ns = 0

        # ── Leg-Level Trailing Target / Profit-Lock (spec §4.7) ──
        # Ratcheting profit-lock evaluated in profit-% terms. Activation arms
        # the lock once profit first reaches the threshold; once armed, a fall
        # back to the locked floor exits the leg (reason TARGET_TRAIL).
        tgt_trail_hit = False
        if self.config.tgt_trail_enabled:
            _prev_tt_active = self._tgt_trail_active
            _prev_tt_stop = self._tgt_trail_stop
            (self._tgt_trail_active, self._tgt_trail_stop,
             self._tgt_trail_anchor, tgt_trail_hit) = advance_trailing_target(
                self._tgt_trail_active, self._tgt_trail_stop,
                self._tgt_trail_anchor, profit_pct,
                self.config.tgt_trail_when_profit_reach,
                self.config.tgt_trail_lock_min_profit,
                self.config.tgt_trail_every, self.config.tgt_trail_by,
            )
            # Spec §4.7 observability tokens: ACTIVATED on the bar profit first
            # reaches the activation threshold; UPDATED whenever the locked floor
            # ratchets up.
            if not _prev_tt_active and self._tgt_trail_active:
                self.log.info(
                    f"LEG_TRAIL_TARGET_ACTIVATED | when_reach={self.config.tgt_trail_when_profit_reach} "
                    f"| lock={self._tgt_trail_stop:.4f} | profit_pct={profit_pct:.4f}"
                )
            elif self._tgt_trail_active and self._tgt_trail_stop != _prev_tt_stop:
                self.log.info(
                    f"LEG_TRAIL_TARGET_UPDATED | current_stop={self._tgt_trail_stop:.4f} "
                    f"| profit_pct={profit_pct:.4f}"
                )

        # Check TP hit — intrabar, mirrored: LONG TP fires on bar HIGH,
        # SHORT TP fires on bar LOW. A trailing-target floor breach also
        # routes through the TP exit path.
        tp_condition = tgt_trail_hit
        if not tp_condition and self.current_tp > 0:
            if is_long and bar_high >= self.current_tp:
                tp_condition = True
            elif is_short and bar_low <= self.current_tp:
                tp_condition = True

        if tp_condition:
            fire = True
            # Target Wait (spec §4.3): a FIXED-TP trigger must persist for the
            # configured duration before firing; resets if price retreats.
            # A trailing-target floor breach is profit-protective — not delayed.
            if not tgt_trail_hit:
                if self.config.tgt_wait_sec > 0:
                    if self._tp_wait_started_ns == 0:
                        self._tp_wait_started_ns = self._current_bar_ts_ns
                    elapsed_ns = self._current_bar_ts_ns - self._tp_wait_started_ns
                    if elapsed_ns < int(self.config.tgt_wait_sec) * 1_000_000_000:
                        fire = False
                elif self.config.tgt_wait_bars > 0:
                    self.tp_wait_count += 1
                    if self.tp_wait_count < self.config.tgt_wait_bars:
                        fire = False
            if fire:
                self._tp_was_trail = tgt_trail_hit
                # Trigger extreme that hit the TP: LONG fires on bar_high, SHORT
                # on bar_low. (Ignored by the PnL-based trailing-target reason.)
                self._handle_exit("tp", is_long, close=close,
                                  trigger_px=(bar_high if is_long else bar_low))
        else:
            # No TP condition this bar — clear the Target-Wait counters.
            self.tp_wait_count = 0
            self._tp_wait_started_ns = 0

    def _handle_exit(self, exit_type: str, was_long: bool, close: float = 0.0,
                     trigger_px: float = 0.0) -> None:
        # Publish this leg's exit event to the cross-slot bus so sibling legs
        # with Hit-On-Leg-SL / Hit-On-Leg-Target can react (spec §3).
        # _exit_events_self mirrors the publish so the two-pass runner can
        # surface this leg's hit timestamps into pass 2's pre-seeded bus.
        if self.config.slot_id:
            entry = self._sibling_bus.setdefault(self.config.slot_id, {})
            _evt_key = "sl_ns" if exit_type == "sl" else "tgt_ns"
            entry[_evt_key] = self._current_bar_ts_ns
            self._exit_events_self[_evt_key] = self._current_bar_ts_ns

        # Live conservative-fill (gated): capture this SL/TP exit's conservative §4.2
        # price so the close fill can publish the conservative-vs-engine delta to the
        # monitor (matches the post-run reprice). Format A uses bid/ask vwap vs the
        # opposite side's extreme; Format B uses the single vwap vs trigger_px (the
        # §4.2 hit); Format C (ltp) has no vwap → no adjustment (fill == engine close).
        if self._cons_vwap_on and self._cons_fill_active and exit_type in ("sl", "tp"):
            _cons = 0.0
            if self._exit_fmt == "bidask":
                if was_long:
                    _vw = self._cons_state["bid"]["vwap"]
                    _hit = self._cons_state["ask"]["lo"] if exit_type == "sl" else self._cons_state["ask"]["hi"]
                    if _vw > 0 and _hit > 0:
                        _cons = min(_vw, _hit)
                else:
                    _vw = self._cons_state["ask"]["vwap"]
                    _hit = self._cons_state["bid"]["hi"] if exit_type == "sl" else self._cons_state["bid"]["lo"]
                    if _vw > 0 and _hit > 0:
                        _cons = max(_vw, _hit)
            elif self._exit_fmt == "ohlcv":
                _vw = self._cons_state["single"]["vwap"]
                if _vw > 0 and trigger_px > 0:
                    _cons = min(_vw, trigger_px) if was_long else max(_vw, trigger_px)
            if _cons > 0:
                if self._live_fill_on:
                    # Pin the conservative price; the venue's ConservativeFillModel
                    # fills this leg's close AT it (single-pass, no post-run reprice).
                    # Keyed per (instrument, strategy) — see _maybe_pin_directional_close.
                    self._fill_px_bus[f"{self.config.instrument_id}|{self.id}"] = _cons
                else:  # cons-bus mode: stash for the close-fill delta to the monitor
                    _q = 0.0
                    try:
                        _op = self.cache.positions_open(
                            instrument_id=self.config.instrument_id, strategy_id=self.id)
                        if _op:
                            _q = abs(float(_op[0].quantity))
                    except Exception:  # noqa: BLE001
                        _q = 0.0
                    if _q > 0:
                        self._pending_cons_px = _cons
                        self._pending_cons_qty = _q
                        self._pending_cons_long = was_long

        raw_action = self.config.on_sl_action if exit_type == "sl" else self.config.on_target_action
        # Action combinations (spec §4.8): on_sl_action / on_target_action may
        # carry up to 3 comma-separated actions. Parse into an ordered list and
        # dispatch each below.
        actions = parse_leg_actions(raw_action)

        # Apply on_sl_action_on / on_target_action_on filter per
        # Other_Settings_Logic.html. "Suppression" downgrades the configured
        # action(s) to plain "close" — position is already squared off, just
        # don't fire re_execute / reverse / execute / re_entry follow-ups.
        if exit_type == "sl":
            filter_cfg = self.config.on_sl_action_on
            if filter_cfg == "OnSL_Only" and self._was_trailed:
                # SL was trailed; OnSL_Only suppresses the action.
                actions = ["close"]
            elif filter_cfg == "OnSL_Trailing_Only" and not self._was_trailed:
                # SL was the fixed initial value; OnSL_Trailing_Only suppresses.
                actions = ["close"]

            # ReExecute_Logics.html P1: suppress re_execute when SL was
            # previously raised to entry by Move SL to Cost. Position is
            # already breakeven; allowing re-execute would re-open exposure.
            # Only the re_execute member is dropped — siblings (e.g. execute)
            # still fire.
            if (
                self.config.no_reexec_sl_cost
                and (self._move_sl_fired_this_position or self._move_sl_fired_today)
                and "re_execute" in actions
            ):
                actions = [a for a in actions if a != "re_execute"] or ["close"]

            # ReExecute_Logics.html P5: same guard for the re_entry action —
            # block price-wait re-entry when SL was moved to cost. Default ON.
            if (
                self.config.no_reentry_sl_cost
                and (self._move_sl_fired_this_position or self._move_sl_fired_today)
                and "re_entry" in actions
            ):
                actions = [a for a in actions if a != "re_entry"] or ["close"]
        else:  # exit_type == "tp"
            filter_cfg = self.config.on_target_action_on
            # _tp_was_trail distinguishes a leg Trailing-Target (profit-lock)
            # exit from a fixed-TP exit, per Other_Settings_Logic.html:
            #   OnTarget_Only          → suppress action on trailing-target exits
            #   OnTarget_Trailing_Only → suppress action on fixed-TP exits
            if filter_cfg == "OnTarget_Only" and self._tp_was_trail:
                actions = ["close"]
            elif filter_cfg == "OnTarget_Trailing_Only" and not self._tp_was_trail:
                actions = ["close"]

        # Build a structured reason for the close order's `tags` so the
        # orderbook's EXIT REASON column shows "Stop Loss" / "Take Profit" /
        # "Trailing SL" / "Reverse on SL" instead of the order-type-derived
        # "Market Exit" placeholder.
        # Reporting price: the TRIGGER EXTREME that actually fired the exit
        # (execution_logic.html §4.1) — Format A bid_high/ask_low, Format B bar
        # high/low — NOT the bar close. This makes "price={px} {op} SL/TP"
        # reflect the real trigger crossing (a short SL fires on the bar HIGH,
        # which can sit above a close that's below the level). Falls back to the
        # close for trigger-less exits (e.g. PnL-based trailing-target).
        px = trigger_px if trigger_px > 0 else close
        if self.entry_price:
            raw_pct = ((px - self.entry_price) / self.entry_price) * 100
        else:
            raw_pct = 0.0
        # Convention: positive pct == in profit (matches profit_pct elsewhere).
        pct = raw_pct if was_long else -raw_pct
        # Format prices at the instrument's own precision so the logged SL is
        # tick-exact (a flat :.4f truncates the 5th decimal on FX, which makes
        # orderbook-only tick-snap verification impossible).
        prec = self.instrument.price_precision if self.instrument else 4
        if exit_type == "sl":
            op = "≤" if was_long else "≥"
            label = "Trailing SL" if self._was_trailed else "Stop Loss"
            if "reverse" in actions:
                label = "Reverse on SL"
            reason = (f"{label}: price={px:.{prec}f} {op} SL={self.current_sl:.{prec}f} "
                      f"(entry {self.entry_price:.{prec}f}, {pct:+.2f}%)")
            # For atr-type SLs, expose the ATR used at entry so a verifier can
            # check expected_sl = entry ∓ mult × ATR purely from the orderbook.
            if self.config.stop_loss_type == "atr":
                reason += f" [atr@entry={self._atr_at_entry:.{prec}f}]"
        else:  # tp
            op = "≥" if was_long else "≤"
            if "reverse" in actions:
                label = "Reverse on TP"
            elif self._tp_was_trail:
                label = "Trailing Target"
            else:
                label = "Take Profit"
            if self._tp_was_trail:
                reason = (f"{label}: profit {pct:+.2f}% fell to locked floor "
                          f"{self._tgt_trail_stop:+.2f}% (entry {self.entry_price:.{prec}f})")
            else:
                reason = (f"{label}: price={px:.{prec}f} {op} TP={self.current_tp:.{prec}f} "
                          f"(entry {self.entry_price:.{prec}f}, {pct:+.2f}%)")

        # 1.2(e) KeepLegRunning: ignore the trigger entirely. Position remains
        # open; SL/TP are disarmed for the rest of this trade so we don't
        # immediately re-fire on the next bar. The next exit only happens via
        # squareoff_time / portfolio clip / manual close. Validation forbids
        # combining it with other actions, so it short-circuits here.
        if "keep_leg_running" in actions:
            self.current_sl = 0.0
            self.current_tp = 0.0
            return

        # Flag the upcoming fill as a close — otherwise on_order_filled would
        # set position_side to the opposite side (SELL closing a LONG would
        # incorrectly mark us as SHORT) and get us stuck in an impossible state.
        saved_entry_price = self.entry_price  # captured before _reset_exit_state wipes it
        self._expecting_close_fill = True
        self._close_with_reason(reason)
        self._reset_exit_state()

        # Dispatch each action in the combination (spec §4.8). "close" is a
        # no-op here — the position is already flat. The others can co-exist
        # (e.g. "re_execute,execute" re-arms this leg AND a sibling).
        for action in actions:
            if action == "re_execute":
                if self.re_execution_count < self.config.max_re_executions:
                    self.re_execution_count += 1
                    # Arm the slot-level re-execution delay (Other Settings spec
                    # §2). Counts from the current bar's timestamp; _check_entries
                    # checks this before allowing the fresh entry on subsequent
                    # bars. delay_between_legs_sec=0 (default) → no block.
                    # ReExecute_Logics.html P2: no_wait_trade_reexec skips the
                    # delay entirely on re-executions.
                    if self.config.delay_between_legs_sec > 0 and not self.config.no_wait_trade_reexec:
                        self._reentry_blocked_until_ns = (
                            self._current_bar_ts_ns
                            + int(self.config.delay_between_legs_sec) * 1_000_000_000
                        )
                    # Allow re-entry on next signal
            elif action == "reverse":
                side = OrderSide.SELL if was_long else OrderSide.BUY
                self._submit_order(side)
                self._set_exit_levels(side)
                # Mark the reversed-into side SYNCHRONOUSLY. The open fill is async
                # (processed after on_bar), so without this the leg's position_side
                # stays None for the rest of THIS bar — and in aggregating mode the
                # same-bar aggregated-window entry check (_evaluate_entry, gated on
                # position_side is None) would fire a CONFLICTING fresh signal entry
                # on the reverse bar (3 orders → net flat → leg/cache desync). The
                # open fill later re-sets the same value (idempotent).
                self.position_side = "LONG" if side == OrderSide.BUY else "SHORT"
            elif action == "execute":
                # 1.2(c) Execute (other leg by leg_id): arm the target slot via
                # the cross-slot bus. The target's _check_entries sees the arm
                # event and flips its _armed_for_entry flag.
                target = self.config.execute_target_leg_id
                if target:
                    entry = self._sibling_bus.setdefault(target, {})
                    entry["arm_ns"] = self._current_bar_ts_ns
            elif action == "re_entry":
                # 1.2(d) ReEntry (price-wait re-entry): set a price trigger; the
                # next signal entry is gated until live price crosses it in the
                # correct direction (back through original entry, by default).
                cap = self.config.max_re_entries
                if cap == 0 or self.re_entry_count < cap:
                    trigger = self.config.reentry_price or saved_entry_price
                    if trigger > 0:
                        self._reentry_armed = True
                        self._reentry_target_price = float(trigger)
                        self._reentry_was_long = was_long

    def _try_reexec_reentry(self, close: float, is_flat: bool) -> bool:
        """Process a pending portfolio-ReExecute re-entry. Returns True when a
        re-entry is pending (so the caller blocks its normal entry path).

        Called on the BASE bar — from ``_manage_open_position`` in aggregating
        mode, and from ``_check_entries`` in the non-aggregating path — so the
        re-entry fires at base-data resolution, NOT only at the aggregated window
        close: plain ReExecute → immediate market on the next base bar; ReExecute
        at Entry Price → fills the instant price returns to the entry price.
        Cleared on fill (on_order_filled) or by a configured squareoff.
        """
        if self._reexec_limit_px <= 0:
            return False
        if is_flat and not self._reexec_limit_submitted:
            if self._reexec_market_mode:
                # Plain ReExecute → "immediately re-opens them" at next-bar
                # market on the original side.
                self._submit_reexec_market()
                self._reexec_limit_submitted = True
            else:
                # ReExecute at Entry Price → re-enter AT the captured entry price
                # E, only once price has RETURNED to it. A plain resting limit at
                # E is immediately marketable when E is on the far side of the
                # breach price (e.g. a long stopped at a loss → market below E),
                # so hold flat until price returns to E, then rest the limit
                # (which fills at E exactly on the touch).
                _E = self._reexec_limit_px
                _premature = (close < _E) if self._reexec_limit_is_long else (close > _E)
                if not _premature:
                    self._submit_reexec_limit()
                    self._reexec_limit_submitted = True
                # else: price hasn't returned to E yet — wait (stay flat).
        return True

    def _check_entries(self, close: float, is_flat: bool, is_long: bool, is_short: bool) -> None:
        # Unified-engine SqOff re-entry block. Rest-of-run (absolute pf_tgt SqOff)
        # blocks permanently; rest-of-day (day-scoped pf_sl SqOff) blocks until the
        # next calendar day. Honour a block whenever one is SET — including a block
        # set by a CROSS-PORTFOLIO SqOff on a target portfolio that has no enforcement
        # of its own. Single-portfolio with no block → the flags are None/False, so
        # this reduces to the original `_pf_monitor_enforced` gate (unchanged).
        if (self._pf_monitor_enforced or self._pf_sl_blocked_forever
                or self._pf_sl_blocked_date is not None):
            if self._pf_sl_blocked_forever:
                return
            if self._pf_sl_blocked_date is not None:
                _d = datetime.fromtimestamp(self._current_bar_ts_ns / 1e9, tz=self._utc_tz).date()
                if _d == self._pf_sl_blocked_date:
                    return
                self._pf_sl_blocked_date = None

        # 1.2(c) Execute: consume any pending arm event from a sibling slot.
        # `arm_ns` set by another leg's "execute" action flips us to armed.
        if not self._armed_for_entry and self.config.slot_id:
            my_evt = self._sibling_bus.get(self.config.slot_id)
            if my_evt and my_evt.get("arm_ns", 0) > 0:
                self._armed_for_entry = True
                my_evt.pop("arm_ns", None)
        if not self._armed_for_entry:
            return

        # Portfolio "ReExecute" re-entry (spec §5.2 / portfolio_sl_tgt.html).
        # Sits ahead of the window gate (re-executions bypass the intraday entry
        # window, §7 P3) and blocks the signal path while pending. In aggregating
        # mode this also runs on the base bar (_manage_open_position); this call
        # covers the non-aggregating path. Cleared on fill or squareoff.
        if self._try_reexec_reentry(close, is_flat):
            return

        # 1.2(d) ReEntry (price-wait): gate the next entry until live price
        # crosses the configured trigger from the same direction as the prior
        # exit. For longs we wait for price to fall back to (or below) trigger;
        # for shorts, rise back to (or above) trigger. Once met, clear the
        # flag; the signal still decides the side.
        if self._reentry_armed:
            crossed = (
                (self._reentry_was_long and close <= self._reentry_target_price)
                or (not self._reentry_was_long and close >= self._reentry_target_price)
            )
            if not crossed:
                return
            self._reentry_armed = False
            self._reentry_target_price = 0.0
            self.re_entry_count += 1

        # Other Settings — re-execution delay. Per spec §2: after a re_execute
        # action fires, block subsequent entries until the configured delay
        # has elapsed. Re-execution sets _reentry_blocked_until_ns to the
        # bar's ts_event + delay; we skip until the current bar passes that.
        if (
            self._reentry_blocked_until_ns > 0
            and self._current_bar_ts_ns < self._reentry_blocked_until_ns
        ):
            return

        # Intraday entry window (spec §9). Blocks FRESH entries outside
        # [entry_start, entry_end] (UTC minute-of-day) — exits still process
        # on every bar, so SL/Target are monitored past End Time until
        # squareoff. ReExecute/ReEntry past End Time are allowed unless
        # no_reentry_after_end is set (spec §7 P3).
        if self._entry_start_min >= 0 or self._entry_end_min >= 0:
            bar_min = (self._current_bar_ts_ns % 86_400_000_000_000) // 60_000_000_000
            if self._entry_start_min >= 0 and bar_min < self._entry_start_min:
                return
            if self._entry_end_min >= 0 and bar_min > self._entry_end_min:
                _is_reexec = self.re_execution_count > 0 or self.re_entry_count > 0
                if not (_is_reexec and not self._no_reentry_after_end):
                    return

        # Day-of-week filter (portfolio.run_on_days). Block FRESH entries on
        # excluded weekdays — exits, SL/Target/trailing, RBO state, squareoff
        # all ran above this point so open positions stay managed every day.
        # Empty set = no filter. The weekday is derived from the bar's
        # ts_event (UTC) using the same integer-modulo trick as
        # core/backtest_runner.py:_filter_bars_by_weekday.
        if self._allowed_weekdays is not None:
            _days = self._current_bar_ts_ns // _NANOS_PER_DAY_MOD
            _weekday = (_EPOCH_WEEKDAY + _days) % 7
            if _weekday not in self._allowed_weekdays:
                return

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

        ret = signal_entry["signal_fn"](**args)
        # Backwards-compat: legacy signal_fn returned a bare OrderSide. New
        # contract is a 2-tuple (side, detailed_reason). Normalize so custom
        # strategies loaded via core/custom_strategy_loader.py keep working.
        if isinstance(ret, tuple) and len(ret) == 2:
            side, detailed_reason = ret
        else:
            side, detailed_reason = ret, None
        if side is not None:
            self._pending_entry_reason = detailed_reason
            self._submit_order(side)
            self._set_exit_levels(side)

    def _set_exit_levels(self, side: OrderSide) -> None:
        # Will be set on next bar when we know the fill price
        # For simplicity, use current close as proxy
        pass

    def on_position_closed(self, event) -> None:
        """Multi-portfolio: accumulate this leg's realized P&L as EACH position closes
        — for ANY reason (SL/Target, squareoff, signal flip / NETTING auto-close,
        re-execute). The backtest cache purges closed positions, so this event is the
        reliable capture point; the scoped portfolio monitor sums legs' published P&L.
        ``realized_pnl`` already reflects the conservative/pinned fill. Inert outside a
        multi-portfolio session."""
        if not self._in_session:
            return
        try:
            rp = getattr(event, "realized_pnl", None)
            if rp is not None:
                self._cum_realized += float(rp.as_double())
        except Exception:  # noqa: BLE001
            pass
        # Publish (instrument, up-to-date realized); the monitor adds live open
        # realized+unrealized itself. Keyed by strategy so the shared-instrument
        # scoped monitor can pick this leg's realized for the right instrument.
        self._legpnl_bus[str(self.id)] = (str(self.config.instrument_id), self._cum_realized)

    def on_order_filled(self, event) -> None:
        """Set exit levels when an order fills.

        Classify CLOSE vs OPEN/entry by the filled order's intrinsic
        ``is_reduce_only`` flag — every close goes through close_all_positions/
        close_position (reduce-only); every entry/reverse-open via _submit_order
        is not. This replaces the positional ``_expecting_close_fill`` boolean,
        which mislabeled fills when a reverse's close found no position (the open
        then got eaten as the "close"), desyncing the leg from the engine cache —
        a bug that fires even single-portfolio (verified). Fall back to the flag
        only if the order can't be read from cache.
        """
        _is_close = self._expecting_close_fill
        try:
            _ord = self.cache.order(event.client_order_id)
            if _ord is not None:
                _is_close = bool(_ord.is_reduce_only)
        except Exception:  # noqa: BLE001 — fall back to the flag
            pass
        if _is_close:
            self._expecting_close_fill = False
            # Live-fill mode: the close just filled at the pinned conservative price;
            # clear the pin so it doesn't affect later orders on this instrument.
            if self._live_fill_on:
                self._fill_px_bus.pop(f"{self.config.instrument_id}|{self.id}", None)
            # Cons-bus: the engine just filled the close at last_px;
            # publish the conservative-vs-engine P&L delta (same as the post-run
            # §4.2 reprice) so the monitor's combined P&L reflects the conservative
            # exit. delta adds to realized: long → (cons - fill)·qty, short → (fill
            # - cons)·qty. Cleared after one use.
            if self._cons_adj_on and self._pending_cons_px > 0 and self._pending_cons_qty > 0:
                try:
                    _fill = float(event.last_px)
                    _cons = self._pending_cons_px
                    _q = self._pending_cons_qty
                    _delta = (_cons - _fill) * _q if self._pending_cons_long else (_fill - _cons) * _q
                    self._cons_bus[0] += _delta
                except Exception:  # noqa: BLE001
                    pass
            self._pending_cons_px = 0.0
            self._pending_cons_qty = 0.0
            return

        # Entry/open fill — clear any stale close-flag (classification is by
        # reduce_only now, not this flag) so it can't carry to a later fill.
        self._expecting_close_fill = False
        self.entry_price = float(event.last_px)
        self.highest_profit = 0.0
        self.sl_wait_count = 0
        self._sl_wait_started_ns = 0
        # Target Wait + leg Trailing-Target state — fresh per position.
        self.tp_wait_count = 0
        self._tp_wait_started_ns = 0
        self._tgt_trail_active = False
        self._tgt_trail_stop = 0.0
        self._tgt_trail_anchor = 0.0
        self._tp_was_trail = False
        # Fresh trade — reset the trailed flag so on_sl_action_on classifies
        # this trade's eventual SL hit independently of the prior trade.
        self._was_trailed = False
        # Clear the re-entry delay block — once a re-entry actually fires,
        # the timer's job is done.
        self._reentry_blocked_until_ns = 0
        # Move SL to Cost: stamp the entry timestamp for safety_sec timing,
        # and reset the per-position fire flag.
        self._entry_filled_at_ns = self._current_bar_ts_ns
        self._move_sl_fired_this_position = False
        self._move_sl_trail_anchor = 0.0

        is_buy = event.order_side == OrderSide.BUY
        self.position_side = "LONG" if is_buy else "SHORT"

        # ReExecute-at-Entry-Price limit just filled (at the exact entry price):
        # clear the one-shot pin so normal signal entries resume after this trade.
        if self._reexec_limit_px > 0:
            self._reexec_limit_px = 0.0
            self._reexec_limit_client_id = None

        # Compute SL (snap to instrument tick — TBD-2 resolved)
        if self.config.stop_loss_type in ("percentage", "trailing"):
            self.current_sl = self._compute_sl_price(is_buy, self.config.stop_loss_value)
        elif self.config.stop_loss_type == "points":
            if is_buy:
                self.current_sl = self._snap_to_tick(self.entry_price - self.config.stop_loss_value)
            else:
                self.current_sl = self._snap_to_tick(self.entry_price + self.config.stop_loss_value)
        elif self.config.stop_loss_type == "atr":
            # Volatility-adaptive SL (spec §1.1 fn.4): distance = k × ATR.
            # BUY  → SL below entry; SELL → SL above entry.
            atr_val = float(self._atr.value) if self._atr is not None and self._atr.initialized else 0.0
            self._atr_at_entry = atr_val
            dist = atr_val * float(self.config.sl_atr_multiplier)
            if dist > 0:
                if is_buy:
                    self.current_sl = self._snap_to_tick(self.entry_price - dist)
                else:
                    self.current_sl = self._snap_to_tick(self.entry_price + dist)
            else:
                self.current_sl = 0.0
        else:
            self._atr_at_entry = 0.0
            self.current_sl = 0.0

        # Compute TP (snap to instrument tick)
        if self.config.target_type == "percentage":
            if is_buy:
                self.current_tp = self._snap_to_tick(self.entry_price * (1 + self.config.target_value / 100))
            else:
                self.current_tp = self._snap_to_tick(self.entry_price * (1 - self.config.target_value / 100))
        elif self.config.target_type == "points":
            if is_buy:
                self.current_tp = self._snap_to_tick(self.entry_price + self.config.target_value)
            else:
                self.current_tp = self._snap_to_tick(self.entry_price - self.config.target_value)
        elif self.config.target_type == "atr":
            # Volatility-adaptive Target (spec §1.1 fn.4): distance = k × ATR.
            # BUY  → TP above entry; SELL → TP below entry.
            atr_val = float(self._tgt_atr.value) if self._tgt_atr is not None and self._tgt_atr.initialized else 0.0
            dist = atr_val * float(self.config.tgt_atr_multiplier)
            if dist > 0:
                if is_buy:
                    self.current_tp = self._snap_to_tick(self.entry_price + dist)
                else:
                    self.current_tp = self._snap_to_tick(self.entry_price - dist)
            else:
                self.current_tp = 0.0
        else:
            self.current_tp = 0.0

    def _compute_sl_price(self, is_long: bool, pct: float) -> float:
        if is_long:
            return self._snap_to_tick(self.entry_price * (1 - pct / 100))
        else:
            return self._snap_to_tick(self.entry_price * (1 + pct / 100))

    def _snap_to_tick(self, price: float) -> float:
        """TBD-2: snap SL/TP trigger prices to the instrument's tick grid.

        Off-tick triggers can't be matched cleanly at fill time. We round to
        the nearest tick using ``instrument.price_increment``. If the
        instrument hasn't been bound yet (early init), the raw value is
        returned — callers re-evaluate on the next bar after fill anyway.
        """
        if not self.instrument or price <= 0:
            return price
        try:
            tick = float(self.instrument.price_increment)
            if tick <= 0:
                return price
            return round(price / tick) * tick
        except Exception:
            return price

    def _reset_exit_state(self) -> None:
        self.entry_price = 0.0
        self.highest_profit = 0.0
        self.current_sl = 0.0
        self.current_tp = 0.0
        self.sl_wait_count = 0
        self._sl_wait_started_ns = 0
        self.tp_wait_count = 0
        self._tp_wait_started_ns = 0
        self._tgt_trail_active = False
        self._tgt_trail_stop = 0.0
        self._tgt_trail_anchor = 0.0
        self.position_side = None

    def _submit_order(self, side: OrderSide) -> None:
        # Attach the indicator-and-condition reason as a tag on the Nautilus
        # order. Tags propagate to fills_report["tags"], which the orderbook
        # builder surfaces via the "ENTRY DETAILED REASON" column. Cleared
        # right after submit so a subsequent reverse/close doesn't reuse it.
        kwargs = dict(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        if self._pending_entry_reason:
            kwargs["tags"] = [self._pending_entry_reason]
        order = self.order_factory.market(**kwargs)
        self.submit_order(order)
        self._pending_entry_reason = None

    def _submit_reexec_limit(self) -> None:
        """Submit the one-shot ReExecute-at-Entry-Price resting LIMIT (spec ✅).

        BUY limit for a long re-execution, SELL for a short, at the captured
        pre-clip entry price. The matching engine fills limits from bar high/low,
        so the re-entry fills AT the exact entry price when a bar touches it —
        not a signal-gated market fill.
        """
        side = OrderSide.BUY if self._reexec_limit_is_long else OrderSide.SELL
        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            price=self.instrument.make_price(self._reexec_limit_px),
            time_in_force=TimeInForce.GTC,
            tags=["ReExecute at Entry Price (limit) — re-entry fired by portfolio SL/Target"],
        )
        self._reexec_limit_client_id = order.client_order_id
        self.submit_order(order)

    def _submit_reexec_market(self) -> None:
        """Submit the one-shot plain-ReExecute immediate MARKET re-entry.

        Spec portfolio_sl_tgt.html: plain "ReExecute" *"Closes all legs and
        immediately re-opens them"*. The replay restarts this slot flat at the
        breach; on the first bar after it we re-open the closed leg at market on
        the original side — no signal, no price-wait. One-shot, isolated to this
        action; cleared on fill (on_order_filled). The matching engine fills the
        market order at the next bar, giving "close at breach → re-open next bar
        at market".
        """
        side = OrderSide.BUY if self._reexec_limit_is_long else OrderSide.SELL
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
            tags=["ReExecute (market) — re-entry fired by portfolio SL/Target"],
        )
        self._reexec_limit_client_id = order.client_order_id
        self.submit_order(order)

    def _cancel_reexec_limit(self) -> None:
        """Abandon a still-resting ReExecute-at-Entry-Price limit (one-shot).

        Called at the daily squareoff cutoff: a re-exec limit unfilled by then is
        cancelled and not resubmitted (re-entries are blocked past squareoff)."""
        if self._reexec_limit_px <= 0:
            return
        oid = self._reexec_limit_client_id
        if oid is not None:
            try:
                order = self.cache.order(oid)
                if order is not None and not order.is_closed:
                    self.cancel_order(order)
            except Exception:  # noqa: BLE001 — best-effort cancel
                pass
        self._reexec_limit_px = 0.0
        self._reexec_limit_client_id = None
        self._reexec_limit_submitted = False

    def on_stop(self) -> None:
        # Drain trailing partial windows (no order action — matches Nautilus,
        # which never emits an in-progress window).
        for agg in (self._primary_agg, self._bid_agg, self._ask_agg):
            if agg is not None:
                agg.flush()
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


def config_from_exit(exit_config: ExitConfig, signal_name: str, signal_params: dict,
                     instrument_id, bar_type, trade_size,
                     order_id_tag: str | None = None,
                     squareoff_time: str | None = None,
                     squareoff_tz: str | None = None,
                     rbo_settings=None,
                     other_settings=None,
                     move_sl_settings=None,
                     entry_start_time: str | None = None,
                     entry_end_time: str | None = None,
                     subscribe_bar_types: list | None = None,
                     allowed_weekdays: list | None = None,
                     portfolio_id: str = "",
                     fill_bus_id: str = "",
                     session_window_self_filter: bool = False,
                     slot_id: str = "",
                     reexec_entry_price: float = 0.0,
                     reexec_entry_was_long: bool = True,
                     reexec_market_mode: bool = False,
                     pf_monitor_enforced: bool = False,
                     pf_monitor_action: str = "sqoff",
                     pf_monitor_market_mode: bool = False,
                     cons_fill_active: bool = False,
                     dir_fill_active: bool = False,
                     auto_bidask: bool = False) -> ManagedExitConfig:
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
    # Base / strategy-subscribe timeframe split. ``bar_type`` here is the BASE
    # bar type — the catalog data fed to the engine, the resolution at which
    # the matching engine fills orders. When the leg selects strategy
    # timeframe(s) (UI), the strategy instead OPERATES on the first composite
    # bar type — its signal, indicators and SL/TP run on that aggregated
    # stream; orders still fill on the base data. Any further selected
    # composites are subscribed-and-available (received, but the single-signal
    # logic uses the primary). Empty selection → strategy runs on the base,
    # the original unchanged behaviour.
    # Custom streaming aggregation. ``bar_type`` (the BASE catalog stream) stays
    # the bar type the strategy subscribes to and the resolution at which the
    # matching engine fills orders. When the leg selects strategy timeframe(s),
    # the FIRST selected string is reinterpreted as the aggregation TARGET: its
    # plain EXTERNAL form (composite ``INTERNAL@`` carrier stripped) becomes
    # ``aggregate_to_bar_type``, and the strategy aggregates the base up to it
    # with core.aggregator.BarAggregator — no Nautilus-internal composite bars.
    # Any further selections are dropped (they were received-and-ignored before).
    _subs = [str(s) for s in (subscribe_bar_types or []) if s]
    _aggregate_to = ""
    if _subs:
        _ext = external_from_composite(_subs[0])
        if _ext:
            try:
                if BarType.from_str(_ext) != bar_type:
                    _aggregate_to = _ext
            except Exception:  # noqa: BLE001 — malformed → no aggregation
                _aggregate_to = ""

    # Three-format engine (spec §3). Resolve the exit-trigger data format and,
    # for Format A, derive the paired BID/ASK bar-type strings from the BASE bar
    # type (the strategy aggregates these base streams to the same windows as
    # the primary when aggregating).
    _fmt = str(getattr(exit_config, "exit_price_format", "ohlcv") or "ohlcv").strip().lower()
    if _fmt not in ("ohlcv", "ltp", "bidask", "mark"):
        _fmt = "ohlcv"
    # Auto-Format-A (spec §4.1): when the leg didn't explicitly pick a non-OHLCV
    # format AND the runner confirmed a complete BID/ASK pair is loaded for this
    # slot (auto_bidask), promote the default OHLCV path to Bid/Ask so SL/Target
    # triggers consult bid_high/ask_low instead of the single-series high/low.
    # Gated on auto_bidask — which the runner sets only when the pair actually
    # exists in the catalog — so a slot without bid/ask data never gets promoted
    # and never stalls waiting on the Format-A (primary,bid,ask) trio buffer.
    # An explicit "ltp" / "mark" / already-"bidask" choice is left untouched.
    if _fmt == "ohlcv" and auto_bidask:
        _fmt = "bidask"
    # "mark" (Mark Price, spec §3) passes through here; the crypto-only gate is
    # enforced in ManagedExitStrategy.on_start (downgrades to OHLCV with a log
    # on non-crypto venues) where the venue is known.
    _bid_bt, _ask_bt = ("", "")
    if _fmt == "bidask":
        _bid_bt, _ask_bt = _derive_bid_ask_bar_types(str(bar_type))
        if not _bid_bt or not _ask_bt:
            # No FX-style bid/ask pair (e.g. crypto LAST bars) — degrade to B.
            _fmt = "ohlcv"

    kwargs = dict(
        instrument_id=instrument_id,
        bar_type=bar_type,
        trade_size=Decimal(str(trade_size)),
        signal_name=signal_name,
        signal_params=signal_params,
        exit_price_format=_fmt,
        bid_bar_type=_bid_bt,
        ask_bar_type=_ask_bt,
        subscribe_bar_types=[],
        aggregate_to_bar_type=_aggregate_to,
        stop_loss_type=_apply_value_mode(
            _canon_exit_type(exit_config.stop_loss_type, _SL_TYPE_CANON),
            getattr(exit_config, "sl_value_is_absolute", None),
        ),
        stop_loss_value=exit_config.stop_loss_value,
        trailing_sl_step=exit_config.trailing_sl_step,
        trailing_sl_offset=exit_config.trailing_sl_offset,
        sl_atr_period=int(getattr(exit_config, "sl_atr_period", 0) or 0),
        sl_atr_multiplier=float(getattr(exit_config, "sl_atr_multiplier", 0.0) or 0.0),
        target_type=_apply_value_mode(
            _canon_exit_type(exit_config.target_type, _TGT_TYPE_CANON),
            getattr(exit_config, "target_value_is_absolute", None),
        ),
        target_value=exit_config.target_value,
        tgt_atr_period=int(getattr(exit_config, "tgt_atr_period", 0) or 0),
        tgt_atr_multiplier=float(getattr(exit_config, "tgt_atr_multiplier", 0.0) or 0.0),
        target_lock_trigger=exit_config.target_lock_trigger or 0.0,
        target_lock_minimum=exit_config.target_lock_minimum or 0.0,
        tgt_trail_enabled=bool(getattr(exit_config, "tgt_trail_enabled", False)),
        tgt_trail_when_profit_reach=float(getattr(exit_config, "tgt_trail_when_profit_reach", 0.0) or 0.0),
        tgt_trail_lock_min_profit=float(getattr(exit_config, "tgt_trail_lock_min_profit", 0.0) or 0.0),
        tgt_trail_every=float(getattr(exit_config, "tgt_trail_every", 0.0) or 0.0),
        tgt_trail_by=float(getattr(exit_config, "tgt_trail_by", 0.0) or 0.0),
        sl_wait_sec=getattr(exit_config, "sl_wait_sec", 0),
        sl_wait_bars=exit_config.sl_wait_bars,
        tgt_wait_sec=int(getattr(exit_config, "tgt_wait_sec", 0) or 0),
        tgt_wait_bars=int(getattr(exit_config, "tgt_wait_bars", 0) or 0),
        on_sl_action=exit_config.on_sl_action,
        on_target_action=exit_config.on_target_action,
        max_re_executions=exit_config.max_re_executions,
        execute_target_leg_id=getattr(exit_config, "execute_target_leg_id", "") or "",
        reentry_price=float(getattr(exit_config, "reentry_price", 0.0) or 0.0),
        max_re_entries=int(getattr(exit_config, "max_re_entries", 0) or 0),
        armed_at_start=bool(getattr(exit_config, "armed_at_start", True)),
        reexec_entry_price=float(reexec_entry_price or 0.0),
        reexec_entry_was_long=bool(reexec_entry_was_long),
        reexec_market_mode=bool(reexec_market_mode),
        squareoff_minute=_parse_squareoff_minute(squareoff_time),
        squareoff_tz=squareoff_tz or "UTC",
        # Intraday entry window (spec §9) — UTC minute-of-day, -1 = disabled.
        entry_start_minute=_parse_squareoff_minute(entry_start_time),
        entry_end_minute=_parse_squareoff_minute(entry_end_time),
        # Day-of-week entry filter (portfolio.run_on_days) — list of UTC
        # weekday ints (0=Mon..6=Sun) OR None. ``None`` → no filter (all days
        # allowed). ``[]`` → explicit "no days allowed". See ManagedExitConfig
        # field-level comment for why we gate here instead of pre-filtering.
        allowed_weekdays=(None if allowed_weekdays is None else list(allowed_weekdays)),
        portfolio_id=portfolio_id,
        fill_bus_id=fill_bus_id,
        session_window_self_filter=session_window_self_filter,
        slot_id=slot_id,
        pf_monitor_enforced=pf_monitor_enforced,
        pf_monitor_action=pf_monitor_action,
        pf_monitor_market_mode=pf_monitor_market_mode,
        cons_fill_active=cons_fill_active,
        dir_fill_active=dir_fill_active,
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
    if other_settings is not None:
        kwargs.update(
            delay_between_legs_sec=other_settings.delay_between_legs_sec,
            on_sl_action_on=other_settings.on_sl_action_on,
            on_target_action_on=other_settings.on_target_action_on,
        )
    if move_sl_settings is not None:
        # ReExecute gating flags (ReExecute_Logics.html P1/P2/P5) are wired
        # regardless of move_sl_enabled — no-ops until the relevant condition.
        kwargs["no_reexec_sl_cost"] = bool(move_sl_settings.no_reexec_sl_cost)
        # ReExecute_Logics.html P3 / spec §9 — block ReExecute & ReEntry past
        # Portfolio End Time. Real gate now that post-window bars are kept.
        kwargs["no_reentry_after_end"] = bool(
            getattr(move_sl_settings, "no_reentry_after_end", False)
        )
        # Portfolio-aggregate Move SL trigger (spec §2.3). agg_trigger_ns and
        # preseeded_bus are pass-2 inputs the two-pass runner injects; they are
        # 0 / empty during pass 1 and whenever the feature is off. Threaded
        # regardless of move_sl_enabled — the aggregate trigger is independent.
        kwargs["move_sl_agg_trigger_ns"] = int(
            getattr(move_sl_settings, "agg_trigger_ns", 0) or 0
        )
        kwargs["move_sl_preseeded_bus"] = dict(
            getattr(move_sl_settings, "preseeded_bus", None) or {}
        )
        kwargs["no_wait_trade_reexec"] = bool(
            getattr(move_sl_settings, "no_wait_trade_reexec", False)
        )
        kwargs["no_reentry_sl_cost"] = bool(
            getattr(move_sl_settings, "no_reentry_sl_cost", True)
        )
        if move_sl_settings.enabled:
            kwargs.update(
                move_sl_enabled=True,
                move_sl_safety_sec=move_sl_settings.safety_sec,
                move_sl_action=move_sl_settings.action,
                move_sl_trail_after=move_sl_settings.trail_after,
                move_sl_no_buy_legs=move_sl_settings.no_buy_legs,
                move_sl_ltp_buffer=getattr(move_sl_settings, "ltp_buffer", 0.0),
                move_sl_hit_on_leg_sl=getattr(move_sl_settings, "hit_on_leg_sl", False),
                move_sl_hit_on_leg_target=getattr(move_sl_settings, "hit_on_leg_target", False),
            )
    return ManagedExitConfig(**kwargs)
