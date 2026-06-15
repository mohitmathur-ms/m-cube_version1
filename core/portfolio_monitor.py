"""Phase 3 (skeleton) — the live portfolio-monitor Strategy for the unified
single-engine path.

A position-less ``Strategy`` that lives alongside the leg strategies in ONE
``BacktestEngine``. It subscribes to every leg's bars and, on each bar, reads the
LIVE combined P&L across all legs straight from the shared ``Portfolio`` — the
thing the per-slot architecture could never see live (and the reason portfolio
SL/Target/ReExecute/Move-SL needed post-run clips, replays, and two-pass runs).

SKELETON SCOPE: it only RECORDS the per-bar combined-P&L curve — it takes NO
trading action yet, so attaching it must be a strict no-op. Subsequent sub-steps
layer on live enforcement (combined SL/Target → SqOff/ReExecute, aggregate
Move-SL, cross-leg Hit-On-Leg), each parity-checked. Gated by ``_USE_PF_MONITOR``
(off by default); never attached on the per-slot path.
"""

from __future__ import annotations

from zoneinfo import ZoneInfo

import pandas as pd

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


class PortfolioMonitorConfig(StrategyConfig, frozen=True):
    """Config for :class:`PortfolioMonitorStrategy`.

    ``monitor_instrument_ids`` — every leg's instrument id (string) to value.
    ``monitor_bar_types`` — every leg's bar type (string) to subscribe to.
    ``starting_capital`` — portfolio capital (for future threshold math).
    """

    monitor_instrument_ids: tuple = ()
    monitor_bar_types: tuple = ()
    starting_capital: float = 0.0
    # Combined-loss stoploss (day-scoped). 0/disabled = observe only.
    pf_sl_value: float = 0.0
    pf_sl_enabled: bool = False
    day_tz: str = "UTC"  # calendar-day boundary for the daily reset
    # Live ENFORCEMENT: when True, on a day-scoped breach the monitor publishes the
    # breach ts to the shared pf-SL fire bus (keyed by portfolio_id) so each leg
    # closes its own position live. False = detection-only (records breaches, no
    # signal). The post-run clip must be skipped when this is True (else double).
    enforce: bool = False
    portfolio_id: str = ""
    # Live action: "sqoff" (day-scoped, once per calendar day) or "reexecute"
    # (trailing-reset baseline — fire each time combined loss drops `pf_sl_value`
    # below the level at the last fire — capped at reexec_cap fires per run, NOT
    # day-scoped, mirroring the two-pass replay's reset-from-clip semantics).
    action: str = "sqoff"
    reexec_cap: int = 0  # 0 → unlimited (hard cap 50, like the replay)
    market_mode: bool = False  # pf_sl ReExecute: market re-entry vs entry-price limit
    # Combined-PROFIT target (symmetric to pf_sl): day-scoped SqOff or trailing-reset
    # ReExecute on the profit side. Same fire bus, action carried per fire.
    pf_tgt_enabled: bool = False
    pf_tgt_value: float = 0.0
    tgt_action: str = "sqoff"
    tgt_market_mode: bool = False
    tgt_reexec_cap: int = 0
    # §5.3 confirmation-delay window (seconds) for the PnL Target path — a breach
    # must HOLD this long before it fires (0 = immediate). The SL side reuses
    # ``pf_sl_delay_sec`` (declared in the underlying block above).
    pf_tgt_delay_sec: int = 0
    # Aggregate Move-SL (spec §2.3): when the COMBINED P&L first crosses
    # agg_threshold in the agg direction, publish that ts so open legs snap SL to
    # cost. Independent of pf_sl; only enforced (enforce=True) attaches it.
    agg_enabled: bool = False
    agg_threshold: float = 0.0
    agg_is_loss: bool = True
    # Underlying-price SL (spec §2.1 / §5.1) — "underlying = self" = the PRIMARY
    # slot's instrument price. ``pf_sl_type`` selects the rule:
    #   "Underlying Movement"       → fire when the underlying close CROSSES pf_sl_value
    #   "Loss and Underlying Range" → fire when combined PnL <= -pf_sl_value AND the
    #                                 underlying close is <= below OR >= above.
    # ``pf_sl_delay_sec`` confirms the hit (spec §5.3). When the sl_type is not an
    # underlying type (or ``underlying_bar_type`` is empty) this path is inert and
    # the PnL Combined-Loss path runs instead. Fires once per run. ``underlying_bar_type``
    # is the PRIMARY slot's bar type string (matched exactly so a same-instrument leg
    # on a different timeframe / a bid/ask pair can't corrupt the underlying series).
    pf_sl_type: str = "Combined Loss"
    pf_sl_underlying_below: float = 0.0
    pf_sl_underlying_above: float = 0.0
    pf_sl_delay_sec: int = 0
    underlying_bar_type: str = ""
    # ── Multi-portfolio scoping (Phase 0, gated by _USE_MULTI_PORTFOLIO) ──
    # When several portfolios share ONE session engine, each portfolio's monitor must
    # value ONLY its own legs. ``scope_strategy_ids`` = the order_id_tags (== strategy
    # tag) of this monitor's legs; empty = whole-engine (the single-portfolio default,
    # which keeps the EXACT existing total_pnl(instrument) path → byte-identical).
    # ``shared_instrument_ids`` = instruments traded by MORE than one portfolio in the
    # session; only those need the per-position strategy-scoped sum. An instrument NOT
    # in this set is exclusive to this scope, so engine-wide total_pnl(instrument) IS
    # this scope's P&L (fast + byte-identical). For single portfolio both are empty.
    scope_strategy_ids: tuple = ()
    shared_instrument_ids: tuple = ()
    # Cross-portfolio action (spec §2.1(h)/(i)/(j) + §2.4 mirror): when this monitor
    # breaches and ``cross_pf_target`` is set, publish a SAME-BAR ``cross_pf_verb``
    # event (sqoff/execute/start) to the target portfolio's legs (multi-portfolio
    # session engine — both portfolios share one in-process engine).
    cross_pf_target: str = ""
    cross_pf_verb: str = ""
    # Progress-only mode: when False the monitor still publishes day-wise UI
    # progress (cheap, every bar) but SKIPS the per-bar combined-P&L valuation and
    # curve growth — so a plain portfolio (no SL/TP/detection) can attach the
    # monitor purely for the UI timestamp without paying the valuation cost.
    track: bool = True


class PortfolioMonitorStrategy(Strategy):
    """Observe-only (skeleton) portfolio monitor. Records combined live P&L.

    ``combined_pnl_curve`` is a list of ``(ts_ns, combined_pnl)`` sampled on every
    bar of any leg — the live equivalent of the post-run MtM curve.
    """

    def __init__(self, config: PortfolioMonitorConfig):
        super().__init__(config)
        self.combined_pnl_curve: list = []
        self.detected_breaches: list = []   # [ts_ns] — day-scoped SL breaches (detection only)
        self._iids = []  # parsed InstrumentId cache
        self._tz = None
        self._cur_day = None
        self._day_start_pnl = 0.0
        self._pf_sl_bus = None  # shared fire bus (set in on_start when enforcing)
        self._pf_agg_bus = None      # aggregate Move-SL trigger bus (set in on_start)
        self._agg_triggered = False  # one-shot: agg trigger already published
        self._prev_open = 0          # PERF: open-position count last bar
        self._last_pnl = 0.0         # PERF: last computed combined P&L (reused while flat)
        self._iid_open = {}          # PERF: per-instrument open count last valuation
        self._iid_pnl = {}           # PERF: per-instrument cached total P&L
        # Per-side state (sl = combined loss, tgt = combined profit).
        self._sl_clipped_today = False   # day-scoped SqOff (pf_sl)
        self._tgt_clipped_today = False
        self._sl_clipped_run = False     # absolute single SqOff (run-scoped)
        self._tgt_clipped_run = False
        self._sl_reexec_baseline = 0.0
        self._sl_reexec_fires = 0
        self._tgt_reexec_baseline = 0.0
        self._tgt_reexec_fires = 0
        # §5.3 confirmation-delay arm time per side (0 = not pending).
        self._sl_pending_at = 0
        self._tgt_pending_at = 0
        self._cons_bus = None        # conservative-fill adjustment accumulator [cum_delta]
        # Underlying-price SL live state ("underlying = self" = primary slot).
        self._u_prev = None          # previous underlying close (Movement cross)
        self._u_close = None         # latest underlying close
        self._u_clipped_run = False  # single-shot (post-run clip is one fire/run)
        self._u_pending_at_ns = 0    # §5.3 confirmation-delay arm time
        self._u_pending_dir = 0      # +1 crossed up / -1 crossed down (Movement)
        # Day-wise UI progress (always on; independent of SL/TP enforcement).
        self._progress_bus = None
        self._prog_bucket = -1       # last published integer UTC day bucket
        self._track = True           # set from config.track in on_start
        # Multi-portfolio scoping (empty = whole-engine = single-portfolio default).
        self._scope_sids = set()     # strategy_id strings this monitor governs
        self._shared_iids = set()    # InstrumentIds traded by >1 portfolio (need per-pos)
        self._last_px = {}           # iid -> last bar close (Price) for unrealized P&L
        self._base_ccy = {}          # venue -> account base Currency (cached)

    def on_start(self) -> None:
        for iid in self.config.monitor_instrument_ids:
            try:
                self._iids.append(InstrumentId.from_str(iid))
            except Exception:  # noqa: BLE001
                pass
        try:
            self._tz = ZoneInfo(self.config.day_tz or "UTC")
        except Exception:  # noqa: BLE001
            self._tz = ZoneInfo("UTC")
        for bt in self.config.monitor_bar_types:
            try:
                self.subscribe_bars(BarType.from_str(bt))
            except Exception:  # noqa: BLE001
                pass
        # Day-wise UI progress bus — set unconditionally so progress surfaces even
        # for portfolios with no SL/TP (the SL/TP day logic below is gated off then).
        from core.managed_strategy import get_pf_progress_bus
        self._progress_bus = get_pf_progress_bus(self.config.portfolio_id)
        self._track = bool(self.config.track)
        # Multi-portfolio scope (empty for single portfolio → whole-engine path).
        self._scope_sids = set(str(s) for s in (self.config.scope_strategy_ids or ()))
        # Per-leg live-P&L bus: a SCOPED monitor sums its scope's legs from here
        # (closed positions are purged from the cache, so legs are the P&L source).
        from core.managed_strategy import get_pf_legpnl_bus
        self._legpnl_bus = get_pf_legpnl_bus(self.config.portfolio_id)
        for s in (self.config.shared_instrument_ids or ()):
            try:
                self._shared_iids.add(InstrumentId.from_str(s))
            except Exception:  # noqa: BLE001
                pass
        if self.config.enforce:
            # Lazy import: managed_strategy must not import this module (no cycle).
            from core.managed_strategy import get_pf_sl_bus, get_pf_agg_bus, get_pf_cons_bus
            self._pf_sl_bus = get_pf_sl_bus(self.config.portfolio_id)
            self._pf_agg_bus = get_pf_agg_bus(self.config.portfolio_id)
            self._cons_bus = get_pf_cons_bus(self.config.portfolio_id)

    def _combined_pnl(self) -> float:
        """Sum of realized+unrealized P&L across all monitored legs (accurate
        Portfolio.total_pnl — handles multi-currency). PERF: total_pnl is the
        dominant cost, so value only instruments that currently have an OPEN
        position (a flat instrument's P&L is its constant realized total between
        trades), plus a one-shot recompute on each open→flat transition to capture
        the realized jump. Flat instruments reuse their cached value.

        Multi-portfolio: when this monitor governs a SUBSET of the engine's legs
        (``_scope_sids`` non-empty), an instrument that is SHARED with another
        portfolio (``_shared_iids``) is valued per-position over the in-scope
        strategy_ids only; every other instrument is exclusive to this scope, so
        engine-wide ``total_pnl(instrument)`` IS this scope's P&L (the existing
        fast path — byte-identical to single-portfolio)."""
        total = 0.0
        scoped = bool(self._scope_sids)
        for iid in self._iids:
            # SHARED instrument (traded by >1 portfolio): total_pnl(iid) is engine-wide
            # (all portfolios), so this scope's share must be reconstructed from the
            # legs — the ONLY case the leg-published path is used.
            if scoped and iid in self._shared_iids:
                total += self._scoped_instrument_pnl(iid)
                continue
            # EXCLUSIVE instrument — single-portfolio session and disjoint multi-
            # portfolio (the common cases): total_pnl(iid) IS this scope's P&L, exact
            # and immediate (no lag) → byte-identical to the single-portfolio path.
            try:
                oc = self.cache.positions_open_count(instrument_id=iid)
            except Exception:  # noqa: BLE001
                oc = 1  # be safe: value it
            if oc > 0 or oc != self._iid_open.get(iid, 0):
                try:
                    m = self.portfolio.total_pnl(iid)
                    self._iid_pnl[iid] = float(m.as_double()) if m is not None else self._iid_pnl.get(iid, 0.0)
                except Exception:  # noqa: BLE001
                    pass
            self._iid_open[iid] = oc
            total += self._iid_pnl.get(iid, 0.0)
        return total

    def _scoped_instrument_pnl(self, iid) -> float:
        """This scope's P&L on a SHARED instrument (traded by >1 portfolio), where the
        engine-wide ``total_pnl(iid)`` can't be used. Reconstructed as:
          • REALIZED — the scope's legs' published cumulative realized FOR THIS
            instrument (legs accumulate it on_position_closed; the backtest cache
            PURGES closed positions, so the legs are the only reliable source).
          • OPEN — the scope's live open positions on this instrument: realized
            (entry commission / partial close) + unrealized, computed here so it is
            current this bar. Open positions are NOT purged, so this is exact.
        The sum equals this scope's contribution to ``total_pnl(iid)`` (verified to the
        cent against the engine). NOTE: same-currency instruments only; a cross-currency
        SHARED instrument would need base-ccy conversion of the published realized."""
        last = self._last_px.get(iid)
        iid_str = str(iid)
        total = 0.0
        bus = getattr(self, "_legpnl_bus", None) or {}
        for sid in self._scope_sids:
            v = bus.get(sid)
            if v is not None and v[0] == iid_str:
                total += float(v[1])
        try:
            for pos in self.cache.positions_open(instrument_id=iid):
                if str(pos.strategy_id) not in self._scope_sids:
                    continue
                if pos.realized_pnl is not None:
                    total += float(pos.realized_pnl.as_double())
                if last is not None:
                    m = pos.unrealized_pnl(last)
                    if m is not None:
                        total += float(m.as_double())
        except Exception:  # noqa: BLE001
            pass
        return total

    def _position_pnl_base(self, pos, last_price, venue, base) -> float:
        """One position's realized + (if open) unrealized P&L, converted to the
        account base currency via cache.get_xrate (the supported live conversion)."""
        out = 0.0
        for money in (pos.realized_pnl,
                      pos.unrealized_pnl(last_price) if (last_price is not None
                                                         and pos.is_open) else None):
            if money is None:
                continue
            val = float(money.as_double())
            if val == 0.0:
                continue
            ccy = money.currency
            if base is not None and ccy != base:
                try:
                    xr = self.cache.get_xrate(venue, ccy, base, price_type=PriceType.MID)
                    val *= float(xr)
                except Exception:  # noqa: BLE001
                    pass  # no rate yet → leave unconverted (best effort)
            out += val
        return out

    def on_bar(self, bar) -> None:
        ts = int(bar.ts_event)

        # Day-wise UI progress (ALWAYS, even in progress-only mode): publish the
        # latest processed day at most once per day. A cheap integer UTC-day bucket
        # guards the (relatively expensive) tz-aware date conversion so it stays off
        # the per-bar hot path (24M bars). The streaming endpoint reads this bus.
        if self._progress_bus is not None:
            _bucket = ts // 86_400_000_000_000
            if _bucket != self._prog_bucket:
                self._prog_bucket = _bucket
                _d = pd.Timestamp(ts, unit="ns", tz="UTC").tz_convert(self._tz).date()
                self._progress_bus[:] = [{"data_ts": ts, "day": str(_d)}]

        # Progress-only monitor: skip all valuation / curve growth / enforcement.
        if not self._track:
            return

        # Multi-portfolio only: remember each instrument's latest close so the
        # per-position scoped P&L (shared instruments) can value unrealized P&L.
        # Inert for single portfolio (scope empty).
        if self._scope_sids:
            self._last_px[bar.bar_type.instrument_id] = bar.close

        # PERF: valuing combined P&L (Portfolio.total_pnl per instrument) on every
        # bar dominates runtime on large catalogs. While NO position is open the
        # combined P&L equals the realized total — unchanged since the last close —
        # so it can't produce a NEW breach. Recompute only when a position is open,
        # or on the first bar after going flat (to capture the realized jump).
        try:
            _open = self.cache.positions_open_count()
        except Exception:  # noqa: BLE001 — be safe: value it
            _open = 1
        if _open > 0 or _open != self._prev_open:
            base = self._combined_pnl()
            self._last_pnl = base
        else:
            base = self._last_pnl
        self._prev_open = _open
        # Add the cumulative conservative-fill adjustment (legs publish their
        # SL/TP conservative-vs-engine delta here) so the monitor decides on
        # conservative P&L consistent with the final report. 0 when the gated
        # live-cons-adj is off → no effect.
        pnl = base + (self._cons_bus[0] if self._cons_bus is not None else 0.0)
        self.combined_pnl_curve.append((ts, pnl))

        # Aggregate Move-SL (spec §2.3) — independent of pf_sl. On the first bar
        # the combined P&L crosses the threshold in the configured direction,
        # publish the ts so open legs snap SL to cost (live replacement for the
        # two-pass discovery's agg_trigger_ns). One-shot.
        if (self.config.enforce and self.config.agg_enabled
                and not self._agg_triggered and self._pf_agg_bus is not None
                and self.config.agg_threshold > 0):
            thr = abs(float(self.config.agg_threshold))
            crossed = (pnl <= -thr) if self.config.agg_is_loss else (pnl >= thr)
            if crossed:
                self._pf_agg_bus.append(ts)
                self._agg_triggered = True

        # Live UNDERLYING-PRICE SL (spec §2.1/§5.1) — tracked on the PRIMARY slot's
        # bars ("underlying = self"); evaluated on each underlying bar, mirroring the
        # post-run `_underlying_sl_clip` iterating the primary price series. Runs
        # independently of the PnL Combined-Loss path below (which is gated off for
        # underlying sl_types so the price level isn't mistaken for a loss amount).
        if self.config.underlying_bar_type and str(bar.bar_type) == self.config.underlying_bar_type:
            self._u_prev = self._u_close
            self._u_close = float(bar.close)
            self._eval_underlying_sl(ts, pnl)

        # Combined SL/Target DETECTION on the LIVE per-bar combined P&L (replaces
        # the post-run clip / two-pass replay). Both sides share the day baseline:
        #   • SqOff      → day-scoped: reset reference each calendar day, fire once
        #                  per day, leg closes + blocks re-entry to EOD.
        #   • ReExecute  → trailing-reset: fire each time the move from the last fire
        #                  breaches `value`, capped at reexec_cap, NOT day-scoped.
        # SL is PnL-based ONLY for "Combined Loss"; underlying sl_types use the
        # underlying path above, so the PnL SL side is disabled for them.
        _sl_underlying = self.config.pf_sl_type in (
            "Underlying Movement", "Loss and Underlying Range")
        _sl_on = (bool(self.config.pf_sl_enabled) and float(self.config.pf_sl_value or 0) > 0
                  and not _sl_underlying)
        _tgt_on = bool(self.config.pf_tgt_enabled) and float(self.config.pf_tgt_value or 0) > 0
        if not (_sl_on or _tgt_on):
            return

        # Shared day boundary for the day-scoped (SqOff) sides.
        day = pd.Timestamp(ts, unit="ns", tz="UTC").tz_convert(self._tz).date()
        if day != self._cur_day:
            self._cur_day = day
            self._day_start_pnl = pnl
            self._sl_clipped_today = False
            self._tgt_clipped_today = False

        # A combined-loss and a combined-profit breach can't occur on the same bar
        # (opposite directions from the day baseline), so order doesn't matter.
        # Semantics match the post-run reference: pf_sl SqOff is DAY-SCOPED (daily
        # loss limit, resets each day); pf_tgt SqOff is ABSOLUTE SINGLE (first time
        # combined profit hits the target → close all + stop for the rest of the
        # run). ReExecute (both sides) is trailing-reset, capped.
        if _sl_on:
            self._eval_side(ts, pnl, is_loss=True, side="sl", day_scoped=True,
                            value=float(self.config.pf_sl_value),
                            action=self.config.action,
                            market=bool(self.config.market_mode),
                            cap=int(self.config.reexec_cap or 0),
                            delay_ns=int(self.config.pf_sl_delay_sec or 0) * 1_000_000_000)
        if _tgt_on:
            self._eval_side(ts, pnl, is_loss=False, side="tgt", day_scoped=False,
                            value=float(self.config.pf_tgt_value),
                            action=self.config.tgt_action,
                            market=bool(self.config.tgt_market_mode),
                            cap=int(self.config.tgt_reexec_cap or 0),
                            delay_ns=int(self.config.pf_tgt_delay_sec or 0) * 1_000_000_000)

    def _fire(self, ts: int, action: str, market: bool, reason: str) -> None:
        """Record + (when enforcing) publish an action-aware breach to the bus. The
        ``reason`` is the display tag the closing fill carries into the orderbook."""
        self.detected_breaches.append(ts)
        if self.config.enforce and self._pf_sl_bus is not None:
            self._pf_sl_bus.append({"ts": int(ts), "action": str(action),
                                    "market": bool(market), "reason": reason})
        # Cross-portfolio action: on this breach fire the configured verb to the
        # TARGET portfolio's legs, live the SAME bar (spec §2.1(h)/(i)/(j)).
        if self.config.enforce and self.config.cross_pf_target and self.config.cross_pf_verb:
            try:
                from core.backtest_runner.cross_portfolio import publish_cross_pf_live
                publish_cross_pf_live(self.config.cross_pf_target,
                                      self.config.cross_pf_verb, int(ts),
                                      self.config.portfolio_id)
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    def _reason(kind: str, value: float, action: str, market: bool) -> str:
        if action == "reexecute":
            label = "ReExecute" if market else "ReExecute at Entry Price"
        else:
            label = "SqOff"
        return f"Portfolio {kind}: combined {value:g} hit -> {label}"

    def _fire_underlying(self, ts: int, level: float) -> None:
        kind = ("underlying movement" if self.config.pf_sl_type == "Underlying Movement"
                else "loss+underlying range")
        # "sqoff_run" = permanent rest-of-run block (legs set _pf_sl_blocked_forever),
        # mirroring the post-run underlying clip which TRUNCATES the whole run at the
        # hit (NOT the day-scoped "sqoff" that resumes next day).
        self._fire(ts, "sqoff_run", False, f"Portfolio Stoploss: {kind} {level:g} hit -> SqOff")
        self._u_clipped_run = True

    def _eval_underlying_sl(self, ts: int, pnl: float) -> None:
        """Live underlying-price SL (spec §2.1/§5.1), evaluated on each underlying
        bar — a faithful port of the post-run ``_underlying_sl_clip``:

        • "Underlying Movement"       → the underlying close CROSSES ``pf_sl_value``
          (either direction; uses prev→current underlying close).
        • "Loss and Underlying Range" → combined PnL <= -value AND the underlying
          close is <= ``below`` OR >= ``above``.

        A hit arms a §5.3 confirmation delay (fire after ``pf_sl_delay_sec`` if it
        still holds; cancel if it recovers). Fires once per run."""
        c = self.config
        if self._u_clipped_run or self._u_close is None:
            return
        if not (c.pf_sl_enabled and float(c.pf_sl_value or 0) > 0):
            return
        level = float(c.pf_sl_value)
        below = float(c.pf_sl_underlying_below or 0)
        above = float(c.pf_sl_underlying_above or 0)
        is_movement = c.pf_sl_type == "Underlying Movement"
        delay_ns = int(c.pf_sl_delay_sec or 0) * 1_000_000_000

        def _range_breached() -> bool:
            return (below > 0 and self._u_close <= below) or (above > 0 and self._u_close >= above)

        def _holds() -> bool:
            if is_movement:
                if self._u_pending_dir > 0:
                    return self._u_close >= level
                if self._u_pending_dir < 0:
                    return self._u_close <= level
                return False
            return pnl <= -level and _range_breached()

        # Re-confirm an armed pending hit before scanning for a fresh one.
        if self._u_pending_at_ns:
            if not _holds():
                self._u_pending_at_ns = 0
                self._u_pending_dir = 0  # recovered — fall through, may re-arm now
            elif (ts - self._u_pending_at_ns) >= delay_ns:
                self._fire_underlying(ts, level)
                return
            else:
                return  # holding within the delay window

        hit, hit_dir = False, 0
        if is_movement:
            if self._u_prev is not None:
                if self._u_prev <= level <= self._u_close:
                    hit, hit_dir = True, 1
                elif self._u_prev >= level >= self._u_close:
                    hit, hit_dir = True, -1
        elif pnl <= -level and _range_breached():
            hit = True
        if hit:
            if delay_ns <= 0:
                self._fire_underlying(ts, level)
            else:
                self._u_pending_at_ns = ts
                self._u_pending_dir = hit_dir

    def _delay_ok(self, side: str, crossed: bool, ts: int, delay_ns: int) -> bool:
        """§4.3/§5.3 confirmation window. A breach ARMS a pending fire; the fire is
        released only once it has HELD for ``delay_ns`` (cleared if it reverses
        first — PORTFOLIO_DELAY_CLEARED). ``delay_ns<=0`` fires immediately. Per-side
        pending state. NOTE: live fires at the CONFIRMATION bar (it cannot close
        retroactively), vs the post-run clip which truncates at the original hit bar."""
        pend = self._sl_pending_at if side == "sl" else self._tgt_pending_at
        if not crossed:
            if pend:  # condition reversed before the window elapsed → clear
                if side == "sl":
                    self._sl_pending_at = 0
                else:
                    self._tgt_pending_at = 0
            return False
        if delay_ns <= 0:
            return True
        if not pend:  # arm
            if side == "sl":
                self._sl_pending_at = ts
            else:
                self._tgt_pending_at = ts
            return False
        if (ts - pend) >= delay_ns:  # held long enough → fire + disarm
            if side == "sl":
                self._sl_pending_at = 0
            else:
                self._tgt_pending_at = 0
            return True
        return False  # holding within the window

    def _eval_side(self, ts, pnl, is_loss, side, value, action, market, cap, day_scoped,
                   delay_ns: int = 0) -> None:
        kind = "Stoploss" if is_loss else "Target"
        if str(action).lower() == "reexecute":
            cap = cap if cap > 0 else 50
            baseline = self._sl_reexec_baseline if side == "sl" else self._tgt_reexec_baseline
            fires = self._sl_reexec_fires if side == "sl" else self._tgt_reexec_fires
            delta = pnl - baseline
            crossed = (delta <= -value) if is_loss else (delta >= value)
            crossed = crossed and fires < cap
            if self._delay_ok(side, crossed, ts, delay_ns):
                self._fire(ts, "reexecute", market, self._reason(kind, value, "reexecute", market))
                if side == "sl":
                    self._sl_reexec_baseline = pnl; self._sl_reexec_fires += 1
                else:
                    self._tgt_reexec_baseline = pnl; self._tgt_reexec_fires += 1
        elif day_scoped:  # SqOff, day-scoped (pf_sl): daily limit, blocks to EOD
            clipped = self._sl_clipped_today if side == "sl" else self._tgt_clipped_today
            delta = pnl - self._day_start_pnl
            crossed = (delta <= -value) if is_loss else (delta >= value)
            if self._delay_ok(side, crossed and not clipped, ts, delay_ns):
                self._fire(ts, "sqoff", market, self._reason(kind, value, "sqoff", market))
                if side == "sl":
                    self._sl_clipped_today = True
                else:
                    self._tgt_clipped_today = True
        else:  # SqOff, absolute single (pf_tgt): hit once → stop rest of run
            clipped = self._sl_clipped_run if side == "sl" else self._tgt_clipped_run
            crossed = (pnl <= -value) if is_loss else (pnl >= value)
            if self._delay_ok(side, crossed and not clipped, ts, delay_ns):
                self._fire(ts, "sqoff_run", market, self._reason(kind, value, "sqoff", market))
                if side == "sl":
                    self._sl_clipped_run = True
                else:
                    self._tgt_clipped_run = True

    def on_stop(self) -> None:
        # Diagnostic only (engine logging is bypassed; use print). Proves the
        # monitor saw the shared combined P&L. Removed once enforcement lands.
        if self.combined_pnl_curve:
            pnls = [p for _, p in self.combined_pnl_curve]
            print(f"[PF_MONITOR] observed {len(pnls)} bars | combined PnL "
                  f"min={min(pnls):.2f} max={max(pnls):.2f} last={pnls[-1]:.2f} "
                  f"| day-scoped SL breaches detected={len(self.detected_breaches)}")
