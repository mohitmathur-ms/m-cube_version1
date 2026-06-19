"""Phase 1 of the one-pass unified-engine execution path (flag ``_USE_UNIFIED_ENGINE``).

Runs ALL enabled legs of a portfolio in ONE ``BacktestEngine`` (multiple venues
and instruments on a single shared timeline) instead of one engine per leg in a
``ProcessPoolExecutor``. Per-leg results are recovered post-run by ``strategy_id``
(exactly like the grouping path), so the returned ``{slot_id: result}`` dict is
shape-identical to ``_run_all_slots`` and ``_merge_portfolio_results`` consumes it
unchanged.

SCOPE (Phase 1 — parity skeleton): no portfolio-monitor yet (combined SL/Target,
ReExecute, agg Move-SL stay on the existing post-run/two-pass path — this function
is only used when none of those is active, see the dispatch guard in
orchestration). No streaming yet (added once parity holds). The goal of Phase 1 is
byte-parity with the per-slot path on a portfolio with no portfolio-level features
and no entry-window/run_on_days bar-filtering, proving the shared multi-instrument
engine + attribution before any behaviour moves into the engine.
"""

from __future__ import annotations

import os
import time as _time
from decimal import Decimal
from pathlib import Path

import pandas as pd

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig, LoggingConfig, RiskEngineConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Currency, Money
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from core.backtest_runner.slot_execution import (
    ManagedExitStrategy,
    STRATEGY_REGISTRY,
    _aggregate_target_for_slot,
    _bidask_data_available,
    _build_close_lookup,
    _build_vwap_lookup,
    _cached_catalog_bars,
    _capture_underlying_curve,
    _config_supports_aggregate_to,
    _extract_slot_from_group_reports,
    _normalize_primary_to_mid,
    _pair_bid_ask_bar_type,
    _same_ts_sort_key,
    _session_start_minute,
    config_from_exit,
    effective_slot_qty,
    normalize_strategy_bar_types,
)
from core.backtest_runner.bar_filters import (
    _allowed_weekdays,
    _filter_bars_by_time_of_day,
    _is_intraday_bar_type,
)
from core.fx_rates import FxRateResolver
from core.venue_config import (
    account_currency_code_for_venue as _ccy_code_for_venue,
    load_adapter_config_for_bar_type,
)


def _unified_active() -> bool:
    """Whether the unified single-engine path should be used.

    DEFAULT: ON. The orchestration guard still restricts it to the cases it
    handles correctly (no ReExecute replay, no aggregate/cross-leg Move-SL, no
    portfolio SL/Target yet) — everything else auto-falls-back to the per-slot
    Path A. Escape hatches: ``_USE_PER_SLOT=1`` forces Path A; ``_USE_UNIFIED_ENGINE=0``
    also disables it.
    """
    if os.environ.get("_USE_PER_SLOT", "0") == "1":
        return False
    return os.environ.get("_USE_UNIFIED_ENGINE", "1") == "1"


def _use_multi_portfolio() -> bool:
    """Phase 0 gate for the multi-portfolio SESSION engine (one engine hosting every
    portfolio of a user's tree + a tier of position-less monitors). DEFAULT: OFF.

    While off, nothing changes: each portfolio runs in its own unified engine exactly
    as today, and the monitor's scope stays empty (whole-engine total_pnl path). The
    flag exists so the session-engine wiring + strategy-scoped monitor P&L + per-leg
    fill-pin re-key can land incrementally behind a parity gate that proves each
    portfolio's standalone result is byte-identical to its in-session result."""
    return os.environ.get("_USE_MULTI_PORTFOLIO", "0") == "1"


def _build_node_run_config(catalog_path, bt_meta, venue_capital, live_fill,
                           portfolio_name, start_date, end_date, chunk_size):
    """Build the ``BacktestRunConfig`` for the high-level ``BacktestNode`` path.

    Mirrors the imperative unified setup (NETTING venues, per-instrument bar data,
    UNIFIED-000 trader) but declaratively, so ``BacktestNode`` streams the catalog in
    ``chunk_size`` row batches. The custom ConservativeFillModel rides through as an
    ``ImportableFillModelConfig`` (carrying ``portfolio_id``) — only when live-fill is
    active. Strategies are attached IMPERATIVELY after ``node.build()`` (not here).
    """
    from nautilus_trader.backtest.config import (
        BacktestDataConfig, BacktestRunConfig, BacktestVenueConfig, ImportableFillModelConfig,
    )
    from nautilus_trader.config import BacktestEngineConfig as _BEC
    from nautilus_trader.config import LoggingConfig as _LC
    from nautilus_trader.config import RiskEngineConfig as _REC
    from nautilus_trader.model.identifiers import TraderId as _TID

    _fm = None
    if live_fill:
        _fm = ImportableFillModelConfig(
            fill_model_path="core.conservative_fill_model:ConservativeFillModel",
            config_path="core.conservative_fill_model:ConservativeFillModelConfig",
            config={"portfolio_id": portfolio_name},
        )
    venues = []
    for ven, cap in venue_capital.items():
        ccy = _ccy_code_for_venue(str(ven))
        venues.append(BacktestVenueConfig(
            name=str(ven), oms_type="NETTING", account_type="MARGIN",
            starting_balances=[f"{cap} {ccy}"], base_currency=ccy,
            default_leverage=1.0, fill_model=_fm,
        ))
    # One data config per instrument, listing ALL its bar types (primary + bid/ask).
    by_instrument: dict[str, set] = {}
    for bts, meta in bt_meta.items():
        iid = str(meta["bt"].instrument_id)
        by_instrument.setdefault(iid, set()).update([bts, *meta["paired_strs"]])
    data = []
    for iid, bts_set in by_instrument.items():
        data.append(BacktestDataConfig(
            catalog_path=catalog_path,
            data_cls="nautilus_trader.model.data:Bar",
            instrument_id=iid,
            bar_types=sorted(bts_set),
            start_time=f"{start_date}T00:00:00+00:00" if start_date else None,
            end_time=f"{end_date}T23:59:59.999999+00:00" if end_date else None,
        ))
    engine_cfg = _BEC(trader_id=_TID("UNIFIED-000"), logging=_LC(bypass_logging=True),
                      risk_engine=_REC(bypass=True), run_analysis=False)
    return BacktestRunConfig(venues=venues, data=data, engine=engine_cfg, chunk_size=chunk_size)


# Engine-wide account report captured during a multi-portfolio SESSION run — one
# shared engine = one account, so there is a single combined account report for the
# whole session. Keyed by the session ``portfolio_name`` ("SESSION"); read by
# ``run_session_backtest`` / the session endpoint to write the combined account CSV.
_SESSION_ENGINE_REPORTS: dict = {}


def get_session_engine_reports(portfolio_name: str):
    """Pop the engine-wide reports captured for the named session run (or {})."""
    return _SESSION_ENGINE_REPORTS.pop(portfolio_name or "SESSION", {})


def _run_portfolio_unified(
    catalog_path: str,
    slot_capital_pairs: list[tuple],   # [(slot, capital), ...] for ALL enabled slots
    custom_strategies_dir: str | None,
    default_start_date: str | None = None,
    default_end_date: str | None = None,
    default_squareoff_time: str | None = None,
    default_squareoff_tz: str | None = None,
    default_run_on_days: list | None = None,
    default_entry_start_time: str | None = None,
    default_entry_end_time: str | None = None,
    default_rbo_settings=None,
    default_other_settings=None,
    default_move_sl_settings=None,
    user_id: str | None = None,
    portfolio_name: str = "",
    default_vwap_fill: bool = False,
    default_directional_fill: bool = False,
    default_capture_underlying: bool = False,
    default_pf_sl_enabled: bool = False,
    default_pf_sl_value: float = 0.0,
    default_day_tz: str = "UTC",
    default_pf_sl_action: str = "sqoff",
    default_pf_sl_reexec_cap: int = 0,
    default_pf_sl_market_mode: bool = False,
    default_pf_tgt_enabled: bool = False,
    default_pf_tgt_value: float = 0.0,
    default_pf_tgt_action: str = "sqoff",
    default_pf_tgt_reexec_cap: int = 0,
    default_pf_tgt_market_mode: bool = False,
    default_pf_sl_type: str = "Combined Loss",
    default_pf_sl_underlying_below: float = 0.0,
    default_pf_sl_underlying_above: float = 0.0,
    default_pf_sl_delay_sec: int = 0,
    default_pf_tgt_delay_sec: int = 0,
    session_pf_specs: list | None = None,
    slot_pf_ids: dict | None = None,
    session_tag_specs: list | None = None,
) -> tuple[dict, list]:
    """Run every leg of a portfolio in ONE engine. Returns ``({slot_id: result}, errors)``.

    Multi-portfolio (Phase 0): when ``session_pf_specs`` is provided, ``slot_capital_pairs``
    holds the legs of SEVERAL portfolios and ``session_pf_specs`` is one spec per portfolio
    ``{id, slot_ids:set, pf_sl:{...}, pf_tgt:{...}, move_sl, day_tz}``; ``slot_pf_ids`` maps
    each ``slot_id`` → its ``portfolio_id``. One scoped ``PortfolioMonitorStrategy`` is built
    per portfolio (``scope_strategy_ids`` = that portfolio's legs). When ``session_pf_specs``
    is None the single-portfolio path runs exactly as before (byte-identical).

    Phase 1: entry-window / run_on_days are handled via the strategy-level gate
    (``allowed_weekdays``) with a CONTIGUOUS data feed — no bar pre-filtering — so
    callers should restrict this path to portfolios without those filters until
    Phase 2 verifies parity for them.
    """
    _t0 = _time.time()
    slot_results: dict = {}
    errors: list = []
    if not slot_capital_pairs:
        return slot_results, errors

    if custom_strategies_dir:
        from core.custom_strategy_loader import get_merged_registry
        registry, _ = get_merged_registry(Path(custom_strategies_dir))
    else:
        registry = STRATEGY_REGISTRY

    catalog = ParquetDataCatalog(catalog_path)
    instrument_map = {inst.id: inst for inst in catalog.instruments()}

    # Force the SIGNAL/decision primary to MID for FX bid/ask data: the strategy
    # fires/triggers on the unbiased midpoint, while fills stay direction-correct
    # (BUY→ASK, SELL→BID) via the matching engine + §4.2. Crypto LAST / already-MID
    # slots are unchanged. Only normalize when the MID series actually exists in the
    # catalog (FX synthesizes MID from bid/ask at load) — else keep the original
    # primary. All three series (MID+BID+ASK) are then loaded for each slot via
    # _pair_bid_ask_bar_type below, and the Format-A leg subscribes to MID+BID+ASK.
    import dataclasses as _dc

    def _mid_primary(slot):
        _mid = _normalize_primary_to_mid(slot.bar_type_str)
        if _mid == slot.bar_type_str:
            return slot
        _sd = slot.start_date or default_start_date
        _ed = slot.end_date or default_end_date
        try:
            if _cached_catalog_bars(catalog_path, _mid, _sd, _ed):
                print(f"[MID_PRIMARY] {slot.bar_type_str} -> {_mid} "
                      f"(signals on MID; fills via bid/ask)")
                return _dc.replace(slot, bar_type_str=_mid)
        except Exception:  # noqa: BLE001 — MID unavailable → keep original primary
            pass
        return slot
    slot_capital_pairs = [(_mid_primary(_s), _c) for _s, _c in slot_capital_pairs]

    allowed_weekdays = _allowed_weekdays(default_run_on_days)

    # Live conservative-fill (single-pass §4.2 via ConservativeFillModel) vs the
    # default post-run reprice. Active only when: the conservative reprice is
    # configured (vwap_exit_fill), the live monitor is enforcing, a portfolio
    # feature is set, and _USE_LIVE_FILL_MODEL=1. When on: legs fill SL/TP closes at
    # the conservative price in-engine, so the post-run reprice (vwap lookup) is
    # skipped and the monitor reads conservative fills directly.
    _cons_fill_active = bool(default_vwap_fill or os.environ.get("_USE_VWAP_FILL", "0") == "1")
    _dir_fill_active = bool(default_directional_fill or os.environ.get("_USE_DIRECTIONAL_FILL", "0") == "1")
    _enf_active = (os.environ.get("_USE_POST_RUN_PF", "0") != "1"
                   and os.environ.get("_USE_PF_ENFORCE", "1") == "1")
    _ms0 = default_move_sl_settings
    _pf_feature = ((bool(default_pf_sl_enabled) and float(default_pf_sl_value or 0) > 0)
                   or (bool(default_pf_tgt_enabled) and float(default_pf_tgt_value or 0) > 0)
                   or bool(_ms0 and (getattr(_ms0, "agg_pnl_enabled", False)
                                     or getattr(_ms0, "hit_on_leg_sl", False)
                                     or getattr(_ms0, "hit_on_leg_target", False))))
    _live_fill = (os.environ.get("_USE_LIVE_FILL_MODEL", "1") != "0"
                  and (_cons_fill_active or _dir_fill_active) and _enf_active and _pf_feature)

    # ── Multi-portfolio SESSION fill regime (per-portfolio) ──────────────────────
    # In a session each portfolio may have its OWN conservative-fill regime: some
    # use the LIVE in-engine ConservativeFillModel (enforcing + vwap/dir + a pf
    # feature → its standalone _live_fill is True), others use the POST-RUN vwap/
    # close reprice. The shared engine has ONE fill model per venue + ONE set of
    # post-run lookups, so we (a) attach the model if ANY portfolio is live, (b)
    # thread each leg's OWN cons/dir flags so live legs pin correctly, (c) build the
    # lookups whenever ANY portfolio wants them, and (d) apply the post-run lookup
    # in _extract ONLY for the legs whose portfolio is NOT live (live legs already
    # filled conservatively in-engine). `_pid_live` maps portfolio_id → live regime.
    _pid_live: dict = {}
    if session_pf_specs is not None:
        _envlf = os.environ.get("_USE_LIVE_FILL_MODEL", "1") != "0"
        _envvw = os.environ.get("_USE_VWAP_FILL", "0") == "1"
        _envdir = os.environ.get("_USE_DIRECTIONAL_FILL", "0") == "1"
        for _sp in session_pf_specs:
            _spvw = bool(_sp.get("vwap_fill")) or _envvw
            _spdir = bool(_sp.get("dir_fill")) or _envdir
            _pid_live[_sp["id"]] = bool(
                _envlf and (_spvw or _spdir) and _enf_active and bool(_sp.get("pf_feature")))
        # Attach the model if ANY portfolio is live; build lookups for the POST-RUN
        # portfolios (the live ones fill in-engine and skip the lookup in _extract).
        _live_fill = any(_pid_live.values())
        _sess_build_vwap = any((bool(_sp.get("vwap_fill")) or _envvw) and not _pid_live[_sp["id"]]
                               for _sp in session_pf_specs)
        _sess_build_close = any((bool(_sp.get("dir_fill")) or _envdir) and not _pid_live[_sp["id"]]
                                for _sp in session_pf_specs)
    else:
        _sess_build_vwap = _sess_build_close = False

    # ── Multi-portfolio SESSION entry-window stream pre-filter (per bar type) ────
    # Single-portfolio filters the shared stream by the portfolio's entry_start so
    # indicator warmup ignores pre-window bars. In a session, portfolios sharing a
    # bar type may have DIFFERENT windows, so we filter each bar type by the EARLIEST
    # entry_start among the portfolios that USE it (None = some user has no window →
    # don't filter that bar type). This keeps the ENGINE's bar set == the single
    # path's for any bar type used by one window (e.g. a non-agg leg whose indicator
    # is auto-fed by register_indicator_for_bars and so can't be gated by the leg's
    # on_bar self-filter). Legs with a LATER window than the earliest still self-filter
    # the gap in on_bar (correct for aggregating legs, which feed indicators manually).
    _bts_win: dict = {}  # bar_type_str -> earliest "HH:MM:SS" start, or None = no filter
    if session_pf_specs is not None:
        _spec_by_id_w = {sp["id"]: sp for sp in session_pf_specs}

        def _hhmm_min(_w):
            try:
                return int(_w[:2]) * 60 + int(_w[3:5])
            except Exception:  # noqa: BLE001
                return None
        for _s, _ in slot_capital_pairs:
            _bt = _s.bar_type_str
            _w = _spec_by_id_w.get(slot_pf_ids.get(_s.slot_id), {}).get("entry_start_time") or None
            if _bt not in _bts_win:
                _bts_win[_bt] = _w
            elif _w is None or _bts_win[_bt] is None:
                _bts_win[_bt] = None          # any no-window user → keep all bars
            else:
                _cur, _new = _hhmm_min(_bts_win[_bt]), _hhmm_min(_w)
                if _new is not None and (_cur is None or _new < _cur):
                    _bts_win[_bt] = _w        # keep the earliest start

    # High-level BacktestNode path (flagged, parity-gated): same engine, built/streamed
    # by the node (chunk_size) instead of the imperative BacktestEngine + add_data loop.
    # Everything else (data-load, strategies, extraction) is reused unchanged.
    _node_path = os.environ.get("_USE_BACKTEST_NODE", "0") == "1"

    # ── Lazy windowed streaming (default; bounds memory for multi-user) ──────────
    # Instead of loading the whole date range into `all_bars`, read the catalog in
    # DATE windows, sort each window (quotes-before-MID), and feed it to the engine
    # (add_data → run(streaming) → clear_data). Byte-identical to the eager path
    # (date-aligned windows never split a timestamp; per-window sort + ascending
    # windows == the global sort), but peak memory ≈ one window, not the full range.
    # Used only when NO eager-bar-derived structure is needed — post-run vwap/close
    # lookups (fill configured + live-fill off), underlying capture, the node path,
    # an entry-window pre-filter, or per-slot date ranges all fall back to eager.
    _need_vwap = (default_vwap_fill or os.environ.get("_USE_VWAP_FILL", "0") == "1") and not _live_fill
    _need_close = (default_directional_fill or os.environ.get("_USE_DIRECTIONAL_FILL", "0") == "1") and not _live_fill
    _uniform_range = all(getattr(_s, "start_date", None) is None
                         and getattr(_s, "end_date", None) is None
                         for _s, _ in slot_capital_pairs)
    # NOTE: includes the node path — when BacktestNode is used, it only BUILDS the
    # engine (venue + FillModel-as-config + instruments); we still stream the data
    # ourselves (windowed, controlled ASK,BID,MID order) instead of node.run(), so the
    # node path gets the SAME deterministic order as the low-level path.
    # Manual windowed chunking is applied ONLY on the BacktestNode path (bounded
    # memory there). The low-level BacktestEngine (unified default) does NOT chunk —
    # it loads + runs eagerly (the trusted reference). So `_lazy_ok` requires _node_path.
    _lazy_ok = (os.environ.get("_USE_LAZY_STREAM", "1") == "1"  # escape: =0 → eager
                and _node_path
                and not _need_vwap and not _need_close
                and not default_capture_underlying and _uniform_range
                and not (default_entry_start_time or default_entry_end_time)
                and bool(default_start_date) and bool(default_end_date))

    # ── Load bars for every DISTINCT bar type, build per-bar-type lookups ──
    # bt_meta[bar_type_str] = {bt, instrument, paired_strs, missing_pairs,
    #                          vwap, close, fx, underlying}
    bt_meta: dict[str, dict] = {}
    all_bars: list = []
    for slot, _cap in slot_capital_pairs:
        bts = slot.bar_type_str
        if bts in bt_meta:
            continue
        bt = BarType.from_str(bts)
        instrument = instrument_map.get(bt.instrument_id)
        if instrument is None:
            raise ValueError(f"No instrument found for {bts}")
        paired = _pair_bid_ask_bar_type(bts)
        if _lazy_ok:
            # Defer the bar read to the windowed feed below; only the metadata is
            # needed now (venue, monitor, leg configs). bar_closes filled post-stream.
            bt_meta[bts] = {
                "bt": bt, "instrument": instrument, "paired_strs": paired,
                "missing_pairs": [], "vwap": None, "close": None,
                "fx": FxRateResolver.from_adapter_config(
                    load_adapter_config_for_bar_type(bts), catalog_path),
                "underlying": None, "bar_closes": [],
            }
            continue
        start_date = slot.start_date or default_start_date
        end_date = slot.end_date or default_end_date
        missing_pairs: list[str] = []
        bars_this: list = []
        for load_bt in [bts, *paired]:
            try:
                cached = _cached_catalog_bars(catalog_path, load_bt, start_date, end_date)
            except Exception:
                cached = None
            if cached:
                bars_this.extend(cached)
            elif load_bt in paired:
                missing_pairs.append(load_bt)
        if not bars_this:
            raise ValueError(f"No bars for {bts} in {start_date}..{end_date}")
        # Entry-window START-side bar pre-filter — matches the per-slot managed
        # path (`_filter_bars_by_time_of_day(bars, start, None)`): drop bars before
        # entry_start so indicator warmup ignores them, but KEEP post-end bars so
        # exits still run (the strategy gate, set below, blocks late entries).
        # Only for intraday bar types (HH:MM window is meaningless on daily bars).
        # Single-portfolio: pre-filter the stream by the portfolio's start-side window.
        # SESSION: filter by the EARLIEST entry_start among the portfolios that use
        # THIS bar type (_bts_win; None = a user has no window → keep all). This keeps
        # the engine's bar set == the single path's for any bar type used by a single
        # window (fixes non-agg legs whose indicators are auto-fed by the engine and
        # so bypass the leg's on_bar self-filter); legs with a later window self-filter
        # the remaining gap in on_bar.
        if _is_intraday_bar_type(bts):
            if session_pf_specs is not None:
                _bt_start = _bts_win.get(bts)
                if _bt_start:
                    bars_this, _ = _filter_bars_by_time_of_day(bars_this, _bt_start, None)
            elif default_entry_start_time or default_entry_end_time:
                bars_this, _ = _filter_bars_by_time_of_day(bars_this, default_entry_start_time, None)
        all_bars.extend(bars_this)
        # Build gate: single-portfolio → the portfolio's own (vwap/dir) AND not live.
        # SESSION → build whenever ANY post-run portfolio wants the lookup (per-leg
        # application in _extract restricts it to that portfolio's legs).
        _want_vwap = (_sess_build_vwap if session_pf_specs is not None
                      else ((default_vwap_fill or os.environ.get("_USE_VWAP_FILL", "0") == "1")
                            and not _live_fill))
        _want_close = (_sess_build_close if session_pf_specs is not None
                       else ((default_directional_fill or os.environ.get("_USE_DIRECTIONAL_FILL", "0") == "1")
                             and not _live_fill))
        vwap = _build_vwap_lookup(bars_this, _session_start_minute(bts)) if _want_vwap else None
        close = _build_close_lookup(bars_this) if _want_close else None
        fx = FxRateResolver.from_adapter_config(
            load_adapter_config_for_bar_type(bts), catalog_path)
        underlying = (_capture_underlying_curve(catalog_path, bts, start_date, end_date)
                      if default_capture_underlying else None)
        # Per-bar (ts_ns, close) for THIS execution bar type — feeds the
        # mark-to-market portfolio-P&L curve. Built from the (post entry-window
        # filter) bars so it aligns with what the engine actually processed.
        bar_closes = sorted(
            (int(b.ts_event), float(b.close))
            for b in bars_this if str(b.bar_type) == bts
        )
        bt_meta[bts] = {
            "bt": bt, "instrument": instrument, "paired_strs": paired,
            "missing_pairs": missing_pairs, "vwap": vwap, "close": close,
            "fx": fx, "underlying": underlying, "bar_closes": bar_closes,
        }

    # Same-ts order: EXPLICIT quotes-before-MID (ASK, then BID, then MID) at each
    # timestamp — a deterministic contract (spec §4.1/§4.2: an exit's conservative
    # fill should see the current minute's bid/ask before the MID trigger). Replaces
    # the old INCIDENTAL MID-first order (a byproduct of load+stable-sort), so the
    # result no longer depends on load sequence or reader. Non-FX (LAST) bars have a
    # single stream → the secondary rank is irrelevant. Nautilus re-sorts on ts_init
    # only, which is stable, so it preserves this tiebreak.
    all_bars.sort(key=_same_ts_sort_key)

    # ── Per-venue capital (account starting balance). Trade size is FIXED, so the
    #    balance never affects fills — this only sets each venue's account seed. ──
    venue_capital: dict = {}
    for slot, cap in slot_capital_pairs:
        ven = bt_meta[slot.bar_type_str]["bt"].instrument_id.venue
        venue_capital[ven] = venue_capital.get(ven, 0.0) + float(cap)

    engine = None
    node = None
    try:
        if _node_path:
            # ── High-level BacktestNode path ── the node builds the engine (NETTING
            # venues + the ConservativeFillModel-as-config), loads instruments from the
            # catalog, and streams the data in chunk_size batches at node.run(). We
            # attach strategies imperatively below (same as the low-level path).
            from nautilus_trader.backtest.node import BacktestNode
            if _live_fill:
                from core.managed_strategy import clear_pf_fill_px_bus
                clear_pf_fill_px_bus(portfolio_name)
            try:
                _chunk_n = int(os.environ.get("_NODE_CHUNK_SIZE", "1000000") or 1000000)
            except ValueError:
                _chunk_n = 1_000_000
            _run_cfg = _build_node_run_config(
                catalog_path, bt_meta, venue_capital, _live_fill, portfolio_name,
                default_start_date, default_end_date, max(1, _chunk_n),
            )
            node = BacktestNode(configs=[_run_cfg])
            node.build()
            engine = node.get_engine(_run_cfg.id)
            if engine is None:
                raise RuntimeError(
                    f"BacktestNode.get_engine returned None for {portfolio_name!r} — "
                    f"catalog {catalog_path!r} may lack data in {default_start_date}..{default_end_date}"
                )
            print(f"[NODE] BacktestNode path active (chunk_size={_chunk_n})")
        else:
            engine = BacktestEngine(config=BacktestEngineConfig(
                trader_id=TraderId("UNIFIED-000"),
                logging=LoggingConfig(bypass_logging=True),
                risk_engine=RiskEngineConfig(bypass=True),
                run_analysis=False,
            ))
            # Live-fill: one ConservativeFillModel per venue (fills SL/TP closes at the
            # leg-pinned conservative price). Fresh price bus per run.
            _fill_model = None
            if _live_fill:
                from core.conservative_fill_model import ConservativeFillModel
                from core.managed_strategy import clear_pf_fill_px_bus
                clear_pf_fill_px_bus(portfolio_name)
                _fill_model = ConservativeFillModel(portfolio_name)
            # One venue per distinct venue, each with its own base currency + account.
            for ven, cap in venue_capital.items():
                ccy = Currency.from_str(_ccy_code_for_venue(str(ven)))
                engine.add_venue(
                    venue=ven,
                    fill_model=_fill_model,
                    # NETTING (not HEDGING): each leg holds ONE position at a time, and an
                    # opposite signal nets/closes/flips it instead of stacking a second
                    # overlapping position — matching the per-slot engine. Positions are
                    # still keyed by (instrument, strategy_id), so distinct legs stay
                    # separate even on the same instrument. HEDGING let a single leg open
                    # two opposing positions on a flipped signal (the double-entry bug).
                    oms_type=OmsType.NETTING,
                    account_type=AccountType.MARGIN,
                    starting_balances=[Money(cap, ccy)],
                    base_currency=ccy,
                    default_leverage=Decimal(1),
                )
            for meta in bt_meta.values():
                engine.add_instrument(meta["instrument"])
        # Data is fed AFTER strategies are added (required for the streaming loop;
        # harmless for the one-shot path). See the feed-and-run block below.

        # ── Portfolio monitor gating ──
        # _USE_PF_MONITOR=1            → attach the monitor (detection-only).
        # + _USE_PF_ENFORCE=1 (+pf_sl) → the monitor PUBLISHES breaches to the
        #   shared fire bus and each leg closes its own position live this bar
        #   (one-pass SqOff). Enforcement registers the monitor FIRST so its signal
        #   is visible to the legs on the SAME bar (else a 1-bar lag).
        # Live portfolio enforcement is the DEFAULT (one-pass live engine). It is
        # active unless explicitly reverted to the post-run/two-pass reconstruction
        # via _USE_POST_RUN_PF=1 (escape hatch). _USE_PF_ENFORCE/_USE_PF_MONITOR
        # default to "1"; _USE_PF_ENFORCE=0 also disables it.
        _post_run = os.environ.get("_USE_POST_RUN_PF", "0") == "1"
        _enforce_flag = (not _post_run) and os.environ.get("_USE_PF_ENFORCE", "1") == "1"
        # Live Move-SL (aggregate-P&L + Hit-On-Leg) enforces via the monitor / shared
        # bus. Hit-On-Leg needs no monitor (legs share the in-process cross-slot bus),
        # but aggregate-P&L does (only the monitor sees combined P&L). Enforce when a
        # pf_sl / pf_tgt action OR a live Move-SL feature is configured.
        _ms = default_move_sl_settings
        _agg_en = bool(_ms and getattr(_ms, "agg_pnl_enabled", False)
                       and float(getattr(_ms, "agg_pnl_threshold", 0) or 0) > 0)
        _hit_en = bool(_ms and (getattr(_ms, "hit_on_leg_sl", False)
                                or getattr(_ms, "hit_on_leg_target", False)))
        _pf_sl_on = bool(default_pf_sl_enabled) and float(default_pf_sl_value or 0.0) > 0
        _pf_tgt_on = bool(default_pf_tgt_enabled) and float(default_pf_tgt_value or 0.0) > 0
        _enforce = _enforce_flag and (_pf_sl_on or _pf_tgt_on or _agg_en or _hit_en)
        # Attach the monitor when enforcing, or for explicit detection-only debug.
        _mon_on = _enforce or os.environ.get("_USE_PF_MONITOR", "0") == "1"

        def _build_monitor():
            from core.portfolio_monitor import (
                PortfolioMonitorConfig,
                PortfolioMonitorStrategy,
            )
            return PortfolioMonitorStrategy(PortfolioMonitorConfig(
                monitor_instrument_ids=tuple(str(m["bt"].instrument_id) for m in bt_meta.values()),
                monitor_bar_types=tuple(bt_meta.keys()),
                starting_capital=float(sum(c for _, c in slot_capital_pairs)),
                pf_sl_enabled=bool(default_pf_sl_enabled),
                pf_sl_value=float(default_pf_sl_value or 0.0),
                day_tz=default_day_tz or "UTC",
                enforce=bool(_enforce),
                portfolio_id=portfolio_name,
                action=str(default_pf_sl_action or "sqoff"),
                reexec_cap=int(default_pf_sl_reexec_cap or 0),
                market_mode=bool(default_pf_sl_market_mode),
                pf_tgt_enabled=bool(default_pf_tgt_enabled),
                pf_tgt_value=float(default_pf_tgt_value or 0.0),
                tgt_action=str(default_pf_tgt_action or "sqoff"),
                tgt_market_mode=bool(default_pf_tgt_market_mode),
                tgt_reexec_cap=int(default_pf_tgt_reexec_cap or 0),
                agg_enabled=_agg_en,
                agg_threshold=float(getattr(_ms, "agg_pnl_threshold", 0) or 0) if _ms else 0.0,
                agg_is_loss=(getattr(_ms, "agg_pnl_direction", "loss") == "loss") if _ms else True,
                # Underlying-price SL (spec §2.1/§5.1) — "underlying = self" = the
                # PRIMARY (first) slot's bar type.
                pf_sl_type=str(default_pf_sl_type or "Combined Loss"),
                pf_sl_underlying_below=float(default_pf_sl_underlying_below or 0.0),
                pf_sl_underlying_above=float(default_pf_sl_underlying_above or 0.0),
                pf_sl_delay_sec=int(default_pf_sl_delay_sec or 0),
                pf_tgt_delay_sec=int(default_pf_tgt_delay_sec or 0),
                underlying_bar_type=(slot_capital_pairs[0][0].bar_type_str
                                     if slot_capital_pairs else ""),
                # track=False = progress-only: publish day-wise UI progress but skip
                # the per-bar valuation/curve. When enforcing or detecting, value too.
                track=bool(_mon_on),
                order_id_tag="MON",
            ))

        # The monitor ALWAYS attaches so the UI gets day-wise progress even for a
        # plain portfolio. Fresh buses (progress + enforcement) per run since the
        # module-level dicts persist across runs/tests.
        from core.managed_strategy import clear_pf_progress_bus
        clear_pf_progress_bus(portfolio_name)
        if session_pf_specs is None and _enforce:
            from core.managed_strategy import (
                clear_pf_sl_bus, clear_pf_agg_bus, clear_cross_slot_bus, clear_pf_cons_bus,
            )
            clear_pf_sl_bus(portfolio_name)
            clear_pf_agg_bus(portfolio_name)
            clear_pf_cons_bus(portfolio_name)
            clear_cross_slot_bus(portfolio_name)  # Hit-On-Leg uses a fresh shared bus
            # Enforcing monitor attaches FIRST so its fire reaches legs same-bar.
            engine.add_strategy(_build_monitor())

        # ── Build + attach one strategy per leg (unique order_id_tag) ──
        # Single-portfolio path. In a multi-portfolio SESSION this loop is skipped
        # (empty iterable) and the per-portfolio scoped block below registers legs.
        order_tags: list[str] = []
        for i, (slot, cap) in enumerate(
                () if session_pf_specs is not None else slot_capital_pairs):
            meta = bt_meta[slot.bar_type_str]
            bt = meta["bt"]
            instrument_id = bt.instrument_id
            order_tag = f"U-{i:03d}"
            order_tags.append(order_tag)
            eff_sq_time = (slot.exit_config.squareoff_time or slot.squareoff_time
                           or default_squareoff_time)
            eff_sq_tz = (slot.exit_config.squareoff_tz or slot.squareoff_tz
                         or default_squareoff_tz)
            slot_qty = effective_slot_qty(slot, user_id)
            _eff_sbt, _ = normalize_strategy_bar_types(
                slot.bar_type_str, getattr(slot, "strategy_bar_types", None) or [])
            managed = (slot.exit_config.has_exit_management() or eff_sq_time
                       or default_rbo_settings is not None or allowed_weekdays is not None)
            if managed:
                cfg = config_from_exit(
                    exit_config=slot.exit_config,
                    signal_name=slot.strategy_name,
                    signal_params=slot.strategy_params,
                    instrument_id=instrument_id,
                    bar_type=bt,
                    trade_size=slot_qty,
                    order_id_tag=order_tag,
                    squareoff_time=eff_sq_time,
                    squareoff_tz=eff_sq_tz,
                    entry_start_time=default_entry_start_time,
                    entry_end_time=default_entry_end_time,
                    rbo_settings=default_rbo_settings,
                    other_settings=default_other_settings,
                    move_sl_settings=default_move_sl_settings,
                    auto_bidask=_bidask_data_available(meta["paired_strs"], meta["missing_pairs"]),
                    allowed_weekdays=(None if allowed_weekdays is None else sorted(allowed_weekdays)),
                    subscribe_bar_types=_eff_sbt,
                    portfolio_id=portfolio_name,
                    slot_id=slot.slot_id,
                    pf_monitor_enforced=_enforce,
                    pf_monitor_action=str(default_pf_sl_action or "sqoff"),
                    pf_monitor_market_mode=bool(default_pf_sl_market_mode),
                    cons_fill_active=bool(default_vwap_fill
                                          or os.environ.get("_USE_VWAP_FILL", "0") == "1"),
                    dir_fill_active=_dir_fill_active,
                )
                strategy = ManagedExitStrategy(cfg)
            else:
                if slot.strategy_name not in registry:
                    raise ValueError(f"Unknown strategy: {slot.strategy_name}")
                entry = registry[slot.strategy_name]
                config_class = entry["config_class"]
                valid = set(entry["params"].keys())
                params = {k: v for k, v in slot.strategy_params.items() if k in valid}
                ckw = {
                    "instrument_id": instrument_id, "bar_type": bt,
                    "trade_size": Decimal(str(slot_qty)), "order_id_tag": order_tag,
                    **params,
                }
                _agg_to = _aggregate_target_for_slot(slot, bt)
                if _agg_to and _config_supports_aggregate_to(config_class):
                    ckw["aggregate_to_bar_type"] = _agg_to
                strategy = entry["strategy_class"](config_class(**ckw))
            engine.add_strategy(strategy)

        # Non-enforcing monitor attaches AFTER the legs (it takes no action, so a
        # 1-bar observation lag is irrelevant). Covers BOTH detection-only
        # (_USE_PF_MONITOR) and progress-only (plain portfolio) — track is set from
        # _mon_on inside _build_monitor. When enforcing it was already added FIRST
        # (above) so its fire reaches legs same-bar — don't double-add.
        if session_pf_specs is None and not _enforce:
            engine.add_strategy(_build_monitor())

        # ── Multi-portfolio SESSION: one scoped PortfolioMonitorStrategy per portfolio,
        # each valuing ONLY its own legs (scope_strategy_ids). Single-portfolio
        # (session_pf_specs is None) skips this entirely → byte-identical. ──
        _slot_enforced: dict = {}
        if session_pf_specs is not None:
            from core.portfolio_monitor import (
                PortfolioMonitorConfig as _PMC, PortfolioMonitorStrategy as _PMS)
            from core.managed_strategy import (
                clear_pf_progress_bus as _cpb, clear_pf_sl_bus as _cslb,
                clear_pf_agg_bus as _cab, clear_cross_slot_bus as _ccsb,
                clear_pf_cons_bus as _ccb,
            )
            _specs_by_id = {sp["id"]: sp for sp in session_pf_specs}
            # SHARED instruments = traded by >1 portfolio → per-position scoped P&L.
            _iid_pids: dict = {}
            for _s, _ in slot_capital_pairs:
                _iid_pids.setdefault(
                    str(bt_meta[_s.bar_type_str]["bt"].instrument_id), set()
                ).add(slot_pf_ids[_s.slot_id])
            _shared_iids = tuple(sorted(i for i, p in _iid_pids.items() if len(p) > 1))
            for _sp in session_pf_specs:
                _pid = _sp["id"]
                _cpb(_pid); _cslb(_pid); _cab(_pid); _ccb(_pid); _ccsb(_pid)
            from core.backtest_runner.cross_portfolio import clear_cross_pf_live_bus
            clear_cross_pf_live_bus()
            # 1) instantiate every leg (collect strategy id + portfolio); add later.
            _built: list = []
            for i, (slot, cap) in enumerate(slot_capital_pairs):
                _pid = slot_pf_ids[slot.slot_id]
                _sp = _specs_by_id[_pid]
                meta = bt_meta[slot.bar_type_str]
                bt = meta["bt"]; instrument_id = bt.instrument_id
                order_tag = f"U-{i:03d}"; order_tags.append(order_tag)
                eff_sq_time = (slot.exit_config.squareoff_time or slot.squareoff_time
                               or _sp.get("squareoff_time"))
                eff_sq_tz = (slot.exit_config.squareoff_tz or slot.squareoff_tz
                             or _sp.get("squareoff_tz"))
                slot_qty = effective_slot_qty(slot, user_id)
                _eff_sbt, _ = normalize_strategy_bar_types(
                    slot.bar_type_str, getattr(slot, "strategy_bar_types", None) or [])
                _awd = _sp.get("allowed_weekdays")
                _enf = bool(_sp.get("enforce"))
                _slot_enforced[slot.slot_id] = _enf
                managed = (slot.exit_config.has_exit_management() or eff_sq_time
                           or _sp.get("rbo_settings") is not None or _awd is not None)
                if managed:
                    cfg = config_from_exit(
                        exit_config=slot.exit_config, signal_name=slot.strategy_name,
                        signal_params=slot.strategy_params, instrument_id=instrument_id,
                        bar_type=bt, trade_size=slot_qty, order_id_tag=order_tag,
                        squareoff_time=eff_sq_time, squareoff_tz=eff_sq_tz,
                        entry_start_time=_sp.get("entry_start_time"),
                        entry_end_time=_sp.get("entry_end_time"),
                        rbo_settings=_sp.get("rbo_settings"),
                        other_settings=_sp.get("other_settings"),
                        move_sl_settings=_sp.get("move_sl_settings"),
                        auto_bidask=_bidask_data_available(meta["paired_strs"], meta["missing_pairs"]),
                        allowed_weekdays=(None if _awd is None else sorted(_awd)),
                        subscribe_bar_types=_eff_sbt, portfolio_id=_pid,
                        fill_bus_id=portfolio_name, slot_id=slot.slot_id,
                        session_window_self_filter=True,
                        pf_monitor_enforced=_enf,
                        pf_monitor_action=str(_sp.get("pf_sl_action") or "sqoff"),
                        pf_monitor_market_mode=bool(_sp.get("pf_sl_market_mode")),
                        cons_fill_active=(bool(_sp.get("vwap_fill"))
                                          or os.environ.get("_USE_VWAP_FILL", "0") == "1"),
                        dir_fill_active=(bool(_sp.get("dir_fill"))
                                         or os.environ.get("_USE_DIRECTIONAL_FILL", "0") == "1"),
                    )
                    strategy = ManagedExitStrategy(cfg)
                else:
                    if slot.strategy_name not in registry:
                        raise ValueError(f"Unknown strategy: {slot.strategy_name}")
                    entry = registry[slot.strategy_name]; config_class = entry["config_class"]
                    valid = set(entry["params"].keys())
                    params = {k: v for k, v in slot.strategy_params.items() if k in valid}
                    ckw = {"instrument_id": instrument_id, "bar_type": bt,
                           "trade_size": Decimal(str(slot_qty)), "order_id_tag": order_tag, **params}
                    _agg_to = _aggregate_target_for_slot(slot, bt)
                    if _agg_to and _config_supports_aggregate_to(config_class):
                        ckw["aggregate_to_bar_type"] = _agg_to
                    strategy = entry["strategy_class"](config_class(**ckw))
                _built.append((slot, strategy, _pid))
            # 2) per-portfolio scope (leg strategy ids) + monitor instruments/bars/capital.
            _scope: dict = {}; _miid: dict = {}; _mbt: dict = {}; _mcap: dict = {}
            for (slot, strategy, _pid) in _built:
                _scope.setdefault(_pid, []).append(str(strategy.id))
                meta = bt_meta[slot.bar_type_str]
                _miid.setdefault(_pid, set()).add(str(meta["bt"].instrument_id))
                _mbt.setdefault(_pid, set()).add(slot.bar_type_str)
            for slot, cap in slot_capital_pairs:
                _p = slot_pf_ids[slot.slot_id]
                _mcap[_p] = _mcap.get(_p, 0.0) + float(cap)
            # 3) one scoped monitor per portfolio.
            def _mk_mon(_k, _sp):
                _pid = _sp["id"]; _ms2 = _sp.get("move_sl_settings")
                _agg2 = bool(_ms2 and getattr(_ms2, "agg_pnl_enabled", False)
                             and float(getattr(_ms2, "agg_pnl_threshold", 0) or 0) > 0)
                return _PMS(_PMC(
                    monitor_instrument_ids=tuple(sorted(_miid.get(_pid, set()))),
                    monitor_bar_types=tuple(sorted(_mbt.get(_pid, set()))),
                    starting_capital=float(_mcap.get(_pid, 0.0)),
                    pf_sl_enabled=bool(_sp.get("pf_sl_enabled")),
                    pf_sl_value=float(_sp.get("pf_sl_value") or 0.0),
                    day_tz=_sp.get("day_tz") or "UTC", enforce=bool(_sp.get("enforce")),
                    portfolio_id=_pid, action=str(_sp.get("pf_sl_action") or "sqoff"),
                    reexec_cap=int(_sp.get("pf_sl_reexec_cap") or 0),
                    market_mode=bool(_sp.get("pf_sl_market_mode")),
                    pf_tgt_enabled=bool(_sp.get("pf_tgt_enabled")),
                    pf_tgt_value=float(_sp.get("pf_tgt_value") or 0.0),
                    tgt_action=str(_sp.get("pf_tgt_action") or "sqoff"),
                    tgt_market_mode=bool(_sp.get("pf_tgt_market_mode")),
                    tgt_reexec_cap=int(_sp.get("pf_tgt_reexec_cap") or 0),
                    agg_enabled=_agg2,
                    agg_threshold=float(getattr(_ms2, "agg_pnl_threshold", 0) or 0) if _ms2 else 0.0,
                    agg_is_loss=(getattr(_ms2, "agg_pnl_direction", "loss") == "loss") if _ms2 else True,
                    pf_sl_type=str(_sp.get("pf_sl_type") or "Combined Loss"),
                    pf_sl_underlying_below=float(_sp.get("pf_sl_underlying_below") or 0.0),
                    pf_sl_underlying_above=float(_sp.get("pf_sl_underlying_above") or 0.0),
                    pf_sl_delay_sec=int(_sp.get("pf_sl_delay_sec") or 0),
                    pf_tgt_delay_sec=int(_sp.get("pf_tgt_delay_sec") or 0),
                    underlying_bar_type=_sp.get("underlying_bar_type") or "",
                    track=True, order_id_tag=f"MON{_k:03d}",
                    scope_strategy_ids=tuple(_scope.get(_pid, [])),
                    shared_instrument_ids=_shared_iids,
                    cross_pf_target=str(_sp.get("cross_pf_target") or ""),
                    cross_pf_verb=str(_sp.get("cross_pf_verb") or ""),
                ))
            _mons = [(_sp, _mk_mon(_k, _sp)) for _k, _sp in enumerate(session_pf_specs)]
            # 3b) TAG monitors (spec §11): one per tag group. Scopes to the legs of
            # ALL portfolios sharing the tag; on a tag combined-loss / combined-profit
            # breach it fires SqOff at EVERY portfolio in the tag (cross_pf_targets),
            # squaring off the whole group live. Tag caps are ABSOLUTE (sl_day_scoped
            # False). The tag monitor has no legs of its own.
            _tag_mons = []
            for _tk, _tg in enumerate(session_tag_specs or []):
                _pids = set(_tg.get("portfolio_ids") or [])
                _tscope = [str(_st.id) for (_sl, _st, _pp) in _built if _pp in _pids]
                if not _tscope:
                    continue
                _tiid, _tbt = set(), set()
                for (_sl, _st, _pp) in _built:
                    if _pp in _pids:
                        _mm = bt_meta[_sl.bar_type_str]
                        _tiid.add(str(_mm["bt"].instrument_id)); _tbt.add(_sl.bar_type_str)
                _ml = float(_tg.get("max_loss") or 0.0)
                _mp = float(_tg.get("max_profit") or 0.0)
                _tsl = _tg.get("trail_sl") or {}      # {every, by} or {}
                _ttg = _tg.get("trail_tgt") or {}     # {when_reach, lock, every, by} or {}
                # Trailing SL needs a base loss cap to ratchet from; default to a large
                # one (effectively the trailing floor only) when no Max-Loss is set.
                _sl_on = (_ml > 0) or bool(_tsl)
                _tag_mons.append(_PMS(_PMC(
                    monitor_instrument_ids=tuple(sorted(_tiid)),
                    monitor_bar_types=tuple(sorted(_tbt)),
                    starting_capital=0.0,
                    pf_sl_enabled=_sl_on, pf_sl_value=_ml, sl_day_scoped=False,
                    pf_tgt_enabled=(_mp > 0) or bool(_ttg), pf_tgt_value=_mp,
                    tgt_day_scoped=False,  # tag/user MAX-PROFIT caps stay ABSOLUTE (rest-of-run)
                    pf_sl_trail_enabled=bool(_tsl),
                    pf_sl_trail_every=float(_tsl.get("every", 0.0) or 0.0),
                    pf_sl_trail_by=float(_tsl.get("by", 0.0) or 0.0),
                    pf_tgt_trail_enabled=bool(_ttg),
                    pf_tgt_trail_when_reach=float(_ttg.get("when_reach", 0.0) or 0.0),
                    pf_tgt_trail_lock=float(_ttg.get("lock", 0.0) or 0.0),
                    pf_tgt_trail_every=float(_ttg.get("every", 0.0) or 0.0),
                    pf_tgt_trail_by=float(_ttg.get("by", 0.0) or 0.0),
                    day_tz=_tg.get("day_tz") or "UTC", enforce=True,
                    portfolio_id=f"TAG::{_tg.get('tag')}", action="sqoff", tgt_action="sqoff",
                    pf_sl_type="Combined Loss", track=True, order_id_tag=f"TAGMON{_tk:03d}",
                    scope_strategy_ids=tuple(_tscope), shared_instrument_ids=_shared_iids,
                    cross_pf_targets=tuple(sorted(_pids)), cross_pf_verb="sqoff",
                )))
            # 4) register: enforcing monitors (incl. tag monitors) FIRST (fire reaches
            #    legs same bar), then legs, then non-enforcing monitors.
            for _m in _tag_mons:
                engine.add_strategy(_m)
            for _sp, _m in _mons:
                if bool(_sp.get("enforce")):
                    engine.add_strategy(_m)
            for (slot, strategy, _pid) in _built:
                engine.add_strategy(strategy)
            for _sp, _m in _mons:
                if not bool(_sp.get("enforce")):
                    engine.add_strategy(_m)

        # ── Feed data + run ──
        # Streaming (gated by _USE_UNIFIED_STREAMING) bounds ENGINE-side memory: the
        # already time-sorted bars are added one COUNT-based chunk at a time,
        # run(streaming=True) advances the engine over each chunk, clear_data() drops
        # ONLY the raw data (all engine/strategy/position/account state persists), and
        # end() finalizes (flushes tail timers, stops engines). Byte-identical to the
        # one-shot path — only HOW the bars are fed differs, not the result.
        if _node_path and not _lazy_ok:
            # Node built the engine, but we STILL feed the data ourselves (sorted
            # ASK,BID,MID) instead of node.run(), so the node path stays in the same
            # deterministic order as the low-level path even when lazy streaming isn't
            # applicable (e.g. post-run lookups needed → eager all_bars here).
            engine.add_data(all_bars)
            engine.run()
        elif _lazy_ok:
            # ── Self-managed windowed streaming (default; bounded memory) ──
            # Read the catalog one DATE window at a time, sort each window
            # (quotes-before-MID via _same_ts_sort_key), feed it, advance, drop it.
            # Peak memory ≈ one window, not the whole range. bar_closes (for the MtM
            # curve) accumulate as lightweight (ts, close) tuples per exec bar type.
            _win_bts: set = set()
            for _m in bt_meta.values():
                _win_bts.add(str(_m["bt"]))
                _win_bts.update(_m["paired_strs"])
            _exec_bts = set(bt_meta.keys())
            _bc_acc: dict = {b: [] for b in _exec_bts}
            _fed_any = False
            _ws = pd.Timestamp(default_start_date, tz="UTC")
            _end_ts = pd.Timestamp(default_end_date, tz="UTC")
            while _ws <= _end_ts:
                _we = min(_ws + pd.Timedelta(days=6), _end_ts)  # 7-day window
                _we_incl = _we + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
                _chunk: list = []
                for _b in _win_bts:
                    try:
                        _c = catalog.bars(bar_types=[_b], start=_ws, end=_we_incl)
                    except Exception:  # noqa: BLE001
                        _c = None
                    if _c:
                        _chunk.extend(_c)
                _ws = _we + pd.Timedelta(days=1)
                if not _chunk:
                    continue
                _chunk.sort(key=_same_ts_sort_key)
                for _bar in _chunk:
                    _sbt = str(_bar.bar_type)
                    if _sbt in _bc_acc:
                        _bc_acc[_sbt].append((int(_bar.ts_event), float(_bar.close)))
                # sort=True: the engine re-sorts on ts_init (STABLE), preserving our
                # quotes-before-MID tiebreak from the pre-sort above — same as the
                # eager path's add_data. (sort=False would trip the engine's
                # not-sorted guard.)
                engine.add_data(_chunk, validate=False, sort=True)
                engine.run(streaming=True)
                engine.clear_data()
                _fed_any = True
            if not _fed_any:
                raise ValueError(f"No bars for {list(_exec_bts)} in "
                                 f"{default_start_date}..{default_end_date}")
            engine.end()
            for _bts in bt_meta:
                bt_meta[_bts]["bar_closes"] = sorted(_bc_acc.get(_bts, []))
            print("[UNIFIED] windowed lazy streaming (bounded memory)")
        elif os.environ.get("_USE_UNIFIED_STREAMING", "0") == "1" and all_bars:
            try:
                _chunk = int(os.environ.get("_UNIFIED_CHUNK_SIZE", "1000000") or 1000000)
            except ValueError:
                _chunk = 1_000_000
            _chunk = max(1, _chunk)
            for _i in range(0, len(all_bars), _chunk):
                # validate=False: a chunk mixes bar types (exec + bid/ask); per-item
                # cache validation is wrong/slow on a mixed stream (matches BacktestNode).
                # sort=True: the engine flags unsorted data and refuses to run otherwise
                # (the slice is already ordered, so this is a cheap no-op merge).
                engine.add_data(all_bars[_i:_i + _chunk], validate=False, sort=True)
                engine.run(streaming=True)
                engine.clear_data()
            engine.end()
        else:
            engine.add_data(all_bars)
            engine.run()

        # ── Per-leg attribution by strategy_id (insertion order) ──
        fills_report = None
        positions_report = None
        try:
            fills_report = engine.trader.generate_order_fills_report()
        except Exception:
            pass
        try:
            positions_report = engine.trader.generate_positions_report()
        except Exception:
            pass
        # SESSION: capture the engine-wide account report (one shared account across
        # all portfolios) so the combined session report has a real, non-approximated
        # account ledger. Single-portfolio path skips this (its account report is
        # generated downstream per the existing flow).
        if session_pf_specs is not None:
            _acct = None
            try:
                _accs = list(engine.kernel.cache.accounts())
                if _accs:
                    from nautilus_trader.model.identifiers import Venue as _Venue
                    _acct = engine.trader.generate_account_report(
                        _Venue(str(_accs[0].id.get_issuer())))
            except Exception:  # noqa: BLE001
                _acct = None
            _SESSION_ENGINE_REPORTS[portfolio_name or "SESSION"] = {
                "account_report": _acct,
            }
        # Exclude the monitor (not a leg) so leg indices line up with slots
        # regardless of whether it was registered first (enforce) or last.
        actual = [s for s in engine.trader.strategies()
                  if type(s).__name__ != "PortfolioMonitorStrategy"]
        actual_ids = [str(s.id) for s in actual]
        elapsed = round(_time.time() - _t0, 3)
        for i, (slot, cap) in enumerate(slot_capital_pairs):
            meta = bt_meta[slot.bar_type_str]
            sid = actual_ids[i] if i < len(actual_ids) else f"ManagedExitStrategy-{order_tags[i]}"
            # SESSION per-leg fill regime: a LIVE leg already filled conservatively
            # in-engine, so it must NOT get the post-run vwap/close reprice; a
            # POST-RUN leg does. (Single-portfolio: _pid_live empty → use meta's.)
            _leg_vwap, _leg_close = meta["vwap"], meta["close"]
            if session_pf_specs is not None and _pid_live.get(slot_pf_ids.get(slot.slot_id)):
                _leg_vwap = _leg_close = None
            try:
                r = _extract_slot_from_group_reports(
                    positions_report, fills_report, sid, slot, cap, meta["fx"],
                    _leg_vwap, _leg_close, meta["bar_closes"],
                )
            except Exception as e:  # noqa: BLE001
                errors.append({"slot_id": slot.slot_id, "error": str(e)})
                continue
            if meta["underlying"] is not None:
                r["underlying_curve"] = meta["underlying"]
            if i < len(actual):
                r["leg_exit_events"] = dict(getattr(actual[i], "_exit_events_self", {}) or {})
            r["elapsed_seconds"] = elapsed
            r["unified_engine"] = True
            # Live enforcement closed legs in-engine → the post-run clip must NOT
            # re-clip (would double-count). _merge reads this flag. In a SESSION each
            # leg's enforce flag comes from ITS portfolio; single-portfolio → _enforce.
            r["pf_monitor_enforced"] = bool(_slot_enforced.get(slot.slot_id, _enforce))
            if session_pf_specs is not None and slot_pf_ids is not None:
                r["portfolio_id"] = slot_pf_ids.get(slot.slot_id, portfolio_name)
            slot_results[slot.slot_id] = r
    finally:
        if engine is not None:
            try:
                engine.dispose()
            except BaseException:
                pass

    return slot_results, errors
