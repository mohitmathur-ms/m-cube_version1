"""Path B (BacktestNode) wiring: the env-flag check, per-day data-config
chunking that expresses run_on_days / entry-window / RBO, and the single
BacktestRunConfig builder shared by all node-based variants."""

from __future__ import annotations

import pandas as pd
from nautilus_trader.backtest.config import BacktestVenueConfig
from nautilus_trader.backtest.config import BacktestDataConfig
from nautilus_trader.backtest.config import BacktestRunConfig
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model import TraderId

from core.backtest_runner.bar_filters import (
    _allowed_weekdays,
    _is_intraday_bar_type,
)
from core.backtest_runner.rbo import (
    _RBOSettings,
    _hms_to_sec,
)


# ─────────────────────────────────────────────────────────────────────────────
# Path B (BacktestNode) helpers — opt-in via _USE_BACKTEST_NODE=1.
# See nautilus_path_a_to_path_b_migration.html for the full design rationale.
# ─────────────────────────────────────────────────────────────────────────────

def _path_b_active() -> bool:
    """True when the env flag opting into the BacktestNode pipeline is set."""
    import os
    return os.environ.get("_USE_BACKTEST_NODE") == "1"


def _sec_to_hms(sec: int) -> str:
    """Seconds-of-day → HH:MM:SS. Inverse of _hms_to_sec."""
    sec = max(0, min(86399, int(sec)))
    return f"{sec // 3600:02d}:{(sec // 60) % 60:02d}:{sec % 60:02d}"


def _chunk_data_configs_for_path_b(
    catalog_path: str,
    instrument_id_str: str,
    bar_type_strs: list[str],
    start_date: str,
    end_date: str,
    entry_start_time: str | None,
    entry_end_time: str | None,
    run_on_days: list | None,
    rbo_settings: "_RBOSettings | None" = None,
) -> list[BacktestDataConfig]:
    """One BacktestDataConfig per allowed day, bounded by the entry window.

    The Nautilus high-level API takes ``BacktestRunConfig.data`` as a list of
    ``BacktestDataConfig`` entries, each with its own ``start_time``/``end_time``.
    By emitting one entry per allowed (day, bar_type) we can express both
    ``run_on_days`` (skip excluded weekdays) and the recurring intraday
    ``entry_start_time``/``entry_end_time`` window — neither of which a single
    contiguous data config can represent.

    All days are walked in UTC. Excluded weekdays are dropped. For each
    included day, ``start_time`` becomes ``YYYY-MM-DDTHH:MM:SS+00:00`` using
    the entry window endpoints (defaulting to 00:00:00 .. 23:59:59.999999
    when one side is unbounded).

    When ``rbo_settings`` is provided, the per-day window is widened to the
    union of the existing entry window and ``[monitoring_start, entry_end +
    buffer]`` so the in-strategy RBO state machine sees the bars it needs to
    build the range and detect breakouts.

    Days with no catalog data are silently skipped by Nautilus — no need to
    pre-filter. ``ValueError`` is raised only if the filter combination
    yields zero configs (e.g. entry window with start > end).
    """
    allowed_weekdays = _allowed_weekdays(run_on_days)
    if allowed_weekdays is not None and not allowed_weekdays:
        raise ValueError(
            "Chunked Path B yielded zero data configs — run_on_days excludes "
            "every weekday."
        )

    win_start = entry_start_time or "00:00:00"
    win_end = entry_end_time or "23:59:59.999999"

    if rbo_settings is not None:
        # Widen to cover the RBO load needs: monitoring window at the start,
        # entry_end + buffer at the tail. Take the union with whatever
        # entry_window the user already set (which we only narrow further
        # never expand). Times are seconds-of-day; convert and string-compare.
        cur_start_sec = _hms_to_sec(win_start)
        cur_end_sec = _hms_to_sec(win_end.split(".")[0])  # strip fractional sec
        new_start_sec = min(cur_start_sec, rbo_settings.monitoring_start_sec)
        new_end_sec = max(
            cur_end_sec,
            rbo_settings.entry_end_sec + rbo_settings.range_buffer_sec,
        )
        win_start = _sec_to_hms(new_start_sec)
        win_end = _sec_to_hms(new_end_sec)

    start = pd.Timestamp(start_date, tz="UTC").normalize()
    end = pd.Timestamp(end_date, tz="UTC").normalize()

    configs: list[BacktestDataConfig] = []
    cur = start
    one_day = pd.Timedelta(days=1)
    while cur <= end:
        if allowed_weekdays is None or cur.weekday() in allowed_weekdays:
            day_str = cur.strftime("%Y-%m-%d")
            configs.append(BacktestDataConfig(
                catalog_path=catalog_path,
                data_cls="nautilus_trader.model.data:Bar",
                instrument_id=instrument_id_str,
                bar_types=bar_type_strs,
                start_time=f"{day_str}T{win_start}+00:00",
                end_time=f"{day_str}T{win_end}+00:00",
            ))
        cur = cur + one_day

    if not configs:
        raise ValueError(
            "Chunked Path B yielded zero data configs — check start/end dates."
        )
    return configs


def _build_run_config(
    catalog_path: str,
    instrument_id,
    bar_type_strs: list[str],
    venue,
    starting_capital: float,
    start_date: str | None,
    end_date: str | None,
    trader_id: str = "BACKTESTER-001",
    chunk_size: int | None = None,
    oms_type: str = "NETTING",
    entry_start_time: str | None = None,
    entry_end_time: str | None = None,
    run_on_days: list | None = None,
    rbo_settings: "_RBOSettings | None" = None,
) -> BacktestRunConfig:
    """Build the three Nautilus config dataclasses and bundle them.

    Single source of the Path B venue/data/engine wiring. Used by all three
    node-based variants (run_backtest_node, _run_single_slot_node,
    _run_slot_group_node) so the configs stay consistent across sites.

    When ``run_on_days`` is set, OR an entry window is set on an intraday bar
    type, this function emits one ``BacktestDataConfig`` per allowed day via
    ``_chunk_data_configs_for_path_b`` to honour the filter. Otherwise it
    emits a single contiguous data config for bit-exact parity with Path A.

    Notes on the Path A → Path B mapping:
      - ``starting_balances`` is ``list[str]`` not ``list[Money]`` (configs are
        msgspec-serialisable).
      - ``base_currency`` is a string ``"USD"`` not the Currency object.
      - ``default_leverage`` is a float ``1.0`` not ``Decimal(1)``.
      - The instrument is loaded from the catalog automatically — no explicit
        ``add_instrument`` call required.
      - ``chunk_size=None`` keeps Path B in load-everything-at-once mode for
        bit-exact parity with Path A. Pass an int (e.g. 100_000) once you've
        verified parity to opt into row-chunked streaming.
    """
    venue_cfg = BacktestVenueConfig(
        name=str(venue),
        oms_type=oms_type,
        account_type="MARGIN",
        starting_balances=[f"{starting_capital} USD"],
        base_currency="USD",
        default_leverage=1.0,
    )

    has_run_on_days = run_on_days is not None
    has_entry_window = bool(entry_start_time) or bool(entry_end_time)
    primary_bt = bar_type_strs[0] if bar_type_strs else None
    # Entry window is a no-op for non-intraday bars (ts_event is at midnight),
    # so don't bother chunking by it in that case. run_on_days is still
    # load-bearing on daily bars and forces chunking regardless.
    entry_window_effective = has_entry_window and (
        primary_bt is None or _is_intraday_bar_type(primary_bt)
    )
    needs_chunking = has_run_on_days or entry_window_effective

    if needs_chunking:
        if not start_date or not end_date:
            raise ValueError(
                "Path B chunking requires concrete start_date and end_date "
                "to enumerate allowed days."
            )
        data_cfgs = _chunk_data_configs_for_path_b(
            catalog_path=catalog_path,
            instrument_id_str=str(instrument_id),
            bar_type_strs=bar_type_strs,
            start_date=start_date,
            end_date=end_date,
            entry_start_time=entry_start_time if entry_window_effective else None,
            entry_end_time=entry_end_time if entry_window_effective else None,
            run_on_days=run_on_days,
            rbo_settings=rbo_settings,
        )
    else:
        # Construct full-day ISO timestamps so end_date is inclusive of the
        # final calendar day, matching Path A (_cached_catalog_bars: +1d -1ns)
        # and the chunked branch above (win_end = "23:59:59.999999"). Bare
        # date strings here would be interpreted by Nautilus as midnight
        # start of end_date, silently dropping all bars on the final day —
        # a Path A vs B parity bug.
        data_cfgs = [BacktestDataConfig(
            catalog_path=catalog_path,
            # Nautilus resolves data_cls via path.rsplit(":", 1) — must use
            # "module.path:ClassName" format. A dot before the class name
            # raises ValueError("not enough values to unpack") in node.build().
            data_cls="nautilus_trader.model.data:Bar",
            instrument_id=str(instrument_id),
            bar_types=bar_type_strs,
            start_time=f"{start_date}T00:00:00+00:00" if start_date else None,
            end_time=f"{end_date}T23:59:59.999999+00:00" if end_date else None,
        )]

    from nautilus_trader.config import RiskEngineConfig
    engine_cfg = BacktestEngineConfig(
        trader_id=TraderId(trader_id),
        logging=LoggingConfig(bypass_logging=True),
        risk_engine=RiskEngineConfig(bypass=True),
        run_analysis=False,
    )
    return BacktestRunConfig(
        venues=[venue_cfg],
        data=data_cfgs,
        engine=engine_cfg,
        chunk_size=chunk_size,
    )


def _path_b_supports_filters(
    run_on_days: list | None,
    entry_start_time: str | None,
    entry_end_time: str | None,
    bar_type_str: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> bool:
    """True when the configured filters can be honoured under Path B.

    Three regimes:

    1. **No filters** (no run_on_days, no entry window) → Path B is trivially
       fine — single contiguous BacktestDataConfig.
    2. **Entry window only, on a non-intraday bar type** → window is a runtime
       no-op (daily/weekly bars have ts_event at midnight; see
       ``_is_intraday_bar_type``), so still single contiguous, still fine.
    3. **Any other filter combination** → ``_build_run_config`` honours the
       filter by emitting one BacktestDataConfig per allowed day (see
       ``_chunk_data_configs_for_path_b``). That requires concrete
       ``start_date`` and ``end_date`` to enumerate days; without them we
       fall back to Path A.

    When this returns False, callers must fall back to Path A so the run
    remains correct rather than silently ignoring user-configured filters.
    """
    has_run_on_days = run_on_days is not None
    has_entry_window = bool(entry_start_time) or bool(entry_end_time)

    if not has_run_on_days and not has_entry_window:
        return True

    if (
        not has_run_on_days
        and has_entry_window
        and bar_type_str
        and not _is_intraday_bar_type(bar_type_str)
    ):
        return True

    # Need to chunk; chunking enumerates days, so we need bounds.
    return bool(start_date) and bool(end_date)
