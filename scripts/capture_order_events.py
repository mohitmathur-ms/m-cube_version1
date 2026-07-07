"""
Capture every order event from a 15-minute BID EURUSD backtest (June 2024)
and join it with the 15-minute bar active at the event's timestamp.

Two-slot portfolio (EMA Cross + 4 Moving Averages), leg-level SL=0.2% / TP=1.0%
square-off-on-hit, portfolio SL/TP disabled. Engine is built in HEDGING mode
in-process (no worker pool) so we can walk ``engine.kernel.cache.orders()``
and each order's full ``.events`` history before disposal.

Output:
    data/order_events/ORDER_TIMESTAMP_15-MINUTE_2024-06-01_2024-06-30.csv
"""

from __future__ import annotations

import sys
import time
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig, LoggingConfig, RiskEngineConfig
from nautilus_trader.model import TraderId
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

from core.models import (
    ExitConfig,
    PortfolioConfig,
    StrategySlotConfig,
    effective_slot_qty,
)
from core.managed_strategy import ManagedExitStrategy, config_from_exit
from core.backtest_runner import _cached_catalog_bars, _pair_bid_ask_bar_type


CATALOG_PATH = str(REPO_ROOT / "catalog")
OUTPUT_DIR = REPO_ROOT / "data" / "order_events"
BAR_TYPE = "EURUSD.FOREX_MS-15-MINUTE-BID-EXTERNAL"
TIMEFRAME_LABEL = "15-MINUTE"
START_DATE = "2024-06-01"
END_DATE = "2024-06-30"


def build_portfolio() -> PortfolioConfig:
    common = dict(
        stop_loss_type="percentage",
        stop_loss_value=0.2,
        target_type="percentage",
        target_value=1.0,
        on_sl_action="close",
        on_target_action="close",
        max_re_executions=0,
    )

    slot_ema = StrategySlotConfig(
        slot_id="fx_eur_ema",
        strategy_name="EMA Cross",
        strategy_params={"fast_ema_period": 10, "slow_ema_period": 30},
        bar_type_str=BAR_TYPE,
        lots=0.01,
        exit_config=ExitConfig(**common),
        enabled=True,
    )

    slot_4ma = StrategySlotConfig(
        slot_id="fx_eur_4ma",
        strategy_name="4 Moving Averages",
        strategy_params={
            "ma1_period": 5,
            "ma2_period": 10,
            "ma3_period": 20,
            "ma4_period": 50,
            "use_ema": True,
        },
        bar_type_str=BAR_TYPE,
        lots=0.01,
        exit_config=ExitConfig(**common),
        enabled=True,
    )

    return PortfolioConfig(
        name=f"ORDER_EVENT_CAPTURE_EURUSD_{TIMEFRAME_LABEL}_{START_DATE}_{END_DATE}",
        starting_capital=100_000.0,
        start_date=START_DATE,
        end_date=END_DATE,
        pf_sl_enabled=False,
        pf_tgt_enabled=False,
        slots=[slot_ema, slot_4ma],
    )


def _ns_to_iso(ns: int) -> str:
    return pd.Timestamp(ns, unit="ns", tz="UTC").isoformat()


def _safe_attr(obj, name):
    v = getattr(obj, name, None)
    return None if v is None else v


def run_engine_and_capture(portfolio: PortfolioConfig) -> tuple[list[dict], list]:
    """Build a HEDGING engine with both slots, run it, return (events, primary_bars)."""
    catalog = ParquetDataCatalog(CATALOG_PATH)
    instrument_map = {inst.id: inst for inst in catalog.instruments()}

    primary_bt = BarType.from_str(BAR_TYPE)
    instrument_id = primary_bt.instrument_id
    instrument = instrument_map.get(instrument_id)
    if instrument is None:
        raise ValueError(f"No instrument {instrument_id} in catalog at {CATALOG_PATH}")

    bar_type_strs = [BAR_TYPE] + _pair_bid_ask_bar_type(BAR_TYPE)
    primary_bars: list | None = None
    all_bars: list = []
    for bt_str in bar_type_strs:
        try:
            cached = _cached_catalog_bars(CATALOG_PATH, bt_str, START_DATE, END_DATE)
        except Exception:
            cached = []
        if bt_str == BAR_TYPE:
            primary_bars = list(cached)
        all_bars.extend(cached)

    if not primary_bars:
        raise ValueError(
            f"No bars loaded for {BAR_TYPE} in {START_DATE}..{END_DATE}"
        )

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("CAPTURE-001"),
            logging=LoggingConfig(bypass_logging=True),
            risk_engine=RiskEngineConfig(bypass=True),
            run_analysis=False,
        )
    )
    engine.add_venue(
        venue=instrument_id.venue,
        oms_type=OmsType.HEDGING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(float(portfolio.starting_capital), USD)],
        base_currency=USD,
        default_leverage=Decimal(1),
    )
    engine.add_instrument(instrument)
    engine.add_data(all_bars)

    for i, slot in enumerate(portfolio.slots):
        order_tag = f"CAP-{i:03d}"
        slot_qty = effective_slot_qty(slot, None)
        managed_cfg = config_from_exit(
            exit_config=slot.exit_config,
            signal_name=slot.strategy_name,
            signal_params=slot.strategy_params,
            instrument_id=instrument_id,
            bar_type=primary_bt,
            trade_size=slot_qty,
            order_id_tag=order_tag,
        )
        engine.add_strategy(ManagedExitStrategy(managed_cfg))

    try:
        engine.run()
        events = _collect_order_events(engine)
    finally:
        try:
            engine.dispose()
        except BaseException:
            pass

    return events, primary_bars


def _collect_order_events(engine) -> list[dict]:
    """Walk every order in the cache and emit one row per event in its history."""
    rows: list[dict] = []
    for order in engine.kernel.cache.orders():
        order_side = str(order.side)
        order_qty = float(order.quantity)
        try:
            filled_qty = float(order.filled_qty)
        except Exception:
            filled_qty = 0.0
        try:
            avg_px = float(order.avg_px) if order.avg_px is not None else None
        except Exception:
            avg_px = None
        tags = ";".join(map(str, order.tags)) if order.tags else ""
        strategy_id = str(order.strategy_id)

        for ev in order.events:
            last_px_raw = _safe_attr(ev, "last_px")
            try:
                last_px = float(last_px_raw) if last_px_raw is not None else None
            except Exception:
                last_px = None
            rows.append(
                {
                    "ts_event_ns": int(ev.ts_event),
                    "ts_event_iso": _ns_to_iso(ev.ts_event),
                    "event_type": type(ev).__name__,
                    "strategy_id": strategy_id,
                    "client_order_id": str(_safe_attr(ev, "client_order_id") or ""),
                    "venue_order_id": str(_safe_attr(ev, "venue_order_id") or ""),
                    "order_side": order_side,
                    "quantity": order_qty,
                    "filled_qty": filled_qty,
                    "avg_px": avg_px,
                    "last_px": last_px,
                    "tags": tags,
                }
            )
    return rows


def _bars_to_dataframe(bars) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "bar_ts_init_ns": [int(b.ts_init) for b in bars],
            "bar_ts_event_ns": [int(b.ts_event) for b in bars],
            "bar_open": [float(b.open) for b in bars],
            "bar_high": [float(b.high) for b in bars],
            "bar_low": [float(b.low) for b in bars],
            "bar_close": [float(b.close) for b in bars],
            "bar_volume": [float(b.volume) for b in bars],
        }
    )


def join_events_with_bars(events: list[dict], bars: list) -> pd.DataFrame:
    if not events:
        return pd.DataFrame()
    ev_df = pd.DataFrame(events).sort_values("ts_event_ns").reset_index(drop=True)
    bars_df = _bars_to_dataframe(bars).sort_values("bar_ts_event_ns").reset_index(drop=True)

    joined = pd.merge_asof(
        ev_df,
        bars_df,
        left_on="ts_event_ns",
        right_on="bar_ts_event_ns",
        direction="backward",
    )
    joined["bar_ts_open_iso"] = pd.to_datetime(
        joined["bar_ts_init_ns"], unit="ns", utc=True
    ).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    joined["bar_ts_close_iso"] = pd.to_datetime(
        joined["bar_ts_event_ns"], unit="ns", utc=True
    ).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return joined.drop(columns=["bar_ts_init_ns", "bar_ts_event_ns"], errors="ignore")


def main() -> int:
    t_start = time.time()
    print(
        f"[1/4] Building portfolio (2 slots, {BAR_TYPE}, "
        f"{START_DATE} -> {END_DATE}, SL=0.2% TP=1.0%)"
    )
    portfolio = build_portfolio()

    print(
        f"[2/4] Running BacktestEngine (HEDGING, {len(portfolio.slots)} strategies)"
    )
    events, bars = run_engine_and_capture(portfolio)
    print(f"      captured {len(events)} order events over {len(bars)} bars")

    print("[3/4] Joining events with active 15-minute bar (merge_asof, backward)")
    df = join_events_with_bars(events, bars)

    print("[4/4] Writing CSV")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = (
        OUTPUT_DIR
        / f"ORDER_TIMESTAMP_{TIMEFRAME_LABEL}_{START_DATE}_{END_DATE}.csv"
    )
    df.to_csv(out_path, index=False)

    print()
    print(f"Wrote {len(df)} rows to {out_path}")
    by_type = Counter(e["event_type"] for e in events)
    if by_type:
        print("Event type breakdown:")
        for k, v in sorted(by_type.items(), key=lambda kv: -kv[1]):
            print(f"  {k:<28} {v}")
    print(f"Elapsed: {time.time() - t_start:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())