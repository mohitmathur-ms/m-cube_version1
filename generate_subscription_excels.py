"""
Generate Excel reports comparing:
  1. What the Strategy subscribes to (via `subscribe_bars()` in on_start)
  2. What the Nautilus Data Engine actually loads (a superset: primary
     bar_type + auto-paired ASK/BID + instrument + venue config)

Outputs three workbooks to ./excels/:
  - strategy_subscriptions.xlsx
  - data_engine_inputs.xlsx
  - summary_report.xlsx

This is a static trace — no backtest is executed.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pandas as pd

from nautilus_trader.model.data import BarType

from core.backtest_runner import _pair_bid_ask_bar_type
from core.venue_config import load_adapter_config_for_bar_type
from strategies import STRATEGY_REGISTRY


# ── Scenario inputs (edit to switch strategy / timeframe / instrument) ─────
STRATEGY_NAME = "EMA Cross"
BAR_TYPE_STR = "EURUSD.FOREX_MS-15-MINUTE-MID-EXTERNAL"
START_DATE = "2024-04-01"
END_DATE = "2024-04-30"
TRADE_SIZE = 0.01
EXTRA_BAR_TYPES: list[str] = []  # e.g. ["EURUSD.FOREX_MS-1-HOUR-MID-EXTERNAL"]


PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "excels"


def parse_bar_type_components(bt_str: str) -> dict:
    """Split '<SYMBOL>.<VENUE>-<STEP>-<AGG>-<PRICE>-<SOURCE>' into parts."""
    parts = bt_str.split("-")
    inst_part = parts[0] if parts else ""
    if "." in inst_part:
        symbol, venue = inst_part.split(".", 1)
    else:
        symbol, venue = inst_part, ""
    step = parts[1] if len(parts) > 1 else ""
    agg = parts[2] if len(parts) > 2 else ""
    price = parts[3] if len(parts) > 3 else ""
    source = parts[4] if len(parts) > 4 else ""
    return {
        "instrument": inst_part,
        "symbol": symbol,
        "venue": venue,
        "step": step,
        "aggregation": agg,
        "price_type": price,
        "source": source,
    }


def build_strategy_config():
    """Build the strategy config exactly the way run_backtest does."""
    registry_entry = STRATEGY_REGISTRY[STRATEGY_NAME]
    config_class = registry_entry["config_class"]
    primary_bar_type = BarType.from_str(BAR_TYPE_STR)
    instrument_id = primary_bar_type.instrument_id

    config_kwargs = {
        "instrument_id": instrument_id,
        "bar_type": primary_bar_type,
        "trade_size": Decimal(str(TRADE_SIZE)),
    }
    if EXTRA_BAR_TYPES and "extra_bar_types" in getattr(
        config_class, "__struct_fields__", ()
    ):
        config_kwargs["extra_bar_types"] = [
            BarType.from_str(bt) for bt in EXTRA_BAR_TYPES
        ]
    return config_class(**config_kwargs)


def build_strategy_subscription_rows(config) -> list[dict]:
    """Mirror what ema_cross.py on_start does: primary bar_type + extras."""
    rows: list[dict] = []
    primary_str = str(config.bar_type)
    comp = parse_bar_type_components(primary_str)
    rows.append({
        "Subscription Role": "primary",
        "Bar Type String": primary_str,
        "Instrument": comp["instrument"],
        "Venue": comp["venue"],
        "Step": comp["step"],
        "Aggregation": comp["aggregation"],
        "Price Type": comp["price_type"],
        "Source": comp["source"],
        "Subscribed Via": "self.subscribe_bars(self.config.bar_type)",
    })
    for bt in getattr(config, "extra_bar_types", None) or []:
        bt_str = str(bt)
        comp = parse_bar_type_components(bt_str)
        rows.append({
            "Subscription Role": "extra",
            "Bar Type String": bt_str,
            "Instrument": comp["instrument"],
            "Venue": comp["venue"],
            "Step": comp["step"],
            "Aggregation": comp["aggregation"],
            "Price Type": comp["price_type"],
            "Source": comp["source"],
            "Subscribed Via": "self.subscribe_bars(bt) for bt in extra_bar_types",
        })
    return rows


def build_engine_bar_rows(config) -> list[dict]:
    """Mirror backtest_runner: primary + its pair(s) + extras + their pair(s)."""
    rows: list[dict] = []
    seen: set[str] = set()

    def emit(bt_str: str, subscribed: bool, why: str) -> None:
        if bt_str in seen:
            return
        seen.add(bt_str)
        comp = parse_bar_type_components(bt_str)
        rows.append({
            "Bar Type String": bt_str,
            "Instrument": comp["instrument"],
            "Step": comp["step"],
            "Aggregation": comp["aggregation"],
            "Price Type": comp["price_type"],
            "Source": comp["source"],
            "Strategy Subscribed?": "Yes" if subscribed else "No",
            "Why Loaded": why,
        })

    primary_str = str(config.bar_type)
    emit(primary_str, True, "Strategy subscription (primary bar_type)")
    for paired in _pair_bid_ask_bar_type(primary_str):
        emit(
            paired,
            False,
            "Auto-paired for fills "
            "(core/backtest_runner.py:_pair_bid_ask_bar_type)",
        )

    for bt in getattr(config, "extra_bar_types", None) or []:
        bt_str = str(bt)
        emit(bt_str, True, "Strategy subscription (extra_bar_types)")
        for paired in _pair_bid_ask_bar_type(bt_str):
            emit(paired, False, "Auto-paired for extra bar_type fills")

    return rows


def build_instrument_rows() -> list[dict]:
    comp = parse_bar_type_components(BAR_TYPE_STR)
    symbol = comp["symbol"]
    base = quote = ""
    if len(symbol) == 6:
        base, quote = symbol[:3], symbol[3:]
    return [{
        "Instrument ID": comp["instrument"],
        "Venue": comp["venue"],
        "Symbol": symbol,
        "Base Currency": base or "(unknown)",
        "Quote Currency": quote or "(unknown)",
        "Loaded From": "catalog.instruments() — backtest_runner.py:1374",
        "Loaded Because": "Matches bar_type's InstrumentId",
    }]


def build_venue_rows() -> list[dict]:
    comp = parse_bar_type_components(BAR_TYPE_STR)
    venue = comp["venue"]
    cfg = load_adapter_config_for_bar_type(BAR_TYPE_STR)
    if cfg:
        return [{
            "Venue": venue,
            "Account Type": cfg.get("account_type", "MARGIN"),
            "Account Base Currency": cfg.get("account_base_currency", "USD"),
            "Config Source": f"adapter_admin/adapters_config/<{venue.lower()}>.json",
            "FX Conversion Defined?": "Yes" if cfg.get("fx_conversion") else "No",
            "Registered Via": "engine.add_venue(...) at backtest_runner.py:1393-1400",
        }]
    return [{
        "Venue": venue,
        "Account Type": "MARGIN (built-in default)",
        "Account Base Currency": "USD (built-in default)",
        "Config Source": "No adapter config found — falling back to defaults",
        "FX Conversion Defined?": "No",
        "Registered Via": "engine.add_venue(...) at backtest_runner.py:1393-1400",
    }]


def build_fx_conversion_rows() -> list[dict]:
    cfg = load_adapter_config_for_bar_type(BAR_TYPE_STR)
    comp = parse_bar_type_components(BAR_TYPE_STR)
    symbol = comp["symbol"]
    base = quote = ""
    if len(symbol) == 6:
        base, quote = symbol[:3], symbol[3:]
    account_base = (cfg or {}).get("account_base_currency", "USD")
    fx_conv = (cfg or {}).get("fx_conversion") or {}

    if not fx_conv:
        matches = (quote == account_base)
        note = (
            f"No fx_conversion rules in adapter config. "
            f"Quote currency '{quote or '?'}' vs account base '{account_base}' — "
            + (
                "matches, no conversion needed."
                if matches
                else "non-base PnL may be silently dropped without a rule."
            )
        )
        return [{"Conversion Pair": "(none)", "Rule": "—", "Note": note}]

    rows = []
    for pair, rule in fx_conv.items():
        rows.append({
            "Conversion Pair": pair,
            "Rule": str(rule),
            "Note": "Loaded lazily by core/fx_rates.py after backtest run",
        })
    return rows


def build_summary_run_config(strategy_rows, engine_bar_rows) -> list[dict]:
    comp = parse_bar_type_components(BAR_TYPE_STR)
    return [
        {"Key": "Strategy", "Value": STRATEGY_NAME},
        {"Key": "Bar Type", "Value": BAR_TYPE_STR},
        {"Key": "Timeframe", "Value": f"{comp['step']}-{comp['aggregation']}"},
        {"Key": "Price Type", "Value": comp["price_type"]},
        {"Key": "Instrument", "Value": comp["instrument"]},
        {"Key": "Venue", "Value": comp["venue"]},
        {"Key": "Date Range", "Value": f"{START_DATE} -> {END_DATE}"},
        {"Key": "Trade Size", "Value": TRADE_SIZE},
        {"Key": "Extra Bar Types",
         "Value": ", ".join(EXTRA_BAR_TYPES) or "(none)"},
        {"Key": "Strategy Subscriptions (count)", "Value": len(strategy_rows)},
        {"Key": "Data Engine Bar Inputs (count)", "Value": len(engine_bar_rows)},
        {"Key": "Difference (count)",
         "Value": len(engine_bar_rows) - len(strategy_rows)},
    ]


def build_side_by_side(strategy_rows, engine_bar_rows) -> list[dict]:
    subscribed_set = {r["Bar Type String"] for r in strategy_rows}
    rows = []
    for r in engine_bar_rows:
        bt = r["Bar Type String"]
        is_sub = bt in subscribed_set
        rows.append({
            "Bar Type": bt,
            "Subscribed by Strategy": "Yes" if is_sub else "No",
            "Loaded by Data Engine": "Yes" if is_sub else "Yes (auto-paired)",
        })
    return rows


WHY_TEXT = """\
The Strategy declares its subscriptions in on_start() at strategies/ema_cross.py:47:
    self.subscribe_bars(self.config.bar_type)
    if self.config.extra_bar_types:
        for bt in self.config.extra_bar_types:
            self.subscribe_bars(bt)

The Data Engine, however, loads a SUPERSET of these bar types. The extras come
from core/backtest_runner.py:_pair_bid_ask_bar_type (lines 103-121), which
auto-adds the opposite quote sides:
  - If MID is requested -> ASK and BID are also loaded
  - If BID is requested -> ASK is also loaded
  - If ASK is requested -> BID is also loaded
  - LAST bars have no pair

WHY: Nautilus's matching engine needs both quote sides to fill FX market orders
at realistic spread prices. Without the opposite side present, fills happen at
the midpoint (for MID) or one-sided (for BID/ASK only), which understates real
spread cost. The Strategy never *sees* these paired bars in on_bar - they're
consumed by the matching engine internally.

Beyond bars, the engine also receives:
  - The instrument definition           (engine.add_instrument at line 1404)
  - The venue config + starting balance (engine.add_venue      at lines 1393-1400)
  - FX rate pairs lazily, after run     (core/fx_rates.py + adapter configs)
    These convert non-base-currency PnL (e.g. JPY from USDJPY trades) back to
    the account base currency. Without them, JPY PnL is silently dropped.
"""


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    config = build_strategy_config()

    strategy_rows = build_strategy_subscription_rows(config)
    engine_bar_rows = build_engine_bar_rows(config)
    instrument_rows = build_instrument_rows()
    venue_rows = build_venue_rows()
    fx_rows = build_fx_conversion_rows()

    out1 = OUTPUT_DIR / "strategy_subscriptions.xlsx"
    with pd.ExcelWriter(out1, engine="openpyxl") as xw:
        pd.DataFrame(strategy_rows).to_excel(
            xw, sheet_name="Subscriptions", index=False
        )

    out2 = OUTPUT_DIR / "data_engine_inputs.xlsx"
    with pd.ExcelWriter(out2, engine="openpyxl") as xw:
        pd.DataFrame(engine_bar_rows).to_excel(xw, sheet_name="Bars", index=False)
        pd.DataFrame(instrument_rows).to_excel(
            xw, sheet_name="Instruments", index=False
        )
        pd.DataFrame(venue_rows).to_excel(xw, sheet_name="Venue", index=False)
        pd.DataFrame(fx_rows).to_excel(
            xw, sheet_name="FX Conversion (Post-Run)", index=False
        )

    out3 = OUTPUT_DIR / "summary_report.xlsx"
    with pd.ExcelWriter(out3, engine="openpyxl") as xw:
        pd.DataFrame(
            build_summary_run_config(strategy_rows, engine_bar_rows)
        ).to_excel(xw, sheet_name="Run Configuration", index=False)
        pd.DataFrame(
            build_side_by_side(strategy_rows, engine_bar_rows)
        ).to_excel(xw, sheet_name="Side-by-Side", index=False)
        pd.DataFrame(
            [{"Explanation": line} for line in WHY_TEXT.splitlines()]
        ).to_excel(xw, sheet_name="Why the Difference", index=False)

    print(f"Wrote 3 workbooks to {OUTPUT_DIR}:")
    print(f"  - {out1.name}")
    print(f"  - {out2.name}")
    print(f"  - {out3.name}")
    print()
    print(f"Strategy subscriptions: {len(strategy_rows)}")
    print(f"Data engine bar inputs: {len(engine_bar_rows)}")


if __name__ == "__main__":
    main()
