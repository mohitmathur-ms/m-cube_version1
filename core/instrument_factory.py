"""
Dynamically create NautilusTrader CurrencyPair instruments for crypto pairs.

Uses the same pattern as TestInstrumentProvider but allows any crypto/USD pair.
"""

from __future__ import annotations

import json
from decimal import Decimal
from functools import lru_cache
from pathlib import Path

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from core.venue_config import (
    load_adapter_config_for_bar_type,
    load_instrument_config,
    symbol_from_bar_type,
    venue_from_bar_type,
)

_DATA_FORMATS_DIR = Path(__file__).resolve().parent.parent / "adapter_admin" / "data_formats"


VENUE = Venue("BINANCE")

# Price precision defaults per QUOTE currency. This is a SAFETY NET used
# only when:
#   (a) the per-asset-class JSON config did not specify price_precision, AND
#   (b) the BASE currency has no explicit entry in BASE_PRICE_PRECISION below.
# Under-specifying precision here is silent and catastrophic: FX minute bars
# stored at precision=2 (1.08 → 1.08) make every bar a doji because open
# equals close. Always pick the precision that preserves the smallest real
# tick you expect in the source data. When adding a new quote currency,
# set it to match the data source, and prefer over-specifying (extra bytes
# in parquet) over under-specifying (lost price ticks).
PRICE_PRECISION = {
    "USD": 5,   # FX pairs /USD trade at 5 decimals (pip = 0.0001)
    "EUR": 5,   # FX pairs /EUR
    "GBP": 5,   # FX pairs /GBP
    "JPY": 3,   # FX /JPY: base is ~100-200, so pip scale = 0.01 → 3 decimals
    "USDT": 8,  # Crypto majors /USDT: price >> 1, 0.01 tick is fine
}

# Currency (Money) precision overrides for fiat currencies.  Nautilus defaults
# are 2 for USD/EUR/GBP (cents) which truncates sub-cent PnL to zero when
# trade_size is small.  Match PRICE_PRECISION so Money objects keep full
# resolution.
CURRENCY_PRECISION = {
    "USD": 5,
    "EUR": 5,
    "GBP": 5,
    "JPY": 3,
}

# ISO-4217 numeric codes for fiat currencies we override above.
_ISO4217 = {"USD": 840, "EUR": 978, "GBP": 826, "JPY": 392}

# Price precision overrides for specific base currencies (high-priced assets)
BASE_PRICE_PRECISION = {
    "BTC": 2,
    "ETH": 2,
    "SOL": 4,
    "XRP": 4,
    "DOGE": 6,
    "ADA": 4,
    "AVAX": 4,
    "LINK": 4,
    "DOT": 4,
    "MATIC": 6,
}


def _get_currency(code: str) -> Currency:
    """Get or create a Currency object by code.

    For known fiat currencies (USD, EUR, GBP, JPY) we override the default
    Nautilus precision (2 for USD = cents) with the value from
    CURRENCY_PRECISION so that Money objects preserve sub-cent PnL values.
    """
    upper = code.upper()
    if upper in CURRENCY_PRECISION:
        return Currency(
            code=upper,
            precision=CURRENCY_PRECISION[upper],
            iso4217=_ISO4217.get(upper, 0),
            name=upper,
            currency_type=2,  # CurrencyType.FIAT
        )
    try:
        return Currency.from_str(code)
    except Exception:
        # For unknown currencies, create a new crypto currency
        # precision=8 is standard for crypto
        return Currency(
            code=code,
            precision=8,
            iso4217=0,
            name=code,
            currency_type=1,  # CurrencyType.CRYPTO
        )


def create_instrument(
    base: str,
    quote: str,
    venue: str = "BINANCE",
    price_precision: int | None = None,
    size_precision: int | None = None,
) -> CurrencyPair:
    """
    Create a CurrencyPair instrument for a crypto or FX pair.

    Parameters
    ----------
    base : str
        Base currency code, e.g. "BTC".
    quote : str
        Quote currency code, e.g. "USD".
    venue : str, default "BINANCE"
        Venue name.
    price_precision : int, optional
        Override price precision. If None, looks up from BASE_PRICE_PRECISION /
        PRICE_PRECISION tables (default precision=2 for unknown pairs).
    size_precision : int, optional
        Override size precision. Defaults to 0 for safety against QUANTITY_MAX overflow.

    Returns
    -------
    CurrencyPair
    """
    base = base.upper()
    quote = quote.upper()
    venue_obj = Venue(venue)

    symbol_str = f"{base}{quote}"
    base_currency = _get_currency(base)
    quote_currency = _get_currency(quote)

    # Determine price precision: explicit override wins, otherwise use table defaults
    if price_precision is not None:
        price_prec = price_precision
    else:
        price_prec = BASE_PRICE_PRECISION.get(base, PRICE_PRECISION.get(quote, 2))
    # Size precision controls the number of decimal places an order/volume
    # quantity may carry. It does NOT change the maximum representable value:

    # Nautilus stores quantities as fixed-point with FIXED_PRECISION=9, so the
    # cap is QUANTITY_MAX (~18.4B) regardless of size_precision. Bar volumes are
    # already clipped to QUANTITY_MAX at CSV-load time (see core/csv_loader.py),

    # so raising precision is safe. We default to 0 (whole units) only when no
    # asset-class override is supplied; crypto sets a non-zero value so
    # fractional lot sizes (e.g. 0.5, 0.001) are tradeable.

    size_prec = size_precision if size_precision is not None else 0

    # Smallest tradeable step / minimum order, derived from precision so that a

    # precision>0 instrument can actually trade fractional sizes. precision 0

    # → step 1 (unchanged for FX/index); precision 2 → 0.01; precision 8 →

    # 0.00000001.
    
    size_step = Quantity(10 ** (-size_prec), precision=size_prec)

    # Per-symbol admin config (lot_size + trade_size cap). Lives in
    # adapter_admin/adapters_config/<venue>.json under the "instruments" key.
    # Missing config → lot_size unset (None) and the default global cap below.
    inst_cfg = load_instrument_config(symbol_str, venue) or {}
    lot_size_cfg = inst_cfg.get("lot_size")
    lot_size_quantity = (
        Quantity(int(lot_size_cfg), precision=size_prec) if lot_size_cfg else None
    )
    cap_cfg = inst_cfg.get("trade_size")
    max_qty_value = int(cap_cfg) if cap_cfg else 9_999_999_999

    return CurrencyPair(
        instrument_id=InstrumentId(
            symbol=Symbol(symbol_str),
            venue=venue_obj,
        ),
        raw_symbol=Symbol(symbol_str),
        base_currency=base_currency,
        quote_currency=quote_currency,
        price_precision=price_prec,
        size_precision=size_prec,
        price_increment=Price(10 ** (-price_prec), precision=price_prec),
        size_increment=size_step,
        lot_size=lot_size_quantity,
        max_quantity=Quantity(max_qty_value, precision=size_prec),
        min_quantity=size_step,
        max_notional=None,
        min_notional=Money(1.00, quote_currency),
        max_price=Price(10_000_000, precision=price_prec),
        min_price=Price(10 ** (-price_prec), precision=price_prec),
        margin_init=Decimal("1.00"),
        margin_maint=Decimal("0.35"),
        maker_fee=Decimal("0.001"),
        taker_fee=Decimal("0.001"),
        ts_event=0,
        ts_init=0,
    )


def _data_format_instrument_cfg(bar_type_str: str) -> dict:
    """Return the ``instrument`` block from the asset class's data-format file.

    Resolves the venue's adapter config → ``asset_class`` →
    ``adapter_admin/data_formats/<asset_class>.json`` and returns its
    ``instrument`` sub-dict (``price_precision``, ``size_precision``,
    ``quote_currency``, ``base_currency_length``, ...). Returns ``{}`` when
    anything along the path is missing — callers then fall back to the
    built-in precision tables in ``create_instrument``.
    """
    adapter_cfg = load_adapter_config_for_bar_type(bar_type_str) or {}
    asset_class = (adapter_cfg.get("asset_class") or "").strip().lower().replace(" ", "_")
    if not asset_class:
        return {}
    fmt_path = _DATA_FORMATS_DIR / f"{asset_class}.json"
    if not fmt_path.exists():
        return {}
    try:
        fmt = json.loads(fmt_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    inst = fmt.get("instrument")
    return inst if isinstance(inst, dict) else {}


_CATALOG_PATH = Path(__file__).resolve().parent.parent / "catalog"


def _catalog_instrument(bar_type_str: str):
    """Return the instrument the backtest engine would actually load for this
    bar type from the ParquetDataCatalog, or ``None`` if it isn't ingested.

    This is the authoritative source — ``core/backtest_runner.py`` resolves the
    instrument exactly this way (``catalog.instruments()`` matched on
    ``BarType.from_str(...).instrument_id``) and calls ``make_qty()`` on it.
    Reading it here means precision validation can never disagree with what the
    backtest will do (and it sidesteps venue-name mismatches between bar types
    and adapter configs, e.g. ``COINBASE`` vs ``COINBASE_MS``).
    """
    try:
        from nautilus_trader.model.data import BarType
        from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

        inst_id = BarType.from_str(bar_type_str).instrument_id
        catalog = ParquetDataCatalog(str(_CATALOG_PATH))
        for inst in catalog.instruments():
            if inst.id == inst_id:
                return inst
    except Exception:
        return None
    return None


@lru_cache(maxsize=256)
def instrument_for_bar_type(bar_type_str: str):
    """Return the instrument used to validate a leg's lots/SL/TGT precision.

    Prefers the **actual catalog instrument** (what the engine loads and calls
    ``make_qty()`` on); only when the symbol hasn't been ingested yet does it
    fall back to building one from the asset class's ``data_formats`` precision,
    mirroring ``core/nautilus_loader.py``. Returns ``None`` when neither is
    available / the bar type can't be parsed.

    NOTE: because the catalog instrument is authoritative, a change to
    ``data_formats`` size/price precision only takes effect after the data is
    **re-ingested** (which rewrites the catalog instrument).
    """
    inst = _catalog_instrument(bar_type_str)
    if inst is not None:
        return inst

    symbol = symbol_from_bar_type(bar_type_str)
    venue = venue_from_bar_type(bar_type_str)
    if not symbol or not venue:
        return None

    inst_cfg = _data_format_instrument_cfg(bar_type_str)

    # Split base/quote the same way nautilus_loader does: FX uses an explicit
    # base_currency_length; otherwise strip the configured quote suffix so the
    # base-currency precision fallback (BASE_PRICE_PRECISION) still works for
    # variable-length crypto symbols like "ADAUSD" -> base "ADA", quote "USD".
    quote = (inst_cfg.get("quote_currency") or "USD").upper()
    base = symbol.upper()
    base_len = inst_cfg.get("base_currency_length")
    if base_len and len(base) > base_len:
        quote = base[base_len:]
        base = base[:base_len]
    elif quote and base.endswith(quote) and len(base) > len(quote):
        base = base[: -len(quote)]

    try:
        return create_instrument(
            base,
            quote,
            venue,
            price_precision=inst_cfg.get("price_precision"),
            size_precision=inst_cfg.get("size_precision"),
        )
    except Exception:
        return None
