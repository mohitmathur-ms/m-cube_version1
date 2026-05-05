"""
Dynamically create NautilusTrader CurrencyPair instruments for crypto pairs.

Uses the same pattern as TestInstrumentProvider but allows any crypto/USD pair.
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from core.venue_config import load_instrument_config


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
    "USDT": 2,  # Crypto majors /USDT: price >> 1, 0.01 tick is fine
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
            currency_type=1,  # CurrencyType.FIAT
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
            currency_type=2,  # CurrencyType.CRYPTO
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
    # Size precision must be low enough that daily volume fits within QUANTITY_MAX (~18.4B).
    # Yahoo Finance volumes for BTC can be 20B+, so we use precision=0 for safety.
    size_prec = size_precision if size_precision is not None else 0

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
        size_increment=Quantity(1, precision=size_prec),
        lot_size=lot_size_quantity,
        max_quantity=Quantity(max_qty_value, precision=size_prec),
        min_quantity=Quantity(1, precision=size_prec),
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
