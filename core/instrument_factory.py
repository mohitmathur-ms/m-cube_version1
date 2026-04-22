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


VENUE = Venue("BINANCE")

# Price precision defaults per quote currency
PRICE_PRECISION = {
    "USD": 2,
    "USDT": 2,
    "EUR": 2,
    "GBP": 2,
}

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
    """Get or create a Currency object by code."""
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
        lot_size=None,
        max_quantity=Quantity(9_999_999_999, precision=size_prec),
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


def create_instrument_from_symbol(symbol: str, venue: str = "CRYPTO") -> CurrencyPair:
    """
    Create a CurrencyPair from a symbol string like "BTC/USD".

    Parameters
    ----------
    symbol : str
        Symbol string, e.g. "BTC/USD".
    venue : str, default "BINANCE"
        Venue name.

    Returns
    -------
    CurrencyPair
    """
    parts = symbol.strip().upper().replace("-", "/").split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid symbol format: '{symbol}'. Expected 'BTC/USD'.")
    return create_instrument(parts[0], parts[1], venue)
