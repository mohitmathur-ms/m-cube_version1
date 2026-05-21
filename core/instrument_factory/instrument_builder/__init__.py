"""CurrencyPair assembly.

Composes the currency, price/size-precision, and lot-size components into a
fully-specified NautilusTrader ``CurrencyPair`` instrument for a crypto or FX
pair.
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from core.instrument_factory.currency import get_currency
from core.instrument_factory.lot_size import resolve_lot_size
from core.instrument_factory.price_size_precision import (
    resolve_price_precision,
    resolve_size_precision,
)

VENUE = Venue("BINANCE")


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
    base_currency = get_currency(base)
    quote_currency = get_currency(quote)

    # Determine price precision: explicit override wins, otherwise use table defaults
    price_prec = resolve_price_precision(base, quote, price_precision)
    size_prec = resolve_size_precision(size_precision)

    # Smallest tradeable step / minimum order, derived from precision so that a
    # precision>0 instrument can actually trade fractional sizes. precision 0
    # → step 1 (unchanged for FX/index); precision 2 → 0.01; precision 8 →
    # 0.00000001.
    size_step = Quantity(10 ** (-size_prec), precision=size_prec)

    # Per-symbol admin config (lot_size + trade_size cap) from
    # adapter_admin/adapters_config/<venue>.json.
    lot_size_quantity, max_qty_value = resolve_lot_size(symbol_str, venue, size_prec)

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
