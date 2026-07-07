"""Currency-object resolution for instrument construction.

Builds NautilusTrader ``Currency`` objects, overriding the default precision
for known fiat currencies so that ``Money`` objects preserve sub-cent PnL.
"""

from __future__ import annotations

from nautilus_trader.model.objects import Currency

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


def get_currency(code: str) -> Currency:
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