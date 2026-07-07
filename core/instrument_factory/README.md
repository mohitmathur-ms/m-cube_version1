# `core/instrument_factory`

Builds NautilusTrader `CurrencyPair` instruments for crypto and FX pairs, and
resolves the instrument used to validate a portfolio leg's lots/SL/TGT
precision. This package was split out of the former monolithic
`core/instrument_factory.py`; each subdirectory owns one logical concern, with
that concern's logic living directly in the directory's `__init__.py`.

## Public API (unchanged after the split)

```python
from core.instrument_factory import create_instrument, instrument_for_bar_type
```

- `create_instrument(base, quote, venue="BINANCE", price_precision=None, size_precision=None) -> CurrencyPair`
  — used by `core/nautilus_loader.py`, `scripts/aggregate_catalog.py`,
  `scripts/ingest_fx_bulk.py`.
- `instrument_for_bar_type(bar_type_str) -> CurrencyPair | None` (`@lru_cache`)
  — used by `server.py` for leg precision validation.

The precision tables (`PRICE_PRECISION`, `BASE_PRICE_PRECISION`,
`CURRENCY_PRECISION`) and the `VENUE` constant are re-exported from the package
root for backward compatibility.

## Components

| Directory | Responsibility | Key symbols |
|---|---|---|
| `currency/` | Build `Currency` objects, overriding fiat precision so `Money` keeps sub-cent PnL. | `get_currency`, `CURRENCY_PRECISION` |
| `price_size_precision/` | Built-in precision safety-net tables + resolution of explicit override → base → quote → default. | `resolve_price_precision`, `resolve_size_precision`, `PRICE_PRECISION`, `BASE_PRICE_PRECISION` |
| `lot_size/` | Resolve `lot_size` + `trade_size` cap from `adapter_admin/adapters_config/<venue>.json` (via `core.venue_config.load_instrument_config`). | `resolve_lot_size` |
| `data_format_config/` | Read the `instrument` block from `adapter_admin/data_formats/<asset_class>.json`. | `data_format_instrument_cfg` |
| `catalog_lookup/` | Return the authoritative instrument the engine would load from the `ParquetDataCatalog`. | `catalog_instrument` |
| `instrument_builder/` | Assemble a fully-specified `CurrencyPair` by composing the four components above. | `create_instrument`, `VENUE` |
| `bar_type_resolver/` | Public entry point: prefer the catalog instrument; otherwise build from `data_formats` precision. | `instrument_for_bar_type` |

## Dependency graph (acyclic)

```
currency ─────────────┐
price_size_precision ─┼─► instrument_builder ─┐
lot_size ─────────────┘                       │
                                              ├─► bar_type_resolver ─► __init__ (public API)
data_format_config ───────────────────────────┤
catalog_lookup ────────────────────────────────┘
```

## Config sources (important)

- **Price/size precision, quote_currency, base_currency_length** come from
  `adapter_admin/data_formats/<asset_class>.json` (`instrument` block).
- **`lot_size` and the `trade_size` cap** come from
  `adapter_admin/adapters_config/<venue>.json` (`instruments` block) — **not**
  from `data_formats/`.
- The **catalog instrument is authoritative**: a change to `data_formats`
  precision only takes effect after the data is re-ingested (which rewrites the
  catalog instrument).
