# Fix Report — Decimal lot sizes for cryptocurrency

**Date:** 2026-05-21
**Asset class:** cryptocurrency (all crypto venues)
**Goal:** Allow fractional/decimal lot sizes (e.g. `0.5`, `0.1`, `0.001` ADA) for crypto,
which previously failed with *"quantity … was rounded to zero due to size increment 1
and size precision 0"* and could not be saved.

---

## Problem

Crypto instruments were built with `size_precision = 0`, and — regardless of precision —
the instrument's `size_increment` and `min_quantity` were **hardcoded to `1`**. So the
smallest tradeable crypto size was 1 whole coin; any fractional lots rounded to zero and
the order (and the whole backtest) failed.

Two things blocked decimal sizing:

1. **`adapter_admin/data_formats/cryptocurrency.json`** had `"size_precision": 0` (whole units only).
2. **`core/instrument_factory.py`** built `size_increment=Quantity(1, …)` and
   `min_quantity=Quantity(1, …)` for *every* precision — so even raising `size_precision`
   alone would not have permitted fractional steps.

A third subtlety: the backtest engine loads the instrument **from the ParquetDataCatalog**
(persisted at data-ingest time), not freshly from `create_instrument`. So a config change
only takes effect once the data is **re-ingested**.

---

## Changes

| File | Change |
|------|--------|
| [core/instrument_factory.py](../core/instrument_factory.py) | `size_increment` and `min_quantity` are now derived from `size_precision` (`size_step = 10**-size_precision`): precision 0 → step `1` (FX/index unchanged), precision 8 → step `0.00000001`. Updated the outdated "volume overflow" comment — `QUANTITY_MAX` (~18.4B) is independent of precision (internal `FIXED_PRECISION=9`), so raising precision is safe. |
| [adapter_admin/data_formats/cryptocurrency.json](../adapter_admin/data_formats/cryptocurrency.json) | `size_precision`: `0 → 8` (full crypto granularity; price precision unchanged at 4). Tunable later via the Adapter Admin panel. |
| [core/instrument_factory.py](../core/instrument_factory.py) | `instrument_for_bar_type()` (used by the save/validation checks) now reads the **actual catalog instrument** first, falling back to building from `data_formats` only when the symbol isn't ingested yet. Validation can no longer disagree with what the engine runs, and it sidesteps the `COINBASE` vs `COINBASE_MS` venue-name mismatch. |
| [core/nautilus_loader.py](../core/nautilus_loader.py) | `save_to_catalog()` now **replaces** a stored instrument when its `size_precision`/`price_precision` differs from the freshly-built one (each instrument lives in its own `currency_pair/<id>/` folder, so the replace is isolated). This makes a re-ingest actually apply the new precision instead of silently keeping the old definition. |

Order sizing depends on the **instrument's** precision, not the bars' volume precision — so
refreshing the instrument definition is sufficient; bar data does not need re-wrangling.

---

## How it behaves now

- **Crypto (size_precision 8):** `size_increment = min_quantity = 0.00000001`; `make_qty(0.5)`,
  `make_qty(0.001)`, `make_qty(1)` all succeed. Up to 8 decimal places allowed.
- **FX / index (size_precision 0):** `size_increment = min_quantity = 1` — **unchanged**.
- **Commodity (size_precision 2):** `size_increment = min_quantity = 0.01` (was `1`). More
  permissive but backward-compatible (whole numbers still valid); this also makes the
  strict save-validation and the runtime instrument consistent for commodity.

The save/live validation (strict precision conformance, added previously) reads the
instrument and therefore automatically allows whatever the instrument's precision permits:
after the change + re-ingest, crypto `0.5`/`0.001` validate and save successfully.

---

## ⚠️ Required action to apply to EXISTING crypto data

Because the engine uses the catalog instrument, you must **reload the crypto data** once:

1. Restart the backend (`python server.py`) and hard-refresh the browser (Ctrl+F5) so the
   new code and `portfolio.js` are loaded.
2. **Re-ingest each crypto symbol** via Load Data. On re-ingest, `save_to_catalog` detects the
   precision change and replaces the stored instrument with the `size_precision = 8` version.
3. After that, decimal lot sizes are accepted in the editor, on save, and at backtest time.

New crypto symbols (not yet in the catalog) work immediately — no extra step.

> Until a symbol is re-ingested, its catalog instrument is still `size_precision 0`, and the
> validator correctly continues to reject fractional lots for it (validation mirrors reality).

---

## Verification performed

- `create_instrument(size_precision=8)` → `size_increment=min_quantity=0.00000001`; `make_qty`
  accepts `0.5`, `0.001`, `1`. FX (precision 0) unchanged; commodity (precision 2) → `0.01` step.
- Re-ingest simulation in a temp catalog: instrument stored at precision 0, re-ingested at
  precision 8 → instrument **replaced**, `make_qty(0.5)`/`make_qty(0.001)` succeed.
- `instrument_for_bar_type()` reads the catalog instrument (verified ADAUSD.COINBASE returns
  its stored precision).
- Regression: `pytest tests/ --ignore=tests/test_aggregator.py` → 189 passed (the 7 failures
  are pre-existing, unrelated); `verify_session_changes.py` → same 6 pre-existing failures.
  Confirmed both are identical with these changes stashed.

---

## Tuning the precision later

`size_precision` for crypto can be changed any time in the Adapter Admin panel (port 5001,
Data Formats → Cryptocurrency) or directly in
[adapter_admin/data_formats/cryptocurrency.json](../adapter_admin/data_formats/cryptocurrency.json).
Re-ingest the affected symbols afterward for the change to take effect.
