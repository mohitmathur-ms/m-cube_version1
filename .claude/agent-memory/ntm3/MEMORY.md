- [Docs library empty](project_docs_library_empty.md) — ntm3 PDFs not present; fall back to nautilus_trader package source (.pyx/.pxd)

- [PDF rendering unavailable](tooling-pdf-no-poppler.md) — Read tool cannot open the ntm3_docs PDFs (no poppler/pdftoppm); fall back to package source.
- [Bracket orders & contingency](bracket-orders-mcube.md) — order_factory.bracket() defaults OUO; m-cube does NOT use native brackets (manual market-order exit engine).
- [OMS types & position IDs](oms-types-position-ids.md) — NETTING/HEDGING/UNSPECIFIED resolution (strategy override -> venue -> NETTING default) and how each assigns PositionIds.
- [MARKET vs LIMIT order types](order-types-market-vs-limit.md) — fill/TIF/post_only/MARKET_TO_LIMIT/trigger mechanics; m-cube exit engine submits only MARKET/GTC.
