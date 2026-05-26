---
name: bracket-orders-mcube
description: NautilusTrader 1.224.0 order_factory.bracket() signature/contingency facts and the fact that m-cube does NOT use native bracket orders
metadata:
  type: project
---

NautilusTrader 1.224.0 `OrderFactory.bracket()` lives in
`venv/Lib/site-packages/nautilus_trader/common/factories.pyx` (def at line ~1193).
Returns an `OrderList`. Key version facts:
- `contingency_type` default = `ContingencyType.OUO` (One-Updates-Other) for the TP/SL pair.
- The entry order is created with `ContingencyType.OTO` and `linked_order_ids=[sl, tp]`
  (entry triggers the two children).
- SL is always a STOP order; `sl_order_type` default `STOP_MARKET` (or `TRAILING_STOP_MARKET`).
- `tp_order_type` default `LIMIT`; `tp_post_only` default True in signature.
- Enum (`nautilus_trader/model/enums.py`): NO_CONTINGENCY=0, OCO=1, OTO=2, OUO=3.
  OCO = fully cancel sibling on fill; OUO = reduce/update sibling qty on partial fill.

**m-cube does NOT use native bracket orders.** `core/managed_strategy.py` runs its own
exit state machine in `on_bar` and submits plain `self.order_factory.market(...)` +
`self.submit_order(order)` (around line 1752-1766) for both entries and exits. SL/TP/
trailing are evaluated in Python per-bar, not delegated to a venue-side OCO/OUO bracket.

**Why:** the m-cube exit engine needs sl_wait_bars confirmation, target-lock, re_execute/
reverse actions, and squareoff precedence (leg>slot>portfolio) that a static venue bracket
cannot express.

**How to apply:** If asked to add SL/TP, extend the L2 manual engine, not bracket().
Only 3 repo files even mention ContingencyType (test_perf_regression, report_generator,
a custom_strategies practice file) — none wire native brackets into the runner.
