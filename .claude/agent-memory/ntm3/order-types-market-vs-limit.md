---
name: order-types-market-vs-limit
description: NautilusTrader 1.224.0 MARKET vs LIMIT order mechanics (fills, TIF sets, post_only, MARKET_TO_LIMIT, emulation triggers) and how m-cube submits entries
metadata:
  type: project
---

Confirmed from `nautilus_trader==1.224.0` package source (PDFs unreadable, see
[[tooling-pdf-no-poppler]]).

**MarketOrder** (`model/orders/market.pyx`): no price protection, high fill
likelihood; TIF set = {GTC, IOC, FOK, DAY, AT_THE_OPEN, AT_THE_CLOSE}. GTD is
rejected (ValueError). MOO = AT_THE_OPEN, MOC = AT_THE_CLOSE. No post_only, no
emulation_trigger param.

**LimitOrder** (`model/orders/limit.pyx`): fills at limit price or better, no
fill guarantee. TIF set adds GTD (GTD requires expire_time_ns > 0). Extra params
the MarketOrder lacks: `post_only` (maker-only; default False), `display_qty`
(iceberg), `emulation_trigger` (TriggerType for local OrderEmulator),
`trigger_instrument_id`.

**MarketToLimitOrder** (`model/orders/market_to_limit.pyx`): submits as market
at best price; any unfilled remainder is re-posted as a limit at the filled
price. TIF = {GTC, IOC, FOK, GTD, DAY}; AT_THE_OPEN/AT_THE_CLOSE rejected.

**TriggerType enum** (`model/enums.py` ~L408): NO_TRIGGER, DEFAULT, BID_ASK,
LAST_PRICE, DOUBLE_LAST, DOUBLE_BID_ASK, LAST_OR_BID_ASK, MID_POINT, MARK_PRICE,
INDEX_PRICE. DEFAULT == BID_ASK. Used for emulated (local) order triggering.

**TimeInForce enum** (`model/enums.py` ~L384): GTC, IOC, FOK, GTD, DAY,
AT_THE_OPEN, AT_THE_CLOSE.

**m-cube fact:** `ManagedExitStrategy._submit_order` (`core/managed_strategy.py`
~L1752-1767) builds entries with `order_factory.market(... time_in_force=GTC)`.
All entries/exits in the exit engine are MARKET/GTC; no LIMIT or post_only path
exists. See also [[bracket-orders-mcube]] (exits are manual market orders too).
