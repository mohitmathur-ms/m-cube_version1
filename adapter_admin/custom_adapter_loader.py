"""
Custom adapter validation, template, and guidelines.

Validates uploaded Python files to ensure they follow the required adapter structure.
Runs validation in a subprocess to prevent NautilusTrader's Rust bindings from crashing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def sanitize_filename(name: str) -> str:
    """Sanitize a filename to be safe for the filesystem."""
    name = re.sub(r"[^\w\-.]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def get_validation_script(file_path: Path, project_dir: Path) -> str:
    """Build a Python script string that validates a custom adapter file in a subprocess."""
    parent_str = str(project_dir).replace("\\", "\\\\")
    file_str = str(file_path).replace("\\", "\\\\")

    return (
        'import sys, json\n'
        f'sys.path.insert(0, "{parent_str}")\n'
        'try:\n'
        '    import importlib.util\n'
        '    from pathlib import Path\n'
        f'    fp = Path("{file_str}")\n'
        '    module_name = "custom_adapter_" + fp.stem\n'
        '    if module_name in sys.modules:\n'
        '        del sys.modules[module_name]\n'
        '    spec = importlib.util.spec_from_file_location(module_name, str(fp))\n'
        '    mod = importlib.util.module_from_spec(spec)\n'
        '    sys.modules[module_name] = mod\n'
        '    spec.loader.exec_module(mod)\n'
        '    errors = []\n'
        '    # Check required exports\n'
        '    adapter_name = getattr(mod, "ADAPTER_NAME", None)\n'
        '    if not adapter_name or not isinstance(adapter_name, str):\n'
        '        errors.append("Missing or invalid ADAPTER_NAME (must be a non-empty string)")\n'
        '    elif len(adapter_name) > 100:\n'
        '        errors.append("ADAPTER_NAME exceeds 100 characters")\n'
        '    data_client = getattr(mod, "DATA_CLIENT_CLASS", None)\n'
        '    exec_client = getattr(mod, "EXEC_CLIENT_CLASS", None)\n'
        '    if data_client is None and exec_client is None:\n'
        '        errors.append("At least one of DATA_CLIENT_CLASS or EXEC_CLIENT_CLASS must be defined (not None)")\n'
        '    if data_client is not None and not isinstance(data_client, type):\n'
        '        errors.append("DATA_CLIENT_CLASS must be a class")\n'
        '    if exec_client is not None and not isinstance(exec_client, type):\n'
        '        errors.append("EXEC_CLIENT_CLASS must be a class")\n'
        '    config_class = getattr(mod, "CONFIG_CLASS", None)\n'
        '    if config_class is None or not isinstance(config_class, type):\n'
        '        errors.append("Missing or invalid CONFIG_CLASS (must be a class)")\n'
        '    factory_class = getattr(mod, "FACTORY_CLASS", None)\n'
        '    if factory_class is None or not isinstance(factory_class, type):\n'
        '        errors.append("Missing or invalid FACTORY_CLASS (must be a class)")\n'
        '    # Check optional PARAMS\n'
        '    params = getattr(mod, "PARAMS", None)\n'
        '    if params is not None and not isinstance(params, dict):\n'
        '        errors.append("PARAMS must be a dict if provided")\n'
        '    if errors:\n'
        '        print(json.dumps({"ok": False, "error": "Validation failed:\\n  - " + "\\n  - ".join(errors)}))\n'
        '    else:\n'
        '        supports_data = data_client is not None\n'
        '        supports_exec = exec_client is not None\n'
        '        param_info = params if isinstance(params, dict) else {}\n'
        '        print(json.dumps({"ok": True, "adapter_name": adapter_name, '
        '"supports_data": supports_data, "supports_exec": supports_exec, '
        '"params": {k: {"label": v.get("label", k), "type": v.get("type", "text")} for k, v in param_info.items()} if param_info else {}}))\n'
        'except SyntaxError as e:\n'
        '    print(json.dumps({"ok": False, "error": f"Syntax error: {e}"}))\n'
        'except ImportError as e:\n'
        '    print(json.dumps({"ok": False, "error": f"Import error: {e}"}))\n'
        'except Exception as e:\n'
        '    print(json.dumps({"ok": False, "error": str(e)}))\n'
    )


def get_adapter_template() -> str:
    """Return a template Python file for custom adapters."""
    return '''"""
Custom Adapter Template for NautilusTrader.

This template shows the required structure for a custom adapter.
Rename this file and modify it for your exchange.

Required exports (module-level constants):
    ADAPTER_NAME        : str   — Display name for this adapter (max 100 chars)
    DATA_CLIENT_CLASS   : class — Your DataClient class (or None if data-only is not supported)
    EXEC_CLIENT_CLASS   : class — Your ExecClient class (or None if execution is not supported)
    CONFIG_CLASS        : class — Your configuration class
    FACTORY_CLASS       : class — Your factory class

Optional exports:
    PARAMS : dict — Parameter definitions for UI form generation.
                    Each key is a param name, value is a dict with:
                    label (str), type ("text"|"password"|"select"|"checkbox"|"number"),
                    required (bool), default (any), sensitive (bool), options (list for select)
"""

import json
import asyncio

from nautilus_trader.config import LiveDataClientConfig, LiveExecClientConfig
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.live.factories import LiveDataClientFactory, LiveExecClientFactory
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import ClientId, Venue, VenueOrderId, TradeId
from nautilus_trader.model.objects import Price, Quantity


# ──────────────────────────────────────────────────────────────────────
#  PARAMS — defines what the admin panel form will show for this adapter
# ──────────────────────────────────────────────────────────────────────

PARAMS = {
    "api_key": {
        "label": "API Key",
        "type": "text",
        "required": True,
        "sensitive": True,
        "placeholder": "Enter your API key",
    },
    "api_secret": {
        "label": "API Secret",
        "type": "password",
        "required": True,
        "sensitive": True,
        "placeholder": "Enter your API secret",
    },
    "ws_url": {
        "label": "WebSocket URL",
        "type": "text",
        "required": True,
        "default": "wss://ws.myexchange.com/stream",
    },
    "base_url": {
        "label": "REST API Base URL",
        "type": "text",
        "required": True,
        "default": "https://api.myexchange.com",
    },
    "testnet": {
        "label": "Use Testnet",
        "type": "checkbox",
        "default": True,
    },
}


# ──────────────────────────────────────────────────────────────────────
#  Configuration classes — inherit from NautilusTrader config base classes
# ──────────────────────────────────────────────────────────────────────

class MyExchangeDataConfig(LiveDataClientConfig):
    """Configuration for MyExchange data connection."""
    api_key: str = ""
    api_secret: str = ""
    ws_url: str = "wss://ws.myexchange.com/stream"
    testnet: bool = True


class MyExchangeExecConfig(LiveExecClientConfig):
    """Configuration for MyExchange execution connection."""
    api_key: str = ""
    api_secret: str = ""
    base_url: str = "https://api.myexchange.com"
    testnet: bool = True


# ──────────────────────────────────────────────────────────────────────
#  DataClient — inherits from LiveMarketDataClient
#
#  This is the "eyes" of your adapter. It connects to the exchange's
#  websocket, receives candle/tick data, converts it to NautilusTrader
#  format, and feeds it to the engine via self._handle_data(bar).
#
#  Methods you MUST implement:
#    _connect()           — open websocket connection to exchange
#    _disconnect()        — close the connection
#    _subscribe_bars()    — tell exchange to stream candles for a symbol
#
#  Key method to call:
#    self._handle_data(bar)  — feeds bar to engine → strategy.on_bar()
# ──────────────────────────────────────────────────────────────────────

class MyExchangeDataClient(LiveMarketDataClient):
    """Receives live market data from MyExchange."""

    def __init__(self, loop, client_id, venue, config, msgbus, cache, clock):
        super().__init__(
            loop=loop,
            client_id=client_id,
            venue=venue,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
        self._config = config
        self._ws = None
        self._bar_type_map = {}  # symbol → BarType

    async def _connect(self):
        """Called when TradingNode starts. Open websocket to exchange."""
        # import websockets
        # self._ws = await websockets.connect(
        #     self._config.ws_url,
        #     extra_headers={"X-API-KEY": self._config.api_key},
        # )
        # self._log.info("Connected to MyExchange websocket")
        # asyncio.create_task(self._listen())
        pass

    async def _disconnect(self):
        """Called when TradingNode stops. Close websocket."""
        if self._ws:
            await self._ws.close()
        self._log.info("Disconnected from MyExchange")

    async def _subscribe_bars(self, bar_type: BarType):
        """Called when strategy calls self.subscribe_bars().

        Tell the exchange to start streaming candles for this symbol.
        Without this, the exchange doesn't know what data to send.
        """
        symbol = bar_type.instrument_id.symbol.value  # e.g. "BTCUSDT"
        self._bar_type_map[symbol] = bar_type
        # await self._ws.send(json.dumps({
        #     "action": "subscribe",
        #     "channel": "kline",
        #     "symbol": symbol,
        #     "interval": "1d",
        # }))
        self._log.info(f"Subscribed to {symbol} candles")

    async def _unsubscribe_bars(self, bar_type: BarType):
        """Called when strategy calls self.unsubscribe_bars()."""
        symbol = bar_type.instrument_id.symbol.value
        self._bar_type_map.pop(symbol, None)
        # await self._ws.send(json.dumps({
        #     "action": "unsubscribe",
        #     "channel": "kline",
        #     "symbol": symbol,
        # }))

    async def _listen(self):
        """Background task: continuously receive messages from exchange websocket."""
        # async for raw_msg in self._ws:
        #     try:
        #         data = json.loads(raw_msg)
        #
        #         # Exchange sends something like:
        #         # {"symbol": "BTCUSDT", "open": "42000", "high": "42500",
        #         #  "low": "41800", "close": "42300", "volume": "150",
        #         #  "timestamp_ms": 1704067200000}
        #
        #         if data.get("type") == "candle":
        #             symbol = data["symbol"]
        #             bar_type = self._bar_type_map.get(symbol)
        #             if bar_type:
        #                 bar = Bar(
        #                     bar_type=bar_type,
        #                     open=Price.from_str(data["open"]),
        #                     high=Price.from_str(data["high"]),
        #                     low=Price.from_str(data["low"]),
        #                     close=Price.from_str(data["close"]),
        #                     volume=Quantity.from_str(data["volume"]),
        #                     ts_event=data["timestamp_ms"] * 1_000_000,  # ms → ns
        #                     ts_init=self._clock.timestamp_ns(),
        #                 )
        #                 # THIS is the key line:
        #                 # Feeds the bar to the engine → engine calls strategy.on_bar(bar)
        #                 self._handle_data(bar)
        #
        #     except Exception as e:
        #         self._log.error(f"Error processing message: {e}")
        pass


# ──────────────────────────────────────────────────────────────────────
#  ExecClient — inherits from LiveExecutionClient
#
#  This is the "hands" of your adapter. It sends orders to the exchange
#  and reports fills/rejections back to the engine.
#
#  Methods you MUST implement:
#    _connect()           — open REST/websocket connection for trading
#    _disconnect()        — close the connection
#    _submit_order()      — convert order to exchange format, send via API
#    _cancel_order()      — cancel order on exchange
#
#  Key methods to call (report results back to engine → strategy):
#    self.generate_order_accepted()  → strategy.on_order_accepted()
#    self.generate_order_filled()    → strategy.on_order_filled()
#    self.generate_order_rejected()  → strategy.on_order_rejected()
#    self.generate_order_canceled()  → strategy.on_order_canceled()
# ──────────────────────────────────────────────────────────────────────

class MyExchangeExecClient(LiveExecutionClient):
    """Sends orders to MyExchange and reports fills back."""

    def __init__(self, loop, client_id, venue, config, msgbus, cache, clock):
        super().__init__(
            loop=loop,
            client_id=client_id,
            venue=venue,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
        self._config = config
        self._session = None
        self._headers = {}

    async def _connect(self):
        """Called when TradingNode starts. Open HTTP session for trading."""
        # import aiohttp
        # self._session = aiohttp.ClientSession()
        # self._headers = {
        #     "X-API-KEY": self._config.api_key,
        #     "X-API-SECRET": self._config.api_secret,
        #     "Content-Type": "application/json",
        # }
        self._log.info("Connected to MyExchange trading API")

    async def _disconnect(self):
        """Called when TradingNode stops. Close HTTP session."""
        if self._session:
            await self._session.close()
        self._log.info("Disconnected from MyExchange trading API")

    async def _submit_order(self, command):
        """Called when strategy calls self.submit_order().

        The engine wraps the order in a SubmitOrder command.
        Extract the order with command.order, convert to exchange format,
        send to exchange API, then report result back.
        """
        order = command.order

        # # Convert NautilusTrader order → exchange API format
        # payload = {
        #     "symbol": order.instrument_id.symbol.value,  # "BTCUSDT"
        #     "side": "buy" if order.side == OrderSide.BUY else "sell",
        #     "quantity": str(order.quantity),
        #     "type": "market",
        #     "client_order_id": order.client_order_id.value,
        # }
        #
        # try:
        #     response = await self._session.post(
        #         f"{self._config.base_url}/v1/order",
        #         headers=self._headers,
        #         json=payload,
        #     )
        #     result = await response.json()
        #
        #     # Exchange responds:
        #     # Success: {"status": "filled", "order_id": "123", "trade_id": "456",
        #     #           "fill_price": "42300.00", "filled_qty": "0.001"}
        #     # Failure: {"status": "rejected", "reason": "Insufficient balance"}
        #
        #     if result["status"] == "filled":
        #         venue_order_id = VenueOrderId(result["order_id"])
        #
        #         # Report accepted → triggers strategy.on_order_accepted()
        #         self.generate_order_accepted(
        #             strategy_id=command.strategy_id,
        #             instrument_id=order.instrument_id,
        #             client_order_id=order.client_order_id,
        #             venue_order_id=venue_order_id,
        #             ts_event=self._clock.timestamp_ns(),
        #         )
        #
        #         # Report filled → triggers strategy.on_order_filled()
        #         #               → engine also updates portfolio + account balance
        #         self.generate_order_filled(
        #             strategy_id=command.strategy_id,
        #             instrument_id=order.instrument_id,
        #             client_order_id=order.client_order_id,
        #             venue_order_id=venue_order_id,
        #             trade_id=TradeId(result["trade_id"]),
        #             order_side=order.side,
        #             order_type=order.order_type,
        #             last_px=Price.from_str(result["fill_price"]),
        #             last_qty=Quantity.from_str(result["filled_qty"]),
        #             currency=order.instrument_id.symbol.currency,
        #             ts_event=self._clock.timestamp_ns(),
        #         )
        #     else:
        #         # Report rejected → triggers strategy.on_order_rejected()
        #         self.generate_order_rejected(
        #             strategy_id=command.strategy_id,
        #             instrument_id=order.instrument_id,
        #             client_order_id=order.client_order_id,
        #             reason=result.get("reason", "Unknown"),
        #             ts_event=self._clock.timestamp_ns(),
        #         )
        #
        # except Exception as e:
        #     self.generate_order_rejected(
        #         strategy_id=command.strategy_id,
        #         instrument_id=order.instrument_id,
        #         client_order_id=order.client_order_id,
        #         reason=f"API error: {e}",
        #         ts_event=self._clock.timestamp_ns(),
        #     )
        pass

    async def _cancel_order(self, command):
        """Called when strategy calls self.cancel_order() or self.cancel_all_orders()."""
        # try:
        #     resp = await self._session.delete(
        #         f"{self._config.base_url}/v1/order/{command.venue_order_id}",
        #         headers=self._headers,
        #     )
        #     result = await resp.json()
        #
        #     if result["status"] == "canceled":
        #         # Report canceled → triggers strategy.on_order_canceled()
        #         self.generate_order_canceled(
        #             strategy_id=command.strategy_id,
        #             instrument_id=command.instrument_id,
        #             client_order_id=command.client_order_id,
        #             venue_order_id=command.venue_order_id,
        #             ts_event=self._clock.timestamp_ns(),
        #         )
        #
        # except Exception as e:
        #     self._log.error(f"Cancel failed: {e}")
        pass


# ──────────────────────────────────────────────────────────────────────
#  Factory — creates client instances when TradingNode starts
#
#  TradingNode calls Factory.create() and passes internal system objects
#  (loop, msgbus, cache, clock). You just forward them to your clients.
#  You never create these objects yourself.
#
#  Parameters passed by TradingNode:
#    loop     — Python asyncio event loop (runs all async operations)
#    name     — Client name string (e.g. "MYEXCHANGE")
#    config   — Your config class instance (API keys, URLs)
#    msgbus   — Internal message bus (how components communicate)
#    cache    — Internal cache (stores instruments, orders, positions)
#    clock    — System clock (provides timestamps via clock.timestamp_ns())
# ──────────────────────────────────────────────────────────────────────

class MyExchangeDataFactory(LiveDataClientFactory):
    """Creates MyExchangeDataClient instances."""

    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        return MyExchangeDataClient(
            loop=loop,
            client_id=ClientId(name),
            venue=Venue("MYEXCHANGE"),  # Change to your venue name
            config=config,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )


class MyExchangeExecFactory(LiveExecClientFactory):
    """Creates MyExchangeExecClient instances."""

    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        return MyExchangeExecClient(
            loop=loop,
            client_id=ClientId(name),
            venue=Venue("MYEXCHANGE"),  # Change to your venue name
            config=config,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )


# ──────────────────────────────────────────────────────────────────────
#  Required module-level exports
#
#  These constants are what the admin panel validates when you upload
#  this file. They must all be present.
# ──────────────────────────────────────────────────────────────────────

ADAPTER_NAME = "My Custom Exchange"              # Display name (max 100 chars)
DATA_CLIENT_CLASS = MyExchangeDataClient         # Set to None if not supported
EXEC_CLIENT_CLASS = MyExchangeExecClient         # Set to None if not supported
CONFIG_CLASS = MyExchangeDataConfig              # Your configuration class
FACTORY_CLASS = MyExchangeDataFactory            # Your factory class
'''


def get_adapter_guidelines() -> str:
    """Return markdown guidelines for creating custom adapters."""
    return """# Custom Adapter Guidelines

## Overview
A custom adapter connects NautilusTrader to any exchange or data source.
It consists of two main components: a **DataClient** (receives prices) and
an **ExecClient** (sends orders).

## Required File Structure

Your `.py` file must export these module-level constants:

| Export | Type | Description |
|---|---|---|
| `ADAPTER_NAME` | `str` | Display name, max 100 characters |
| `DATA_CLIENT_CLASS` | `class` or `None` | Your data client class |
| `EXEC_CLIENT_CLASS` | `class` or `None` | Your execution client class |
| `CONFIG_CLASS` | `class` | Configuration class |
| `FACTORY_CLASS` | `class` | Factory that creates clients |

At least one of `DATA_CLIENT_CLASS` or `EXEC_CLIENT_CLASS` must be non-None.

## Optional Exports

| Export | Type | Description |
|---|---|---|
| `PARAMS` | `dict` | Parameter definitions for admin panel form |

### PARAMS Format
```python
PARAMS = {
    "api_key": {
        "label": "API Key",           # Display label
        "type": "text",                # text, password, select, checkbox, number
        "required": True,              # Is this field mandatory?
        "sensitive": True,             # Should it be masked in the UI?
        "default": "",                 # Default value
        "placeholder": "Enter key",    # Input placeholder text
        "options": ["A", "B"],         # Only for type="select"
    },
}
```

## DataClient Methods

| Method | When Called | What to Do |
|---|---|---|
| `_connect()` | TradingNode starts | Open websocket/API connection |
| `_disconnect()` | TradingNode stops | Close all connections |
| `_subscribe_bars(bar_type)` | Strategy subscribes | Tell exchange to stream candles |
| `_handle_data(bar)` | You receive data | Feed bar to engine (calls `strategy.on_bar()`) |

## ExecClient Methods

| Method | When Called | What to Do |
|---|---|---|
| `_connect()` | TradingNode starts | Open trading API connection |
| `_disconnect()` | TradingNode stops | Close connections |
| `_submit_order(command)` | Strategy places order | Send to exchange, report fill/reject |
| `_cancel_order(command)` | Strategy cancels order | Cancel on exchange, report result |

## ExecClient Reporting Methods

After your exchange responds, report back using:
- `self.generate_order_accepted(...)` → triggers `strategy.on_order_accepted()`
- `self.generate_order_filled(...)` → triggers `strategy.on_order_filled()`
- `self.generate_order_rejected(...)` → triggers `strategy.on_order_rejected()`
- `self.generate_order_canceled(...)` → triggers `strategy.on_order_canceled()`

## Tips

1. **Start with testnet** — Always test on testnet before going live
2. **Handle reconnection** — WebSocket connections can drop; implement auto-reconnect
3. **Rate limiting** — Respect exchange rate limits to avoid bans
4. **Error handling** — Catch network errors gracefully, report rejections to engine
5. **Logging** — Use `self._log` for structured logging

## Example Flow

```
Exchange sends candle → DataClient._listen() receives
    → DataClient._parse_bar() converts format
    → DataClient._handle_data(bar) feeds to engine
    → Engine calls strategy.on_bar(bar)
    → Strategy calls self.submit_order(order)
    → ExecClient._submit_order(command)
    → ExecClient sends to exchange API
    → Exchange fills → ExecClient.generate_order_filled()
    → Strategy.on_order_filled() reacts
```
"""
