"""Async Delta Exchange India REST + WebSocket client with HMAC auth."""

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import time
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import aiohttp

from signal_bot.config import settings

logger = logging.getLogger(__name__)

ENDPOINTS = {
    "testnet": {
        "rest": "https://cdn-ind.testnet.deltaex.org",
        "ws": "wss://socketv2-ind-elb.testnet.deltaex.org",
    },
    "production": {
        "rest": "https://api.india.delta.exchange",
        "ws": "wss://socket.india.delta.exchange",
    },
}

WS_AUTH_PATH = {"testnet": "/testnet", "production": "/live"}

WATCHED_BINANCE_SYMBOLS: list[str] = settings.symbols


class DeltaSymbolMap:
    """Translates Binance → Delta symbols and exposes product metadata."""

    def __init__(self, client: "DeltaClient"):
        self.client = client
        self._products: dict[str, dict] = {}  # delta_symbol -> product dict
        self._binance_to_delta: dict[str, str] = {}
        self._delta_to_binance: dict[str, str] = {}
        self._product_id_to_binance: dict[int, str] = {}

    async def load(self) -> None:
        """Fetch products from Delta and build lookup tables."""
        raw = await self.client.get_products()
        products = raw.get("products", []) if isinstance(raw, dict) else raw

        for p in products:
            delta_sym = p.get("symbol", "")
            if not delta_sym:
                continue
            self._products[delta_sym] = p

        missing: list[str] = []
        for binance_sym in WATCHED_BINANCE_SYMBOLS:
            base = binance_sym.replace("USDT", "")
            delta_sym = f"{base}USD"
            if delta_sym in self._products:
                self._binance_to_delta[binance_sym] = delta_sym
                self._delta_to_binance[delta_sym] = binance_sym
                pid = self._products[delta_sym].get("id")
                if pid is not None:
                    self._product_id_to_binance[int(pid)] = binance_sym
            else:
                missing.append(f"{binance_sym} → {delta_sym}")
                logger.error("Symbol %s not found on Delta India as %s", binance_sym, delta_sym)

        if missing:
            logger.error(
                "Missing %d tradable symbol(s) on Delta India: %s",
                len(missing),
                missing,
            )
            raise RuntimeError(
                f"Symbols not available on Delta India: {missing}. "
                "Update WATCHED_BINANCE_SYMBOLS or check exchange listings."
            )

        logger.info(
            "DeltaSymbolMap loaded %d products; mapped %d/%d symbols",
            len(self._products),
            len(self._binance_to_delta),
            len(WATCHED_BINANCE_SYMBOLS),
        )

    # ------------------------------------------------------------------
    # lookups
    # ------------------------------------------------------------------
    def get_delta_symbol(self, binance_symbol: str) -> str:
        return self._binance_to_delta[binance_symbol]

    def get_binance_symbol(self, delta_symbol: str) -> str:
        return self._delta_to_binance[delta_symbol]

    def get_product_id(self, binance_symbol: str) -> int:
        delta_sym = self._binance_to_delta[binance_symbol]
        return int(self._products[delta_sym]["id"])

    def get_product(self, binance_symbol: str) -> dict:
        delta_sym = self._binance_to_delta[binance_symbol]
        return self._products[delta_sym]

    def resolve_by_product_id(self, product_id: int) -> str | None:
        return self._product_id_to_binance.get(product_id)


# ---------------------------------------------------------------------------
# REST client
# ---------------------------------------------------------------------------


class DeltaClient:
    """Async REST client for Delta Exchange India."""

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        mode: str | None = None,
        request_timeout: float = 10.0,
    ):
        self.api_key = (api_key or settings.delta_api_key or "").strip()
        self.api_secret = (api_secret or settings.delta_api_secret or "").strip()
        self.mode = (mode or settings.delta_mode).lower()
        if self.mode not in ENDPOINTS:
            raise ValueError(f"Invalid DELTA_MODE={self.mode}; choose testnet or production")
        self.rest_url = ENDPOINTS[self.mode]["rest"]
        self.ws_url = ENDPOINTS[self.mode]["ws"]
        self.symbol_map = DeltaSymbolMap(self)
        # Bounded timeouts: an order placement that hangs for aiohttp's default
        # 5 minutes is indistinguishable from a lost order.
        self.request_timeout = request_timeout
        self._session: aiohttp.ClientSession | None = None
        self._products_cache: list[dict] | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.request_timeout)
            )
        await self.symbol_map.load()

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> "DeltaClient":
        await self.initialize()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # signing
    # ------------------------------------------------------------------
    def _sign_request(
        self,
        method: str,
        path: str,
        query_string: str = "",
        body: str = "",
    ) -> dict[str, str]:
        timestamp = str(int(time.time()))
        message = (
            method.upper()
            + timestamp
            + path
            + (("?" + query_string) if query_string else "")
            + body
        )
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "api-key": self.api_key,
            "signature": signature,
            "timestamp": timestamp,
        }

    # ------------------------------------------------------------------
    # low-level request
    # ------------------------------------------------------------------
    async def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        data: dict | None = None,
    ) -> dict:
        if self._session is None:
            raise RuntimeError("DeltaClient not initialized. Call initialize() or use async with.")

        query_string = urlencode(sorted(params.items())) if params else ""
        body_str = json.dumps(data, separators=(",", ":"), sort_keys=True) if data else ""

        headers = self._sign_request(method, path, query_string, body_str)
        if data:
            headers["Content-Type"] = "application/json"

        url = f"{self.rest_url}{path}"
        if query_string:
            url += "?" + query_string

        logger.debug("%s %s", method.upper(), url)
        async with self._session.request(
            method,
            url,
            headers=headers,
            data=body_str if body_str else None,
        ) as resp:
            text = await resp.text()
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                payload = None

            if not resp.ok:
                if payload is not None and isinstance(payload, dict):
                    msg = payload.get("message", payload.get("error", text[:200]))
                else:
                    msg = text[:200] if text else f"HTTP {resp.status}"
                raise aiohttp.ClientResponseError(
                    resp.request_info,
                    resp.history,
                    status=resp.status,
                    message=f"Delta API error ({resp.status}): {msg}",
                )

            if payload is None:
                raise aiohttp.ClientResponseError(
                    resp.request_info,
                    resp.history,
                    status=resp.status,
                    message=f"Non-JSON response: {text[:200]}",
                )
            return payload

    # ------------------------------------------------------------------
    # public / un-authed helpers
    # ------------------------------------------------------------------
    async def get_l2_orderbook(self, symbol: str) -> dict:
        """Fetch L2 orderbook for a symbol (Delta format, e.g. BTCUSD)."""
        return await self._request("GET", f"/v2/l2orderbook/{symbol}")

    async def get_products(self) -> dict:
        """Fetch all products.  Result cached in memory on first call."""
        if self._products_cache is not None:
            return {"products": self._products_cache}
        resp = await self._request("GET", "/v2/products")
        if isinstance(resp, dict):
            self._products_cache = resp.get("products") or resp.get("result") or []
        else:
            self._products_cache = resp
        return {"products": self._products_cache}

    # ------------------------------------------------------------------
    # wallet
    # ------------------------------------------------------------------
    async def get_balance(self) -> dict:
        """Return wallet balances (list of assets)."""
        return await self._request("GET", "/v2/wallet/balances")

    # ------------------------------------------------------------------
    # positions
    # ------------------------------------------------------------------
    async def get_open_positions(
        self,
        product_id: int | None = None,
        underlying_asset_symbol: str | None = None,
    ) -> dict:
        params = {}
        if product_id is not None:
            params["product_id"] = product_id
        if underlying_asset_symbol is not None:
            params["underlying_asset_symbol"] = underlying_asset_symbol
        return await self._request("GET", "/v2/positions", params=params or None)

    async def set_leverage(self, product_id: int, leverage: int) -> dict:
        path = f"/v2/products/{product_id}/orders/leverage"
        return await self._request("POST", path, data={"leverage": int(leverage)})

    async def set_margin_mode(self, product_id: int, mode: str) -> dict:
        """Delta India is isolated-margin-only; this is a no-op."""
        logger.debug("margin mode is not configurable on Delta India, defaulting to isolated")
        return {"success": True, "message": "margin mode is not configurable on Delta India"}

    # ------------------------------------------------------------------
    # orders
    # ------------------------------------------------------------------
    async def get_open_orders(self) -> dict:
        return await self._request("GET", "/v2/orders")

    async def place_bracket_order(
        self,
        symbol: str,
        side: str,
        size: int,
        stop_loss_price: Decimal,
        take_profit_price: Decimal,
        client_order_id: str,
        order_type: str = "limit_ioc",
        entry_price: Decimal | None = None,
    ) -> dict:
        """Place a bracket order with embedded SL/TP.

        *symbol* may be a **Binance** symbol (e.g. ``BTCUSDT``) or a **Delta**
        symbol (e.g. ``BTCUSD``).  Binance symbols are translated automatically.

        For *market_order* types, omit *entry_price*.
        """
        if symbol in self.symbol_map._binance_to_delta:
            product_id = self.symbol_map.get_product_id(symbol)
        elif symbol in self.symbol_map._products:
            product_id = self.symbol_map._products[symbol]["id"]
        else:
            for p in self._products_cache or []:
                if p.get("symbol") == symbol:
                    product_id = p["id"]
                    break
            else:
                raise ValueError(f"Cannot resolve product_id for symbol {symbol}")

        body: dict[str, Any] = {
            "product_id": product_id,
            "size": size,
            "side": side.lower(),
            "order_type": order_type,
            "client_order_id": client_order_id,
            "bracket_stop_loss_price": str(stop_loss_price),
            "bracket_stop_loss_limit_price": str(stop_loss_price),
            "bracket_take_profit_price": str(take_profit_price),
            "bracket_take_profit_limit_price": str(take_profit_price),
        }
        if entry_price is not None and "limit" in order_type:
            body["limit_price"] = str(entry_price)
        return await self._request("POST", "/v2/orders", data=body)

    async def cancel_order(
        self,
        order_id: str,
        product_id: int | None = None,
        product_symbol: str | None = None,
    ) -> dict:
        body: dict[str, Any] = {"id": int(order_id)}
        if product_id is not None:
            body["product_id"] = product_id
        elif product_symbol is not None:
            body["product_symbol"] = product_symbol
        return await self._request("DELETE", "/v2/orders", data=body)

    async def cancel_all_orders(self, product_id: int | None = None) -> dict:
        body: dict[str, Any] = {}
        if product_id is not None:
            body["product_id"] = product_id
        return await self._request("DELETE", "/v2/orders", data=body or None)

    async def get_order_status(self, order_id: str) -> dict:
        return await self._request("GET", f"/v2/orders/{order_id}")

    async def get_order_by_client_id(self, client_order_id: str) -> dict:
        """Look up an order by our deterministic client_order_id (open or historical)."""
        return await self._request("GET", f"/v2/orders/client_order_id/{client_order_id}")

    async def get_fills(self, product_id: int | None = None, page_size: int = 50) -> dict:
        params: dict[str, Any] = {"page_size": page_size}
        if product_id is not None:
            params["product_ids"] = product_id
        return await self._request("GET", "/v2/fills", params=params)

    async def place_order(self, body: dict[str, Any]) -> dict:
        return await self._request("POST", "/v2/orders", data=body)

    async def attach_position_bracket(
        self, product_id: int, stop_loss_price: Decimal, take_profit_price: Decimal | None = None
    ) -> dict:
        """Attach SL (and optional TP) to an existing position (recovery procedure)."""
        body: dict[str, Any] = {
            "product_id": product_id,
            "stop_loss_order": {"order_type": "market_order", "stop_price": str(stop_loss_price)},
            "bracket_stop_trigger_method": "last_traded_price",
        }
        if take_profit_price is not None:
            body["take_profit_order"] = {
                "order_type": "limit_order",
                "stop_price": str(take_profit_price),
                "limit_price": str(take_profit_price),
            }
        return await self._request("POST", "/v2/orders/bracket", data=body)

    async def place_market_close(self, symbol: str, product_id: int) -> dict:
        """Close an open position with a reduce-only market order."""
        positions_resp = await self.get_open_positions(product_id=product_id)
        raw = None
        if isinstance(positions_resp, dict):
            raw = positions_resp.get("positions") or positions_resp.get("result")

        # India API may return a single dict or a list under "result"
        positions = []
        if isinstance(raw, list):
            positions = raw
        elif isinstance(raw, dict):
            positions = [raw]

        position = None
        for p in positions:
            if p.get("product_id") == product_id or p.get("symbol") == symbol:
                position = p
                break

        # India API may return position without product_id/symbol fields;
        # if we queried by product_id, the returned position is for that product.
        if not position and len(positions) == 1:
            position = positions[0]

        if not position:
            raise ValueError(f"No open position for {symbol}")

        raw_size = int(position.get("size", 0))
        size = abs(raw_size)
        side = position.get("side", "").lower()
        if side in ("buy", "long"):
            close_side = "sell"
        elif side in ("sell", "short"):
            close_side = "buy"
        else:
            # Infer from signed size: positive = long -> sell to close
            close_side = "sell" if raw_size > 0 else "buy"

        body = {
            "product_id": product_id,
            "size": size,
            "side": close_side,
            "order_type": "market_order",
            "reduce_only": True,
        }
        return await self._request("POST", "/v2/orders", data=body)

    async def get_order_history(
        self,
        product_id: int,
        start: str,
        end: str,
    ) -> dict:
        """*start* and *end* should be ISO-8601 strings."""
        return await self._request(
            "GET",
            "/v2/orders/history",
            params={
                "product_id": product_id,
                "start_time": start,
                "end_time": end,
            },
        )


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------


class DeltaWebSocket:
    """Private WebSocket connection to Delta Exchange India.

    Emits parsed JSON events onto *delta_events* queue.
    """

    def __init__(
        self,
        client: DeltaClient,
        delta_events,
        on_connect=None,
        on_disconnect=None,
        heartbeat: float = 20.0,
    ):
        """*delta_events* is an ``asyncio.Queue`` (raw messages) or an async callable.

        ``on_connect`` / ``on_disconnect`` are async callbacks; the execution layer
        uses them to block trading while the private stream is down and to
        reconcile before resuming.
        """
        self.client = client
        self.delta_events = delta_events
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.heartbeat = heartbeat
        self._ws_url = client.ws_url
        self._running = False
        self._task: asyncio.Task | None = None
        self.connected = False
        self._reported_down = False

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ------------------------------------------------------------------
    # main loop
    # ------------------------------------------------------------------
    async def _run(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                await self._connect_and_listen()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Delta WS error. Reconnecting in %.0fs…", backoff)
                await self._mark_disconnected(f"error: {type(exc).__name__}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
            else:
                await self._mark_disconnected("closed")
                if self._running:
                    await asyncio.sleep(backoff)

    async def _emit(self, event: dict) -> None:
        if isinstance(self.delta_events, asyncio.Queue):
            await self.delta_events.put(event)
        else:
            await self.delta_events(event)

    async def _mark_disconnected(self, reason: str) -> None:
        """Report the transition to 'down' once, not on every failed reconnect attempt."""
        was_connected, self.connected = self.connected, False
        if was_connected:
            self._reported_down = False
        if self._reported_down:
            return
        self._reported_down = True
        if self.on_disconnect is not None:
            try:
                await self.on_disconnect(reason)
            except Exception:
                logger.exception("on_disconnect callback failed")

    async def _connect_and_listen(self) -> None:
        session = self.client._session
        if session is None:
            raise RuntimeError("DeltaClient session not initialized")

        headers = {
            "Origin": settings.delta_ws_origin,
            "User-Agent": "Mozilla/5.0 (signalbot/1.0)",
        }
        async with session.ws_connect(
            self._ws_url, headers=headers, heartbeat=self.heartbeat
        ) as ws:
            await self._authenticate(ws)
            await ws.send_json(
                {
                    "type": "subscribe",
                    "payload": {
                        "channels": ["private_positions", "private_orders"],
                    },
                }
            )
            logger.info("Delta WS subscribed to private_positions + private_orders")
            self.connected = True
            if self.on_connect is not None:
                try:
                    await self.on_connect()
                except Exception:
                    logger.exception("on_connect callback failed")

            async for msg in ws:
                if not self._running:
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        event = json.loads(msg.data)
                    except json.JSONDecodeError:
                        logger.warning("Delta WS non-JSON: %s", msg.data[:200])
                        continue
                    await self._emit(event)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    logger.warning("Delta WS closed / error")
                    break

    async def _authenticate(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        path = WS_AUTH_PATH.get(self.client.mode, "/live")
        timestamp = str(int(time.time()))
        message = "GET" + timestamp + path
        signature = hmac.new(
            self.client.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        await ws.send_json(
            {
                "type": "auth",
                "payload": {
                    "api-key": self.client.api_key,
                    "signature": signature,
                    "timestamp": timestamp,
                },
            }
        )
        logger.debug("Delta WS auth sent")


# ---------------------------------------------------------------------------
# CLI sanity check
# ---------------------------------------------------------------------------


async def _cli_check() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    async with DeltaClient() as client:
        logger.info("Delta mode: %s", client.mode)
        logger.info("REST endpoint: %s", client.rest_url)

        # balance
        bal = await client.get_balance()
        logger.info(
            "Balance response keys: %s", list(bal.keys()) if isinstance(bal, dict) else "N/A"
        )
        balances = []
        if isinstance(bal, dict):
            balances = bal.get("balances") or bal.get("result") or []
        for b in balances:
            if b.get("asset_symbol", "").upper() in ("USDT", "USD"):
                logger.info("USDT balance: %s", b)

        # positions
        positions = await client.get_open_positions(underlying_asset_symbol="BTC")
        pos_list = []
        if isinstance(positions, dict):
            pos_list = positions.get("positions") or positions.get("result") or []
        logger.info("Open positions: %d", len(pos_list))
        for p in pos_list:
            logger.info("  %s", p)

    logger.info("OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Delta Exchange India client sanity check")
    parser.add_argument("--check", action="store_true", help="Connect and print basic account info")
    args = parser.parse_args()

    if args.check:
        asyncio.run(_cli_check())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
