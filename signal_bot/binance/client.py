"""Async Binance USD-M futures REST client and private user-data stream.

Endpoints follow https://developers.binance.com/docs/derivatives/ (USDⓈ-M futures):

* demo trading: REST ``https://demo-fapi.binance.com``, sockets ``wss://demo-fstream.binance.com``;
* production:   REST ``https://fapi.binance.com``,      sockets ``wss://fstream.binance.com``.

Since 2025-12-09 conditional orders (``STOP_MARKET``, ``TAKE_PROFIT_MARKET``, trailing stops)
are *algo orders*: ``POST /fapi/v1/algoOrder`` and friends.  ``POST /fapi/v1/order`` refuses
those types with ``-4120 STOP_ORDER_SWITCH_ALGO``.

Signed endpoints take ``timestamp`` and ``recvWindow`` and an HMAC-SHA256 ``signature`` of
the url-encoded parameters; everything is sent in the query string (allowed for every
method).  The clock offset to Binance is measured at start-up and on ``-1021``.

Read-only check (no orders):  ``python -m signal_bot.binance.client --check``
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import time
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import aiohttp

from signal_bot.config import settings

logger = logging.getLogger(__name__)

ENDPOINTS = {
    "demo": {"rest": "https://demo-fapi.binance.com", "ws": "wss://demo-fstream.binance.com"},
    "production": {"rest": "https://fapi.binance.com", "ws": "wss://fstream.binance.com"},
}
# Documented for production: wss://fstream.binance.com/private/ws/<listenKey>.  The demo
# socket is checked by --check (it tries this path and the older /ws/ path).
USER_WS_PATH = "/private/ws/"
LISTEN_KEY_KEEPALIVE_SECONDS = 30 * 60  # a listenKey lives 60 minutes without a PUT
RECV_WINDOW_MS = 5000

# Error codes the adapter treats specially.
CODE_UNKNOWN_EXECUTION = -1007  # "Timeout waiting for response... execution status unknown"
CODE_TIMESTAMP = -1021
CODE_ORDER_NOT_FOUND = -2013
CODE_UNKNOWN_ORDER_CANCEL = -2011
CODE_NO_NEED_MARGIN_TYPE = -4046
CODE_NO_NEED_POSITION_MODE = -4059
CODE_WOULD_TRIGGER = -2021


class BinanceAPIError(aiohttp.ClientResponseError):
    """A non-2xx Binance response; ``code`` is Binance's error code (``None`` if absent)."""

    def __init__(self, request_info, history, status: int, code: int | None, msg: str):
        super().__init__(
            request_info,
            history,
            status=status,
            message=f"Binance API error ({status}, code {code}): {msg}",
        )
        self.binance_code = code
        self.msg = msg


@dataclass(frozen=True)
class SymbolRules:
    """Trading rules for one USD-M perpetual from ``GET /fapi/v1/exchangeInfo``."""

    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal | None
    market_max_qty: Decimal | None
    min_notional: Decimal


def parse_rules(
    info: dict, wanted: list[str] | None = None
) -> tuple[dict[str, SymbolRules], list[str]]:
    """Rules for trading USD-M perpetuals in ``wanted`` (all if ``None``) and the missing ones."""

    def d(value, default=None):
        return Decimal(str(value)) if value not in (None, "") else default

    rules: dict[str, SymbolRules] = {}
    for row in info.get("symbols", []):
        sym = row.get("symbol")
        if wanted is not None and sym not in wanted:
            continue
        if row.get("contractType") != "PERPETUAL" or row.get("status") != "TRADING":
            continue
        filters = {f.get("filterType"): f for f in row.get("filters", [])}
        price = filters.get("PRICE_FILTER", {})
        lot = filters.get("LOT_SIZE", {})
        market = filters.get("MARKET_LOT_SIZE", {})
        notional = filters.get("MIN_NOTIONAL", {})
        rules[sym] = SymbolRules(
            symbol=sym,
            tick_size=d(price.get("tickSize"), Decimal("0.01")),
            step_size=d(lot.get("stepSize"), Decimal("0.001")),
            min_qty=d(lot.get("minQty"), Decimal("0")),
            max_qty=d(lot.get("maxQty")),
            market_max_qty=d(market.get("maxQty")),
            min_notional=d(notional.get("notional") or notional.get("minNotional"), Decimal("0")),
        )
    missing = [s for s in (wanted or []) if s not in rules]
    return rules, missing


def _fmt(value: Any) -> str:
    """Decimals without exponent notation (Binance rejects 1E-5); booleans in lower case."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    return str(value)


class BinanceFuturesClient:
    """Async REST client for Binance USD-M futures (one-way position mode)."""

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        mode: str | None = None,
        request_timeout: float = 10.0,
        symbols: list[str] | None = None,
    ):
        self.api_key = (api_key or settings.binance_futures_api_key or "").strip()
        self.api_secret = (api_secret or settings.binance_futures_api_secret or "").strip()
        self.mode = (mode or settings.binance_trade_mode).lower()
        if self.mode not in ENDPOINTS:
            raise ValueError(f"Invalid BINANCE_TRADE_MODE={self.mode}; choose demo or production")
        self.rest_url = ENDPOINTS[self.mode]["rest"]
        self.ws_url = ENDPOINTS[self.mode]["ws"]
        self.user_ws_path = settings.binance_user_ws_path or USER_WS_PATH
        self.watched_symbols = list(symbols) if symbols else list(settings.symbols)
        self.request_timeout = request_timeout
        self.rules: dict[str, SymbolRules] = {}
        self.missing: list[str] = []
        self.time_offset_ms = 0
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.request_timeout)
            )
        await self.sync_time()
        await self.load_rules()

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> BinanceFuturesClient:
        await self.initialize()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def sync_time(self) -> None:
        before = time.time() * 1000
        body = await self._request("GET", "/fapi/v1/time")
        after = time.time() * 1000
        self.time_offset_ms = int(body["serverTime"] - (before + after) / 2)

    async def load_rules(self) -> None:
        info = await self._request("GET", "/fapi/v1/exchangeInfo")
        self.rules, self.missing = parse_rules(info, self.watched_symbols)
        if self.missing:
            logger.warning("Not tradable on Binance %s futures: %s", self.mode, self.missing)
        if not self.rules:
            raise RuntimeError(f"none of {self.watched_symbols} trades on Binance {self.mode}")

    # ------------------------------------------------------------------
    # signing and transport
    # ------------------------------------------------------------------
    def signed_query(self, params: dict[str, Any]) -> str:
        """``params`` plus recvWindow/timestamp, url-encoded, with the HMAC signature appended."""
        full = {k: v for k, v in params.items() if v is not None}
        full["recvWindow"] = RECV_WINDOW_MS
        full["timestamp"] = int(time.time() * 1000) + self.time_offset_ms
        query = urlencode([(k, _fmt(v)) for k, v in full.items()])
        signature = hmac.new(
            self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return f"{query}&signature={signature}"

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        api_key: bool = False,
        _retry_clock: bool = True,
    ) -> Any:
        if self._session is None:
            raise RuntimeError("BinanceFuturesClient not initialized; use async with")
        headers = {}
        if signed or api_key:
            headers["X-MBX-APIKEY"] = self.api_key
        if signed:
            query = self.signed_query(params or {})
        else:
            query = urlencode([(k, _fmt(v)) for k, v in (params or {}).items() if v is not None])
        url = f"{self.rest_url}{path}" + (f"?{query}" if query else "")
        logger.debug("%s %s%s", method, self.rest_url, path)
        async with self._session.request(method, url, headers=headers) as resp:
            text = await resp.text()
            try:
                payload = json.loads(text) if text else None
            except json.JSONDecodeError:
                payload = None
            if not resp.ok:
                code = payload.get("code") if isinstance(payload, dict) else None
                msg = payload.get("msg") if isinstance(payload, dict) else (text[:200] or "")
                if code == CODE_TIMESTAMP and signed and _retry_clock:
                    await self.sync_time()
                    return await self._request(method, path, params, signed, api_key, False)
                raise BinanceAPIError(resp.request_info, resp.history, resp.status, code, str(msg))
            if payload is None:
                raise BinanceAPIError(
                    resp.request_info, resp.history, resp.status, None, f"non-JSON: {text[:200]}"
                )
            return payload

    # ------------------------------------------------------------------
    # market data (public, on the trading venue)
    # ------------------------------------------------------------------
    async def depth(self, symbol: str, limit: int = 20) -> dict:
        return await self._request("GET", "/fapi/v1/depth", {"symbol": symbol, "limit": limit})

    # ------------------------------------------------------------------
    # account configuration
    # ------------------------------------------------------------------
    async def position_mode(self) -> bool:
        """``True`` in hedge mode (dual-side), ``False`` in one-way mode."""
        body = await self._request("GET", "/fapi/v1/positionSide/dual", signed=True)
        return bool(body.get("dualSidePosition"))

    async def multi_assets_mode(self) -> bool:
        """``True`` when the account is in Multi-Assets mode (isolated margin unavailable)."""
        body = await self._request("GET", "/fapi/v1/multiAssetsMargin", signed=True)
        return bool(body.get("multiAssetsMargin"))

    async def set_single_asset_mode(self) -> None:
        try:
            await self._request(
                "POST", "/fapi/v1/multiAssetsMargin", {"multiAssetsMargin": "false"}, signed=True
            )
        except BinanceAPIError as exc:
            if "no need" not in str(exc.msg).lower():
                raise

    async def set_one_way_mode(self) -> None:
        try:
            await self._request(
                "POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "false"}, signed=True
            )
        except BinanceAPIError as exc:
            if exc.binance_code != CODE_NO_NEED_POSITION_MODE:
                raise

    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        return await self._request(
            "POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": int(leverage)}, signed=True
        )

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> None:
        try:
            await self._request(
                "POST",
                "/fapi/v1/marginType",
                {"symbol": symbol, "marginType": margin_type},
                signed=True,
            )
        except BinanceAPIError as exc:
            if exc.binance_code != CODE_NO_NEED_MARGIN_TYPE:
                raise

    # ------------------------------------------------------------------
    # orders
    # ------------------------------------------------------------------
    async def new_order(self, **params: Any) -> dict:
        return await self._request("POST", "/fapi/v1/order", params, signed=True)

    async def get_order(
        self, symbol: str, client_order_id: str | None = None, order_id: str | int | None = None
    ) -> dict:
        return await self._request(
            "GET",
            "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id, "orderId": order_id},
            signed=True,
        )

    async def cancel_order(
        self, symbol: str, client_order_id: str | None = None, order_id: str | int | None = None
    ) -> dict:
        return await self._request(
            "DELETE",
            "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id, "orderId": order_id},
            signed=True,
        )

    async def open_orders(self, symbol: str | None = None) -> list[dict]:
        return await self._request("GET", "/fapi/v1/openOrders", {"symbol": symbol}, signed=True)

    async def user_trades(
        self,
        symbol: str,
        order_id: str | int | None = None,
        start_time_ms: int | None = None,
        limit: int = 1000,
    ) -> list[dict]:
        return await self._request(
            "GET",
            "/fapi/v1/userTrades",
            {"symbol": symbol, "orderId": order_id, "startTime": start_time_ms, "limit": limit},
            signed=True,
        )

    # ------------------------------------------------------------------
    # algo (conditional) orders: stops and take-profits
    # ------------------------------------------------------------------
    async def new_algo_order(self, **params: Any) -> dict:
        return await self._request(
            "POST", "/fapi/v1/algoOrder", {"algoType": "CONDITIONAL", **params}, signed=True
        )

    async def cancel_algo_order(
        self, client_algo_id: str | None = None, algo_id: str | int | None = None
    ) -> dict:
        return await self._request(
            "DELETE",
            "/fapi/v1/algoOrder",
            {"clientAlgoId": client_algo_id, "algoId": algo_id},
            signed=True,
        )

    async def cancel_all_algo_orders(self, symbol: str) -> Any:
        return await self._request(
            "DELETE", "/fapi/v1/algoOpenOrders", {"symbol": symbol}, signed=True
        )

    async def get_algo_order(self, client_algo_id: str) -> dict:
        return await self._request(
            "GET", "/fapi/v1/algoOrder", {"clientAlgoId": client_algo_id}, signed=True
        )

    async def open_algo_orders(self, symbol: str | None = None) -> list[dict]:
        body = await self._request(
            "GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol}, signed=True
        )
        if isinstance(body, dict):  # tolerate a wrapped list
            body = body.get("orders") or body.get("rows") or []
        return body

    # ------------------------------------------------------------------
    # positions and balances
    # ------------------------------------------------------------------
    async def position_risk(self, symbol: str | None = None) -> list[dict]:
        return await self._request("GET", "/fapi/v3/positionRisk", {"symbol": symbol}, signed=True)

    async def income(self, start_time_ms: int | None = None, limit: int = 1000) -> list[dict]:
        """Income history (``REALIZED_PNL``, ``COMMISSION``, ``FUNDING_FEE``, ...); without
        ``startTime`` Binance returns only the last 7 days."""
        return await self._request(
            "GET", "/fapi/v1/income", {"startTime": start_time_ms, "limit": limit}, signed=True
        )

    async def account(self) -> dict:
        return await self._request("GET", "/fapi/v3/account", signed=True)

    # ------------------------------------------------------------------
    # user data stream
    # ------------------------------------------------------------------
    async def create_listen_key(self) -> str:
        body = await self._request("POST", "/fapi/v1/listenKey", api_key=True)
        return body["listenKey"]

    async def keepalive_listen_key(self) -> None:
        await self._request("PUT", "/fapi/v1/listenKey", api_key=True)


# ---------------------------------------------------------------------------
# private user-data stream
# ---------------------------------------------------------------------------


class BinanceUserStream:
    """Private user-data socket: ``ORDER_TRADE_UPDATE``, ``ALGO_UPDATE``, ``ACCOUNT_UPDATE``.

    Same lifecycle as :class:`signal_bot.delta.client.DeltaWebSocket`: ``start()``,
    ``stop()``, ``connected``; raw messages go to *sink* (an async callable or a queue).
    A listenKey is created on each connect and kept alive every 30 minutes.  Binance closes
    every connection after 24 hours; the loop reconnects.
    """

    def __init__(
        self,
        client: BinanceFuturesClient,
        sink,
        on_connect=None,
        on_disconnect=None,
        heartbeat: float = 20.0,
        ws_path: str | None = None,
    ):
        self.client = client
        self.sink = sink
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.heartbeat = heartbeat
        self.ws_path = ws_path or client.user_ws_path
        self.connected = False
        self._running = False
        self._task: asyncio.Task | None = None
        self._reported_down = False

    def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="binance_user_stream")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                await self._connect_and_listen()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except BinanceAPIError as exc:
                logger.error("Binance listenKey request failed: %s", exc)
                await self._mark_disconnected(f"listen_key: {exc.status}")
                await asyncio.sleep(min(backoff * 5, 300.0))
                backoff = min(backoff * 2, 60.0)
            except Exception as exc:
                logger.exception("Binance user stream error; reconnecting in %.0fs", backoff)
                await self._mark_disconnected(f"error: {type(exc).__name__}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
            else:
                await self._mark_disconnected("closed")
                if self._running:
                    await asyncio.sleep(1.0)

    async def _emit(self, msg: dict) -> None:
        if isinstance(self.sink, asyncio.Queue):
            await self.sink.put(msg)
        else:
            await self.sink(msg)

    async def _mark_disconnected(self, reason: str) -> None:
        was, self.connected = self.connected, False
        if was:
            self._reported_down = False
        if self._reported_down:
            return
        self._reported_down = True
        if self.on_disconnect is not None:
            try:
                await self.on_disconnect(reason)
            except Exception:
                logger.exception("on_disconnect callback failed")

    async def _keepalive(self) -> None:
        while True:
            await asyncio.sleep(LISTEN_KEY_KEEPALIVE_SECONDS)
            try:
                await self.client.keepalive_listen_key()
            except Exception:
                logger.exception("Binance listenKey keepalive failed")

    async def _connect_and_listen(self) -> None:
        session = self.client._session
        if session is None:
            raise RuntimeError("BinanceFuturesClient session not initialized")
        listen_key = await self.client.create_listen_key()
        url = f"{self.client.ws_url}{self.ws_path}{listen_key}"
        keepalive = asyncio.create_task(self._keepalive(), name="binance_listen_key_keepalive")
        try:
            async with session.ws_connect(url, heartbeat=self.heartbeat) as ws:
                logger.info("Binance %s user-data stream connected", self.client.mode)
                self.connected = True
                self._reported_down = False
                if self.on_connect is not None:
                    await self.on_connect()
                async for message in ws:
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(data, dict) and data.get("e") == "listenKeyExpired":
                            logger.warning("Binance listenKey expired; reconnecting")
                            return
                        await self._emit(data)
                    elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        return
        finally:
            keepalive.cancel()


# ---------------------------------------------------------------------------
# read-only check
# ---------------------------------------------------------------------------


async def _probe_socket(client: BinanceFuturesClient, path: str, seconds: float) -> dict:
    """Open the user stream on *path* for a few seconds; report whether it connected."""
    out: dict[str, Any] = {"path": path}
    try:
        key = await client.create_listen_key()
        async with client._session.ws_connect(f"{client.ws_url}{path}{key}", heartbeat=10.0) as ws:
            out["connected"] = True
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=seconds)
                out["first_message"] = str(msg.type)
            except TimeoutError:
                out["first_message"] = "none (normal while the account is idle)"
    except aiohttp.WSServerHandshakeError as exc:
        out["connected"] = False
        out["error"] = f"handshake HTTP {exc.status}"
    except Exception as exc:
        out["connected"] = False
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


async def _cli_check(symbols: list[str]) -> dict:
    report: dict[str, Any] = {"mode": settings.binance_trade_mode}
    async with BinanceFuturesClient(symbols=symbols) as client:
        report["rest_url"] = client.rest_url
        report["clock_offset_ms"] = client.time_offset_ms
        report["tradable"] = sorted(client.rules)
        report["not_tradable"] = client.missing
        report["rules"] = {
            s: {k: str(v) for k, v in asdict(r).items() if k != "symbol"}
            for s, r in sorted(client.rules.items())
        }
        if not (client.api_key and client.api_secret):
            report["auth"] = "skipped: BINANCE_FUTURES_API_KEY / _SECRET not set"
            return report
        try:
            report["hedge_mode"] = await client.position_mode()
            account = await client.account()
            report["auth"] = "ok"
            report["wallet_balance"] = account.get("totalWalletBalance")
            report["margin_balance"] = account.get("totalMarginBalance")
            report["available_balance"] = account.get("availableBalance")
            report["open_positions"] = [
                {"symbol": p.get("symbol"), "positionAmt": p.get("positionAmt")}
                for p in await client.position_risk()
                if Decimal(str(p.get("positionAmt") or 0)) != 0
            ]
            report["open_orders"] = len(await client.open_orders())
            report["open_algo_orders"] = len(await client.open_algo_orders())
        except BinanceAPIError as exc:
            report["auth"] = f"FAILED: {exc.message}"
            return report
        # The venue's prices against the production feed the strategy reads.
        basis = {}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as public:
            for s in sorted(client.rules)[:3]:
                venue = await client.depth(s, 5)
                mid = (Decimal(venue["bids"][0][0]) + Decimal(venue["asks"][0][0])) / 2
                async with public.get(
                    "https://fapi.binance.com/fapi/v1/ticker/price", params={"symbol": s}
                ) as r:
                    ref = Decimal((await r.json())["price"])
                basis[s] = {
                    "venue_mid": str(mid),
                    "production": str(ref),
                    "basis_pct": f"{(mid - ref) / ref * 100:.4f}",
                }
        report["price_vs_production"] = basis
        report["user_stream"] = [
            await _probe_socket(client, path, 5.0)
            for path in dict.fromkeys([client.user_ws_path, "/private/ws/", "/ws/"])
        ]
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Binance USD-M futures connectivity check")
    ap.add_argument("--check", action="store_true", required=True, help="read-only: no orders")
    ap.add_argument(
        "--symbols", default=None, help="comma-separated; default: the strategy universe"
    )
    args = ap.parse_args()
    if args.symbols:
        symbols = args.symbols.split(",")
    else:
        import tomllib

        # the bot's own lookup: relative paths also resolve inside the installed package
        from signal_bot.main import resolve_strategy_config

        with resolve_strategy_config(settings.strategy_config).open("rb") as fh:
            doc = tomllib.load(fh)
        symbols = list(doc.get("universe", {}).get("symbols") or settings.symbols)
    print(json.dumps(asyncio.run(_cli_check(symbols)), indent=2, default=str))


if __name__ == "__main__":
    main()
