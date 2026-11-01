"""Delta Exchange India implementation of :class:`ExchangeAdapter`.

Responsibilities beyond plain REST calls:

* classify every failure into the adapter error taxonomy (rejected / ambiguous /
  auth / unavailable) — the execution engine's safety depends on it;
* normalise Delta REST and WebSocket payloads into venue-neutral objects and
  bus events with deterministic ids (so redelivered messages dedupe).

Socket messages follow the documented private channels (``orders``,
``positions``, ``user_trades`` / ``v2/user_trades``): flat update messages and
``action: "snapshot"`` messages with a ``result`` list.  The legacy
``{"channel": ..., "data": {...}}`` shape is still accepted.  Field mappings
marked ``VERIFY`` need confirming with a real testnet fill before live use.
"""

from __future__ import annotations

import logging
import time
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import aiohttp

from signal_bot.delta.client import DeltaClient
from signal_bot.events.bus import EventBus
from signal_bot.events.envelope import Event, EventType, deterministic_event_id
from signal_bot.execution.adapter import (
    AmbiguousOrderOutcome,
    AuthenticationFailed,
    BookLevel,
    BookSnapshot,
    ExchangeFill,
    ExchangeOrder,
    ExchangePosition,
    ExchangeUnavailable,
    FillPurpose,
    InstrumentSpec,
    OrderRejected,
    OrderRequest,
    VenueOrderState,
)
from signal_bot.observability import metrics

logger = logging.getLogger(__name__)

VENUE = "delta"


def _dec(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def _ts(value: Any) -> datetime:
    """Delta timestamps are microseconds (int) or ISO strings."""
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, int | float):
        seconds = value / 1_000_000 if value > 1e14 else value / 1000 if value > 1e11 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def _result(resp: Any) -> Any:
    if isinstance(resp, dict) and "result" in resp:
        return resp["result"]
    return resp


def _venue_state(raw: dict[str, Any]) -> VenueOrderState:
    state = str(raw.get("state", "")).lower()
    size = _dec(raw.get("size"))
    unfilled = _dec(raw.get("unfilled_size"), str(size))
    filled = size - unfilled
    if state in ("filled", "closed") and unfilled == 0:
        return VenueOrderState.FILLED
    if state in ("cancelled", "canceled", "closed"):
        return VenueOrderState.CANCELLED  # "closed" with unfilled > 0: IOC remainder cancelled
    if state == "rejected":
        return VenueOrderState.REJECTED
    if state in ("open", "pending"):
        return VenueOrderState.PARTIALLY_FILLED if filled > 0 else VenueOrderState.OPEN
    return VenueOrderState.OPEN


def _purpose(raw: dict[str, Any]) -> FillPurpose:
    # VERIFY: bracket legs carry stop_order_type on Delta.
    kind = str(raw.get("stop_order_type") or raw.get("order_purpose") or "").lower()
    if "stop_loss" in kind:
        return FillPurpose.STOP_LOSS
    if "take_profit" in kind:
        return FillPurpose.TAKE_PROFIT
    if raw.get("reduce_only"):
        return FillPurpose.CLOSE
    return FillPurpose.ENTRY


def parse_order(raw: dict[str, Any]) -> ExchangeOrder:
    size = _dec(raw.get("size"))
    unfilled = _dec(raw.get("unfilled_size"), str(size))
    avg = raw.get("average_fill_price")
    return ExchangeOrder(
        client_order_id=str(raw.get("client_order_id") or ""),
        exchange_order_id=str(raw.get("id") or raw.get("order_id") or "") or None,
        symbol=str(raw.get("product_symbol") or raw.get("symbol") or ""),
        side=str(raw.get("side", "")).lower(),
        size=size,
        filled_size=size - unfilled,
        avg_fill_price=_dec(avg) if avg not in (None, "") else None,
        state=_venue_state(raw),
        updated_at=_ts(raw.get("updated_at") or raw.get("timestamp") or raw.get("created_at")),
        fees=_dec(raw.get("paid_commission") or raw.get("commission")),
        reason=raw.get("cancellation_reason") or raw.get("reason"),
        purpose=_purpose(raw),
    )


def parse_book(symbol: str, resp: Any) -> BookSnapshot:
    body = _result(resp) or {}

    def levels(rows) -> tuple[BookLevel, ...]:
        out = []
        for row in rows or []:
            if isinstance(row, dict):
                out.append(BookLevel(_dec(row.get("price")), _dec(row.get("size"))))
            else:
                out.append(BookLevel(_dec(row[0]), _dec(row[1] if len(row) > 1 else 0)))
        return tuple(out)

    bids = levels(body.get("buy") or body.get("bids"))
    asks = levels(body.get("sell") or body.get("asks"))
    ts = _ts(body.get("last_updated_at") or body.get("timestamp")) if body else None
    return BookSnapshot(symbol, bids, asks, ts or datetime.now(timezone.utc))


class DeltaExchangeAdapter:
    venue = VENUE

    def __init__(self, client: DeltaClient) -> None:
        self.client = client

    # ------------------------------------------------------------------
    def instrument(self, binance_symbol: str) -> InstrumentSpec:
        sm = self.client.symbol_map
        product = sm.get_product(binance_symbol)
        return InstrumentSpec(
            symbol=sm.get_delta_symbol(binance_symbol),
            product_id=int(product["id"]),
            tick_size=_dec(product.get("tick_size"), "1"),
            lot_size=_dec(product.get("min_size") or product.get("minimum_order_size"), "1"),
            contract_value=_dec(product.get("contract_value"), "1"),
        )

    def binance_symbol(self, venue_symbol: str) -> str | None:
        try:
            return self.client.symbol_map.get_binance_symbol(venue_symbol)
        except KeyError:
            return None

    def _product_id(self, venue_symbol: str) -> int:
        product = self.client.symbol_map._products.get(venue_symbol)
        if product is None:
            raise KeyError(venue_symbol)
        return int(product["id"])

    async def _call(self, operation: str, coro):
        start = time.perf_counter()
        outcome = "ok"
        try:
            return await coro
        except Exception:
            outcome = "error"
            raise
        finally:
            metrics.EXCHANGE_LATENCY.labels(VENUE, operation, outcome).observe(
                time.perf_counter() - start
            )

    # ------------------------------------------------------------------
    async def order_book(self, symbol: str) -> BookSnapshot:
        try:
            resp = await self._call("order_book", self.client.get_l2_orderbook(symbol))
        except Exception as exc:
            raise ExchangeUnavailable(f"order book: {exc}") from exc
        return parse_book(symbol, resp)

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        try:
            await self._call(
                "set_leverage", self.client.set_leverage(self._product_id(symbol), leverage)
            )
        except aiohttp.ClientResponseError as exc:
            if exc.status in (401, 403):
                raise AuthenticationFailed(str(exc)) from exc
            raise ExchangeUnavailable(str(exc)) from exc

    async def place_order(self, request: OrderRequest) -> ExchangeOrder:
        try:
            resp = await self._call(
                "place_order",
                self.client.place_bracket_order(
                    symbol=request.symbol,
                    side=request.side,
                    size=int(request.size),
                    entry_price=request.limit_price,
                    stop_loss_price=request.stop_loss_price,
                    take_profit_price=request.take_profit_price,
                    client_order_id=request.client_order_id,
                    order_type=request.order_type,
                ),
            )
        except aiohttp.ClientConnectorError as exc:
            # TCP connect failed: the request never left this host.
            raise OrderRejected(f"connection_failed_before_send: {exc}") from exc
        except aiohttp.ClientResponseError as exc:
            if exc.status in (401, 403):
                raise AuthenticationFailed(str(exc)) from exc
            if 400 <= exc.status < 500:
                # 4xx (incl. 429): the venue processed and refused the request.
                raise OrderRejected(f"http_{exc.status}: {exc.message}") from exc
            raise AmbiguousOrderOutcome(f"http_{exc.status}: {exc.message}") from exc
        except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
            raise AmbiguousOrderOutcome(f"{type(exc).__name__}: {exc}") from exc

        body = _result(resp)
        if isinstance(resp, dict) and resp.get("success") is False:
            raise OrderRejected(str(resp.get("error") or resp))
        if not isinstance(body, dict) or not (body.get("id") or body.get("order_id")):
            # 2xx without an order id: we cannot prove acceptance or rejection.
            raise AmbiguousOrderOutcome(f"unrecognised placement response: {str(resp)[:200]}")
        body.setdefault("client_order_id", request.client_order_id)
        body.setdefault("product_symbol", request.symbol)
        body.setdefault("size", str(request.size))
        return parse_order(body)

    async def get_order(self, client_order_id: str) -> ExchangeOrder | None:
        try:
            resp = await self._call(
                "get_order", self.client.get_order_by_client_id(client_order_id)
            )
        except aiohttp.ClientResponseError as exc:
            if exc.status == 404:
                return None
            if exc.status in (401, 403):
                raise AuthenticationFailed(str(exc)) from exc
            raise ExchangeUnavailable(str(exc)) from exc
        except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
            raise ExchangeUnavailable(str(exc)) from exc
        body = _result(resp)
        if not body:
            return None
        return parse_order(body)

    async def cancel_order(self, client_order_id: str, exchange_order_id: str | None) -> None:
        # Delta's DELETE /v2/orders needs the product as well as the order id.
        found = await self.get_order(client_order_id)
        if found is None or not (exchange_order_id or found.exchange_order_id):
            return
        exchange_order_id = exchange_order_id or found.exchange_order_id
        try:
            product_id = self._product_id(found.symbol)
        except KeyError:
            product_id = None
        try:
            await self._call(
                "cancel_order",
                self.client.cancel_order(
                    exchange_order_id, product_id=product_id, product_symbol=found.symbol or None
                ),
            )
        except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
            raise ExchangeUnavailable(str(exc)) from exc

    async def open_orders(self) -> list[ExchangeOrder]:
        try:
            resp = await self._call("open_orders", self.client.get_open_orders())
        except Exception as exc:
            raise ExchangeUnavailable(str(exc)) from exc
        rows = _result(resp)
        if isinstance(resp, dict) and "orders" in resp:
            rows = resp["orders"]
        return [parse_order(r) for r in (rows or []) if isinstance(r, dict)]

    async def positions(self) -> list[ExchangePosition]:
        out: list[ExchangePosition] = []
        # A stop counts as present if the position carries bracket SL fields *or* a
        # reduce-side stop-loss order is resting for that product (VERIFY on testnet).
        stop_orders = {
            o.symbol for o in await self.open_orders() if o.purpose == FillPurpose.STOP_LOSS
        }
        for venue_symbol, product in self.client.symbol_map._products.items():
            if venue_symbol not in self.client.symbol_map._delta_to_binance:
                continue
            try:
                resp = await self._call(
                    "positions", self.client.get_open_positions(product_id=int(product["id"]))
                )
            except Exception as exc:
                raise ExchangeUnavailable(f"positions {venue_symbol}: {exc}") from exc
            rows = _result(resp)
            if isinstance(resp, dict) and "positions" in resp:
                rows = resp["positions"]
            if isinstance(rows, dict):
                rows = [rows]
            for row in rows or []:
                size = _dec(row.get("size"))
                if size == 0:
                    continue
                out.append(
                    ExchangePosition(
                        symbol=str(row.get("product_symbol") or venue_symbol),
                        size=size,
                        entry_price=_dec(row.get("entry_price"))
                        if row.get("entry_price")
                        else None,
                        # VERIFY: Delta exposes attached bracket prices on the position.
                        has_stop_loss=bool(row.get("bracket_stop_loss_price"))
                        or str(row.get("product_symbol") or venue_symbol) in stop_orders,
                        has_take_profit=bool(row.get("bracket_take_profit_price")),
                        stop_loss_price=_dec(row["bracket_stop_loss_price"])
                        if row.get("bracket_stop_loss_price")
                        else None,
                        updated_at=_ts(row.get("updated_at")),
                    )
                )
        return out

    async def fills_for(self, client_order_id: str) -> list[ExchangeFill]:
        order = await self.get_order(client_order_id)
        if order is None or not order.exchange_order_id:
            return []
        try:
            pid = self._product_id(order.symbol)
            resp = await self._call("fills", self.client.get_fills(product_id=pid))
        except Exception as exc:
            raise ExchangeUnavailable(str(exc)) from exc
        out = []
        for row in _result(resp) or []:
            if str(row.get("order_id")) != order.exchange_order_id:
                continue
            # Fills of the entry order itself: entries by definition.
            out.append(replace(parse_fill(row, client_order_id), purpose=FillPurpose.ENTRY))
        return out

    async def exit_fills(
        self, symbol: str, client_order_id: str, since: datetime | None = None
    ) -> list[ExchangeFill]:
        """Delta bracket children carry no client id: match by product, side and time."""
        order = await self.get_order(client_order_id)
        if order is None:
            return []
        exit_side = "sell" if order.side == "buy" else "buy"
        try:
            resp = await self._call(
                "fills", self.client.get_fills(product_id=self._product_id(symbol))
            )
        except Exception as exc:
            raise ExchangeUnavailable(str(exc)) from exc
        out = []
        for row in _result(resp) or []:
            fill = parse_fill(row, client_order_id)
            if fill.side != exit_side or str(row.get("order_id")) == order.exchange_order_id:
                continue
            if since is not None and fill.occurred_at < since:
                continue
            if fill.purpose == FillPurpose.ENTRY:
                fill = replace(fill, purpose=FillPurpose.UNKNOWN)
            out.append(fill)
        return out

    async def place_protective_stop(
        self, symbol: str, side: str, size: Decimal, stop_price: Decimal, client_order_id: str
    ) -> ExchangeOrder:
        try:
            resp = await self._call(
                "protective_stop",
                self.client.attach_position_bracket(self._product_id(symbol), stop_price),
            )
        except aiohttp.ClientResponseError as exc:
            raise OrderRejected(f"http_{exc.status}: {exc.message}") from exc
        except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
            raise AmbiguousOrderOutcome(str(exc)) from exc
        body = _result(resp) or {}
        body.setdefault("client_order_id", client_order_id)
        body.setdefault("product_symbol", symbol)
        body.setdefault("size", str(size))
        body.setdefault("state", "open")
        body.setdefault("stop_order_type", "stop_loss_order")
        return parse_order(body)

    async def close_position(self, symbol: str, client_order_id: str) -> ExchangeOrder:
        try:
            resp = await self._call(
                "close_position",
                self.client.place_market_close(symbol, self._product_id(symbol)),
            )
        except aiohttp.ClientResponseError as exc:
            raise OrderRejected(f"http_{exc.status}: {exc.message}") from exc
        except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
            raise AmbiguousOrderOutcome(str(exc)) from exc
        body = _result(resp) or {}
        body.setdefault("client_order_id", client_order_id)
        body.setdefault("product_symbol", symbol)
        body.setdefault("reduce_only", True)
        return parse_order(body)

    async def available_balance(self) -> Decimal:
        resp = await self._call("balance", self.client.get_balance())
        rows = _result(resp)
        if isinstance(resp, dict) and "balances" in resp:
            rows = resp["balances"]
        for row in rows or []:
            asset = str(row.get("asset_symbol") or row.get("asset") or "").upper()
            if asset in ("USD", "USDT"):
                return _dec(row.get("available_balance") or row.get("balance"))
        raise ExchangeUnavailable("no USD/USDT balance row in wallet response")


def _fill_purpose(row: dict[str, Any]) -> FillPurpose:
    """A fill message can't say whether it opened or closed our trade.

    Liquidation/ADL are explicit; otherwise the purpose stays UNKNOWN and the order
    store classifies it against the local order's side (same side = entry).
    """
    reason = str(row.get("reason") or row.get("R") or row.get("fill_type") or "").lower()
    if reason in ("liquidation", "adl"):
        return FillPurpose.LIQUIDATION
    purpose = _purpose(row)
    return FillPurpose.UNKNOWN if purpose == FillPurpose.ENTRY else purpose


def parse_fill(row: dict[str, Any], client_order_id: str | None = None) -> ExchangeFill:
    """Parse a fill from REST /v2/fills, the ``user_trades`` channel or ``v2/user_trades``.

    v2/user_trades uses short keys: f=fill id, o=order id, c=client order id, S=side,
    s=size, p=price, r=role, t=timestamp (µs), R=reason.  It carries no commission,
    so ``c`` must never be read as a fee.
    """
    fill_id = row.get("fill_id") or row.get("f") or row.get("id")
    role = str(row.get("role") or row.get("r") or "").lower()
    return ExchangeFill(
        fill_id=f"delta:{fill_id}",
        client_order_id=client_order_id or row.get("client_order_id") or row.get("c") or None,
        exchange_order_id=str(row.get("order_id") or row.get("o") or "") or None,
        symbol=str(row.get("product_symbol") or row.get("symbol") or row.get("sy") or ""),
        side=str(row.get("side") or row.get("S") or "").lower(),
        qty=_dec(row.get("size") or row.get("s")),
        price=_dec(row.get("price") or row.get("p")),
        fee=_dec(row.get("commission")),
        liquidity="maker" if role == "maker" else "taker",
        purpose=_fill_purpose(row),
        occurred_at=_ts(row.get("created_at") or row.get("timestamp") or row.get("t")),
    )


# ---------------------------------------------------------------------------
# WebSocket → bus
# ---------------------------------------------------------------------------


ORDER_CHANNELS = ("orders", "private_orders")
POSITION_CHANNELS = ("positions", "private_positions")
FILL_CHANNELS = ("user_trades", "v2/user_trades", "fills")
CONTROL_TYPES = ("heartbeat", "subscriptions", "key-auth", "unauth", "pong")


def _items(msg: dict[str, Any]) -> list[dict[str, Any]]:
    """Updates are flat messages; snapshots carry a ``result`` list (action=snapshot)."""
    if isinstance(msg.get("data"), dict):  # legacy {"channel": ..., "data": {...}} shape
        return [msg["data"]]
    if msg.get("action") == "snapshot" and isinstance(msg.get("result"), list):
        symbol = msg.get("symbol")
        return [{**row, "symbol": row.get("symbol") or symbol} for row in msg["result"]]
    return [msg]


def _order_event(raw: dict[str, Any]) -> Event | None:
    order = parse_order(raw)
    if not order.client_order_id:
        return None
    return Event.create(
        EventType.EXCHANGE_ORDER_UPDATE,
        order.to_payload(VENUE),
        source=VENUE,
        event_id=deterministic_event_id(
            VENUE,
            "order",
            order.exchange_order_id,
            order.state,
            order.filled_size,
            order.updated_at.isoformat(),
        ),
        occurred_at=order.updated_at,
    )


def _position_event(raw: dict[str, Any]) -> Event:
    symbol = str(raw.get("product_symbol") or raw.get("symbol") or "")
    pos = ExchangePosition(
        symbol=symbol,
        size=_dec(raw.get("size")),
        entry_price=_dec(raw.get("entry_price")) if raw.get("entry_price") else None,
        updated_at=_ts(raw.get("updated_at") or raw.get("timestamp")),
        extra={"product_id": raw.get("product_id")},
    )
    payload = pos.to_payload(VENUE)
    payload["product_id"] = raw.get("product_id")
    return Event.create(
        EventType.EXCHANGE_POSITION_UPDATE,
        payload,
        source=VENUE,
        event_id=deterministic_event_id(VENUE, "position", symbol, pos.size, payload["updated_at"]),
        occurred_at=pos.updated_at,
    )


def _fill_event(raw: dict[str, Any]) -> Event:
    fill = parse_fill(raw)
    return Event.create(
        EventType.EXCHANGE_FILL,
        fill.to_payload(VENUE),
        source=VENUE,
        event_id=deterministic_event_id(VENUE, "fill", fill.fill_id),
        occurred_at=fill.occurred_at,
    )


def normalize_ws_message(msg: dict[str, Any]) -> list[Event]:
    """Translate one raw Delta socket message into zero or more bus events."""
    if not isinstance(msg, dict):
        return []
    channel = msg.get("channel") or msg.get("type") or ""
    if channel in CONTROL_TYPES:
        return []
    events: list[Event] = []
    for raw in _items(msg):
        if channel in ORDER_CHANNELS:
            event = _order_event(raw)
            if event is not None:
                events.append(event)
        elif channel in POSITION_CHANNELS:
            events.append(_position_event(raw))
        elif channel in FILL_CHANNELS:
            events.append(_fill_event(raw))
    return events


class DeltaStreamBridge:
    """Sink for :class:`DeltaWebSocket`: normalises messages and publishes them."""

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus

    async def handle_raw(self, msg: dict[str, Any]) -> None:
        for event in normalize_ws_message(msg):
            await self.bus.publish(event)

    async def on_connect(self) -> None:
        metrics.EXCHANGE_STREAM_CONNECTED.labels(VENUE).set(1)
        await self.bus.publish(Event.create(EventType.STREAM_CONNECTED, {"venue": VENUE}, VENUE))

    async def on_disconnect(self, reason: str) -> None:
        metrics.EXCHANGE_STREAM_CONNECTED.labels(VENUE).set(0)
        await self.bus.publish(
            Event.create(EventType.STREAM_DISCONNECTED, {"venue": VENUE, "reason": reason}, VENUE)
        )
