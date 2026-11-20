"""Binance USD-M futures implementation of :class:`ExchangeAdapter` (demo and production).

How the venue-neutral model maps onto Binance:

* **Contracts.**  The engine sizes in whole contracts.  On Binance one contract is one
  ``stepSize`` of the coin (``InstrumentSpec.contract_value = stepSize``), so a position of
  ``n`` contracts is ``n * stepSize`` coins.  Quantities are converted at this boundary only.
* **Brackets.**  Binance has no entry-plus-bracket order.  An entry is a ``LIMIT``/``IOC``
  order; as soon as it fills, a ``STOP_MARKET`` (and, if set, a ``TAKE_PROFIT_MARKET``)
  *algo order* is attached, reduce-only for the filled quantity, before :meth:`place_order`
  returns.
  If attaching fails the engine's protection check sees no stop and re-attaches through
  :meth:`place_protective_stop`, or flattens.
* **Stops cannot be edited.**  :meth:`amend_stop` places the new stop first and then
  cancels the old one, so the position is never without a stop.  Client algo ids alternate
  ``<coid>S0`` / ``<coid>S1`` (Binance ids are unique among open orders, max 36 chars).
* **Leftover conditional orders.**  A stop left over from a closed position would reduce the
  *next* position on that symbol.  Before every entry the symbol's open algo
  orders are cancelled (the symbol must be flat), and the stream bridge sweeps a symbol once
  its position goes flat.
* **Order lookup needs the symbol.**  Symbols are remembered per client order id; after a
  restart the tradable symbols are searched.
* Position mode is one-way, Multi-Assets mode is switched off and margin is isolated (set
  before the first order on a symbol).  Exits are reduce-only, never ``closePosition``:
  Binance allows one closePosition stop per direction (``-4130``).

Error taxonomy (see :mod:`signal_bot.execution.adapter`): 401/403 and key errors are
authentication failures; ``-1007``, 408 and 5xx on a placement are *ambiguous* (Binance:
"execution status unknown"); other 4xx are definitive rejections.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import replace
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any

import aiohttp

from signal_bot.binance.client import (
    CODE_ORDER_NOT_FOUND,
    CODE_UNKNOWN_EXECUTION,
    CODE_UNKNOWN_ORDER_CANCEL,
    CODE_WOULD_TRIGGER,
    BinanceAPIError,
    BinanceFuturesClient,
    SymbolRules,
)
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

VENUE = "binance"
AUTH_CODES = (-2014, -2015, -1022)  # bad key format / invalid key, IP or permissions / signature
STOP_TYPES = ("STOP_MARKET", "STOP")
TAKE_PROFIT_TYPES = ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")
WORKING_TYPE = "CONTRACT_PRICE"  # trigger on the traded price, as the research fill model does
FLAT_SWEEP_DELAY_SECONDS = 2.0


def _dec(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def _ts(ms: Any) -> datetime:
    if ms in (None, "", 0):
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


_STATES = {
    "NEW": VenueOrderState.OPEN,
    "PARTIALLY_FILLED": VenueOrderState.PARTIALLY_FILLED,
    "FILLED": VenueOrderState.FILLED,
    "CANCELED": VenueOrderState.CANCELLED,
    "EXPIRED": VenueOrderState.EXPIRED,  # e.g. the unfilled rest of an IOC order
    "EXPIRED_IN_MATCH": VenueOrderState.EXPIRED,
    "REJECTED": VenueOrderState.REJECTED,
}

_ALGO_STATES = {
    "NEW": VenueOrderState.OPEN,
    "TRIGGERING": VenueOrderState.OPEN,
    "TRIGGERED": VenueOrderState.OPEN,
    "FINISHED": VenueOrderState.FILLED,
    "CANCELED": VenueOrderState.CANCELLED,
    "EXPIRED": VenueOrderState.EXPIRED,
    "REJECTED": VenueOrderState.REJECTED,
}


def _purpose(order_type: str, client_id: str, reduce_only: bool) -> FillPurpose:
    order_type = (order_type or "").upper()
    if order_type in STOP_TYPES:
        return FillPurpose.STOP_LOSS
    if order_type in TAKE_PROFIT_TYPES:
        return FillPurpose.TAKE_PROFIT
    if order_type == "LIQUIDATION" or str(client_id).startswith(("autoclose", "adl_autoclose")):
        return FillPurpose.LIQUIDATION
    if reduce_only:
        return FillPurpose.CLOSE
    return FillPurpose.ENTRY


def sl_client_id(coid: str, previous: str | None = None) -> str:
    """Client algo id for a stop: ``<coid>S0`` or, replacing ``...S0``, ``<coid>S1``."""
    return f"{coid}S1" if previous and previous.endswith("S0") else f"{coid}S0"


def parse_order(raw: dict[str, Any], step: Decimal) -> ExchangeOrder:
    """A regular order from REST (``/fapi/v1/order``)."""
    orig_type = raw.get("origType") or raw.get("type") or ""
    avg = raw.get("avgPrice")
    stop = raw.get("stopPrice")
    status = str(raw.get("status") or "").upper()
    return ExchangeOrder(
        client_order_id=str(raw.get("clientOrderId") or ""),
        exchange_order_id=str(raw["orderId"]) if raw.get("orderId") is not None else None,
        symbol=str(raw.get("symbol") or ""),
        side=str(raw.get("side") or "").lower(),
        size=_dec(raw.get("origQty")) / step,
        filled_size=_dec(raw.get("executedQty")) / step,
        avg_fill_price=_dec(avg) if avg not in (None, "") and _dec(avg) > 0 else None,
        state=_STATES.get(status, VenueOrderState.OPEN),
        updated_at=_ts(raw.get("updateTime") or raw.get("time")),
        reason=status.lower() if status in ("EXPIRED", "EXPIRED_IN_MATCH") else None,
        purpose=_purpose(orig_type, raw.get("clientOrderId") or "", bool(raw.get("reduceOnly"))),
        stop_price=_dec(stop) if stop not in (None, "") and _dec(stop) > 0 else None,
    )


def parse_algo_order(raw: dict[str, Any], step: Decimal) -> ExchangeOrder:
    """An algo (conditional) order from REST (``/fapi/v1/algoOrder``, ``openAlgoOrders``)."""
    order_type = str(raw.get("orderType") or raw.get("type") or "")
    trigger = raw.get("triggerPrice")
    return ExchangeOrder(
        client_order_id=str(raw.get("clientAlgoId") or ""),
        exchange_order_id=str(raw["algoId"]) if raw.get("algoId") is not None else None,
        symbol=str(raw.get("symbol") or ""),
        side=str(raw.get("side") or "").lower(),
        size=_dec(raw.get("quantity")) / step,
        filled_size=_dec(raw.get("actualQty")) / step,
        avg_fill_price=None,
        state=_ALGO_STATES.get(str(raw.get("algoStatus") or "").upper(), VenueOrderState.OPEN),
        updated_at=_ts(raw.get("updateTime") or raw.get("createTime")),
        purpose=_purpose(order_type, "", bool(raw.get("reduceOnly"))),
        stop_price=_dec(trigger) if trigger not in (None, "") else None,
    )


def parse_fill(
    row: dict[str, Any], step: Decimal, client_order_id: str | None = None
) -> ExchangeFill:
    """A fill from REST ``/fapi/v1/userTrades``.  Its purpose is left to the order store."""
    return ExchangeFill(
        fill_id=f"{VENUE}:{row.get('symbol')}:{row.get('id')}",
        client_order_id=client_order_id,
        exchange_order_id=str(row["orderId"]) if row.get("orderId") is not None else None,
        symbol=str(row.get("symbol") or ""),
        side=str(row.get("side") or "").lower(),
        qty=_dec(row.get("qty")) / step,
        price=_dec(row.get("price")),
        fee=_dec(row.get("commission")),
        liquidity="maker" if row.get("maker") else "taker",
        purpose=FillPurpose.UNKNOWN,
        occurred_at=_ts(row.get("time")),
    )


def parse_book(symbol: str, body: dict[str, Any]) -> BookSnapshot:
    def levels(rows) -> tuple[BookLevel, ...]:
        return tuple(BookLevel(_dec(r[0]), _dec(r[1])) for r in rows or [])

    return BookSnapshot(
        symbol,
        levels(body.get("bids")),
        levels(body.get("asks")),
        _ts(body.get("E") or body.get("T")),
    )


class BinanceFuturesAdapter:
    venue = VENUE

    def __init__(self, client: BinanceFuturesClient, margin_type: str = "ISOLATED") -> None:
        self.client = client
        self.margin_type = margin_type
        self._symbol_of: dict[str, str] = {}  # client order id -> symbol
        self._margin_set: set[str] = set()

    # ------------------------------------------------------------------
    # instruments
    # ------------------------------------------------------------------
    def _rules(self, symbol: str) -> SymbolRules:
        return self.client.rules[symbol]

    def instrument(self, binance_symbol: str) -> InstrumentSpec:
        r = self._rules(binance_symbol)
        cap = r.market_max_qty or r.max_qty
        return InstrumentSpec(
            symbol=binance_symbol,
            product_id=0,
            tick_size=r.tick_size,
            lot_size=Decimal("1"),
            contract_value=r.step_size,
            max_contracts=(cap / r.step_size).to_integral_value(ROUND_DOWN) if cap else None,
        )

    def binance_symbol(self, venue_symbol: str) -> str | None:
        return venue_symbol if venue_symbol in self.client.rules else None

    def _step(self, symbol: str) -> Decimal:
        r = self.client.rules.get(symbol)
        return r.step_size if r else Decimal("1")

    def _qty(self, symbol: str, contracts: Decimal) -> Decimal:
        step = self._rules(symbol).step_size
        return (Decimal(contracts) * step).quantize(step, rounding=ROUND_DOWN)

    def _price(self, symbol: str, price: Decimal) -> Decimal:
        tick = self._rules(symbol).tick_size
        return (Decimal(price) / tick).to_integral_value() * tick

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

    @staticmethod
    def _is_auth(exc: BinanceAPIError) -> bool:
        return exc.status in (401, 403) or exc.binance_code in AUTH_CODES

    def _read_error(self, exc: Exception) -> Exception:
        """Classify a failed read or cancel."""
        if isinstance(exc, BinanceAPIError) and self._is_auth(exc):
            return AuthenticationFailed(str(exc))
        if isinstance(exc, AuthenticationFailed | ExchangeUnavailable):
            return exc
        return ExchangeUnavailable(str(exc))

    def _write_error(self, exc: Exception) -> Exception:
        """Classify a failed placement: definitive rejection vs unknown outcome."""
        if isinstance(exc, BinanceAPIError):
            if self._is_auth(exc):
                return AuthenticationFailed(str(exc))
            if exc.binance_code == CODE_UNKNOWN_EXECUTION or exc.status == 408 or exc.status >= 500:
                return AmbiguousOrderOutcome(f"http_{exc.status}: {exc.msg}")
            if exc.binance_code == CODE_WOULD_TRIGGER:
                return OrderRejected(f"would_trigger_immediately: {exc.msg}")
            return OrderRejected(f"binance_{exc.binance_code}: {exc.msg}")
        if isinstance(exc, aiohttp.ClientConnectorError):
            return OrderRejected(f"connection_failed_before_send: {exc}")
        return AmbiguousOrderOutcome(f"{type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------
    # setup
    # ------------------------------------------------------------------
    async def prepare(self) -> None:
        """One-way position mode (one position per symbol) and single-asset margin mode,
        which isolated margin requires.  Both can only change while nothing is open."""
        try:
            if await self.client.position_mode():
                await self.client.set_one_way_mode()
                logger.info("Binance position mode switched to one-way")
            if await self.client.multi_assets_mode():
                try:
                    await self.client.set_single_asset_mode()
                    logger.info("Binance Multi-Assets mode switched off (isolated margin needs it)")
                except BinanceAPIError as exc:
                    if self._is_auth(exc):
                        raise
                    logger.warning(
                        "Multi-Assets mode still on (%s): positions will use cross margin",
                        exc.msg,
                    )
        except BinanceAPIError as exc:
            raise self._read_error(exc) from exc

    def _exit_order(
        self, symbol: str, side: str, kind: str, trigger: Decimal, qty: Decimal, caid: str
    ) -> dict[str, Any]:
        """A reduce-only conditional exit for an exact quantity.

        Not ``closePosition``: Binance allows only one closePosition stop per direction
        (``-4130``), which would make "place the new stop, then cancel the old" impossible.
        A reduce-only order can never open or flip a position.
        """
        return {
            "symbol": symbol,
            "side": side.upper(),
            "type": kind,
            "triggerPrice": self._price(symbol, trigger),
            "quantity": qty,
            "reduceOnly": "true",
            "workingType": WORKING_TYPE,
            "clientAlgoId": caid,
        }

    async def _with_fill_price(self, order: ExchangeOrder) -> ExchangeOrder:
        """Binance may answer a filled IOC order without ``avgPrice``: read it back."""
        if order.filled_size <= 0 or order.avg_fill_price is not None:
            return order
        try:
            raw = await self._call(
                "get_order",
                self.client.get_order(order.symbol, client_order_id=order.client_order_id),
            )
            refreshed = parse_order(raw, self._step(order.symbol))
            if refreshed.avg_fill_price is not None:
                return replace(order, avg_fill_price=refreshed.avg_fill_price)
        except Exception:
            pass  # fall back to the fills
        try:
            rows = await self._call(
                "fills", self.client.user_trades(order.symbol, order_id=order.exchange_order_id)
            )
            qty = sum((_dec(r.get("qty")) for r in rows or []), Decimal("0"))
            if qty > 0:
                value = sum((_dec(r.get("qty")) * _dec(r.get("price")) for r in rows), Decimal("0"))
                return replace(order, avg_fill_price=value / qty)
        except Exception:
            logger.warning("Fill price of %s not read back", order.client_order_id)
        return order

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        try:
            if symbol not in self._margin_set:
                try:
                    await self._call(
                        "margin_type", self.client.set_margin_type(symbol, self.margin_type)
                    )
                except BinanceAPIError as exc:
                    if self._is_auth(exc):
                        raise
                    # e.g. a position or open order on the symbol; leverage still applies
                    logger.warning("Margin type for %s not changed: %s", symbol, exc.msg)
                self._margin_set.add(symbol)
            await self._call("set_leverage", self.client.set_leverage(symbol, leverage))
        except BinanceAPIError as exc:
            raise self._read_error(exc) from exc
        except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
            raise ExchangeUnavailable(str(exc)) from exc

    # ------------------------------------------------------------------
    # market data
    # ------------------------------------------------------------------
    async def order_book(self, symbol: str) -> BookSnapshot:
        try:
            body = await self._call("order_book", self.client.depth(symbol, 20))
        except Exception as exc:
            raise ExchangeUnavailable(f"order book: {exc}") from exc
        return parse_book(symbol, body)

    # ------------------------------------------------------------------
    # orders
    # ------------------------------------------------------------------
    async def _clear_stale_conditionals(self, symbol: str) -> None:
        """Cancel leftover stops/targets on a *flat* symbol before a new entry."""
        rows = await self.client.position_risk(symbol)
        if any(_dec(r.get("positionAmt")) != 0 for r in rows or []):
            raise OrderRejected(f"position_already_open:{symbol}")
        if await self.client.open_algo_orders(symbol):
            await self.client.cancel_all_algo_orders(symbol)
            logger.info("Cancelled leftover conditional orders on %s before entry", symbol)

    async def place_order(self, request: OrderRequest) -> ExchangeOrder:
        symbol = request.symbol
        rules = self._rules(symbol)
        qty = self._qty(symbol, request.size)
        if qty <= 0 or qty < rules.min_qty:
            raise OrderRejected(f"below_min_qty: {qty} < {rules.min_qty}")
        if request.limit_price is not None and qty * request.limit_price < rules.min_notional:
            raise OrderRejected(
                f"below_min_notional: {qty * request.limit_price} < {rules.min_notional}"
            )
        if not request.reduce_only:
            try:
                await self._call("clear_conditionals", self._clear_stale_conditionals(symbol))
            except OrderRejected:
                raise
            except Exception as exc:
                # Nothing was sent: refuse rather than risk an old stop closing the new trade.
                raise OrderRejected(f"stale_conditional_check_failed: {exc}") from exc

        params: dict[str, Any] = {
            "symbol": symbol,
            "side": request.side.upper(),
            "quantity": qty,
            "newClientOrderId": request.client_order_id,
            "newOrderRespType": "RESULT",
        }
        if request.order_type == "market":
            params["type"] = "MARKET"
        else:
            params["type"] = "LIMIT"
            params["timeInForce"] = "IOC" if request.order_type == "limit_ioc" else "GTC"
            params["price"] = self._price(symbol, request.limit_price)
        if request.reduce_only:
            params["reduceOnly"] = "true"

        self._symbol_of[request.client_order_id] = symbol
        try:
            body = await self._call("place_order", self.client.new_order(**params))
        except Exception as exc:
            raise self._write_error(exc) from exc
        if not isinstance(body, dict) or body.get("orderId") is None:
            raise AmbiguousOrderOutcome(f"unrecognised placement response: {str(body)[:200]}")
        if not body.get("symbol"):
            body = {**body, "symbol": symbol}
        order = await self._with_fill_price(parse_order(body, rules.step_size))

        if (
            order.filled_size > 0
            and request.stop_loss_price is not None
            and not request.reduce_only
        ):
            await self._attach_exits(
                symbol,
                request.side,
                request.client_order_id,
                order.filled_size * rules.step_size,
                request.stop_loss_price,
                request.take_profit_price,
            )
        return order

    async def _attach_exits(
        self,
        symbol: str,
        entry_side: str,
        coid: str,
        qty: Decimal,
        stop: Decimal,
        take_profit: Decimal | None,
    ) -> None:
        """Stop (and target) right after the entry fill.  A failure is left to the engine's
        protection check, which re-attaches the stop or flattens the position."""
        exit_side = "SELL" if entry_side.lower() == "buy" else "BUY"
        try:
            await self._call(
                "protective_stop",
                self.client.new_algo_order(
                    **self._exit_order(
                        symbol, exit_side, "STOP_MARKET", stop, qty, sl_client_id(coid)
                    )
                ),
            )
        except Exception:
            logger.exception("Attaching the stop for %s on %s failed", coid, symbol)
        if take_profit is None:
            return
        try:
            await self._call(
                "take_profit",
                self.client.new_algo_order(
                    **self._exit_order(
                        symbol, exit_side, "TAKE_PROFIT_MARKET", take_profit, qty, f"{coid}T0"
                    )
                ),
            )
        except Exception:
            logger.exception("Attaching the take-profit for %s on %s failed", coid, symbol)

    async def _find_symbol(self, client_order_id: str) -> tuple[str | None, dict | None]:
        """(symbol, raw order) for a client order id; (None, None) if no symbol has it."""
        known = self._symbol_of.get(client_order_id)
        candidates = [known] if known else sorted(self.client.rules)
        for symbol in candidates:
            try:
                raw = await self._call(
                    "get_order", self.client.get_order(symbol, client_order_id=client_order_id)
                )
            except BinanceAPIError as exc:
                if exc.binance_code == CODE_ORDER_NOT_FOUND:
                    continue
                raise self._read_error(exc) from exc
            except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
                raise ExchangeUnavailable(str(exc)) from exc
            self._symbol_of[client_order_id] = symbol
            return symbol, raw
        return None, None

    async def get_order(self, client_order_id: str) -> ExchangeOrder | None:
        symbol, raw = await self._find_symbol(client_order_id)
        if raw is None:
            return None
        return parse_order(raw, self._step(symbol))

    async def cancel_order(self, client_order_id: str, exchange_order_id: str | None) -> None:
        symbol = self._symbol_of.get(client_order_id)
        if symbol is None:
            symbol, raw = await self._find_symbol(client_order_id)
            if raw is None:
                return
        try:
            await self._call(
                "cancel_order",
                self.client.cancel_order(
                    symbol,
                    client_order_id=None if exchange_order_id else client_order_id,
                    order_id=exchange_order_id,
                ),
            )
        except BinanceAPIError as exc:
            if exc.binance_code in (CODE_UNKNOWN_ORDER_CANCEL, CODE_ORDER_NOT_FOUND):
                return  # already filled, expired or cancelled
            raise self._read_error(exc) from exc
        except (TimeoutError, aiohttp.ClientError, ConnectionError) as exc:
            raise ExchangeUnavailable(str(exc)) from exc

    async def open_orders(self) -> list[ExchangeOrder]:
        try:
            plain = await self._call("open_orders", self.client.open_orders())
            algos = await self._call("open_orders", self.client.open_algo_orders())
        except Exception as exc:
            raise self._read_error(exc) from exc
        out = [parse_order(r, self._step(r.get("symbol"))) for r in plain or []]
        out += [parse_algo_order(r, self._step(r.get("symbol"))) for r in algos or []]
        return [o for o in out if o.symbol in self.client.rules]

    async def positions(self) -> list[ExchangePosition]:
        try:
            rows = await self._call("positions", self.client.position_risk())
            algos = await self._call("open_orders", self.client.open_algo_orders())
        except Exception as exc:
            raise self._read_error(exc) from exc
        by_symbol: dict[str, list[ExchangeOrder]] = {}
        for r in algos or []:
            o = parse_algo_order(r, self._step(r.get("symbol")))
            by_symbol.setdefault(o.symbol, []).append(o)
        out: list[ExchangePosition] = []
        for row in rows or []:
            symbol = str(row.get("symbol") or "")
            amount = _dec(row.get("positionAmt"))
            if amount == 0 or symbol not in self.client.rules:
                continue
            exit_side = "sell" if amount > 0 else "buy"
            resting = [o for o in by_symbol.get(symbol, []) if o.side == exit_side]
            stops = [o for o in resting if o.purpose == FillPurpose.STOP_LOSS]
            targets = [o for o in resting if o.purpose == FillPurpose.TAKE_PROFIT]
            # Two stops only briefly, while one replaces the other: report the tighter one.
            prices = [o.stop_price for o in stops if o.stop_price is not None]
            stop_price = (max(prices) if amount > 0 else min(prices)) if prices else None
            out.append(
                ExchangePosition(
                    symbol=symbol,
                    size=amount / self._step(symbol),
                    entry_price=_dec(row.get("entryPrice")) if row.get("entryPrice") else None,
                    has_stop_loss=bool(stops),
                    has_take_profit=bool(targets),
                    stop_loss_price=stop_price,
                    take_profit_price=targets[0].stop_price if targets else None,
                    updated_at=_ts(row.get("updateTime")),
                    extra={"unrealized_pnl": row.get("unRealizedProfit")},
                )
            )
        return out

    async def fills_for(self, client_order_id: str) -> list[ExchangeFill]:
        symbol, raw = await self._find_symbol(client_order_id)
        if raw is None or raw.get("orderId") is None:
            return []
        try:
            rows = await self._call(
                "fills", self.client.user_trades(symbol, order_id=raw["orderId"])
            )
        except Exception as exc:
            raise self._read_error(exc) from exc
        step = self._step(symbol)
        return [
            replace(parse_fill(r, step, client_order_id), purpose=FillPurpose.ENTRY)
            for r in rows or []
            if str(r.get("orderId")) == str(raw["orderId"])
        ]

    async def exit_fills(
        self, symbol: str, client_order_id: str, since: datetime | None = None
    ) -> list[ExchangeFill]:
        """Fills on *symbol*, on the exit side, after the entry (stops carry their own ids)."""
        _, raw = await self._find_symbol(client_order_id)
        if raw is None:
            return []
        exit_side = "sell" if str(raw.get("side")).upper() == "BUY" else "buy"
        start = since or _ts(raw.get("time"))
        try:
            rows = await self._call(
                "fills",
                self.client.user_trades(symbol, start_time_ms=int(start.timestamp() * 1000)),
            )
        except Exception as exc:
            raise self._read_error(exc) from exc
        step = self._step(symbol)
        out = []
        for r in rows or []:
            fill = parse_fill(r, step, client_order_id)
            if fill.side != exit_side or str(r.get("orderId")) == str(raw.get("orderId")):
                continue
            out.append(fill)
        return out

    async def place_protective_stop(
        self, symbol: str, side: str, size: Decimal, stop_price: Decimal, client_order_id: str
    ) -> ExchangeOrder:
        """A standalone reduce-only stop for the whole position (``side`` is the exit side)."""
        try:
            existing = [
                o
                for o in await self._call("open_orders", self.client.open_algo_orders(symbol))
                if str(o.get("clientAlgoId") or "").startswith(client_order_id)
            ]
        except Exception:
            existing = []
        previous = existing[0].get("clientAlgoId") if existing else None
        caid = sl_client_id(client_order_id, previous)
        qty = self._qty(symbol, size)
        try:  # cover what is actually open, not what was asked for
            rows = await self._call("positions", self.client.position_risk(symbol))
            held = abs(sum((_dec(r.get("positionAmt")) for r in rows or []), Decimal("0")))
            if held > 0:
                qty = held
        except Exception:
            pass
        try:
            body = await self._call(
                "protective_stop",
                self.client.new_algo_order(
                    **self._exit_order(symbol, side, "STOP_MARKET", stop_price, qty, caid)
                ),
            )
        except Exception as exc:
            raise self._write_error(exc) from exc
        base = {
            "symbol": symbol,
            "side": side.upper(),
            "orderType": "STOP_MARKET",
            "triggerPrice": str(stop_price),
            "algoStatus": "NEW",
            "clientAlgoId": caid,
        }
        return parse_algo_order(
            {**base, **(body if isinstance(body, dict) else {})}, self._step(symbol)
        )

    async def close_position(self, symbol: str, client_order_id: str) -> ExchangeOrder:
        try:
            rows = await self._call("positions", self.client.position_risk(symbol))
        except Exception as exc:
            raise self._read_error(exc) from exc
        amount = sum((_dec(r.get("positionAmt")) for r in rows or []), Decimal("0"))
        if amount == 0:
            return ExchangeOrder(
                client_order_id=client_order_id,
                exchange_order_id=None,
                symbol=symbol,
                side="",
                size=Decimal("0"),
                filled_size=Decimal("0"),
                avg_fill_price=None,
                state=VenueOrderState.FILLED,
                updated_at=datetime.now(timezone.utc),
                reason="already_flat",
                purpose=FillPurpose.CLOSE,
            )
        try:
            body = await self._call(
                "close_position",
                self.client.new_order(
                    symbol=symbol,
                    side="SELL" if amount > 0 else "BUY",
                    type="MARKET",
                    quantity=abs(amount),
                    reduceOnly="true",
                    newClientOrderId=f"{client_order_id}X"[:36],
                    newOrderRespType="RESULT",
                ),
            )
        except Exception as exc:
            raise self._write_error(exc) from exc
        await self.sweep_conditional_orders(symbol)
        closed = await self._with_fill_price(parse_order(body, self._step(symbol)))
        return replace(closed, purpose=FillPurpose.CLOSE)

    async def amend_stop(
        self, symbol: str, client_order_id: str, stop_price: Decimal
    ) -> ExchangePosition | None:
        """Place the new stop, then cancel the old one: never a moment without a stop."""
        try:
            rows = await self._call("positions", self.client.position_risk(symbol))
            resting = [
                o
                for o in await self._call("open_orders", self.client.open_algo_orders(symbol))
                if str(o.get("orderType") or "").upper() in STOP_TYPES
            ]
        except Exception as exc:
            raise AmbiguousOrderOutcome(f"amend_stop read: {exc}") from exc
        amount = sum((_dec(r.get("positionAmt")) for r in rows or []), Decimal("0"))
        if amount == 0:
            return None
        previous = next(
            (
                str(o.get("clientAlgoId"))
                for o in resting
                if str(o.get("clientAlgoId") or "").startswith(client_order_id)
            ),
            None,
        )
        try:
            await self._call(
                "amend_stop",
                self.client.new_algo_order(
                    **self._exit_order(
                        symbol,
                        "SELL" if amount > 0 else "BUY",
                        "STOP_MARKET",
                        stop_price,
                        abs(amount),
                        sl_client_id(client_order_id, previous),
                    )
                ),
            )
        except Exception as exc:
            raise self._write_error(exc) from exc
        for old in resting:
            try:
                await self._call(
                    "amend_stop", self.client.cancel_algo_order(algo_id=old.get("algoId"))
                )
            except Exception:
                # Two stops is safe (both close the whole position); the leftover is swept
                # when the position goes flat and before the next entry on the symbol.
                logger.warning("Old stop %s on %s not cancelled", old.get("clientAlgoId"), symbol)
        return next((p for p in await self.positions() if p.symbol == symbol), None)

    async def sweep_conditional_orders(self, symbol: str) -> None:
        """Cancel leftover stops/targets once *symbol* is flat."""
        try:
            rows = await self.client.position_risk(symbol)
            if any(_dec(r.get("positionAmt")) != 0 for r in rows or []):
                return
            if await self.client.open_algo_orders(symbol):
                await self.client.cancel_all_algo_orders(symbol)
                logger.info("Swept leftover conditional orders on flat %s", symbol)
        except Exception:
            logger.exception("Sweeping conditional orders on %s failed", symbol)

    # ------------------------------------------------------------------
    # balances
    # ------------------------------------------------------------------
    async def account_equity(self) -> Decimal:
        """``totalMarginBalance``: wallet balance plus unrealised PnL (USD-M, single-asset)."""
        try:
            body = await self._call("balance", self.client.account())
        except Exception as exc:
            raise self._read_error(exc) from exc
        return _dec(body.get("totalMarginBalance"))

    async def available_balance(self) -> Decimal:
        try:
            body = await self._call("balance", self.client.account())
        except Exception as exc:
            raise self._read_error(exc) from exc
        return _dec(body.get("availableBalance"))


# ---------------------------------------------------------------------------
# user-data stream -> bus
# ---------------------------------------------------------------------------


def _order_event(o: dict[str, Any], step: Decimal, event_ms: Any) -> Event | None:
    """``ORDER_TRADE_UPDATE.o`` -> an order update (skipped without a client id)."""
    if not o.get("c"):
        return None
    order = ExchangeOrder(
        client_order_id=str(o.get("c")),
        exchange_order_id=str(o.get("i")) if o.get("i") is not None else None,
        symbol=str(o.get("s") or ""),
        side=str(o.get("S") or "").lower(),
        size=_dec(o.get("q")) / step,
        filled_size=_dec(o.get("z")) / step,
        avg_fill_price=_dec(o.get("ap")) if _dec(o.get("ap")) > 0 else None,
        state=_STATES.get(str(o.get("X") or "").upper(), VenueOrderState.OPEN),
        updated_at=_ts(o.get("T") or event_ms),
        purpose=_purpose(str(o.get("ot") or o.get("o") or ""), str(o.get("c")), bool(o.get("R"))),
        stop_price=_dec(o.get("sp")) if _dec(o.get("sp")) > 0 else None,
    )
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


def _fill_event(o: dict[str, Any], step: Decimal) -> Event:
    """``ORDER_TRADE_UPDATE.o`` with execution type ``TRADE`` -> a fill."""
    purpose = _purpose(
        str(o.get("ot") or o.get("o") or ""), str(o.get("c") or ""), bool(o.get("R"))
    )
    fill = ExchangeFill(
        fill_id=f"{VENUE}:{o.get('s')}:{o.get('t')}",  # same id as REST userTrades
        client_order_id=str(o.get("c")) if o.get("c") else None,
        exchange_order_id=str(o.get("i")) if o.get("i") is not None else None,
        symbol=str(o.get("s") or ""),
        side=str(o.get("S") or "").lower(),
        qty=_dec(o.get("l")) / step,
        price=_dec(o.get("L")),
        fee=_dec(o.get("n")),
        liquidity="maker" if o.get("m") else "taker",
        # Entries stay UNKNOWN: the order store classifies them against the local order.
        purpose=FillPurpose.UNKNOWN if purpose == FillPurpose.ENTRY else purpose,
        occurred_at=_ts(o.get("T")),
    )
    return Event.create(
        EventType.EXCHANGE_FILL,
        fill.to_payload(VENUE),
        source=VENUE,
        event_id=deterministic_event_id(VENUE, "fill", fill.fill_id),
        occurred_at=fill.occurred_at,
    )


def _position_events(msg: dict[str, Any], step_of) -> list[Event]:
    events = []
    for p in (msg.get("a") or {}).get("P") or []:
        symbol = str(p.get("s") or "")
        pos = ExchangePosition(
            symbol=symbol,
            size=_dec(p.get("pa")) / step_of(symbol),
            entry_price=_dec(p.get("ep")) if _dec(p.get("ep")) > 0 else None,
            updated_at=_ts(msg.get("T") or msg.get("E")),
            extra={"unrealized_pnl": p.get("up")},
        )
        payload = pos.to_payload(VENUE)
        events.append(
            Event.create(
                EventType.EXCHANGE_POSITION_UPDATE,
                payload,
                source=VENUE,
                event_id=deterministic_event_id(
                    VENUE, "position", symbol, pos.size, payload["updated_at"]
                ),
                occurred_at=pos.updated_at,
            )
        )
    return events


def normalize_ws_message(msg: dict[str, Any], step_of) -> list[Event]:
    """One raw user-data message -> zero or more bus events.  *step_of(symbol)* -> stepSize."""
    if not isinstance(msg, dict):
        return []
    kind = msg.get("e")
    if kind == "ORDER_TRADE_UPDATE":
        o = msg.get("o") or {}
        step = step_of(str(o.get("s") or ""))
        events = []
        if str(o.get("x") or "").upper() == "TRADE" and _dec(o.get("l")) > 0:
            events.append(_fill_event(o, step))
        order_event = _order_event(o, step, msg.get("E"))
        if order_event is not None:
            events.append(order_event)
        return events
    if kind == "ACCOUNT_UPDATE":
        return _position_events(msg, step_of)
    return []  # ALGO_UPDATE is handled by the bridge; other events are informational


class BinanceStreamBridge:
    """Sink for :class:`BinanceUserStream`: normalises messages, publishes them, and sweeps
    leftover conditional orders once a position goes flat."""

    def __init__(self, bus: EventBus, adapter: BinanceFuturesAdapter) -> None:
        self.bus = bus
        self.adapter = adapter
        self._sweeps: set[asyncio.Task] = set()

    def _schedule_sweep(self, symbol: str) -> None:
        async def later():
            await asyncio.sleep(FLAT_SWEEP_DELAY_SECONDS)
            await self.adapter.sweep_conditional_orders(symbol)

        task = asyncio.create_task(later(), name=f"binance_sweep_{symbol}")
        self._sweeps.add(task)
        task.add_done_callback(self._sweeps.discard)

    async def handle_raw(self, msg: dict[str, Any]) -> None:
        for event in normalize_ws_message(msg, self.adapter._step):
            await self.bus.publish(event)
        if not isinstance(msg, dict):
            return
        if msg.get("e") == "ACCOUNT_UPDATE":
            for p in (msg.get("a") or {}).get("P") or []:
                if _dec(p.get("pa")) == 0 and p.get("s") in self.adapter.client.rules:
                    self._schedule_sweep(str(p.get("s")))
        elif msg.get("e") == "ALGO_UPDATE":
            o = msg.get("o") or {}
            if str(o.get("X") or "").upper() == "REJECTED":
                logger.error(
                    "Binance rejected conditional order %s on %s: %s",
                    o.get("caid"),
                    o.get("s"),
                    o.get("rm"),
                )

    async def on_connect(self) -> None:
        metrics.EXCHANGE_STREAM_CONNECTED.labels(VENUE).set(1)
        await self.bus.publish(Event.create(EventType.STREAM_CONNECTED, {"venue": VENUE}, VENUE))

    async def on_disconnect(self, reason: str) -> None:
        metrics.EXCHANGE_STREAM_CONNECTED.labels(VENUE).set(0)
        await self.bus.publish(
            Event.create(EventType.STREAM_DISCONNECTED, {"venue": VENUE, "reason": reason}, VENUE)
        )
