"""A deterministic in-process exchange with order-book matching and fault injection.

It implements :class:`~signal_bot.execution.adapter.ExchangeAdapter` and
publishes the same normalised events a real venue stream would, so the
execution engine, outcome monitor and reconciler run unchanged against it.

Matching model (shared with the paper broker):

* IOC/market orders walk the visible book level by level (VWAP fill price,
  partial fill when liquidity runs out, remainder cancelled);
* bracket legs live on the position: the stop is a stop-market that walks the
  book when the best bid/ask crosses it (gaps fill at the book, not the stop
  price); the take-profit is a resting limit that fills at its price;
* taker/maker fees, periodic funding and a virtual ledger with mark-to-market
  equity.

Faults are queued per operation with :class:`FaultPlan`, e.g.
``venue.faults.add("place_order", "timeout_after_accept")`` reproduces "the
exchange accepted the order but the HTTP response was lost".
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any

from signal_bot.core.clock import SYSTEM_CLOCK, Clock
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

logger = logging.getLogger(__name__)

ZERO = Decimal("0")


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


@dataclass
class CostModel:
    taker_fee: Decimal = Decimal("0.0005")
    maker_fee: Decimal = Decimal("0.0002")
    latency_seconds: float = 0.0
    liquidity_fraction: Decimal = Decimal("1")  # share of displayed size we can take
    funding_rate: Decimal = Decimal("0.0001")  # per funding interval; longs pay if > 0
    funding_interval_hours: int = 8


@dataclass
class Fault:
    op: str
    action: str
    times: int = 1  # -1 = forever
    delay: float = 0.0
    params: dict[str, Any] = field(default_factory=dict)
    match: Callable[[Any], bool] | None = None


class FaultPlan:
    """Queue of faults consumed by venue operations (first match wins)."""

    ACTIONS = {
        "timeout_before_accept",  # request lost before the venue saw it
        "timeout_after_accept",  # venue accepted; response lost
        "server_error_after_accept",
        "reject",
        "auth_failure",
        "unavailable",
        "delay",
        "not_found",
        "stale_book",
        "empty_book",
        "no_protective_legs",  # bracket legs silently not attached
        "duplicate_events",  # each event published twice with the same id
        "duplicate_fills_new_id",  # same fill redelivered under a different event id
        "reorder_events",  # events of one operation published in reverse order
        "drop_events",  # events lost (stream silently missed them)
    }

    def __init__(self) -> None:
        self._faults: list[Fault] = []
        self.triggered: list[tuple[str, str]] = []

    def add(self, op: str, action: str, times: int = 1, delay: float = 0.0, **params) -> Fault:
        if action not in self.ACTIONS:
            raise ValueError(f"unknown fault action {action!r}")
        fault = Fault(op, action, times, delay, params, params.pop("match", None))
        self._faults.append(fault)
        return fault

    def take(self, op: str, subject: Any = None) -> Fault | None:
        for fault in self._faults:
            if fault.op != op or fault.times == 0:
                continue
            if fault.match is not None and not fault.match(subject):
                continue
            if fault.times > 0:
                fault.times -= 1
            self.triggered.append((op, fault.action))
            return fault
        return None

    def clear(self) -> None:
        self._faults.clear()


# ---------------------------------------------------------------------------
# internal state
# ---------------------------------------------------------------------------


@dataclass
class _Order:
    request: OrderRequest
    exchange_order_id: str
    state: VenueOrderState
    filled: Decimal = ZERO
    notional: Decimal = ZERO  # sum(qty * price)
    fees: Decimal = ZERO
    updated_at: datetime | None = None
    reason: str | None = None
    purpose: FillPurpose = FillPurpose.ENTRY
    seq: int = 0

    @property
    def avg_price(self) -> Decimal | None:
        return (self.notional / self.filled) if self.filled > 0 else None

    def snapshot(self) -> ExchangeOrder:
        return ExchangeOrder(
            client_order_id=self.request.client_order_id,
            exchange_order_id=self.exchange_order_id,
            symbol=self.request.symbol,
            side=self.request.side,
            size=self.request.size,
            filled_size=self.filled,
            avg_fill_price=self.avg_price,
            state=self.state,
            updated_at=self.updated_at,
            fees=self.fees,
            reason=self.reason,
            purpose=self.purpose,
        )


@dataclass
class _Position:
    symbol: str
    size: Decimal = ZERO  # signed contracts
    entry_price: Decimal = ZERO
    owner: str | None = None  # client_order_id of the entry that opened it
    stop_price: Decimal | None = None
    take_profit_price: Decimal | None = None
    leverage: int = 1


@dataclass(frozen=True)
class LedgerEntry:
    entry_id: str
    kind: str  # fee | realized_pnl | funding | deposit
    symbol: str | None
    amount: Decimal
    balance_after: Decimal
    reference: str | None
    occurred_at: datetime


# ---------------------------------------------------------------------------
# venue
# ---------------------------------------------------------------------------


class SimulatedVenue:
    def __init__(
        self,
        instruments: dict[str, InstrumentSpec],
        bus: EventBus | None = None,
        *,
        venue: str = "sim",
        clock: Clock = SYSTEM_CLOCK,
        costs: CostModel | None = None,
        initial_balance: Decimal = Decimal("10000"),
    ) -> None:
        self.venue = venue
        self.bus = bus
        self.clock = clock
        self.costs = costs or CostModel()
        self.faults = FaultPlan()
        self.latency_in_calls = True
        self._instruments = dict(instruments)
        self._venue_to_binance = {spec.symbol: b for b, spec in instruments.items()}
        self._books: dict[str, BookSnapshot] = {}
        self._orders: dict[str, _Order] = {}
        self._positions: dict[str, _Position] = {}
        self._fills: list[ExchangeFill] = []
        self._ledger: list[LedgerEntry] = []
        self._order_counter = 0
        self._fill_counter = 0
        self._stream_connected = True
        self._last_funding: dict[str, datetime] = {}
        self.cash = Decimal(str(initial_balance))
        self.initial_balance = self.cash
        self._post(
            LedgerEntry("deposit:initial", "deposit", None, self.cash, self.cash, None, clock.now())
        )
        self.calls: dict[str, int] = {}

    # ------------------------------------------------------------------
    # market data
    # ------------------------------------------------------------------
    def instrument(self, binance_symbol: str) -> InstrumentSpec:
        return self._instruments[binance_symbol]

    def binance_symbol(self, venue_symbol: str) -> str | None:
        return self._venue_to_binance.get(venue_symbol)

    def set_book(
        self,
        symbol: str,
        bid: Decimal,
        ask: Decimal,
        size: Decimal = Decimal("1000"),
        levels: int = 5,
        tick: Decimal | None = None,
    ) -> BookSnapshot:
        tick = tick or self._tick(symbol)
        bids = tuple(BookLevel(bid - tick * i, size) for i in range(levels))
        asks = tuple(BookLevel(ask + tick * i, size) for i in range(levels))
        return self.set_snapshot(BookSnapshot(symbol, bids, asks, self.clock.now()))

    def set_snapshot(self, snapshot: BookSnapshot) -> BookSnapshot:
        self._books[snapshot.symbol] = snapshot
        return snapshot

    async def on_book(self, snapshot: BookSnapshot) -> None:
        """Advance the market: update the book, then trigger protective legs and funding."""
        self.set_snapshot(snapshot)
        await self._apply_funding(snapshot.symbol)
        await self._check_triggers(snapshot.symbol)

    async def move_market(self, symbol: str, bid: Decimal, ask: Decimal, size=Decimal("1000")):
        await self.on_book(self.set_book(symbol, bid, ask, size))

    def _tick(self, symbol: str) -> Decimal:
        b = self._venue_to_binance.get(symbol)
        return self._instruments[b].tick_size if b else Decimal("0.5")

    def _spec(self, symbol: str) -> InstrumentSpec:
        return self._instruments[self._venue_to_binance[symbol]]

    # ------------------------------------------------------------------
    # adapter API
    # ------------------------------------------------------------------
    async def _enter(self, op: str, subject: Any = None) -> Fault | None:
        self.calls[op] = self.calls.get(op, 0) + 1
        fault = self.faults.take(op, subject)
        if self.costs.latency_seconds and self.latency_in_calls:
            await asyncio.sleep(self.costs.latency_seconds)
        if fault is None:
            return None
        if fault.delay:
            await asyncio.sleep(fault.delay)
        if fault.action == "auth_failure":
            raise AuthenticationFailed(f"{op}: invalid api key (injected)")
        if fault.action == "unavailable":
            raise ExchangeUnavailable(f"{op}: 503 (injected)")
        if fault.action == "reject":
            raise OrderRejected(f"{op}: {fault.params.get('reason', 'rejected (injected)')}")
        if fault.action == "timeout_before_accept":
            raise AmbiguousOrderOutcome(f"{op}: timeout before accept (injected)")
        return fault

    async def order_book(self, symbol: str) -> BookSnapshot:
        fault = await self._enter("order_book", symbol)
        book = self._books.get(symbol)
        if book is None or (fault and fault.action == "empty_book"):
            return BookSnapshot(symbol, (), (), self.clock.now())
        if fault and fault.action == "stale_book":
            age = fault.params.get("age_seconds", 120)
            return replace(book, ts=self.clock.now() - timedelta(seconds=age))
        return book

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        await self._enter("set_leverage", symbol)
        self._positions.setdefault(symbol, _Position(symbol)).leverage = leverage

    async def place_order(self, request: OrderRequest) -> ExchangeOrder:
        fault = await self._enter("place_order", request)
        if request.client_order_id in self._orders:
            raise OrderRejected("duplicate client_order_id")
        if request.size <= 0:
            raise OrderRejected("size must be positive")

        self._order_counter += 1
        order = _Order(
            request=request,
            exchange_order_id=f"{self.venue}-{self._order_counter}",
            state=VenueOrderState.OPEN,
            updated_at=self.clock.now(),
            purpose=FillPurpose.CLOSE if request.reduce_only else FillPurpose.ENTRY,
        )
        self._orders[request.client_order_id] = order
        events: list[Event] = [self._order_event(order)]

        if request.order_type in ("limit_ioc", "market"):
            events += self._match_taker(order, order.purpose)
            if order.filled < request.size:
                order.state = VenueOrderState.CANCELLED
                order.reason = "ioc_remainder_cancelled" if order.filled > 0 else "ioc_no_liquidity"
            else:
                order.state = VenueOrderState.FILLED
            order.updated_at = self.clock.now()
            events.append(self._order_event(order))
            if order.filled > 0 and order.purpose == FillPurpose.ENTRY:
                no_legs = fault is not None and fault.action == "no_protective_legs"
                events += self._attach_legs(order, attach=not no_legs)

        await self._emit(events, fault)
        if fault and fault.action == "timeout_after_accept":
            raise AmbiguousOrderOutcome("place_order: response lost after accept (injected)")
        if fault and fault.action == "server_error_after_accept":
            raise AmbiguousOrderOutcome("place_order: HTTP 502 after accept (injected)")
        return order.snapshot()

    async def get_order(self, client_order_id: str) -> ExchangeOrder | None:
        fault = await self._enter("get_order", client_order_id)
        if fault and fault.action == "not_found":
            return None
        order = self._orders.get(client_order_id)
        return order.snapshot() if order else None

    async def cancel_order(self, client_order_id: str, exchange_order_id: str | None) -> None:
        fault = await self._enter("cancel_order", client_order_id)
        order = self._orders.get(client_order_id)
        if order is None or order.state not in (
            VenueOrderState.OPEN,
            VenueOrderState.PARTIALLY_FILLED,
        ):
            return
        order.state = VenueOrderState.CANCELLED
        order.reason = "cancelled_by_user"
        order.updated_at = self.clock.now()
        await self._emit([self._order_event(order)], fault)

    async def open_orders(self) -> list[ExchangeOrder]:
        await self._enter("open_orders")
        return [
            o.snapshot()
            for o in self._orders.values()
            if o.state in (VenueOrderState.OPEN, VenueOrderState.PARTIALLY_FILLED)
        ]

    async def positions(self) -> list[ExchangePosition]:
        await self._enter("positions")
        return [self._position_view(p) for p in self._positions.values() if p.size != 0]

    async def fills_for(self, client_order_id: str) -> list[ExchangeFill]:
        await self._enter("fills", client_order_id)
        return [f for f in self._fills if f.client_order_id == client_order_id]

    async def exit_fills(
        self, symbol: str, client_order_id: str, since: datetime | None = None
    ) -> list[ExchangeFill]:
        await self._enter("fills", client_order_id)
        return [
            f
            for f in self._fills
            if f.client_order_id == client_order_id and f.purpose != FillPurpose.ENTRY
        ]

    def funding_between(self, symbol: str, start: datetime, end: datetime) -> Decimal:
        return sum(
            (
                e.amount
                for e in self._ledger
                if e.kind == "funding" and e.symbol == symbol and start <= e.occurred_at <= end
            ),
            ZERO,
        )

    async def place_protective_stop(
        self, symbol: str, side: str, size: Decimal, stop_price: Decimal, client_order_id: str
    ) -> ExchangeOrder:
        fault = await self._enter("protective", client_order_id)
        pos = self._positions.get(symbol)
        if pos is None or pos.size == 0:
            raise OrderRejected("no position to protect")
        pos.stop_price = stop_price
        self._order_counter += 1
        leg = _Order(
            request=OrderRequest(client_order_id, symbol, side, size, "stop_market", stop_price),
            exchange_order_id=f"{self.venue}-{self._order_counter}",
            state=VenueOrderState.OPEN,
            updated_at=self.clock.now(),
            purpose=FillPurpose.STOP_LOSS,
        )
        await self._emit([self._position_event(pos)], fault)
        return leg.snapshot()

    async def close_position(self, symbol: str, client_order_id: str) -> ExchangeOrder:
        fault = await self._enter("close_position", client_order_id)
        pos = self._positions.get(symbol)
        if pos is None or pos.size == 0:
            raise OrderRejected("no position to close")
        side = "sell" if pos.size > 0 else "buy"
        self._order_counter += 1
        order = _Order(
            request=OrderRequest(
                client_order_id, symbol, side, abs(pos.size), "market", reduce_only=True
            ),
            exchange_order_id=f"{self.venue}-{self._order_counter}",
            state=VenueOrderState.OPEN,
            updated_at=self.clock.now(),
            purpose=FillPurpose.CLOSE,
        )
        events = self._match_taker(order, FillPurpose.CLOSE, owner=pos.owner)
        order.state = VenueOrderState.FILLED if order.filled > 0 else VenueOrderState.CANCELLED
        await self._emit(events, fault)
        return order.snapshot()

    async def available_balance(self) -> Decimal:
        await self._enter("balance")
        return self.equity()

    # ------------------------------------------------------------------
    # stream control (fault injection)
    # ------------------------------------------------------------------
    async def disconnect_stream(self, reason: str = "socket closed (injected)") -> None:
        self._stream_connected = False
        if self.bus:
            await self.bus.publish(
                Event.create(
                    EventType.STREAM_DISCONNECTED,
                    {"venue": self.venue, "reason": reason},
                    self.venue,
                )
            )

    async def reconnect_stream(self) -> None:
        self._stream_connected = True
        if self.bus:
            await self.bus.publish(
                Event.create(EventType.STREAM_CONNECTED, {"venue": self.venue}, self.venue)
            )

    @property
    def stream_connected(self) -> bool:
        return self._stream_connected

    # ------------------------------------------------------------------
    # matching
    # ------------------------------------------------------------------
    def _match_taker(
        self, order: _Order, purpose: FillPurpose, owner: str | None = None
    ) -> list[Event]:
        req = order.request
        book = self._books.get(req.symbol)
        if book is None:
            return []
        levels = list(book.asks if req.side == "buy" else book.bids)
        remaining = req.size - order.filled
        events: list[Event] = []
        consumed: list[BookLevel] = []
        lot = self._spec(req.symbol).lot_size
        for level in levels:
            if remaining <= 0:
                consumed.append(level)
                continue
            if req.limit_price is not None and req.order_type == "limit_ioc":
                if (req.side == "buy" and level.price > req.limit_price) or (
                    req.side == "sell" and level.price < req.limit_price
                ):
                    consumed.append(level)
                    continue
            available = (level.size * self.costs.liquidity_fraction).quantize(
                lot, rounding=ROUND_DOWN
            )
            qty = min(remaining, available)
            if qty <= 0:
                consumed.append(level)
                continue
            events += self._fill(order, qty, level.price, "taker", purpose, owner)
            remaining -= qty
            left = level.size - qty
            if left > 0:
                consumed.append(BookLevel(level.price, left))
        new_levels = tuple(consumed)
        if req.side == "buy":
            self._books[req.symbol] = replace(book, asks=new_levels)
        else:
            self._books[req.symbol] = replace(book, bids=new_levels)
        return events

    def _fill(
        self,
        order: _Order,
        qty: Decimal,
        price: Decimal,
        liquidity: str,
        purpose: FillPurpose,
        owner: str | None = None,
    ) -> list[Event]:
        req = order.request
        spec = self._spec(req.symbol)
        fee_rate = self.costs.taker_fee if liquidity == "taker" else self.costs.maker_fee
        fee = qty * spec.contract_value * price * fee_rate
        self._fill_counter += 1
        fill = ExchangeFill(
            fill_id=f"{self.venue}:fill:{self._fill_counter}",
            client_order_id=owner or req.client_order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=req.symbol,
            side=req.side,
            qty=qty,
            price=price,
            fee=fee,
            liquidity=liquidity,
            purpose=purpose,
            occurred_at=self.clock.now(),
        )
        self._fills.append(fill)
        order.filled += qty
        order.notional += qty * price
        order.fees += fee
        order.updated_at = self.clock.now()
        if order.filled < req.size:
            order.state = VenueOrderState.PARTIALLY_FILLED
        self._book_fee(fill)
        events = [self._fill_event(fill)]
        events += self._apply_to_position(fill, owner or req.client_order_id)
        if order.state == VenueOrderState.PARTIALLY_FILLED:
            events.append(self._order_event(order))
        return events

    def _apply_to_position(self, fill: ExchangeFill, owner: str) -> list[Event]:
        pos = self._positions.setdefault(fill.symbol, _Position(fill.symbol))
        spec = self._spec(fill.symbol)
        signed = fill.qty if fill.side == "buy" else -fill.qty
        if pos.size == 0 or (pos.size > 0) == (signed > 0):
            new_size = pos.size + signed
            pos.entry_price = (
                (abs(pos.size) * pos.entry_price + fill.qty * fill.price) / abs(new_size)
                if new_size != 0
                else ZERO
            )
            pos.size = new_size
            if pos.owner is None:
                pos.owner = owner
        else:
            closing = min(abs(signed), abs(pos.size))
            direction = Decimal("1") if pos.size > 0 else Decimal("-1")
            pnl = closing * spec.contract_value * (fill.price - pos.entry_price) * direction
            self._post_amount(f"{fill.fill_id}:pnl", "realized_pnl", fill.symbol, pnl, fill.fill_id)
            pos.size += signed
            if pos.size == 0:
                pos.entry_price = ZERO
                pos.owner = None
                pos.stop_price = None
                pos.take_profit_price = None
        return [self._position_event(pos)]

    def _attach_legs(self, order: _Order, attach: bool) -> list[Event]:
        pos = self._positions.get(order.request.symbol)
        if pos is None:
            return []
        if attach:
            pos.stop_price = order.request.stop_loss_price
            pos.take_profit_price = order.request.take_profit_price
        return [self._position_event(pos)]

    async def _check_triggers(self, symbol: str) -> None:
        pos = self._positions.get(symbol)
        book = self._books.get(symbol)
        if pos is None or pos.size == 0 or book is None or not book.bids or not book.asks:
            return
        long = pos.size > 0
        exit_side = "sell" if long else "buy"
        bid, ask = book.best_bid, book.best_ask
        purpose = None
        if pos.stop_price is not None and (
            (long and bid <= pos.stop_price) or (not long and ask >= pos.stop_price)
        ):
            purpose = FillPurpose.STOP_LOSS
        elif pos.take_profit_price is not None and (
            (long and bid >= pos.take_profit_price) or (not long and ask <= pos.take_profit_price)
        ):
            purpose = FillPurpose.TAKE_PROFIT
        if purpose is None:
            return

        owner = pos.owner
        self._order_counter += 1
        leg = _Order(
            request=OrderRequest(
                f"{owner}:{purpose}",
                symbol,
                exit_side,
                abs(pos.size),
                "market" if purpose == FillPurpose.STOP_LOSS else "limit",
                pos.take_profit_price if purpose == FillPurpose.TAKE_PROFIT else None,
                reduce_only=True,
            ),
            exchange_order_id=f"{self.venue}-{self._order_counter}",
            state=VenueOrderState.OPEN,
            updated_at=self.clock.now(),
            purpose=purpose,
        )
        if purpose == FillPurpose.STOP_LOSS:
            events = self._match_taker(leg, purpose, owner=owner)
        else:
            events = self._fill(leg, abs(pos.size), pos.take_profit_price, "maker", purpose, owner)
        await self._emit(events, None)

    # ------------------------------------------------------------------
    # account
    # ------------------------------------------------------------------
    def _post(self, entry: LedgerEntry) -> None:
        self._ledger.append(entry)

    def _post_amount(self, entry_id, kind, symbol, amount: Decimal, reference) -> None:
        if any(e.entry_id == entry_id for e in self._ledger[-50:]):
            return
        self.cash += amount
        self._post(
            LedgerEntry(entry_id, kind, symbol, amount, self.cash, reference, self.clock.now())
        )

    def _book_fee(self, fill: ExchangeFill) -> None:
        self._post_amount(f"{fill.fill_id}:fee", "fee", fill.symbol, -fill.fee, fill.fill_id)

    async def _apply_funding(self, symbol: str) -> None:
        pos = self._positions.get(symbol)
        hours = self.costs.funding_interval_hours
        if pos is None or pos.size == 0 or not hours or self.costs.funding_rate == 0:
            return
        now = self.clock.now()
        boundary = now.replace(minute=0, second=0, microsecond=0)
        boundary -= timedelta(hours=boundary.hour % hours)
        last = self._last_funding.get(symbol)
        if last is None:
            self._last_funding[symbol] = boundary
            return
        if boundary <= last:
            return
        self._last_funding[symbol] = boundary
        book = self._books[symbol]
        mark = book.mid or pos.entry_price
        notional = abs(pos.size) * self._spec(symbol).contract_value * mark
        direction = Decimal("1") if pos.size > 0 else Decimal("-1")
        amount = -direction * notional * self.costs.funding_rate
        self._post_amount(
            f"funding:{symbol}:{boundary.isoformat()}", "funding", symbol, amount, None
        )

    def unrealized_pnl(self) -> Decimal:
        total = ZERO
        for pos in self._positions.values():
            if pos.size == 0:
                continue
            book = self._books.get(pos.symbol)
            mark = book.mid if book and book.mid else pos.entry_price
            direction = Decimal("1") if pos.size > 0 else Decimal("-1")
            total += (
                abs(pos.size)
                * self._spec(pos.symbol).contract_value
                * (mark - pos.entry_price)
                * direction
            )
        return total

    def equity(self) -> Decimal:
        return self.cash + self.unrealized_pnl()

    @property
    def ledger(self) -> list[LedgerEntry]:
        return list(self._ledger)

    @property
    def fills(self) -> list[ExchangeFill]:
        return list(self._fills)

    def position(self, symbol: str) -> ExchangePosition | None:
        pos = self._positions.get(symbol)
        return self._position_view(pos) if pos and pos.size != 0 else None

    def _position_view(self, pos: _Position) -> ExchangePosition:
        return ExchangePosition(
            symbol=pos.symbol,
            size=pos.size,
            entry_price=pos.entry_price or None,
            has_stop_loss=pos.stop_price is not None,
            has_take_profit=pos.take_profit_price is not None,
            stop_loss_price=pos.stop_price,
            take_profit_price=pos.take_profit_price,
            updated_at=self.clock.now(),
        )

    # ------------------------------------------------------------------
    # events
    # ------------------------------------------------------------------
    def _order_event(self, order: _Order) -> Event:
        order.seq += 1
        snap = order.snapshot()
        return Event.create(
            EventType.EXCHANGE_ORDER_UPDATE,
            snap.to_payload(self.venue),
            source=self.venue,
            event_id=deterministic_event_id(
                self.venue, "order", order.exchange_order_id, order.seq
            ),
            occurred_at=snap.updated_at,
        )

    def _fill_event(self, fill: ExchangeFill) -> Event:
        return Event.create(
            EventType.EXCHANGE_FILL,
            fill.to_payload(self.venue),
            source=self.venue,
            event_id=deterministic_event_id(self.venue, "fill", fill.fill_id),
            occurred_at=fill.occurred_at,
        )

    def _position_event(self, pos: _Position) -> Event:
        view = self._position_view(pos)
        payload = view.to_payload(self.venue)
        payload["size"] = str(pos.size)
        return Event.create(
            EventType.EXCHANGE_POSITION_UPDATE,
            payload,
            source=self.venue,
            event_id=deterministic_event_id(
                self.venue, "position", pos.symbol, len(self._fills), pos.size, pos.stop_price
            ),
            occurred_at=self.clock.now(),
        )

    async def _emit(self, events: list[Event], fault: Fault | None) -> None:
        if self.bus is None or not events:
            return
        if not self._stream_connected:
            return  # private stream is down: the client never sees these
        action = fault.action if fault else None
        if action == "drop_events":
            return
        stream_fault = self.faults.take("stream")
        if stream_fault is not None:
            action = stream_fault.action
            if action == "drop_events":
                return
        if action == "reorder_events":
            events = list(reversed(events))
        for event in events:
            await self.bus.publish(event)
            if action == "duplicate_events":
                await self.bus.publish(event)
            elif action == "duplicate_fills_new_id" and event.event_type == EventType.EXCHANGE_FILL:
                await self.bus.publish(replace(event, event_id=event.event_id + ":redelivery"))
