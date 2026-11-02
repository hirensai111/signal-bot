"""Venue-neutral exchange interface.

The execution engine, reconciler and outcome monitor only talk to an
:class:`ExchangeAdapter`.  Delta (live), the paper broker and the
fault-injecting simulator all implement it, so the *same* execution logic is
exercised in tests, paper trading and production.

Error taxonomy matters more than anything else here:

* :class:`OrderRejected` — the venue definitively refused the order.  Safe to
  mark the order rejected.
* :class:`AmbiguousOrderOutcome` — we do not know whether the venue accepted
  the order (timeout, connection reset, 5xx after the request left).  The order
  must go to ``submission_unknown`` and be resolved by lookup — never resent.
* :class:`AuthenticationFailed` — credentials/permissions problem.  Definitive,
  and trading must halt.
* :class:`ExchangeUnavailable` — a read/cancel failed transiently.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol


class ExchangeError(Exception):
    pass


class OrderRejected(ExchangeError):
    pass


class AmbiguousOrderOutcome(ExchangeError):
    pass


class AuthenticationFailed(ExchangeError):
    pass


class ExchangeUnavailable(ExchangeError):
    pass


class VenueOrderState(StrEnum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class FillPurpose(StrEnum):
    ENTRY = "entry"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    CLOSE = "close"  # manual / recovery flatten
    LIQUIDATION = "liquidation"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class InstrumentSpec:
    symbol: str  # venue symbol, e.g. BTCUSD
    product_id: int
    tick_size: Decimal
    lot_size: Decimal
    contract_value: Decimal
    max_contracts: Decimal | None = None  # venue position size limit (Delta position_size_limit)


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class BookSnapshot:
    symbol: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    ts: datetime

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> Decimal | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0].price + self.asks[0].price) / 2


@dataclass(frozen=True)
class OrderRequest:
    client_order_id: str
    symbol: str  # venue symbol
    side: str  # "buy" | "sell"
    size: Decimal  # contracts
    order_type: str  # "limit_ioc" | "market" | "limit"
    limit_price: Decimal | None = None
    stop_loss_price: Decimal | None = None
    take_profit_price: Decimal | None = None
    reduce_only: bool = False
    # Exchange-native trailing stop distance in price units (Delta bracket_trail_amount).
    trail_amount: Decimal | None = None


@dataclass(frozen=True)
class ExchangeOrder:
    client_order_id: str
    exchange_order_id: str | None
    symbol: str
    side: str
    size: Decimal
    filled_size: Decimal
    avg_fill_price: Decimal | None
    state: VenueOrderState
    updated_at: datetime
    fees: Decimal = Decimal("0")
    reason: str | None = None
    purpose: FillPurpose = FillPurpose.ENTRY
    stop_price: Decimal | None = None  # trigger price of a stop order
    trail_amount: Decimal | None = None  # set on an exchange trailing stop

    def to_payload(self, venue: str) -> dict[str, Any]:
        d = asdict(self)
        d["venue"] = venue
        d["state"] = str(self.state)
        d["purpose"] = str(self.purpose)
        return d


@dataclass(frozen=True)
class ExchangeFill:
    fill_id: str
    client_order_id: str | None
    exchange_order_id: str | None
    symbol: str
    side: str
    qty: Decimal
    price: Decimal
    fee: Decimal
    liquidity: str  # "maker" | "taker"
    purpose: FillPurpose
    occurred_at: datetime

    def to_payload(self, venue: str) -> dict[str, Any]:
        d = asdict(self)
        d["venue"] = venue
        d["purpose"] = str(self.purpose)
        return d


@dataclass(frozen=True)
class ExchangePosition:
    symbol: str
    size: Decimal  # signed: + long, - short (contracts)
    entry_price: Decimal | None
    has_stop_loss: bool = False
    has_take_profit: bool = False
    stop_loss_price: Decimal | None = None
    take_profit_price: Decimal | None = None
    updated_at: datetime | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    trail_amount: Decimal | None = None  # exchange trailing stop distance, if one is attached

    def to_payload(self, venue: str) -> dict[str, Any]:
        def s(v):
            return None if v is None else str(v)

        return {
            "venue": venue,
            "symbol": self.symbol,
            "size": str(self.size),
            "entry_price": s(self.entry_price),
            "has_stop_loss": self.has_stop_loss,
            "has_take_profit": self.has_take_profit,
            "stop_loss_price": s(self.stop_loss_price),
            "take_profit_price": s(self.take_profit_price),
            "trail_amount": s(self.trail_amount),
            "updated_at": None if self.updated_at is None else self.updated_at.isoformat(),
        }


class ExchangeAdapter(Protocol):
    """What the execution core needs from a venue."""

    venue: str

    def instrument(self, binance_symbol: str) -> InstrumentSpec: ...

    def binance_symbol(self, venue_symbol: str) -> str | None: ...

    async def order_book(self, symbol: str) -> BookSnapshot: ...

    async def set_leverage(self, symbol: str, leverage: int) -> None: ...

    async def place_order(self, request: OrderRequest) -> ExchangeOrder: ...

    async def get_order(self, client_order_id: str) -> ExchangeOrder | None:
        """Look an order up by our client id.  ``None`` means *definitively not found*."""
        ...

    async def cancel_order(self, client_order_id: str, exchange_order_id: str | None) -> None: ...

    async def open_orders(self) -> list[ExchangeOrder]: ...

    async def positions(self) -> list[ExchangePosition]: ...

    async def fills_for(self, client_order_id: str) -> list[ExchangeFill]: ...

    async def exit_fills(
        self, symbol: str, client_order_id: str, since: datetime | None = None
    ) -> list[ExchangeFill]:
        """Fills that reduced the position opened by *client_order_id*."""
        ...

    async def place_protective_stop(
        self, symbol: str, side: str, size: Decimal, stop_price: Decimal, client_order_id: str
    ) -> ExchangeOrder: ...

    async def close_position(self, symbol: str, client_order_id: str) -> ExchangeOrder: ...

    async def amend_stop(
        self, symbol: str, client_order_id: str, stop_price: Decimal
    ) -> ExchangePosition | None:
        """Move the resting protective stop of the position on *symbol* to *stop_price*.

        Returns the venue's view of the position afterwards (``None`` if flat).  Raises
        :class:`OrderRejected` if the venue refused, :class:`AmbiguousOrderOutcome` if the
        outcome is unknown (callers re-read positions to find out).
        """
        ...

    async def available_balance(self) -> Decimal: ...

    async def account_equity(self) -> Decimal:
        """Wallet balance plus unrealised PnL (the capital the portfolio rules share)."""
        ...


def _d(value: Any) -> Decimal | None:
    return None if value is None or value == "" else Decimal(str(value))


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def order_from_payload(p: dict[str, Any]) -> ExchangeOrder:
    """Inverse of :meth:`ExchangeOrder.to_payload` (bus/journal payloads are JSON)."""
    return ExchangeOrder(
        client_order_id=p["client_order_id"],
        exchange_order_id=p.get("exchange_order_id"),
        symbol=p.get("symbol", ""),
        side=p.get("side", ""),
        size=_d(p.get("size")) or Decimal("0"),
        filled_size=_d(p.get("filled_size")) or Decimal("0"),
        avg_fill_price=_d(p.get("avg_fill_price")),
        state=VenueOrderState(p["state"]),
        updated_at=_dt(p["updated_at"]),
        fees=_d(p.get("fees")) or Decimal("0"),
        reason=p.get("reason"),
        purpose=FillPurpose(p.get("purpose", FillPurpose.ENTRY)),
        stop_price=_d(p.get("stop_price")),
        trail_amount=_d(p.get("trail_amount")),
    )


def fill_from_payload(p: dict[str, Any]) -> ExchangeFill:
    return ExchangeFill(
        fill_id=p["fill_id"],
        client_order_id=p.get("client_order_id"),
        exchange_order_id=p.get("exchange_order_id"),
        symbol=p.get("symbol", ""),
        side=p.get("side", ""),
        qty=_d(p.get("qty")) or Decimal("0"),
        price=_d(p.get("price")) or Decimal("0"),
        fee=_d(p.get("fee")) or Decimal("0"),
        liquidity=p.get("liquidity", "taker"),
        purpose=FillPurpose(p.get("purpose", FillPurpose.UNKNOWN)),
        occurred_at=_dt(p["occurred_at"]),
    )
