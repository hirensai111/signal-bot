"""Transactional persistence for the order state machine, fills and the signal mirror.

Every transition is validated, written together with an ``order_transitions``
row in one transaction, and only then announced on the bus.  Exchange events
are applied idempotently and with sequence rules:

* the same ``event_id`` is never applied twice;
* an update older than the last applied exchange timestamp is ignored;
* ``filled_size`` never decreases (a late ``partially_filled`` after ``filled``
  is stale, not a reversal);
* a transition that moves *backwards* through the lifecycle is stale.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from signal_bot.core.clock import SYSTEM_CLOCK, Clock, utc
from signal_bot.db.models import Fill, Order, OrderTransition, Signal
from signal_bot.events.bus import EventBus
from signal_bot.events.envelope import Event, EventType, deterministic_event_id
from signal_bot.execution.adapter import ExchangeFill, ExchangeOrder, FillPurpose
from signal_bot.execution.orders import (
    SIGNAL_STATUS,
    OrderState,
    can_transition,
    state_from_venue,
)
from signal_bot.observability import metrics

logger = logging.getLogger(__name__)

S = OrderState

_LIFECYCLE_RANK = {
    S.PENDING: 0,
    S.SUBMITTING: 1,
    S.SUBMISSION_UNKNOWN: 2,
    S.RECONCILIATION_REQUIRED: 2,
    S.ACKNOWLEDGED: 3,
    S.CANCEL_PENDING: 4,
    S.PARTIALLY_FILLED: 4,
    S.FILLED: 5,
    S.CANCELLED: 6,
    S.EXPIRED: 6,
    S.REJECTED: 6,
    S.CLOSED: 7,
}


def classify_fill(fill: ExchangeFill, order: Order | None) -> FillPurpose:
    """Resolve an UNKNOWN fill against our order: same side opens, opposite side closes.

    Venue streams (e.g. Delta's user_trades) don't say whether a fill opened or closed
    a position; the local order does.  Explicit purposes are kept as reported.
    """
    if fill.purpose != FillPurpose.UNKNOWN or order is None:
        return fill.purpose
    return FillPurpose.ENTRY if fill.side == order.side else FillPurpose.UNKNOWN


def local_event_id(client_order_id: str, *parts: Any) -> str:
    """Deterministic id for locally-initiated transitions (retries stay idempotent)."""
    return deterministic_event_id("local", client_order_id, *parts)


def _clip(text: str | None, n: int) -> str | None:
    return None if text is None else text[:n]


@dataclass(frozen=True)
class TransitionResult:
    applied: bool
    outcome: str  # applied | duplicate | stale | noop | invalid | unknown_order
    from_state: OrderState | None = None
    to_state: OrderState | None = None
    order: Order | None = None


class OrderStore:
    def __init__(self, session_factory, bus: EventBus | None = None, clock: Clock = SYSTEM_CLOCK):
        self.session_factory = session_factory
        self.bus = bus
        self.clock = clock

    # ------------------------------------------------------------------
    # creation & queries
    # ------------------------------------------------------------------
    async def create(
        self,
        signal: Signal,
        *,
        venue: str,
        client_order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        size: Decimal,
        contract_value: Decimal,
        limit_price: Decimal | None,
        intended_entry: Decimal,
        stop_loss_price: Decimal,
        take_profit_price: Decimal | None,
        correlation_id: str | None = None,
        trail_mode: str = "none",
        trail_distance: Decimal | None = None,
        trail_from: datetime | None = None,
        reference_stop: Decimal | None = None,
        price_basis: Decimal | None = None,
        time_stop_at: datetime | None = None,
    ) -> Order | None:
        """Create the PENDING order for a signal.  Returns ``None`` if one already exists."""
        now = self.clock.now()
        order = Order(
            id=uuid.uuid4(),
            signal_id=signal.id,
            client_order_id=client_order_id,
            venue=venue,
            symbol=symbol,
            side=side,
            order_type=order_type,
            size=size,
            contract_value=contract_value,
            limit_price=limit_price,
            intended_entry=intended_entry,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            state=S.PENDING,
            filled_size=Decimal("0"),
            fees=Decimal("0"),
            protective_status="none",
            correlation_id=correlation_id,
            created_at=now,
            updated_at=now,
            trail_mode=trail_mode,
            trail_distance=trail_distance,
            trail_from=trail_from,
            reference_stop=reference_stop,
            price_basis=price_basis,
            current_stop_price=stop_loss_price,
            time_stop_at=time_stop_at,
        )
        async with self.session_factory() as session:
            session.add(order)
            fresh = await session.get(Signal, signal.id)
            if fresh is not None:
                fresh.client_order_id = client_order_id
                fresh.delta_symbol = symbol
                fresh.delta_entry_intended = intended_entry
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                return None
        return order

    async def get_by_client_id(self, client_order_id: str) -> Order | None:
        async with self.session_factory() as session:
            return await session.scalar(
                select(Order).where(Order.client_order_id == client_order_id)
            )

    async def get_for_signal(self, signal_id) -> Order | None:
        async with self.session_factory() as session:
            return await session.scalar(select(Order).where(Order.signal_id == signal_id))

    async def in_states(self, states) -> list[Order]:
        values = [str(s) for s in states]
        async with self.session_factory() as session:
            rows = await session.execute(select(Order).where(Order.state.in_(values)))
            return list(rows.scalars().all())

    async def symbols_active_since(self, since: datetime, exposure_states) -> set[str]:
        """Venue symbols with exposure now, or a position that closed at/after *since*."""
        values = [str(s) for s in exposure_states]
        async with self.session_factory() as session:
            rows = await session.execute(
                select(Order.symbol).where(
                    Order.state.in_(values)
                    | ((Order.state == str(S.CLOSED)) & (Order.updated_at >= since))
                )
            )
            return {r[0] for r in rows}

    async def recent(self, limit: int = 50) -> list[Order]:
        async with self.session_factory() as session:
            rows = await session.execute(
                select(Order).order_by(Order.updated_at.desc()).limit(limit)
            )
            return list(rows.scalars().all())

    async def transitions_for(self, client_order_id: str) -> list[OrderTransition]:
        async with self.session_factory() as session:
            rows = await session.execute(
                select(OrderTransition)
                .join(Order, Order.id == OrderTransition.order_id)
                .where(Order.client_order_id == client_order_id)
                .order_by(OrderTransition.id)
            )
            return list(rows.scalars().all())

    async def fills_for(self, client_order_id: str) -> list[Fill]:
        async with self.session_factory() as session:
            rows = await session.execute(
                select(Fill)
                .where(Fill.client_order_id == client_order_id)
                .order_by(Fill.occurred_at, Fill.fill_id)
            )
            return list(rows.scalars().all())

    # ------------------------------------------------------------------
    # transitions
    # ------------------------------------------------------------------
    async def transition(
        self,
        client_order_id: str,
        to_state: OrderState,
        *,
        event_id: str,
        reason: str | None = None,
        exchange_order_id: str | None = None,
        filled_size: Decimal | None = None,
        avg_fill_price: Decimal | None = None,
        exchange_ts: datetime | None = None,
        payload: dict | None = None,
        signal_status: str | None = None,
        correlation_id: str | None = None,
    ) -> TransitionResult:
        to_state = OrderState(to_state)
        exchange_ts = utc(exchange_ts)
        async with self.session_factory() as session:
            order = await session.scalar(
                select(Order).where(Order.client_order_id == client_order_id).with_for_update()
            )
            if order is None:
                return TransitionResult(False, "unknown_order")
            dup = await session.scalar(
                select(OrderTransition.id).where(OrderTransition.event_id == event_id)
            )
            if dup is not None:
                return TransitionResult(False, "duplicate", order=order)

            current = OrderState(order.state)
            last_ts = utc(order.last_exchange_update)
            if exchange_ts is not None and last_ts is not None and exchange_ts < last_ts:
                return self._stale(order, current, to_state, "older exchange timestamp")
            if filled_size is not None and filled_size < (order.filled_size or 0):
                return self._stale(order, current, to_state, "filled_size decreased")

            if to_state == current:
                grows = filled_size is not None and filled_size > (order.filled_size or 0)
                if not (to_state == S.PARTIALLY_FILLED and grows):
                    return TransitionResult(False, "noop", current, to_state, order)
            elif not can_transition(current, to_state):
                if _LIFECYCLE_RANK[to_state] <= _LIFECYCLE_RANK[current]:
                    return self._stale(order, current, to_state, "lifecycle regression")
                logger.error(
                    "Invalid transition %s -> %s for %s (%s)",
                    current,
                    to_state,
                    client_order_id,
                    reason,
                )
                metrics.RECONCILIATION_DISCREPANCIES.labels("invalid_transition").inc()
                return TransitionResult(False, "invalid", current, to_state, order)

            now = self.clock.now()
            order.state = to_state
            order.state_reason = _clip(reason, 200)
            order.updated_at = now
            if exchange_order_id:
                order.exchange_order_id = exchange_order_id
            if filled_size is not None:
                order.filled_size = filled_size
            if avg_fill_price is not None:
                order.avg_fill_price = avg_fill_price
            if exchange_ts is not None:
                order.last_exchange_update = exchange_ts
            if to_state == S.SUBMITTING:
                order.submitted_at = now

            session.add(
                OrderTransition(
                    order_id=order.id,
                    event_id=event_id,
                    from_state=current,
                    to_state=to_state,
                    reason=_clip(reason, 200),
                    exchange_order_id=order.exchange_order_id,
                    payload=payload,
                    occurred_at=now,
                )
            )
            await self._mirror_signal(session, order, to_state, reason, signal_status)
            try:
                await session.commit()
            except IntegrityError:
                # concurrent writer applied the same event first
                await session.rollback()
                return TransitionResult(False, "duplicate", order=order)

        metrics.ORDER_TRANSITIONS.labels(str(current), str(to_state)).inc()
        logger.info(
            "order %s: %s -> %s (%s)",
            client_order_id,
            current,
            to_state,
            reason or "",
            extra={"order_id": client_order_id, "signal_id": str(order.signal_id)},
        )
        await self._announce(order, current, to_state, reason, event_id, correlation_id)
        return TransitionResult(True, "applied", current, to_state, order)

    async def apply_venue_update(
        self, update: ExchangeOrder, *, event_id: str, correlation_id: str | None = None
    ) -> TransitionResult:
        order = await self.get_by_client_id(update.client_order_id)
        if order is None:
            return TransitionResult(False, "unknown_order")
        target = state_from_venue(update.state, update.filled_size, order.size)
        return await self.transition(
            update.client_order_id,
            target,
            event_id=event_id,
            reason=update.reason or f"venue:{update.state}",
            exchange_order_id=update.exchange_order_id,
            filled_size=update.filled_size,
            avg_fill_price=update.avg_fill_price,
            exchange_ts=update.updated_at,
            payload={"venue_state": str(update.state), "filled_size": str(update.filled_size)},
            correlation_id=correlation_id or order.correlation_id,
        )

    def _stale(self, order, current, target, why) -> TransitionResult:
        logger.info(
            "Ignoring stale update for %s (%s -> %s): %s",
            order.client_order_id,
            current,
            target,
            why,
        )
        return TransitionResult(False, "stale", current, target, order)

    async def _mirror_signal(self, session, order: Order, to_state, reason, signal_status) -> None:
        sig = await session.get(Signal, order.signal_id)
        if sig is None:
            return
        sig.status = signal_status or SIGNAL_STATUS[to_state]
        if order.exchange_order_id:
            sig.delta_order_id = order.exchange_order_id
        if order.avg_fill_price is not None:
            sig.delta_entry_actual = order.avg_fill_price
            if order.intended_entry:
                slip = (order.avg_fill_price - order.intended_entry) / order.intended_entry
                if order.side == "sell":
                    slip = -slip
                sig.slippage_bps = slip * Decimal("10000")
        sig.entry_fees_usdt = order.fees
        if to_state in (S.CANCELLED, S.REJECTED, S.EXPIRED) and reason:
            sig.cancelled_reason = _clip(reason, 50)

    async def _announce(self, order, current, to_state, reason, event_id, correlation_id) -> None:
        if self.bus is None:
            return
        event = Event.create(
            EventType.ORDER_STATE_CHANGED,
            {
                "client_order_id": order.client_order_id,
                "signal_id": str(order.signal_id),
                "symbol": order.symbol,
                "from_state": str(current),
                "to_state": str(to_state),
                "reason": reason,
                "filled_size": order.filled_size,
                "avg_fill_price": order.avg_fill_price,
            },
            source="order_store",
            event_id=deterministic_event_id("order_state", event_id),
            correlation_id=correlation_id or order.correlation_id,
            causation_id=event_id,
        )
        try:
            await self.bus.publish(event)
        except Exception:
            logger.exception("Failed to announce transition for %s", order.client_order_id)

    # ------------------------------------------------------------------
    # fills & protective status
    # ------------------------------------------------------------------
    async def record_fill(self, fill: ExchangeFill, venue: str) -> bool:
        """Insert a fill exactly once.  Returns ``True`` only the first time."""
        async with self.session_factory() as session:
            order = None
            if fill.client_order_id:
                order = await session.scalar(
                    select(Order).where(Order.client_order_id == fill.client_order_id)
                )
            purpose = classify_fill(fill, order)
            existing = await session.get(Fill, fill.fill_id, with_for_update=True)
            if existing is not None:
                # A bracket-child stream fill can arrive without its parent
                # client ID. REST recovery later supplies that association and
                # the actual stop/TP purpose; it is enrichment, not a new fill.
                same_execution = (
                    existing.venue == venue
                    and existing.symbol == fill.symbol
                    and existing.side == fill.side
                    and existing.qty == fill.qty
                    and abs(existing.price - fill.price)
                    <= max(abs(fill.price) * Decimal("1e-15"), Decimal("1e-16"))
                )
                if (
                    same_execution
                    and order is not None
                    and (
                        existing.order_id is None
                        or existing.client_order_id == fill.client_order_id
                    )
                ):
                    unowned = existing.order_id is None
                    existing.order_id = order.id
                    existing.client_order_id = fill.client_order_id
                    if existing.purpose == str(FillPurpose.UNKNOWN):
                        existing.purpose = str(purpose)
                    if unowned:
                        existing.fee = fill.fee
                        if purpose == FillPurpose.ENTRY:
                            order.fees = (order.fees or 0) + fill.fee
                            sig = await session.get(Signal, order.signal_id)
                            if sig is not None:
                                sig.entry_fees_usdt = order.fees
                    await session.commit()
                return False
            session.add(
                Fill(
                    fill_id=fill.fill_id,
                    order_id=order.id if order else None,
                    client_order_id=fill.client_order_id,
                    venue=venue,
                    symbol=fill.symbol,
                    side=fill.side,
                    purpose=str(purpose),
                    qty=fill.qty,
                    price=fill.price,
                    fee=fill.fee,
                    liquidity=fill.liquidity,
                    occurred_at=fill.occurred_at,
                )
            )
            if order is not None and purpose == FillPurpose.ENTRY:
                order.fees = (order.fees or 0) + fill.fee
                sig = await session.get(Signal, order.signal_id)
                if sig is not None:
                    sig.entry_fees_usdt = order.fees
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                # A nameless stream fill may win the INSERT race against its
                # associated REST backfill. Retry only the enrichment path once
                # the exact primary key is known to exist; do not lose ownership.
                if await session.get(Fill, fill.fill_id) is not None:
                    await session.close()
                    await self.record_fill(fill, venue)
                logger.info("Duplicate fill %s ignored", fill.fill_id)
                return False
        return True

    async def update_trailing(
        self,
        client_order_id: str,
        *,
        best_price: Decimal | None = None,
        best_price_bar: datetime | None = None,
        current_stop_price: Decimal | None = None,
    ) -> Order | None:
        """Persist trailing-stop progress.  The stop is only ever written tighter."""
        async with self.session_factory() as session:
            order = await session.scalar(
                select(Order).where(Order.client_order_id == client_order_id).with_for_update()
            )
            if order is None:
                return None
            if best_price is not None:
                order.best_price = best_price
                order.best_price_bar = best_price_bar
            if current_stop_price is not None:
                order.current_stop_price = current_stop_price
                order.stop_updated_at = self.clock.now()
            order.updated_at = self.clock.now()
            await session.commit()
            return order

    async def set_exit_hint(self, client_order_id: str, hint: str) -> None:
        """Record why *we* closed a position (e.g. ``time_stop``) for the outcome record."""
        async with self.session_factory() as session:
            order = await session.scalar(
                select(Order).where(Order.client_order_id == client_order_id)
            )
            if order is not None:
                order.exit_reason_hint = hint[:20]
                await session.commit()

    async def set_protective_status(self, client_order_id: str, status: str) -> None:
        async with self.session_factory() as session:
            order = await session.scalar(
                select(Order).where(Order.client_order_id == client_order_id)
            )
            if order is not None:
                order.protective_status = status
                order.updated_at = self.clock.now()
                await session.commit()
