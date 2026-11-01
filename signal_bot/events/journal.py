"""Durable event journal (PostgreSQL/SQLite via SQLAlchemy) plus an in-memory twin.

Guarantees:

* ``append`` is idempotent on ``event_id`` — appending the same exchange fact
  twice returns the originally stored event and ``is_new=False``.
* ``sequence`` is strictly increasing and defines replay order.
* Consumer checkpoints only move forward, so a crash between processing and
  acknowledging causes redelivery (at-least-once), never loss.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Protocol

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from signal_bot.core.clock import utc
from signal_bot.db.models import ConsumerCheckpoint, EventRecord
from signal_bot.events.envelope import Event

logger = logging.getLogger(__name__)


class EventJournal(Protocol):
    async def append(self, event: Event) -> tuple[Event, bool]: ...

    async def read(
        self,
        after_sequence: int = 0,
        event_types: Iterable[str] | None = None,
        limit: int = 1000,
    ) -> list[Event]: ...

    async def latest_sequence(self) -> int: ...

    async def get_checkpoint(self, consumer: str) -> int: ...

    async def set_checkpoint(self, consumer: str, sequence: int) -> None: ...


def _record_to_event(rec: EventRecord) -> Event:
    return Event(
        event_type=rec.event_type,
        payload=rec.payload,
        source=rec.source,
        event_id=rec.event_id,
        occurred_at=utc(rec.occurred_at),
        recorded_at=utc(rec.recorded_at),
        correlation_id=rec.correlation_id,
        causation_id=rec.causation_id,
        schema_version=rec.schema_version,
        sequence=rec.sequence,
    )


class SqlEventJournal:
    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory

    async def append(self, event: Event) -> tuple[Event, bool]:
        async with self.session_factory() as session:
            rec = EventRecord(
                event_id=event.event_id,
                event_type=event.event_type,
                source=event.source,
                occurred_at=event.occurred_at,
                recorded_at=event.recorded_at,
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
                schema_version=event.schema_version,
                payload=event.payload,
            )
            session.add(rec)
            try:
                await session.commit()
                return event.with_sequence(rec.sequence), True
            except IntegrityError:
                await session.rollback()
        async with self.session_factory() as session:
            existing = await session.scalar(
                select(EventRecord).where(EventRecord.event_id == event.event_id)
            )
            if existing is None:  # pragma: no cover - integrity error for another reason
                raise RuntimeError(f"journal append failed for event {event.event_id}")
            return _record_to_event(existing), False

    async def read(
        self,
        after_sequence: int = 0,
        event_types: Iterable[str] | None = None,
        limit: int = 1000,
    ) -> list[Event]:
        stmt = select(EventRecord).where(EventRecord.sequence > after_sequence)
        if event_types:
            stmt = stmt.where(EventRecord.event_type.in_([str(t) for t in event_types]))
        stmt = stmt.order_by(EventRecord.sequence).limit(limit)
        async with self.session_factory() as session:
            rows = (await session.execute(stmt)).scalars().all()
        return [_record_to_event(r) for r in rows]

    async def latest_sequence(self) -> int:
        async with self.session_factory() as session:
            value = await session.scalar(select(func.max(EventRecord.sequence)))
        return int(value or 0)

    async def get_checkpoint(self, consumer: str) -> int:
        async with self.session_factory() as session:
            cp = await session.get(ConsumerCheckpoint, consumer)
            return int(cp.last_sequence) if cp else 0

    async def set_checkpoint(self, consumer: str, sequence: int) -> None:
        async with self.session_factory() as session:
            cp = await session.get(ConsumerCheckpoint, consumer)
            if cp is None:
                session.add(
                    ConsumerCheckpoint(
                        consumer=consumer,
                        last_sequence=sequence,
                        updated_at=datetime.now(timezone.utc),
                    )
                )
            elif sequence > cp.last_sequence:
                cp.last_sequence = sequence
                cp.updated_at = datetime.now(timezone.utc)
            await session.commit()


class InMemoryJournal:
    """Same semantics as :class:`SqlEventJournal`; used by backtests and unit tests."""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self._by_id: dict[str, Event] = {}
        self._checkpoints: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self.fail_appends = False  # fault-injection hook: simulate database outage

    async def append(self, event: Event) -> tuple[Event, bool]:
        if self.fail_appends:
            raise ConnectionError("journal unavailable (injected)")
        async with self._lock:
            if event.event_id in self._by_id:
                return self._by_id[event.event_id], False
            stored = event.with_sequence(len(self._events) + 1)
            self._events.append(stored)
            self._by_id[event.event_id] = stored
            return stored, True

    async def read(
        self,
        after_sequence: int = 0,
        event_types: Iterable[str] | None = None,
        limit: int = 1000,
    ) -> list[Event]:
        types = {str(t) for t in event_types} if event_types else None
        out = [
            e
            for e in self._events
            if (e.sequence or 0) > after_sequence and (types is None or e.event_type in types)
        ]
        return out[:limit]

    async def latest_sequence(self) -> int:
        return len(self._events)

    async def get_checkpoint(self, consumer: str) -> int:
        return self._checkpoints.get(consumer, 0)

    async def set_checkpoint(self, consumer: str, sequence: int) -> None:
        if sequence > self._checkpoints.get(consumer, 0):
            self._checkpoints[consumer] = sequence

    @property
    def events(self) -> list[Event]:
        return list(self._events)
