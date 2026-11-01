"""In-process publish/subscribe with one queue per subscriber.

This replaces the shared ``asyncio.Queue`` objects that several consumers used
to ``.get()`` from concurrently.  ``asyncio.Queue`` *distributes* items between
competing getters, so an order-fill could be swallowed by the outcome monitor
and never reach the execution engine.  Here every matching subscriber receives
its own copy of every event.

Durable events are journaled *before* delivery.  If the journal is unavailable
the event is still delivered (consumers keep working — e.g. positions stay
visible while the database is down) and is buffered for a later journal write.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable, Iterable

from signal_bot.events.envelope import Event
from signal_bot.events.journal import EventJournal
from signal_bot.observability import metrics

logger = logging.getLogger(__name__)

Predicate = Callable[[Event], bool]


class Subscription:
    """A named consumer with its own queue and replay checkpoint."""

    def __init__(
        self,
        bus: EventBus,
        name: str,
        event_types: frozenset[str] | None,
        predicate: Predicate | None,
        maxsize: int,
    ) -> None:
        self.bus = bus
        self.name = name
        self.event_types = event_types
        self.predicate = predicate
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)
        self.delivered = 0
        self.acked = 0
        self._last_acked_sequence = 0

    def matches(self, event: Event) -> bool:
        if self.event_types is not None and event.event_type not in self.event_types:
            return False
        return self.predicate is None or self.predicate(event)

    async def get(self, timeout: float | None = None) -> Event | None:
        """Next event, or ``None`` on timeout."""
        try:
            if timeout is None:
                event = await self.queue.get()
            else:
                event = await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except TimeoutError:
            return None
        metrics.EVENT_QUEUE_LAG.labels(self.name).set(self.queue.qsize())
        return event

    async def ack(self, event: Event) -> None:
        """Mark an event fully processed.  Only advances the durable checkpoint."""
        self.acked += 1
        if event.sequence is None or self.bus.journal is None:
            return
        if event.sequence <= self._last_acked_sequence:
            return
        self._last_acked_sequence = event.sequence
        try:
            await self.bus.journal.set_checkpoint(self.name, event.sequence)
        except Exception:
            # Not fatal: the event will be redelivered on replay and consumers are idempotent.
            logger.warning("checkpoint write failed for %s@%s", self.name, event.sequence)

    @property
    def lag(self) -> int:
        return self.queue.qsize()


class EventBus:
    def __init__(self, journal: EventJournal | None = None) -> None:
        self.journal = journal
        self._subs: dict[str, Subscription] = {}
        self._pending_journal: deque[Event] = deque()
        self._journal_lock = asyncio.Lock()
        self.journal_healthy = True
        self.published = 0
        self.duplicates = 0

    # ------------------------------------------------------------------
    def subscribe(
        self,
        name: str,
        event_types: Iterable[str] | None = None,
        predicate: Predicate | None = None,
        maxsize: int = 10_000,
    ) -> Subscription:
        if name in self._subs:
            raise ValueError(f"subscriber {name!r} already registered")
        types = frozenset(str(t) for t in event_types) if event_types is not None else None
        sub = Subscription(self, name, types, predicate, maxsize)
        self._subs[name] = sub
        return sub

    def unsubscribe(self, name: str) -> None:
        self._subs.pop(name, None)

    @property
    def subscriptions(self) -> dict[str, Subscription]:
        return dict(self._subs)

    # ------------------------------------------------------------------
    async def publish(self, event: Event, *, durable: bool | None = None) -> Event:
        """Journal (if durable) then fan out.  Returns the event with its sequence.

        A duplicate ``event_id`` already present in the journal is *not*
        redelivered: the exchange told us something we already know.
        """
        durable = event.is_durable if durable is None else durable
        if durable and self.journal is not None:
            stored = await self._journal_append(event)
            if stored is None:  # duplicate
                return event
            event = stored
        self.published += 1
        metrics.EVENTS_PUBLISHED.labels(event.event_type).inc()
        for sub in list(self._subs.values()):
            if sub.matches(event):
                # Backpressure rather than drop: losing a fill is worse than slowing ingest.
                await sub.queue.put(event)
                sub.delivered += 1
                metrics.EVENT_QUEUE_LAG.labels(sub.name).set(sub.queue.qsize())
        return event

    async def _journal_append(self, event: Event) -> Event | None:
        assert self.journal is not None
        try:
            if self._pending_journal:
                await self.flush_pending()
            stored, is_new = await self.journal.append(event)
        except Exception:
            metrics.JOURNAL_WRITE_FAILURES.inc()
            if self.journal_healthy:
                logger.error("Event journal unavailable; buffering durable events in memory")
            self.journal_healthy = False
            self._pending_journal.append(event)
            metrics.JOURNAL_PENDING.set(len(self._pending_journal))
            return event
        self.journal_healthy = True
        if not is_new:
            self.duplicates += 1
            metrics.EVENTS_DUPLICATE.labels(event.event_type).inc()
            logger.debug("duplicate event suppressed: %s %s", event.event_type, event.event_id)
            return None
        return stored

    async def flush_pending(self) -> int:
        """Retry journal writes buffered during an outage.  Returns events written."""
        if self.journal is None:
            return 0
        written = 0
        async with self._journal_lock:
            while self._pending_journal:
                event = self._pending_journal[0]
                await self.journal.append(event)  # raises -> stays buffered
                self._pending_journal.popleft()
                written += 1
            metrics.JOURNAL_PENDING.set(0)
            self.journal_healthy = True
        return written

    @property
    def pending_journal_writes(self) -> int:
        return len(self._pending_journal)

    # ------------------------------------------------------------------
    async def replay(self, subscription: Subscription, batch: int = 500) -> int:
        """Re-deliver journaled events after the subscriber's checkpoint.

        Call during startup *before* live streams begin publishing so the
        consumer sees history in sequence order.  Consumers must be idempotent:
        events processed but not acknowledged before a crash arrive again.
        """
        if self.journal is None:
            return 0
        after = await self.journal.get_checkpoint(subscription.name)
        subscription._last_acked_sequence = after
        delivered = 0
        while True:
            events = await self.journal.read(after, subscription.event_types, limit=batch)
            if not events:
                break
            for event in events:
                if subscription.matches(event):
                    await subscription.queue.put(event)
                    delivered += 1
                after = event.sequence or after
            if len(events) < batch:
                break
        if delivered:
            logger.info(
                "Replayed %d events to %s from sequence > %s", delivered, subscription.name, after
            )
        return delivered

    def stats(self) -> dict:
        return {
            "published": self.published,
            "duplicates_suppressed": self.duplicates,
            "journal_healthy": self.journal_healthy,
            "journal_pending": len(self._pending_journal),
            "subscribers": {
                name: {"lag": s.lag, "delivered": s.delivered, "acked": s.acked}
                for name, s in self._subs.items()
            },
        }
