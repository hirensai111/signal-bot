"""Event bus: broadcast semantics, durability, dedupe, outage buffering and replay."""

import asyncio

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.db.models import Base
from signal_bot.events import (
    Event,
    EventBus,
    EventType,
    InMemoryJournal,
    SqlEventJournal,
    deterministic_event_id,
)


def _fill(fill_id: str, qty: str = "1") -> Event:
    return Event.create(
        EventType.EXCHANGE_FILL,
        {"fill_id": fill_id, "qty": qty, "client_order_id": "sig_x"},
        source="test",
        event_id=deterministic_event_id("test", "fill", fill_id),
    )


async def test_every_subscriber_receives_every_matching_event():
    """The core P0 fix: competing consumers no longer split the event stream."""
    bus = EventBus(InMemoryJournal())
    execution = bus.subscribe(
        "execution", [EventType.EXCHANGE_FILL, EventType.EXCHANGE_ORDER_UPDATE]
    )
    outcomes = bus.subscribe(
        "outcomes", [EventType.EXCHANGE_FILL, EventType.EXCHANGE_POSITION_UPDATE]
    )

    for i in range(50):
        await bus.publish(_fill(f"f{i}"))
    await bus.publish(
        Event.create(EventType.EXCHANGE_POSITION_UPDATE, {"symbol": "BTCUSD", "size": "0"}, "test")
    )
    await bus.publish(
        Event.create(EventType.EXCHANGE_ORDER_UPDATE, {"client_order_id": "sig_x"}, "test")
    )

    assert execution.lag == 51  # 50 fills + 1 order update
    assert outcomes.lag == 51  # 50 fills + 1 position update


async def test_duplicate_exchange_event_is_journaled_and_delivered_once():
    journal = InMemoryJournal()
    bus = EventBus(journal)
    sub = bus.subscribe("execution")

    first = await bus.publish(_fill("abc"))
    again = await bus.publish(_fill("abc"))

    assert first.sequence == 1
    assert again.sequence is None  # duplicate: not re-sequenced
    assert sub.lag == 1
    assert len(journal.events) == 1
    assert bus.duplicates == 1


async def test_non_durable_events_skip_the_journal():
    journal = InMemoryJournal()
    bus = EventBus(journal)
    sub = bus.subscribe("features")
    await bus.publish(Event.create(EventType.CANDLE_CLOSED, {"symbol": "BTCUSDT"}, "test"))
    assert sub.lag == 1
    assert journal.events == []


async def test_journal_outage_still_delivers_and_flushes_later():
    """Database down: consumers keep seeing exchange facts; nothing is lost."""
    journal = InMemoryJournal()
    bus = EventBus(journal)
    sub = bus.subscribe("execution")

    journal.fail_appends = True
    await bus.publish(_fill("during-outage-1"))
    await bus.publish(_fill("during-outage-2"))
    assert sub.lag == 2
    assert bus.journal_healthy is False
    assert bus.pending_journal_writes == 2

    journal.fail_appends = False
    await bus.publish(_fill("after-recovery"))
    assert bus.journal_healthy is True
    assert bus.pending_journal_writes == 0
    assert [e.payload["fill_id"] for e in journal.events] == [
        "during-outage-1",
        "during-outage-2",
        "after-recovery",
    ]


async def test_replay_redelivers_unacknowledged_events_after_restart():
    journal = InMemoryJournal()
    bus = EventBus(journal)
    sub = bus.subscribe("outcomes", [EventType.EXCHANGE_FILL])
    for i in range(5):
        await bus.publish(_fill(f"f{i}"))

    # consumer processes and acks the first three, then "crashes"
    for _ in range(3):
        ev = await sub.get(timeout=1)
        await sub.ack(ev)

    restarted = EventBus(journal)
    sub2 = restarted.subscribe("outcomes", [EventType.EXCHANGE_FILL])
    replayed = await restarted.replay(sub2)
    assert replayed == 2
    got = [(await sub2.get(timeout=1)).payload["fill_id"] for _ in range(2)]
    assert got == ["f3", "f4"]


async def test_subscriber_names_are_unique():
    bus = EventBus()
    bus.subscribe("a")
    with pytest.raises(ValueError):
        bus.subscribe("a")


async def test_get_times_out_with_none():
    bus = EventBus()
    sub = bus.subscribe("a")
    assert await sub.get(timeout=0.01) is None


async def test_sql_journal_is_idempotent_and_ordered(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'journal.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    journal = SqlEventJournal(factory)

    a, new_a = await journal.append(_fill("a"))
    b, new_b = await journal.append(_fill("b"))
    a2, new_a2 = await journal.append(_fill("a"))
    assert (new_a, new_b, new_a2) == (True, True, False)
    assert a2.sequence == a.sequence < b.sequence
    assert await journal.latest_sequence() == b.sequence

    events = await journal.read(0, [EventType.EXCHANGE_FILL])
    assert [e.payload["fill_id"] for e in events] == ["a", "b"]

    await journal.set_checkpoint("c1", b.sequence)
    await journal.set_checkpoint("c1", a.sequence)  # never moves backwards
    assert await journal.get_checkpoint("c1") == b.sequence
    await engine.dispose()


async def test_backpressure_never_drops():
    bus = EventBus()
    sub = bus.subscribe("slow", maxsize=2)

    async def producer():
        for i in range(5):
            await bus.publish(Event.create(EventType.CANDLE_CLOSED, {"i": i}, "t"))

    task = asyncio.create_task(producer())
    seen = []
    for _ in range(5):
        ev = await sub.get(timeout=1)
        seen.append(ev.payload["i"])
    await task
    assert seen == [0, 1, 2, 3, 4]
