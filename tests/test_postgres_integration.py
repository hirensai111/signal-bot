"""Integration tests against a real PostgreSQL (CI service container).

Set TEST_POSTGRES_URL, e.g.
postgresql+asyncpg://signalbot:signalbot@localhost:5432/signalbot_test
These cover what SQLite cannot: BIGINT identity sequences, row locks
(SELECT ... FOR UPDATE), ON CONFLICT on real constraints and concurrent writers.
"""

import asyncio
import os
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from signal_bot.db.models import Candle1m, Fill, OrderTransition
from signal_bot.events import Event, EventType, SqlEventJournal, deterministic_event_id
from signal_bot.execution.orders import OrderState
from signal_bot.ingest.binance_ws import Kline
from signal_bot.storage.writer import QueueWriter
from tests.harness import Harness

PG_URL = os.environ.get("TEST_POSTGRES_URL")
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not PG_URL, reason="TEST_POSTGRES_URL not set"),
]


@pytest.fixture
async def h(tmp_path):
    harness = await Harness.create(tmp_path, db_url=PG_URL)
    yield harness
    await harness.close()


async def test_journal_append_is_idempotent_under_concurrency(h):
    journal = SqlEventJournal(h.raw_factory)
    event = Event.create(
        EventType.EXCHANGE_FILL,
        {"fill_id": "pg-1"},
        "test",
        event_id=deterministic_event_id("pg", "fill", 1),
    )
    results = await asyncio.gather(*(journal.append(event) for _ in range(10)))
    assert sum(1 for _, new in results if new) == 1
    assert len({stored.sequence for stored, _ in results}) == 1


async def test_concurrent_duplicate_transitions_apply_once(h):
    sig = await h.make_signal()
    await h.store.create(
        sig,
        venue="sim",
        client_order_id="sig_pg",
        symbol="BTCUSD",
        side="buy",
        order_type="limit",
        size=Decimal("10"),
        contract_value=Decimal("0.001"),
        limit_price=Decimal("65000"),
        intended_entry=Decimal("65000"),
        stop_loss_price=Decimal("64000"),
        take_profit_price=Decimal("67000"),
    )
    await h.store.transition("sig_pg", OrderState.SUBMITTING, event_id="pg-submit")
    outcomes = await asyncio.gather(
        *(
            h.store.transition("sig_pg", OrderState.ACKNOWLEDGED, event_id="pg-ack")
            for _ in range(8)
        )
    )
    assert sum(r.applied for r in outcomes) == 1
    async with h.raw_factory() as s:
        n = await s.scalar(select(func.count()).select_from(OrderTransition))
    assert n == 2


async def test_full_trade_lifecycle_and_duplicate_fills_on_postgres(h):
    h.venue.faults.add("place_order", "timeout_after_accept")
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.order_for(sig)).state == OrderState.FILLED
    h.venue.faults.add("stream", "duplicate_fills_new_id", times=-1)
    await h.venue.move_market("BTCUSD", Decimal("67100"), Decimal("67100.5"))
    await h.drain()
    assert (await h.order_for(sig)).state == OrderState.CLOSED
    assert len(await h.outcomes()) == 1
    async with h.raw_factory() as s:
        fills = await s.scalar(select(func.count()).select_from(Fill))
    assert fills == 2
    assert h.venue.calls["place_order"] == 1


async def test_writer_on_conflict_on_postgres(h):
    q: asyncio.Queue = asyncio.Queue()
    writer = QueueWriter(q, session_factory=h.raw_factory, flush_interval=0.01)
    k = Kline(
        "BTCUSDT",
        "1m",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        *(Decimal("1"),) * 6,
        1,
        Decimal("1"),
        True,
    )
    for _ in range(3):
        await q.put(k)
    task = asyncio.create_task(writer.run())
    await asyncio.sleep(0.3)
    writer.shutdown()
    await task
    async with h.raw_factory() as s:
        assert await s.scalar(select(func.count()).select_from(Candle1m)) == 1
