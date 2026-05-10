"""Tests for the async storage writer."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.config import settings
from signal_bot.db.models import AggTrade as AggTradeModel, Candle1m, Candle5m
from signal_bot.ingest.binance_ws import AggTrade, Kline
from signal_bot.storage.writer import QueueWriter

TEST_DB_URL = settings.database_url.rsplit("/", 1)[0] + "/signal_bot_test"
_test_engine = create_async_engine(TEST_DB_URL, echo=False, future=True)
TestSessionLocal = async_sessionmaker(
    bind=_test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


def _kline(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    open_time: datetime | None = None,
    open_p: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close_p: float = 100.5,
    closed: bool = True,
) -> Kline:
    return Kline(
        symbol=symbol,
        interval=interval,
        open_time=open_time or datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        open=Decimal(str(open_p)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close_p)),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=5,
        taker_buy_volume=Decimal("5"),
        closed=closed,
    )


def _agg_trade(
    symbol: str = "BTCUSDT",
    trade_id: int = 1,
    price: float = 100.5,
    qty: float = 1.0,
    ts: datetime | None = None,
) -> AggTrade:
    return AggTrade(
        symbol=symbol,
        trade_id=trade_id,
        price=Decimal(str(price)),
        qty=Decimal(str(qty)),
        quote_qty=Decimal(str(price * qty)),
        ts=ts or datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        is_buyer_maker=False,
    )


@pytest.mark.asyncio
async def test_writer_persists_closed_candles_and_trades() -> None:
    queue = asyncio.Queue()
    writer = QueueWriter(
        queue,
        session_factory=TestSessionLocal,
        flush_interval=60.0,
        batch_size=2,
    )

    await queue.put(_kline(interval="1m", open_time=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)))
    await queue.put(_kline(interval="5m", open_time=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)))
    await queue.put(_agg_trade())

    task = asyncio.create_task(writer.run())
    await asyncio.sleep(0.3)
    writer.shutdown()
    await task

    async with TestSessionLocal() as session:
        c1 = (await session.execute(select(Candle1m))).scalars().all()
        c5 = (await session.execute(select(Candle5m))).scalars().all()
        at = (await session.execute(select(AggTradeModel))).scalars().all()
        assert len(c1) == 1
        assert len(c5) == 1
        assert len(at) == 1
        assert c1[0].symbol == "BTCUSDT"
        assert at[0].trade_id == 1


@pytest.mark.asyncio
async def test_writer_dedupes_on_conflict() -> None:
    queue = asyncio.Queue()
    writer = QueueWriter(
        queue,
        session_factory=TestSessionLocal,
        flush_interval=60.0,
        batch_size=2,
    )

    ts = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    await queue.put(_kline(interval="1m", open_time=ts, close_p=100.0))
    await queue.put(_kline(interval="1m", open_time=ts, close_p=101.0))

    task = asyncio.create_task(writer.run())
    await asyncio.sleep(0.3)
    writer.shutdown()
    await task

    async with TestSessionLocal() as session:
        rows = (await session.execute(select(Candle1m))).scalars().all()
        assert len(rows) == 1
        assert rows[0].close == Decimal("100")


@pytest.mark.asyncio
async def test_writer_emits_closed_candle_events() -> None:
    queue = asyncio.Queue()
    writer = QueueWriter(
        queue,
        session_factory=TestSessionLocal,
        flush_interval=60.0,
        batch_size=10,
    )

    await queue.put(_kline(interval="1m", closed=True))
    await queue.put(_kline(interval="1m", closed=False))

    task = asyncio.create_task(writer.run())
    await asyncio.sleep(0.3)
    writer.shutdown()
    await task

    # Only the closed candle should be emitted
    assert writer.closed_candle_queue.qsize() == 1
    event = await writer.closed_candle_queue.get()
    assert isinstance(event, Kline)
    assert event.closed is True


@pytest.mark.asyncio
async def test_writer_routes_htf_candles_to_event_queue_not_db() -> None:
    queue = asyncio.Queue()
    writer = QueueWriter(
        queue,
        session_factory=TestSessionLocal,
        flush_interval=60.0,
        batch_size=10,
    )

    await queue.put(_kline(interval="1h", closed=True))
    await queue.put(_kline(interval="4h", closed=True))

    task = asyncio.create_task(writer.run())
    await asyncio.sleep(0.3)
    writer.shutdown()
    await task

    assert writer.closed_candle_queue.qsize() == 2

    async with TestSessionLocal() as session:
        assert len((await session.execute(select(Candle1m))).scalars().all()) == 0
        assert len((await session.execute(select(Candle5m))).scalars().all()) == 0
