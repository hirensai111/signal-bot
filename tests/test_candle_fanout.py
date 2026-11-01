"""Integration: every relevant closed candle reaches its intended feature engine.

Regression test for the shared ``closed_candle_queue`` bug where the HTF scanner
and the microstructure engine competed for one queue and discarded each other's
candles.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.db.models import Base
from signal_bot.features.htf_levels import HtfLevelScanner
from signal_bot.features.microstructure import MicrostructureEngine
from signal_bot.ingest.binance_ws import Kline
from signal_bot.storage.writer import CandleFanout, QueueWriter

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
MINUTES = {"1m": 1, "5m": 5, "1h": 60, "4h": 240}


def _k(interval: str, i: int, symbol: str = "BTCUSDT") -> Kline:
    return Kline(
        symbol=symbol,
        interval=interval,
        open_time=T0 + timedelta(minutes=MINUTES[interval] * i),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=5,
        taker_buy_volume=Decimal("5"),
        closed=True,
    )


async def _session_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fanout.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def test_every_candle_reaches_its_engine_through_the_real_pipeline(tmp_path):
    engine, factory = await _session_factory(tmp_path)
    ingest: asyncio.Queue = asyncio.Queue()
    writer = QueueWriter(ingest, session_factory=factory, flush_interval=0.05)

    htf = HtfLevelScanner(writer.subscribe("htf", {"1h", "4h"}), session_factory=factory)
    micro = MicrostructureEngine(
        writer.subscribe("micro", {"1m", "5m"}), session_factory=factory, settle_delay=0
    )
    sweep_q = writer.subscribe("sweep", {"1m", "5m"})

    htf_seen: list[tuple[str, datetime]] = []
    micro_seen: list[tuple[str, datetime]] = []

    async def record_htf(c):
        htf_seen.append((c.interval, c.open_time))

    async def record_micro(c):
        micro_seen.append((c.interval, c.open_time))

    htf._process_candle = record_htf
    micro._process_candle = record_micro

    # Interleave timeframes the way the live socket does.
    expected_htf, expected_ltf = [], []
    for i in range(40):
        for interval in ("1m", "5m", "1h", "4h"):
            k = _k(interval, i)
            await ingest.put(k)
            (expected_htf if interval in ("1h", "4h") else expected_ltf).append(
                (interval, k.open_time)
            )

    tasks = [asyncio.create_task(t) for t in (writer.run(), htf.run(), micro.run())]
    for _ in range(200):
        if len(htf_seen) == len(expected_htf) and len(micro_seen) == len(expected_ltf):
            break
        await asyncio.sleep(0.02)
    for c in (writer, htf, micro):
        c.shutdown()
    await asyncio.gather(*tasks)
    await engine.dispose()

    assert htf_seen == expected_htf
    assert micro_seen == expected_ltf
    assert sweep_q.qsize() == len(expected_ltf)


async def test_duplicate_and_out_of_order_candles_are_not_forwarded():
    fanout = CandleFanout()
    q = fanout.subscribe("engine")
    assert await fanout.publish(_k("1m", 2)) == 1
    assert await fanout.publish(_k("1m", 2)) == 0  # duplicate (backfill/live overlap)
    assert await fanout.publish(_k("1m", 1)) == 0  # late, older than last forwarded
    assert await fanout.publish(_k("1m", 3)) == 1
    assert await fanout.publish(_k("1m", 1, symbol="ETHUSDT")) == 1  # independent key
    assert q.qsize() == 3
    assert fanout.dropped == 2


async def test_interval_filters_are_independent():
    fanout = CandleFanout()
    htf = fanout.subscribe("htf", {"1h"})
    ltf = fanout.subscribe("ltf", {"1m"})
    everything = fanout.subscribe("all")
    await fanout.publish(_k("1h", 0))
    await fanout.publish(_k("1m", 0))
    assert (htf.qsize(), ltf.qsize(), everything.qsize()) == (1, 1, 2)
