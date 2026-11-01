"""Tests for HTF fractal level detection."""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.config import settings
from signal_bot.db.models import HtfLevel
from signal_bot.features.htf_levels import HtfLevelScanner
from signal_bot.ingest.binance_ws import Kline

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
    interval: str = "1h",
    open_time: datetime | None = None,
    open_p: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close_p: float = 100.5,
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
        closed=True,
    )


@pytest.mark.asyncio
async def test_swing_high_detection() -> None:
    scanner = HtfLevelScanner(asyncio.Queue(), lookback=2, session_factory=TestSessionLocal)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    candles = [
        _kline(open_time=base, high=100, low=90, close_p=95),
        _kline(open_time=base + timedelta(hours=1), high=101, low=91, close_p=96),
        _kline(open_time=base + timedelta(hours=2), high=110, low=95, close_p=100),  # swing high
        _kline(open_time=base + timedelta(hours=3), high=102, low=92, close_p=97),
        _kline(open_time=base + timedelta(hours=4), high=103, low=93, close_p=98),
    ]

    for c in candles:
        await scanner._process_candle(c)

    async with TestSessionLocal() as session:
        levels = (await session.execute(select(HtfLevel))).scalars().all()
        assert len(levels) == 1
        assert levels[0].level_type == "swing_high"
        assert levels[0].price == Decimal("110")
        assert levels[0].active is True


@pytest.mark.asyncio
async def test_swing_low_detection() -> None:
    scanner = HtfLevelScanner(asyncio.Queue(), lookback=2, session_factory=TestSessionLocal)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    candles = [
        _kline(open_time=base, high=110, low=100, close_p=105),
        _kline(open_time=base + timedelta(hours=1), high=109, low=101, close_p=104),
        _kline(open_time=base + timedelta(hours=2), high=108, low=85, close_p=90),  # swing low
        _kline(open_time=base + timedelta(hours=3), high=107, low=99, close_p=103),
        _kline(open_time=base + timedelta(hours=4), high=106, low=98, close_p=102),
    ]

    for c in candles:
        await scanner._process_candle(c)

    async with TestSessionLocal() as session:
        levels = (await session.execute(select(HtfLevel))).scalars().all()
        assert len(levels) == 1
        assert levels[0].level_type == "swing_low"
        assert levels[0].price == Decimal("85")


@pytest.mark.asyncio
async def test_level_breaking() -> None:
    scanner = HtfLevelScanner(asyncio.Queue(), lookback=1, session_factory=TestSessionLocal)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    # Swing high at hour 1 (high=105)
    await scanner._process_candle(_kline(open_time=base, high=100, low=90, close_p=95))
    await scanner._process_candle(
        _kline(open_time=base + timedelta(hours=1), high=105, low=95, close_p=100)
    )
    await scanner._process_candle(
        _kline(open_time=base + timedelta(hours=2), high=100, low=90, close_p=95)
    )

    # Now close above the swing high -> break it
    await scanner._process_candle(
        _kline(open_time=base + timedelta(hours=3), high=106, low=100, close_p=106)
    )

    async with TestSessionLocal() as session:
        levels = (await session.execute(select(HtfLevel))).scalars().all()
        assert len(levels) == 2
        broken = [l for l in levels if not l.active]
        assert len(broken) == 1
        assert broken[0].level_type == "swing_high"
        assert broken[0].broken_at.replace(tzinfo=None) == (base + timedelta(hours=3)).replace(
            tzinfo=None
        )


@pytest.mark.asyncio
async def test_swing_low_breaking() -> None:
    scanner = HtfLevelScanner(asyncio.Queue(), lookback=1, session_factory=TestSessionLocal)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    # Swing low at hour 1 (low=90)
    await scanner._process_candle(_kline(open_time=base, high=110, low=100, close_p=105))
    await scanner._process_candle(
        _kline(open_time=base + timedelta(hours=1), high=105, low=90, close_p=100)
    )
    await scanner._process_candle(
        _kline(open_time=base + timedelta(hours=2), high=110, low=100, close_p=105)
    )

    # Close below the swing low -> break it
    await scanner._process_candle(
        _kline(open_time=base + timedelta(hours=3), high=89, low=85, close_p=85)
    )

    async with TestSessionLocal() as session:
        levels = (await session.execute(select(HtfLevel))).scalars().all()
        assert len(levels) == 2
        broken = [l for l in levels if not l.active]
        assert len(broken) == 1
        assert broken[0].level_type == "swing_low"
        assert broken[0].broken_at.replace(tzinfo=None) == (base + timedelta(hours=3)).replace(
            tzinfo=None
        )


@pytest.mark.asyncio
async def test_prune_keeps_last_20_active() -> None:
    scanner = HtfLevelScanner(asyncio.Queue(), lookback=1, session_factory=TestSessionLocal)
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    # Generate 25 swing highs with closes=0 so none are broken
    for i in range(25):
        await scanner._process_candle(
            _kline(open_time=base + timedelta(hours=i * 3), high=i, low=0, close_p=0)
        )
        await scanner._process_candle(
            _kline(open_time=base + timedelta(hours=i * 3 + 1), high=i + 100, low=0, close_p=0)
        )
        await scanner._process_candle(
            _kline(open_time=base + timedelta(hours=i * 3 + 2), high=i + 50, low=0, close_p=0)
        )

    async with TestSessionLocal() as session:
        all_levels = (
            (await session.execute(select(HtfLevel).order_by(HtfLevel.formed_at))).scalars().all()
        )
        active = [l for l in all_levels if l.active]
        assert len(active) == 20
        assert len(all_levels) == 25
        # Oldest 5 levels pruned (inactive); newest 20 active
        assert all(not lvl.active for lvl in all_levels[:5])
        assert all(lvl.active for lvl in all_levels[5:])
