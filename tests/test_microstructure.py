"""Tests for microstructure engine (CVD + volume metrics)."""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.config import settings
from signal_bot.db.models import AggTrade as AggTradeModel, Candle1m, CandleFeature
from signal_bot.features.microstructure import MicrostructureEngine, get_recent_features
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
    interval: str = "1m",
    open_time: datetime | None = None,
    volume: float = 10.0,
) -> Kline:
    return Kline(
        symbol=symbol,
        interval=interval,
        open_time=open_time or datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal(str(volume)),
        quote_volume=Decimal("1000"),
        trade_count=5,
        taker_buy_volume=Decimal("5"),
        closed=True,
    )


@pytest.mark.asyncio
async def test_cvd_and_volume_metrics() -> None:
    engine = MicrostructureEngine(
        asyncio.Queue(), session_factory=TestSessionLocal, settle_delay=0
    )
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    # Seed 19 historical candles with volume=10
    async with TestSessionLocal() as session:
        for i in range(19):
            session.add(
                Candle1m(
                    symbol="BTCUSDT",
                    open_time=base + timedelta(minutes=i),
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
            )
        await session.commit()

    # Current candle time
    t20 = base + timedelta(minutes=19)

    # Insert agg_trades for the current candle window
    async with TestSessionLocal() as session:
        session.add_all(
            [
                AggTradeModel(
                    symbol="BTCUSDT",
                    trade_id=1,
                    price=Decimal("100"),
                    qty=Decimal("5"),
                    quote_qty=Decimal("500"),
                    ts=t20,
                    is_buyer_maker=False,
                ),
                AggTradeModel(
                    symbol="BTCUSDT",
                    trade_id=2,
                    price=Decimal("100"),
                    qty=Decimal("2"),
                    quote_qty=Decimal("200"),
                    ts=t20 + timedelta(seconds=30),
                    is_buyer_maker=True,
                ),
            ]
        )
        await session.commit()

    current_candle = _kline(open_time=t20, volume=20.0)
    await engine._process_candle(current_candle)

    async with TestSessionLocal() as session:
        feature = await session.scalar(
            select(CandleFeature).where(
                CandleFeature.symbol == "BTCUSDT",
                CandleFeature.timeframe == "1m",
                CandleFeature.open_time == t20,
            )
        )

    assert feature is not None
    assert feature.cvd_delta == Decimal("3")  # 5 buy - 2 sell
    assert feature.cvd_cumulative == Decimal("3")

    # Volume stats: 19 historical @ 10 + current @ 20
    mean = Decimal("10.5")
    variance = Decimal("4.75")
    std = variance.sqrt()
    expected_z = (Decimal("20") - mean) / std
    expected_r = Decimal("20") / mean

    assert float(feature.vol_ratio_20) == pytest.approx(float(expected_r), rel=1e-4)
    assert float(feature.vol_zscore) == pytest.approx(float(expected_z), rel=1e-4)


@pytest.mark.asyncio
async def test_cvd_cumulative_across_candles() -> None:
    engine = MicrostructureEngine(
        asyncio.Queue(), session_factory=TestSessionLocal, settle_delay=0
    )
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    # Candle 1: net +5
    async with TestSessionLocal() as session:
        session.add(
            AggTradeModel(
                symbol="BTCUSDT",
                trade_id=1,
                price=Decimal("100"),
                qty=Decimal("10"),
                quote_qty=Decimal("1000"),
                ts=base,
                is_buyer_maker=False,
            )
        )
        session.add(
            AggTradeModel(
                symbol="BTCUSDT",
                trade_id=2,
                price=Decimal("100"),
                qty=Decimal("5"),
                quote_qty=Decimal("500"),
                ts=base,
                is_buyer_maker=True,
            )
        )
        await session.commit()

    await engine._process_candle(_kline(open_time=base, volume=10.0))

    # Candle 2: net -2
    t2 = base + timedelta(minutes=1)
    async with TestSessionLocal() as session:
        session.add(
            AggTradeModel(
                symbol="BTCUSDT",
                trade_id=3,
                price=Decimal("100"),
                qty=Decimal("3"),
                quote_qty=Decimal("300"),
                ts=t2,
                is_buyer_maker=False,
            )
        )
        session.add(
            AggTradeModel(
                symbol="BTCUSDT",
                trade_id=4,
                price=Decimal("100"),
                qty=Decimal("5"),
                quote_qty=Decimal("500"),
                ts=t2,
                is_buyer_maker=True,
            )
        )
        await session.commit()

    await engine._process_candle(_kline(open_time=t2, volume=10.0))

    async with TestSessionLocal() as session:
        f1 = await session.scalar(
            select(CandleFeature).where(CandleFeature.open_time == base)
        )
        f2 = await session.scalar(
            select(CandleFeature).where(CandleFeature.open_time == t2)
        )

    assert f1.cvd_cumulative == Decimal("5")
    assert f2.cvd_cumulative == Decimal("3")  # 5 + (-2)


@pytest.mark.asyncio
async def test_get_recent_features_returns_dataframe() -> None:
    engine = MicrostructureEngine(
        asyncio.Queue(), session_factory=TestSessionLocal, settle_delay=0
    )
    base = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

    async with TestSessionLocal() as session:
        for i in range(5):
            session.add(
                Candle1m(
                    symbol="ETHUSDT",
                    open_time=base + timedelta(minutes=i),
                    open=Decimal("200"),
                    high=Decimal("201"),
                    low=Decimal("199"),
                    close=Decimal("200"),
                    volume=Decimal("10"),
                    quote_volume=Decimal("2000"),
                    trade_count=5,
                    taker_buy_volume=Decimal("5"),
                    closed=True,
                )
            )
        await session.commit()

    for i in range(5):
        await engine._process_candle(
            _kline(symbol="ETHUSDT", open_time=base + timedelta(minutes=i), volume=10.0)
        )

    df = await get_recent_features(
        "ETHUSDT", "1m", n=3, session_factory=TestSessionLocal
    )

    assert len(df) == 3
    assert list(df.columns) == [
        "symbol",
        "timeframe",
        "open_time",
        "cvd_delta",
        "cvd_cumulative",
        "vol_zscore",
        "vol_ratio_20",
    ]
    assert df["symbol"].unique().tolist() == ["ETHUSDT"]
    assert df["timeframe"].unique().tolist() == ["1m"]
