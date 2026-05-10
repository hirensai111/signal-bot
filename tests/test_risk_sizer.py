"""Tests for risk sizing and signal persistence."""

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.config import settings
from signal_bot.db.models import Signal
from signal_bot.signals.risk_sizer import RiskSizer
from signal_bot.signals.sweep_detector import SignalCandidate

TEST_DB_URL = settings.database_url.rsplit("/", 1)[0] + "/signal_bot_test"
_test_engine = create_async_engine(TEST_DB_URL, echo=False, future=True)
TestSessionLocal = async_sessionmaker(
    bind=_test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


def _candidate(
    entry: float = 100.0,
    stop: float = 99.0,
    proposed_tp: float = 102.0,
    side: str = "long",
) -> SignalCandidate:
    return SignalCandidate(
        symbol="BTCUSDT",
        side=side,
        entry=Decimal(str(entry)),
        stop=Decimal(str(stop)),
        proposed_tp=Decimal(str(proposed_tp)),
        pattern_features={"test": True},
        swept_level_timeframe="1h",
    )


@pytest.mark.asyncio
async def test_normal_leverage_1x() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
    )
    # 1% stop distance -> leverage = ceil(0.01 / 0.01) = 1
    candidate = _candidate(entry=100.0, stop=99.0)
    signal = await sizer.size_and_persist(candidate, "1m")

    assert signal is not None
    assert signal.leverage == 1
    assert signal.risk_pct == Decimal("1.0")
    assert signal.account_size_assumed == Decimal("10000")
    assert signal.est_hold_minutes == 30
    assert signal.status == "pending"


@pytest.mark.asyncio
async def test_tight_stop_leverage_2x() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
    )
    # 0.5% stop distance -> leverage = ceil(0.01 / 0.005) = 2
    candidate = _candidate(entry=100.0, stop=99.5)
    signal = await sizer.size_and_persist(candidate, "1m")

    assert signal is not None
    assert signal.leverage == 2


@pytest.mark.asyncio
async def test_exact_cap_passes() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
    )
    # 0.05% stop distance -> leverage = ceil(0.01 / 0.0005) = 20
    candidate = _candidate(entry=100.0, stop=99.95)
    signal = await sizer.size_and_persist(candidate, "1m")

    assert signal is not None
    assert signal.leverage == 20


@pytest.mark.asyncio
async def test_exceeds_cap_returns_none() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
    )
    # 0.04% stop distance -> leverage = ceil(0.01 / 0.0004) = 25
    candidate = _candidate(entry=100.0, stop=99.96)
    signal = await sizer.size_and_persist(candidate, "1m")

    assert signal is None


@pytest.mark.asyncio
async def test_zero_stop_returns_none() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
    )
    candidate = _candidate(entry=100.0, stop=100.0)
    signal = await sizer.size_and_persist(candidate, "1m")

    assert signal is None


@pytest.mark.asyncio
async def test_5m_hold_time() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
    )
    candidate = _candidate(entry=100.0, stop=99.0)
    signal = await sizer.size_and_persist(candidate, "5m")

    assert signal is not None
    assert signal.est_hold_minutes == 120


@pytest.mark.asyncio
async def test_custom_hold_time_map() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
        hold_time_map={"1m": 15, "5m": 60},
    )
    candidate = _candidate(entry=100.0, stop=99.0)
    signal = await sizer.size_and_persist(candidate, "1m")

    assert signal is not None
    assert signal.est_hold_minutes == 15


@pytest.mark.asyncio
async def test_signal_persisted_to_db() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
    )
    candidate = _candidate(entry=100.0, stop=99.0)
    signal = await sizer.size_and_persist(candidate, "1m")

    assert signal is not None

    async with TestSessionLocal() as session:
        row = await session.scalar(
            select(Signal).where(Signal.id == signal.id)
        )
        assert row is not None
        assert row.status == "pending"
        assert row.side == "long"
        assert row.entry == Decimal("100")
        assert row.stop_loss == Decimal("99")
        assert row.take_profit == Decimal("102")


@pytest.mark.asyncio
async def test_short_side_sizing() -> None:
    sizer = RiskSizer(
        session_factory=TestSessionLocal,
        account_size=10000.0,
        risk_pct=1.0,
        max_leverage=20,
    )
    # Short: entry=100, stop=101 -> 1% stop distance -> leverage=1
    candidate = _candidate(entry=100.0, stop=101.0, side="short")
    signal = await sizer.size_and_persist(candidate, "1m")

    assert signal is not None
    assert signal.side == "short"
    assert signal.leverage == 1
    assert signal.stop_loss == Decimal("101")
