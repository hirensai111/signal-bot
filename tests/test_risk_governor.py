"""Tests for RiskGovernor."""

import os
import tempfile
import uuid
from collections.abc import AsyncGenerator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.db.models import Base, RiskState, Signal
from signal_bot.risk.governor import RiskGovernor

# Shared temp-file SQLite engine so multiple sessions see the same data.
_fd, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_fd)
_TEST_ENGINE = create_async_engine(f"sqlite+aiosqlite:///{_DB_PATH}", echo=False)
_TestSessionLocal = async_sessionmaker(
    bind=_TEST_ENGINE, class_=AsyncSession, expire_on_commit=False
)


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db() -> AsyncGenerator[None, None]:
    async with _TEST_ENGINE.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await _TEST_ENGINE.dispose()
    try:
        os.unlink(_DB_PATH)
    except PermissionError:
        pass  # Windows may keep the file locked briefly


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    async with _TestSessionLocal() as session:
        yield session


@pytest_asyncio.fixture(autouse=True)
async def clean_tables(db_session: AsyncSession) -> None:
    """Truncate tables before every test to isolate state."""
    await db_session.execute(delete(RiskState))
    await db_session.execute(delete(Signal))
    await db_session.commit()


@pytest_asyncio.fixture
async def governor() -> AsyncGenerator[RiskGovernor, None]:
    g = RiskGovernor(
        session_factory=_TestSessionLocal,
        delta_client=None,
        telegram=None,
        daily_loss_limit_r=-3.0,
        max_trades_per_day=10,
        max_concurrent_positions=3,
        max_consecutive_losses=5,
        min_balance_usdt=50.0,
    )
    yield g


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _seed_risk_state(session: AsyncSession, **kwargs) -> RiskState:
    existing = await session.get(RiskState, 1)
    if existing:
        for k, v in kwargs.items():
            setattr(existing, k, v)
        await session.commit()
        return existing
    state = RiskState(id=1, **kwargs)
    session.add(state)
    await session.commit()
    return state


async def _seed_signal(session: AsyncSession, status: str = "triggered") -> Signal:
    signal = Signal(
        symbol="BTCUSDT",
        side="long",
        entry=Decimal("65000"),
        stop_loss=Decimal("64000"),
        take_profit=Decimal("67000"),
        leverage=10,
        risk_pct=Decimal("1.0"),
        account_size_assumed=Decimal("10000"),
        est_hold_minutes=30,
        status=status,
    )
    session.add(signal)
    await session.commit()
    return signal


# ---------------------------------------------------------------------------
# 1. pre-trade checks — each limit breached individually
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kill_switch_blocks(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session, kill_switch_active=True, kill_switch_reason="panic")
    allowed, reason = await governor.check(None)
    assert not allowed
    assert "kill_switch_active" in reason


@pytest.mark.asyncio
async def test_daily_loss_limit_blocks(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session, daily_pnl_r=Decimal("-4.0"))
    allowed, reason = await governor.check(None)
    assert not allowed
    assert "daily_loss_limit_r" in reason


@pytest.mark.asyncio
async def test_max_trades_per_day_blocks(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session, daily_trade_count=10)
    allowed, reason = await governor.check(None)
    assert not allowed
    assert "max_trades_per_day" in reason


@pytest.mark.asyncio
async def test_max_concurrent_positions_blocks(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session, current_open_positions=3)
    for _ in range(3):
        await _seed_signal(db_session, status="triggered")
    allowed, reason = await governor.check(None)
    assert not allowed
    assert "max_concurrent_positions" in reason


@pytest.mark.asyncio
async def test_max_consecutive_losses_blocks(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session, consecutive_losses=5)
    allowed, reason = await governor.check(None)
    assert not allowed
    assert "max_consecutive_losses" in reason


@pytest.mark.asyncio
async def test_min_balance_blocks(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session)
    delta_client = MagicMock()
    delta_client.get_balance = AsyncMock(
        return_value={"balances": [{"asset": "USDT", "available_balance": "10.0"}]}
    )
    governor.delta_client = delta_client
    allowed, reason = await governor.check(None)
    assert not allowed
    assert "min_balance_usdt" in reason


@pytest.mark.asyncio
async def test_all_clear_allows(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session)
    allowed, reason = await governor.check(None)
    assert allowed
    assert reason is None


# ---------------------------------------------------------------------------
# 2. outcome sequences
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_win_resets_consecutive_losses(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session, consecutive_losses=3, daily_trade_count=2)
    sid = uuid.uuid4()
    await governor.record_outcome(sid, Decimal("2.0"), won=True)
    state = await db_session.get(RiskState, 1)
    assert state.consecutive_losses == 0
    assert state.daily_pnl_r == Decimal("2.0")
    assert state.daily_trade_count == 3


@pytest.mark.asyncio
async def test_loss_increments_streak(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session, consecutive_losses=2, daily_trade_count=1)
    sid = uuid.uuid4()
    await governor.record_outcome(sid, Decimal("-1.0"), won=False)
    state = await db_session.get(RiskState, 1)
    assert state.consecutive_losses == 3
    assert state.daily_pnl_r == Decimal("-1.0")
    assert state.daily_trade_count == 2


@pytest.mark.asyncio
async def test_consecutive_losses_auto_trip_kill_switch(
    governor: RiskGovernor, db_session: AsyncSession
):
    await _seed_risk_state(db_session, consecutive_losses=4, daily_trade_count=4)
    sid = uuid.uuid4()
    await governor.record_outcome(sid, Decimal("-1.0"), won=False)
    state = await db_session.get(RiskState, 1)
    assert state.kill_switch_active is True
    assert "max_consecutive_losses" in (state.kill_switch_reason or "")


# ---------------------------------------------------------------------------
# 3. daily reset
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_daily_reset_zeros_counters(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(
        db_session,
        daily_pnl_r=Decimal("-5.0"),
        daily_trade_count=7,
        consecutive_losses=3,
        last_reset_date=date.today() - timedelta(days=1),
    )
    await governor._do_daily_reset()
    state = await db_session.get(RiskState, 1)
    assert state.daily_pnl_r == Decimal("0")
    assert state.daily_trade_count == 0
    assert state.consecutive_losses == 0
    assert state.last_reset_date == date.today()


@pytest.mark.asyncio
async def test_daily_reset_preserves_kill_switch(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(
        db_session,
        kill_switch_active=True,
        kill_switch_reason="manual",
        daily_pnl_r=Decimal("-5.0"),
        last_reset_date=date.today() - timedelta(days=1),
    )
    await governor._do_daily_reset()
    state = await db_session.get(RiskState, 1)
    assert state.kill_switch_active is True
    assert state.kill_switch_reason == "manual"


# ---------------------------------------------------------------------------
# 4. management API
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_clear_kill_switch(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session)
    await governor.set_kill_switch("operator panic")
    state = await db_session.get(RiskState, 1)
    await db_session.refresh(state)
    assert state.kill_switch_active is True
    assert state.kill_switch_reason == "operator panic"

    await governor.clear_kill_switch()
    state = await db_session.get(RiskState, 1)
    await db_session.refresh(state)
    assert state.kill_switch_active is False
    assert state.kill_switch_reason is None


@pytest.mark.asyncio
async def test_get_status(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(
        db_session,
        daily_pnl_r=Decimal("-1.5"),
        daily_trade_count=3,
        consecutive_losses=1,
    )
    status = await governor.get_status()
    assert status["kill_switch_active"] is False
    assert status["daily_pnl_r"] == Decimal("-1.5")
    assert status["daily_trade_count"] == 3
    assert status["consecutive_losses"] == 1


# ---------------------------------------------------------------------------
# 5. alert rate limiting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_alert_rate_limit(governor: RiskGovernor, db_session: AsyncSession):
    await _seed_risk_state(db_session, kill_switch_active=True)
    assert governor._should_alert("kill_switch_active: reason1") is True
    assert governor._should_alert("kill_switch_active: reason2") is False
    past = datetime.now(timezone.utc) - timedelta(hours=2)
    governor._alert_cooldown["kill_switch_active"] = past
    assert governor._should_alert("kill_switch_active: reason3") is True
