"""Tests for Reconciler: missed fills, orphan positions, orphan orders,
and the engine-startup blocking behaviour.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.db.models import Base, RiskState, Signal, SignalOutcome
from signal_bot.execution.engine import EXEC_MODE, ExecutionEngine
from signal_bot.tracking.reconciliation import Reconciler

TEST_DB_URL = "sqlite+aiosqlite:///./signal_bot_test_recon.db"
_test_engine = create_async_engine(TEST_DB_URL, echo=False, future=True)
TestSessionLocal = async_sessionmaker(
    bind=_test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _setup_tables():
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


def _make_delta_client():
    client = MagicMock()
    client.symbol_map = MagicMock()
    client.symbol_map.get_binance_symbol = MagicMock(return_value="BTCUSDT")
    client.symbol_map.resolve_by_product_id = MagicMock(return_value="BTCUSDT")
    client.symbol_map._products = {"BTCUSD": {"id": 27}}
    return client


async def _seed_signal(
    session,
    status: str = "triggered",
    client_order_id: str | None = None,
    delta_order_id: str | None = None,
    delta_symbol: str = "BTCUSD",
    symbol: str = "BTCUSDT",
    side: str = "long",
    entry: float = 100.0,
    stop: float = 99.0,
    tp: float = 102.0,
) -> Signal:
    signal = Signal(
        symbol=symbol,
        side=side,
        entry=Decimal(str(entry)),
        stop_loss=Decimal(str(stop)),
        take_profit=Decimal(str(tp)),
        leverage=5,
        risk_pct=Decimal("1.0"),
        account_size_assumed=Decimal("10000"),
        est_hold_minutes=30,
        status=status,
        created_at=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        client_order_id=client_order_id,
        delta_order_id=delta_order_id,
        delta_symbol=delta_symbol,
        delta_entry_actual=Decimal(str(entry)),
    )
    session.add(signal)
    await session.commit()
    await session.refresh(signal)
    return signal


@pytest.mark.asyncio
async def test_synced_state_nothing_to_do() -> None:
    """When Delta and DB match, reconciler makes no changes."""
    delta_client = _make_delta_client()
    delta_client.get_open_positions = AsyncMock(
        return_value={
            "positions": [
                {"product_id": 27, "size": "10", "product_symbol": "BTCUSD", "entry_price": "100"}
            ]
        }
    )
    delta_client.get_open_orders = AsyncMock(return_value={"orders": []})

    reconciler = Reconciler(delta_client, session_factory=TestSessionLocal)

    async with TestSessionLocal() as session:
        await _seed_signal(session, status="triggered", delta_symbol="BTCUSD")

    reconciler.start()
    await reconciler.wait_for_first_run()
    await reconciler.stop()

    async with TestSessionLocal() as session:
        result = await session.execute(select(Signal))
        signals = result.scalars().all()
        assert len(signals) == 1
        assert signals[0].status == "triggered"


@pytest.mark.asyncio
async def test_missed_fill_order_filled() -> None:
    """Signal 'submitted' but order not open on Delta → check status, mark triggered."""
    delta_client = _make_delta_client()
    delta_client.get_open_positions = AsyncMock(return_value={"positions": []})
    delta_client.get_open_orders = AsyncMock(return_value={"orders": []})
    delta_client.get_order_status = AsyncMock(
        return_value={
            "order": {
                "id": "ord-123",
                "state": "filled",
                "average_fill_price": "100.5",
            }
        }
    )

    reconciler = Reconciler(delta_client, session_factory=TestSessionLocal)

    async with TestSessionLocal() as session:
        signal = await _seed_signal(
            session,
            status="submitted",
            client_order_id="sig_123",
            delta_order_id="ord-123",
        )

    reconciler.start()
    await reconciler.wait_for_first_run()
    await reconciler.stop()

    async with TestSessionLocal() as session:
        updated = await session.get(Signal, signal.id)
        assert updated.status == "triggered"
        assert updated.delta_entry_actual == Decimal("100.5")


@pytest.mark.asyncio
async def test_missed_fill_order_cancelled() -> None:
    """Signal 'submitted' but order was cancelled on Delta."""
    delta_client = _make_delta_client()
    delta_client.get_open_positions = AsyncMock(return_value={"positions": []})
    delta_client.get_open_orders = AsyncMock(return_value={"orders": []})
    delta_client.get_order_status = AsyncMock(
        return_value={"order": {"id": "ord-123", "state": "cancelled"}}
    )

    reconciler = Reconciler(delta_client, session_factory=TestSessionLocal)

    async with TestSessionLocal() as session:
        signal = await _seed_signal(
            session,
            status="submitted",
            delta_order_id="ord-123",
        )

    reconciler.start()
    await reconciler.wait_for_first_run()
    await reconciler.stop()

    async with TestSessionLocal() as session:
        updated = await session.get(Signal, signal.id)
        assert updated.status == "cancelled"
        assert updated.cancelled_reason == "missed_cancelled"


@pytest.mark.asyncio
async def test_missed_close_reconstructs_outcome() -> None:
    """Signal 'triggered' but no position on Delta → reconstruct outcome from history."""
    delta_client = _make_delta_client()
    delta_client.get_open_positions = AsyncMock(return_value={"positions": []})
    delta_client.get_open_orders = AsyncMock(return_value={"orders": []})
    delta_client.get_order_history = AsyncMock(
        return_value={
            "orders": [
                {"id": "ord-entry", "state": "filled", "average_fill_price": "100"},
                {
                    "id": "ord-tp",
                    "state": "filled",
                    "average_fill_price": "102",
                    "bracket_take_profit_price": "102",
                },
            ]
        }
    )

    governor = MagicMock()
    governor.record_outcome = AsyncMock()

    reconciler = Reconciler(
        delta_client,
        session_factory=TestSessionLocal,
        governor=governor,
    )

    async with TestSessionLocal() as session:
        signal = await _seed_signal(
            session,
            status="triggered",
            delta_order_id="ord-entry",
            delta_symbol="BTCUSD",
        )

    reconciler.start()
    await reconciler.wait_for_first_run()
    await reconciler.stop()

    async with TestSessionLocal() as session:
        updated = await session.get(Signal, signal.id)
        assert updated.status == "tp"

        outcome = await session.scalar(
            select(SignalOutcome).where(SignalOutcome.signal_id == signal.id)
        )
        assert outcome is not None
        assert outcome.exit_reason == "tp_hit"
        assert outcome.exit_price == Decimal("102")

    governor.record_outcome.assert_awaited_once()


@pytest.mark.asyncio
async def test_orphan_position_creates_alert() -> None:
    """Delta has a position with no matching signal → CRITICAL alert."""
    delta_client = _make_delta_client()
    delta_client.get_open_positions = AsyncMock(
        return_value={
            "positions": [
                {"product_id": 27, "size": "10", "product_symbol": "BTCUSD", "entry_price": "100"}
            ]
        }
    )
    delta_client.get_open_orders = AsyncMock(return_value={"orders": []})

    telegram = MagicMock()
    telegram.application = MagicMock()
    telegram.application.bot = AsyncMock()
    telegram.chat_id = "123"

    reconciler = Reconciler(
        delta_client,
        session_factory=TestSessionLocal,
        telegram=telegram,
    )

    # No signals in DB
    reconciler.start()
    await reconciler.wait_for_first_run()
    await reconciler.stop()

    telegram.application.bot.send_message.assert_awaited_once()
    args, kwargs = telegram.application.bot.send_message.call_args
    text = args[1] if len(args) > 1 else kwargs.get("text", "")
    assert "Orphan position" in text


@pytest.mark.asyncio
async def test_orphan_order_creates_alert() -> None:
    """Delta has an open order with sig_ prefix but no matching signal."""
    delta_client = _make_delta_client()
    delta_client.get_open_positions = AsyncMock(return_value={"positions": []})
    delta_client.get_open_orders = AsyncMock(
        return_value={
            "orders": [
                {
                    "id": "ord-999",
                    "client_order_id": "sig_orphan",
                    "product_symbol": "BTCUSD",
                    "state": "open",
                }
            ]
        }
    )

    telegram = MagicMock()
    telegram.application = MagicMock()
    telegram.application.bot = AsyncMock()
    telegram.chat_id = "123"

    reconciler = Reconciler(
        delta_client,
        session_factory=TestSessionLocal,
        telegram=telegram,
    )

    reconciler.start()
    await reconciler.wait_for_first_run()
    await reconciler.stop()

    telegram.application.bot.send_message.assert_awaited_once()
    args, kwargs = telegram.application.bot.send_message.call_args
    text = args[1] if len(args) > 1 else kwargs.get("text", "")
    assert "Orphan order" in text


@pytest.mark.asyncio
async def test_startup_rebuilds_open_positions() -> None:
    """On startup, risk_state.current_open_positions is rebuilt from Delta."""
    delta_client = _make_delta_client()
    delta_client.get_open_positions = AsyncMock(
        return_value={
            "positions": [
                {"product_id": 27, "size": "5", "product_symbol": "BTCUSD"},
                {"product_id": 28, "size": "3", "product_symbol": "ETHUSD"},
            ]
        }
    )
    delta_client.get_open_orders = AsyncMock(return_value={"orders": []})
    delta_client.symbol_map.get_binance_symbol = MagicMock(
        side_effect=lambda s: s.replace("USD", "USDT")
    )
    delta_client.symbol_map._products = {"BTCUSD": {"id": 27}, "ETHUSD": {"id": 28}}

    reconciler = Reconciler(delta_client, session_factory=TestSessionLocal)

    # No pre-seeded risk_state — _rebuild_open_positions should create it

    reconciler.start()
    await reconciler.wait_for_first_run()
    await reconciler.stop()

    async with TestSessionLocal() as session:
        state = await session.get(RiskState, 1)
        assert state.current_open_positions == 2


@pytest.mark.asyncio
async def test_engine_blocks_until_reconciliation() -> None:
    """ExecutionEngine.start() waits for first reconciliation before consuming signals."""
    delta_client = _make_delta_client()
    delta_client.get_open_positions = AsyncMock(return_value={"positions": []})
    delta_client.get_open_orders = AsyncMock(return_value={"orders": []})

    reconciler = Reconciler(delta_client, session_factory=TestSessionLocal)

    # Do NOT start the reconciler yet
    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=TestSessionLocal,
        exec_mode=EXEC_MODE.DRY_RUN,
        reconciler=reconciler,
    )

    start_task = asyncio.create_task(engine.start())
    await asyncio.sleep(0.2)

    # Engine should still be waiting
    assert not engine._running

    # Now start reconciler
    reconciler.start()
    await start_task

    assert engine._running
    await engine.stop()
    await reconciler.stop()
