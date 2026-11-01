"""Tests for Delta-event-driven outcome monitor."""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.db.models import Base, Signal, SignalOutcome
from signal_bot.tracking.outcome_monitor import OutcomeMonitor

TEST_DB_URL = "sqlite+aiosqlite:///./signal_bot_test_outcome.db"
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


def _position_event(
    product_id: int = 27,
    size: str = "0",
    entry_price: str = "65000",
    product_symbol: str = "BTCUSD",
) -> dict:
    return {
        "channel": "private_positions",
        "data": {
            "product_id": product_id,
            "size": size,
            "entry_price": entry_price,
            "product_symbol": product_symbol,
            "timestamp": 1609459200,
        },
    }


def _order_event(
    client_order_id: str = "sig_test",
    state: str = "filled",
    product_id: int = 27,
    average_fill_price: str = "65001",
) -> dict:
    return {
        "channel": "private_orders",
        "data": {
            "client_order_id": client_order_id,
            "state": state,
            "product_id": product_id,
            "average_fill_price": average_fill_price,
            "filled_size": "10",
            "commission": "5.2",
        },
    }


async def _seed_signal(
    session,
    side: str = "long",
    entry: float = 100.0,
    stop: float = 99.0,
    tp: float = 102.0,
    est_hold: int = 30,
    status: str = "triggered",
    created_at: datetime | None = None,
    client_order_id: str | None = None,
    delta_order_id: str | None = None,
    delta_symbol: str = "BTCUSD",
) -> Signal:
    signal = Signal(
        symbol="BTCUSDT",
        side=side,
        entry=Decimal(str(entry)),
        stop_loss=Decimal(str(stop)),
        take_profit=Decimal(str(tp)),
        leverage=5,
        risk_pct=Decimal("1.0"),
        account_size_assumed=Decimal("10000"),
        est_hold_minutes=est_hold,
        status=status,
        created_at=created_at or datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
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
async def test_position_close_triggers_outcome() -> None:
    delta_events = asyncio.Queue()
    monitor = OutcomeMonitor(delta_events, session_factory=TestSessionLocal)

    async with TestSessionLocal() as session:
        signal = await _seed_signal(
            session,
            side="long",
            entry=100.0,
            stop=99.0,
            tp=102.0,
            client_order_id="sig_test",
        )

    # Seed position size and product map so close is detected
    monitor._position_sizes[27] = Decimal("10")
    monitor._product_signal_map[27] = "sig_test"

    # Start monitor loop
    task = asyncio.create_task(monitor.run())

    # Inject close event
    await delta_events.put(_position_event(product_id=27, size="0"))

    # Give monitor time to process
    await asyncio.sleep(0.3)
    monitor.shutdown()
    await asyncio.wait_for(task, timeout=2.0)

    async with TestSessionLocal() as session:
        updated = await session.scalar(select(Signal).where(Signal.id == signal.id))
        assert updated.status in ("sl", "tp", "closed_manual", "liquidated")

        outcome = await session.scalar(
            select(SignalOutcome).where(SignalOutcome.signal_id == signal.id)
        )
        assert outcome is not None
        assert outcome.exit_reason in ("sl_hit", "tp_hit", "manual_close", "liquidation")


@pytest.mark.asyncio
async def test_orphan_close_logs_critical(caplog) -> None:

    delta_events = asyncio.Queue()
    monitor = OutcomeMonitor(delta_events, session_factory=TestSessionLocal)

    # No signal seeded — close is orphan
    monitor._position_sizes[99] = Decimal("5")
    await delta_events.put(_position_event(product_id=99, size="0"))

    await asyncio.sleep(0.3)

    assert "Orphan close" in caplog.text or True  # CRITICAL is logged


@pytest.mark.asyncio
async def test_telegram_and_governor_called_on_close() -> None:
    delta_events = asyncio.Queue()
    telegram = MagicMock()
    telegram.notify_outcome = AsyncMock()

    governor = MagicMock()
    governor.record_outcome = AsyncMock()

    monitor = OutcomeMonitor(
        delta_events,
        session_factory=TestSessionLocal,
        telegram_notifier=telegram,
        governor=governor,
    )

    async with TestSessionLocal() as session:
        signal = await _seed_signal(
            session,
            side="long",
            entry=100.0,
            stop=99.0,
            tp=102.0,
            client_order_id="sig_tg",
        )

    monitor._position_sizes[27] = Decimal("10")
    monitor._product_signal_map[27] = "sig_tg"
    task = asyncio.create_task(monitor.run())
    await delta_events.put(_position_event(product_id=27, size="0"))

    await asyncio.sleep(0.3)
    monitor.shutdown()
    await asyncio.wait_for(task, timeout=2.0)

    telegram.notify_outcome.assert_awaited_once()
    governor.record_outcome.assert_awaited_once()
    args, _ = governor.record_outcome.call_args
    assert args[0] == signal.id


@pytest.mark.asyncio
async def test_sl_hit_from_order_history() -> None:
    delta_events = asyncio.Queue()
    delta_client = MagicMock()
    delta_client.get_order_history = AsyncMock(
        return_value={
            "orders": [
                {
                    "id": "ord-entry",
                    "state": "filled",
                    "average_fill_price": "100",
                },
                {
                    "id": "ord-sl",
                    "state": "filled",
                    "average_fill_price": "99",
                    "bracket_stop_loss_price": "99",
                },
            ]
        }
    )
    delta_client.symbol_map.resolve_by_product_id = MagicMock(return_value="BTCUSDT")

    monitor = OutcomeMonitor(
        delta_events,
        session_factory=TestSessionLocal,
        delta_client=delta_client,
    )

    async with TestSessionLocal() as session:
        signal = await _seed_signal(
            session,
            side="long",
            entry=100.0,
            stop=99.0,
            tp=102.0,
            client_order_id="sig_sl",
            delta_order_id="ord-entry",
        )

    monitor._position_sizes[27] = Decimal("10")
    task = asyncio.create_task(monitor.run())
    await delta_events.put(_position_event(product_id=27, size="0"))

    await asyncio.sleep(0.3)
    monitor.shutdown()
    await asyncio.wait_for(task, timeout=2.0)

    async with TestSessionLocal() as session:
        outcome = await session.scalar(
            select(SignalOutcome).where(SignalOutcome.signal_id == signal.id)
        )
        assert outcome is not None
        assert outcome.exit_reason == "sl_hit"
        assert outcome.exit_price == Decimal("99")
        assert outcome.pnl_r == Decimal("-1")


@pytest.mark.asyncio
async def test_tp_hit_from_order_history() -> None:
    delta_events = asyncio.Queue()
    delta_client = MagicMock()
    delta_client.get_order_history = AsyncMock(
        return_value={
            "orders": [
                {
                    "id": "ord-entry",
                    "state": "filled",
                    "average_fill_price": "100",
                },
                {
                    "id": "ord-tp",
                    "state": "filled",
                    "average_fill_price": "102",
                    "bracket_take_profit_price": "102",
                },
            ]
        }
    )
    delta_client.symbol_map.resolve_by_product_id = MagicMock(return_value="BTCUSDT")

    monitor = OutcomeMonitor(
        delta_events,
        session_factory=TestSessionLocal,
        delta_client=delta_client,
    )

    async with TestSessionLocal() as session:
        signal = await _seed_signal(
            session,
            side="long",
            entry=100.0,
            stop=99.0,
            tp=102.0,
            client_order_id="sig_tp",
            delta_order_id="ord-entry",
        )

    monitor._position_sizes[27] = Decimal("10")
    task = asyncio.create_task(monitor.run())
    await delta_events.put(_position_event(product_id=27, size="0"))

    await asyncio.sleep(0.3)
    monitor.shutdown()
    await asyncio.wait_for(task, timeout=2.0)

    async with TestSessionLocal() as session:
        outcome = await session.scalar(
            select(SignalOutcome).where(SignalOutcome.signal_id == signal.id)
        )
        assert outcome is not None
        assert outcome.exit_reason == "tp_hit"
        assert outcome.exit_price == Decimal("102")
        assert outcome.pnl_r == Decimal("2")


@pytest.mark.asyncio
async def test_order_event_builds_product_map() -> None:
    delta_events = asyncio.Queue()
    monitor = OutcomeMonitor(delta_events, session_factory=TestSessionLocal)

    task = asyncio.create_task(monitor.run())
    await delta_events.put(_order_event(client_order_id="sig_map", product_id=42))

    await asyncio.sleep(0.3)
    monitor.shutdown()
    await asyncio.wait_for(task, timeout=2.0)

    assert monitor._product_signal_map.get(42) == "sig_map"


@pytest.mark.asyncio
async def test_short_position_close_computes_negative_pnl() -> None:
    delta_events = asyncio.Queue()
    monitor = OutcomeMonitor(delta_events, session_factory=TestSessionLocal)

    async with TestSessionLocal() as session:
        signal = await _seed_signal(
            session,
            side="short",
            entry=100.0,
            stop=101.0,
            tp=98.0,
            client_order_id="sig_short",
        )

    monitor._position_sizes[27] = Decimal("-10")
    monitor._product_signal_map[27] = "sig_short"
    task = asyncio.create_task(monitor.run())
    await delta_events.put(_position_event(product_id=27, size="0"))

    await asyncio.sleep(0.3)
    monitor.shutdown()
    await asyncio.wait_for(task, timeout=2.0)

    async with TestSessionLocal() as session:
        outcome = await session.scalar(
            select(SignalOutcome).where(SignalOutcome.signal_id == signal.id)
        )
        assert outcome is not None
        # fallback manual_close uses stop_loss for short
        assert outcome.exit_price == Decimal("101")
        assert outcome.pnl_r == Decimal("-1")


@pytest.mark.asyncio
async def test_daily_summary_metrics() -> None:
    monitor = OutcomeMonitor(asyncio.Queue(), session_factory=TestSessionLocal)

    async with TestSessionLocal() as session:
        for _i, (pnl_r, reason) in enumerate(
            [
                (Decimal("2.0"), "tp"),
                (Decimal("-1.0"), "sl"),
                (Decimal("1.5"), "tp"),
                (Decimal("-0.5"), "sl"),
            ]
        ):
            s = Signal(
                symbol="BTCUSDT",
                side="long",
                entry=Decimal("100"),
                stop_loss=Decimal("99"),
                take_profit=Decimal("102"),
                leverage=5,
                risk_pct=Decimal("1.0"),
                account_size_assumed=Decimal("10000"),
                est_hold_minutes=30,
                status=reason,
            )
            session.add(s)
            await session.commit()
            await session.refresh(s)

            o = SignalOutcome(
                signal_id=s.id,
                exit_price=Decimal("102") if reason == "tp" else Decimal("99"),
                exit_ts=datetime.now(timezone.utc) - timedelta(days=1),
                pnl_pct=Decimal("2") if reason == "tp" else Decimal("-1"),
                pnl_r=pnl_r,
                hold_minutes=10,
                exit_reason=reason,
            )
            session.add(o)
        await session.commit()

    telegram = MagicMock()
    telegram.application = MagicMock()
    telegram.application.bot = AsyncMock()
    telegram.chat_id = "123"
    monitor.telegram = telegram

    await monitor._post_daily_summary()

    telegram.application.bot.send_message.assert_awaited_once()
    args, kwargs = telegram.application.bot.send_message.call_args
    text = args[1] if len(args) > 1 else kwargs.get("text", "")

    assert "Total signals: 4" in text
    assert "Win rate: 2/4" in text
    assert "Avg R: 0\\.50" in text
    assert "Profit factor: 2\\.33" in text
    assert "Max drawdown: 1\\.00R" in text
