"""Tests for Telegram notifier formatting and resilience."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.config import settings
from signal_bot.db.models import Signal, SignalOutcome
from signal_bot.delivery.telegram_bot import TelegramNotifier

TEST_DB_URL = settings.database_url.rsplit("/", 1)[0] + "/signal_bot_test"
_test_engine = create_async_engine(TEST_DB_URL, echo=False, future=True)
TestSessionLocal = async_sessionmaker(
    bind=_test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


def _signal(**kwargs) -> Signal:
    defaults = {
        "symbol": "BTCUSDT",
        "side": "long",
        "entry": Decimal("100"),
        "stop_loss": Decimal("99"),
        "take_profit": Decimal("102"),
        "leverage": 5,
        "risk_pct": Decimal("1.0"),
        "account_size_assumed": Decimal("10000"),
        "est_hold_minutes": 30,
        "pattern_features": {"sweep_low": "98.5"},
        "status": "triggered",
    }
    defaults.update(kwargs)
    return Signal(**defaults)


def _outcome(**kwargs) -> SignalOutcome:
    defaults = {
        "exit_price": Decimal("102"),
        "exit_ts": datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc),
        "pnl_pct": Decimal("2.0"),
        "pnl_r": Decimal("2.0"),
        "hold_minutes": 30,
        "exit_reason": "tp",
    }
    defaults.update(kwargs)
    return SignalOutcome(**defaults)


@pytest.mark.asyncio
async def test_format_signal_long() -> None:
    notifier = TelegramNotifier(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", chat_id="123"
    )
    signal = _signal(
        side="long", entry=Decimal("100"), stop_loss=Decimal("99"), take_profit=Decimal("102")
    )
    text = notifier._format_signal(signal)

    assert "LONG" in text
    assert "BTCUSDT" in text
    assert "100" in text
    assert "99" in text
    assert "102" in text
    assert "5x" in text
    assert "R:R" in text
    assert "sweep_low" in text


@pytest.mark.asyncio
async def test_format_signal_short() -> None:
    notifier = TelegramNotifier(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", chat_id="123"
    )
    signal = _signal(
        side="short", entry=Decimal("100"), stop_loss=Decimal("101"), take_profit=Decimal("98")
    )
    text = notifier._format_signal(signal)

    assert "🔴" in text
    assert "SHORT" in text


@pytest.mark.asyncio
async def test_format_outcome_tp() -> None:
    notifier = TelegramNotifier(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", chat_id="123"
    )
    signal = _signal()
    outcome = _outcome(
        exit_reason="tp", pnl_pct=Decimal("2.03"), pnl_r=Decimal("2.0"), hold_minutes=47
    )
    text = notifier._format_outcome(signal, outcome)

    assert "✅" in text
    assert "BTCUSDT" in text
    assert "TP" in text
    assert "+2.03%" in text
    assert "+2.0R" in text
    assert "47 min" in text


@pytest.mark.asyncio
async def test_format_outcome_sl() -> None:
    notifier = TelegramNotifier(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", chat_id="123"
    )
    signal = _signal()
    outcome = _outcome(
        exit_reason="sl", pnl_pct=Decimal("-1.02"), pnl_r=Decimal("-1.0"), hold_minutes=12
    )
    text = notifier._format_outcome(signal, outcome)

    assert "❌" in text
    assert "SL" in text
    assert "-1.02%" in text
    assert "-1.0R" in text
    assert "12 min" in text


@pytest.mark.asyncio
async def test_notify_signal_swallows_exception() -> None:
    notifier = TelegramNotifier(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", chat_id="123"
    )
    signal = _signal()

    notifier.application.bot = AsyncMock()
    notifier.application.bot.send_message = AsyncMock(side_effect=Exception("network down"))
    await notifier.notify_signal(signal)  # must not raise


@pytest.mark.asyncio
async def test_notify_outcome_swallows_exception() -> None:
    notifier = TelegramNotifier(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", chat_id="123"
    )
    signal = _signal()
    outcome = _outcome()

    notifier.application.bot = AsyncMock()
    notifier.application.bot.send_message = AsyncMock(side_effect=Exception("network down"))
    await notifier.notify_outcome(signal, outcome)  # must not raise


@pytest.mark.asyncio
async def test_cmd_status_with_data() -> None:
    notifier = TelegramNotifier(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", chat_id="123"
    )

    async with TestSessionLocal() as session:
        # Open signal
        s1 = _signal(symbol="BTCUSDT", side="long", entry=Decimal("100"), leverage=5)
        session.add(s1)
        # Triggered but not open
        s2 = _signal(symbol="ETHUSDT", side="short", entry=Decimal("200"), leverage=3, status="sl")
        session.add(s2)
        await session.commit()
        await session.refresh(s1)
        await session.refresh(s2)

        # Today's outcome
        o1 = _outcome(
            signal_id=s2.id,
            exit_ts=datetime.now(timezone.utc),
            pnl_r=Decimal("-1.0"),
            pnl_pct=Decimal("-1.0"),
            exit_reason="sl",
        )
        session.add(o1)
        await session.commit()

    update = MagicMock()
    update.message.reply_text = AsyncMock()
    context = MagicMock()

    await notifier._cmd_status(update, context)

    args, kwargs = update.message.reply_text.call_args
    text = args[0]
    assert "Open Signals" in text
    assert "BTCUSDT" in text
    assert "Today's PnL" in text
    assert "Trades: 1" in text
    # MarkdownV2 escaping: minus sign and dot may be backslash-escaped
    assert "Total R: -1.00" in text or "Total R: \\-1\\.00" in text


@pytest.mark.asyncio
async def test_cmd_status_empty() -> None:
    notifier = TelegramNotifier(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", chat_id="123"
    )

    update = MagicMock()
    update.message.reply_text = AsyncMock()
    context = MagicMock()

    await notifier._cmd_status(update, context)

    args, kwargs = update.message.reply_text.call_args
    text = args[0]
    assert "None" in text
    assert "No closed trades today" in text
