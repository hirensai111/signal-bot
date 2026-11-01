"""Tests for database models and schema creation."""

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from signal_bot.db.models import (
    AggTrade,
    Candle1m,
    Candle5m,
    HtfLevel,
    Instrument,
    Signal,
    SignalOutcome,
)


class TestInstrument:
    async def test_create_and_fetch(self, db_session) -> None:
        inst = Instrument(
            symbol="TESTUSDT",
            base_asset="TEST",
            quote_asset="USDT",
            tick_size=Decimal("0.01"),
            min_qty=Decimal("0.001"),
            active=True,
        )
        db_session.add(inst)
        await db_session.commit()

        fetched = await db_session.scalar(select(Instrument).where(Instrument.symbol == "TESTUSDT"))
        assert fetched is not None
        assert fetched.base_asset == "TEST"
        assert fetched.tick_size == Decimal("0.01")


class TestCandles:
    async def test_candle_1m_primary_key(self, db_session) -> None:
        candle = Candle1m(
            symbol="BTCUSDT",
            open_time=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            open=Decimal("42000"),
            high=Decimal("42100"),
            low=Decimal("41900"),
            close=Decimal("42050"),
            volume=Decimal("10.5"),
            quote_volume=Decimal("441000"),
            trade_count=1500,
            taker_buy_volume=Decimal("5.2"),
            closed=True,
        )
        db_session.add(candle)
        await db_session.commit()

        fetched = await db_session.scalar(
            select(Candle1m).where(
                Candle1m.symbol == "BTCUSDT",
                Candle1m.open_time == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            )
        )
        assert fetched is not None
        assert fetched.close == Decimal("42050")

    async def test_candle_5m_primary_key(self, db_session) -> None:
        candle = Candle5m(
            symbol="ETHUSDT",
            open_time=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            open=Decimal("2200"),
            high=Decimal("2210"),
            low=Decimal("2190"),
            close=Decimal("2205"),
            volume=Decimal("100"),
            quote_volume=Decimal("220000"),
            trade_count=3000,
            taker_buy_volume=Decimal("50"),
            closed=True,
        )
        db_session.add(candle)
        await db_session.commit()

        fetched = await db_session.scalar(
            select(Candle5m).where(
                Candle5m.symbol == "ETHUSDT",
                Candle5m.open_time == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            )
        )
        assert fetched is not None
        assert fetched.high == Decimal("2210")


class TestAggTrades:
    async def test_create_and_fetch(self, db_session) -> None:
        trade = AggTrade(
            symbol="SOLUSDT",
            trade_id=12345,
            price=Decimal("100.00"),
            qty=Decimal("10"),
            quote_qty=Decimal("1000"),
            ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            is_buyer_maker=False,
        )
        db_session.add(trade)
        await db_session.commit()

        fetched = await db_session.scalar(
            select(AggTrade).where(
                AggTrade.symbol == "SOLUSDT",
                AggTrade.trade_id == 12345,
            )
        )
        assert fetched is not None
        assert fetched.is_buyer_maker is False


class TestHtfLevels:
    async def test_create_and_fetch(self, db_session) -> None:
        level = HtfLevel(
            symbol="LINKUSDT",
            timeframe="1H",
            level_type="swing_high",
            price=Decimal("15.50"),
            formed_at=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            active=True,
        )
        db_session.add(level)
        await db_session.commit()

        fetched = await db_session.scalar(select(HtfLevel).where(HtfLevel.symbol == "LINKUSDT"))
        assert fetched is not None
        assert fetched.level_type == "swing_high"
        assert fetched.broken_at is None


class TestSignals:
    async def test_create_signal_and_outcome(self, db_session) -> None:
        signal = Signal(
            symbol="AVAXUSDT",
            side="long",
            entry=Decimal("20.00"),
            stop_loss=Decimal("19.50"),
            take_profit=Decimal("21.00"),
            leverage=10,
            risk_pct=Decimal("1.0"),
            account_size_assumed=Decimal("10000"),
            est_hold_minutes=30,
            pattern_features={"sweep": True, "divergence": "bullish"},
            status="pending",
        )
        db_session.add(signal)
        await db_session.commit()
        await db_session.refresh(signal)

        assert signal.id is not None

        outcome = SignalOutcome(
            signal_id=signal.id,
            exit_price=Decimal("21.00"),
            exit_ts=datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc),
            pnl_pct=Decimal("5.0"),
            pnl_r=Decimal("2.0"),
            hold_minutes=25,
            exit_reason="tp",
        )
        db_session.add(outcome)
        await db_session.commit()

        fetched_signal = await db_session.scalar(select(Signal).where(Signal.id == signal.id))
        assert fetched_signal is not None
        assert fetched_signal.outcome is not None
        assert fetched_signal.outcome.exit_reason == "tp"
        assert fetched_signal.outcome.pnl_r == Decimal("2.0")
