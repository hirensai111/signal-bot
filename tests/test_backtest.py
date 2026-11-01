"""Tests for the backtest runner."""

import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.backtest.runner import BacktestRunner
from signal_bot.config import settings
from signal_bot.signals.sweep_detector import SignalCandidate, SweepDetector

TEST_DB_URL = settings.database_url.rsplit("/", 1)[0] + "/signal_bot_test"
_test_engine = create_async_engine(TEST_DB_URL, echo=False, future=True)
TestSessionLocal = async_sessionmaker(
    bind=_test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


def _make_candles(symbol: str, start: datetime, n: int, base_price: float = 100.0):
    """Generate n 1m candles with a sweep+reclaim pattern at the end."""
    candles = []
    for i in range(n):
        # Normal candles around base_price
        o = base_price
        h = base_price + 1.0
        l = base_price - 1.0
        c = base_price
        # At candle n-3: sweep low below 99.0
        if i == n - 3:
            l = base_price - 2.5  # sweeps 99.0
        # At candle n-1: reclaim above 99.0 with high volume
        if i == n - 1:
            h = base_price + 2.0
            c = base_price + 1.5
        candles.append(
            {
                "symbol": symbol,
                "open_time": start + timedelta(minutes=i),
                "open": Decimal(str(o)),
                "high": Decimal(str(h)),
                "low": Decimal(str(l)),
                "close": Decimal(str(c)),
                "volume": Decimal("100" if i == n - 1 else "10"),
                "quote_volume": Decimal("10000" if i == n - 1 else "1000"),
                "trade_count": 50 if i == n - 1 else 5,
                "taker_buy_volume": Decimal("50" if i == n - 1 else "5"),
                "closed": True,
            }
        )
    return candles


def _make_trades(symbol: str, start: datetime, n: int):
    """Generate dummy aggTrades."""
    trades = []
    for i in range(n):
        ts = start + timedelta(minutes=i, seconds=30)
        trades.append(
            {
                "symbol": symbol,
                "trade_id": i + 1,
                "price": Decimal("100"),
                "qty": Decimal("5"),
                "quote_qty": Decimal("500"),
                "ts": ts,
                "is_buyer_maker": i % 2 == 0,
            }
        )
    return trades


def _mock_candidate(candle):
    return SignalCandidate(
        symbol=candle.symbol,
        side="long",
        entry=candle.close,
        stop=candle.close * Decimal("0.99"),
        proposed_tp=candle.close * Decimal("1.02"),
        pattern_features={"test": True},
        swept_level_timeframe="1h",
    )


@pytest.mark.asyncio
async def test_backtest_replay_produces_trades() -> None:
    symbol = "BTCUSDT"
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=4)

    # 240 1m candles = 4h
    candles = _make_candles(symbol, start, 240)
    trades = _make_trades(symbol, start, 100)

    with tempfile.TemporaryDirectory() as tmpdir:
        runner = BacktestRunner(symbol, start, end, "1m", cache_dir=tmpdir)

        with patch.object(runner, "_fetch_candles", return_value=candles):
            with patch.object(runner, "_fetch_agg_trades", return_value=trades):
                with patch.object(SweepDetector, "_evaluate", side_effect=_mock_candidate):
                    await runner.run()

        # Check CSV was written
        csv_files = list(Path(tmpdir).parent.glob("backtest_*.csv"))
        assert len(csv_files) == 1
        df = __import__("pandas").read_csv(csv_files[0])
        assert len(df) > 0


@pytest.mark.asyncio
async def test_backtest_metrics_computation() -> None:
    symbol = "BTCUSDT"
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=4)

    candles = _make_candles(symbol, start, 240)
    trades = _make_trades(symbol, start, 100)

    with tempfile.TemporaryDirectory() as tmpdir:
        runner = BacktestRunner(symbol, start, end, "1m", cache_dir=tmpdir)

        with patch.object(runner, "_fetch_candles", return_value=candles):
            with patch.object(runner, "_fetch_agg_trades", return_value=trades):
                with patch.object(SweepDetector, "_evaluate", side_effect=_mock_candidate):
                    await runner.run()

        csv_files = list(Path(tmpdir).parent.glob("backtest_*.csv"))
        df = __import__("pandas").read_csv(csv_files[0])

        # Every trade should have net_pnl_pct after fees/slippage
        assert "net_pnl_pct" in df.columns
        assert "pnl_r" in df.columns
        assert "exit_reason" in df.columns

        # Verify fee impact: net should always be less than gross (fees reduce PnL)
        for _, row in df.iterrows():
            assert row["net_pnl_pct"] < row["gross_pnl_pct"]


@pytest.mark.asyncio
async def test_slippage_and_fee_math() -> None:
    from signal_bot.backtest.runner import (
        ENTRY_FEE,
        ENTRY_SLIPPAGE,
        SL_FEE,
        SL_SLIPPAGE,
        TP_FEE,
        TP_SLIPPAGE,
    )

    runner = BacktestRunner("BTCUSDT", datetime.now(timezone.utc), datetime.now(timezone.utc), "1m")

    from signal_bot.db.models import Signal

    signal = Signal(
        symbol="BTCUSDT",
        side="long",
        entry=Decimal("100"),
        stop_loss=Decimal("99"),
        take_profit=Decimal("102"),
        leverage=5,
        risk_pct=Decimal("1.0"),
        account_size_assumed=Decimal("10000"),
        est_hold_minutes=30,
        status="triggered",
    )

    # Entry: 5 bps slippage on limit-IOC
    entry_slipped = runner._slipped_entry(signal)
    assert entry_slipped == Decimal("100") * (1 + ENTRY_SLIPPAGE)

    # TP exit: zero slippage, maker fee
    exit_slipped_tp = runner._slipped_exit(signal, Decimal("102"), "tp")
    assert exit_slipped_tp == Decimal("102") * (1 - TP_SLIPPAGE)

    # SL exit: 5 bps slippage, taker fee
    exit_slipped_sl = runner._slipped_exit(signal, Decimal("99"), "sl")
    assert exit_slipped_sl == Decimal("99") * (1 - SL_SLIPPAGE)

    # Net PnL for long TP: ~0.12% total cost
    gross_tp = (exit_slipped_tp - entry_slipped) / entry_slipped
    cost_tp = ENTRY_FEE + ENTRY_SLIPPAGE + TP_FEE + TP_SLIPPAGE
    net_tp = gross_tp - cost_tp
    assert net_tp < gross_tp
    assert cost_tp == Decimal("0.0012")  # 0.05% + 0.05% + 0.02% + 0%

    # Net PnL for long SL: ~0.20% total cost
    gross_sl = (exit_slipped_sl - entry_slipped) / entry_slipped
    cost_sl = ENTRY_FEE + ENTRY_SLIPPAGE + SL_FEE + SL_SLIPPAGE
    net_sl = gross_sl - cost_sl
    assert net_sl < gross_sl
    assert cost_sl == Decimal("0.0020")  # 0.05% + 0.05% + 0.05% + 0.05%

    # Short side
    signal.side = "short"
    entry_slipped = runner._slipped_entry(signal)
    assert entry_slipped == Decimal("100") * (1 - ENTRY_SLIPPAGE)

    exit_slipped_tp = runner._slipped_exit(signal, Decimal("99"), "tp")
    assert exit_slipped_tp == Decimal("99") * (1 + TP_SLIPPAGE)


@pytest.mark.asyncio
async def test_exit_check_logic() -> None:
    runner = BacktestRunner("BTCUSDT", datetime.now(timezone.utc), datetime.now(timezone.utc), "1m")

    from signal_bot.db.models import Signal
    from signal_bot.ingest.binance_ws import Kline

    signal = Signal(
        symbol="BTCUSDT",
        side="long",
        entry=Decimal("100"),
        stop_loss=Decimal("99"),
        take_profit=Decimal("102"),
        leverage=5,
        risk_pct=Decimal("1.0"),
        account_size_assumed=Decimal("10000"),
        est_hold_minutes=10,
        status="triggered",
        created_at=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
    )

    # SL hit
    candle = Kline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
        open=Decimal("100"),
        high=Decimal("100"),
        low=Decimal("98.5"),
        close=Decimal("99"),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=5,
        taker_buy_volume=Decimal("5"),
        closed=True,
    )
    info = runner._check_exit(signal, candle)
    assert info is not None
    assert info["exit_reason"] == "sl"
    assert info["exit_price"] == Decimal("99")

    # TP hit
    candle = Kline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
        open=Decimal("100"),
        high=Decimal("102.5"),
        low=Decimal("100"),
        close=Decimal("102"),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=5,
        taker_buy_volume=Decimal("5"),
        closed=True,
    )
    info = runner._check_exit(signal, candle)
    assert info is not None
    assert info["exit_reason"] == "tp"

    # No hit
    candle = Kline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        open=Decimal("100"),
        high=Decimal("100.5"),
        low=Decimal("99.5"),
        close=Decimal("100"),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=5,
        taker_buy_volume=Decimal("5"),
        closed=True,
    )
    info = runner._check_exit(signal, candle)
    assert info is None

    # Expiry
    candle = Kline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2024, 1, 1, 0, 31, tzinfo=timezone.utc),
        open=Decimal("100"),
        high=Decimal("100.5"),
        low=Decimal("99.5"),
        close=Decimal("100"),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=5,
        taker_buy_volume=Decimal("5"),
        closed=True,
    )
    info = runner._check_exit(signal, candle)
    assert info is not None
    assert info["exit_reason"] == "expired"


@pytest.mark.asyncio
async def test_htf_candle_formation() -> None:
    symbol = "BTCUSDT"
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    candles = []
    for i in range(60):
        candles.append(
            {
                "symbol": symbol,
                "open_time": start + timedelta(minutes=i),
                "open": Decimal("100"),
                "high": Decimal(str(101 + i % 5)),
                "low": Decimal(str(99 - i % 3)),
                "close": Decimal("100"),
                "volume": Decimal("10"),
                "quote_volume": Decimal("1000"),
                "trade_count": 5,
                "taker_buy_volume": Decimal("5"),
                "closed": True,
            }
        )

    runner = BacktestRunner(symbol, start, start + timedelta(hours=1), "1m")
    htf = runner._form_htf_from_1m(candles, "1h", 60)
    assert len(htf) == 1
    assert htf[0]["interval"] == "1h"
    assert htf[0]["high"] == Decimal("105")  # max of 101..105
    assert htf[0]["low"] == Decimal("97")  # min of 99,98,97,99,98,97...
