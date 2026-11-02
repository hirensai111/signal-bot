"""Full-stack replay: recorded 4h candles through the live components == the research backtest.

Real BTCUSDT 4h bars are fed to the production trend pipeline, risk governor,
execution engine, trailing-stop manager and outcome monitor, trading against the
simulated venue whose book is walked through each bar (open, then the side that
threatens the stop, then the other extreme, then close).  The trades that come
out must be the research backtest's trades: same signal bars, same entry bars,
same exit bars, same kind of exit.  Prices differ only by venue microstructure
(book fills and tick rounding instead of the bar-open fill model).
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pandas as pd
import pytest
from sqlalchemy import select

from signal_bot.core.clock import utc
from signal_bot.db.models import Order, Signal, TradeResult
from signal_bot.execution.engine import ExecutionConfig
from signal_bot.execution.trailing import TrailingStopManager
from signal_bot.features.candle_history import CandleHistory
from signal_bot.research.data import Dataset
from signal_bot.research.fill_model import CostModel, Sizing, simulate
from signal_bot.research.strategies import TrendBreakout
from signal_bot.risk.portfolio import PortfolioLimits
from signal_bot.signals.trend_live import LivePortfolio, TrendPipeline, TrendSignalGenerator
from tests.harness import Harness
from tests.test_trend_live import frozen_params, klines, real_frame

pytestmark = pytest.mark.replay

WARMUP = 1000
STEP = timedelta(hours=4)
D = Decimal
LOOSE = PortfolioLimits(
    max_open_risk=1.0,
    max_correlated_risk=1.0,
    max_gross_leverage=100.0,
    drawdown_halt=1.0,
    position_leverage=1,
)


async def replay(tmp_path, frame: pd.DataFrame, bars: int):
    cfg = ExecutionConfig(
        unknown_lookup_attempts=2,
        unknown_lookup_backoff=0.001,
        protective_confirm_timeout=0.01,
        protective_poll_interval=0.005,
        protective_retry_attempts=2,
        no_fill_timeout=30,
        max_signal_age_seconds=3600,
        basis_abort_pct=Decimal("0.01"),
    )
    ks = klines(frame)
    h = await Harness.create(
        tmp_path,
        config=cfg,
        clock_start=ks[WARMUP].open_time - STEP,
        governor_kwargs={
            "max_trades_per_day": 1000,
            "max_consecutive_losses": 1000,
            "daily_loss_limit_r": -1000,
        },
    )
    history = CandleHistory("4h")
    trailing = TrailingStopManager(
        h.venue, h.store, history, bus=h.bus, alerts=h.alerts, clock=h.clock
    )
    h.reconciler.trailing = trailing
    gen = TrendSignalGenerator(frozen_params(), history, min_history_bars=WARMUP)
    portfolio = LivePortfolio(h.venue, h.store, history, h.factory, LOOSE, clock=h.clock)
    signals: asyncio.Queue = asyncio.Queue()
    pipe = TrendPipeline(
        gen,
        portfolio,
        asyncio.Queue(),
        signals,
        h.bus,
        h.factory,
        universe=["BTCUSDT"],
        tradable={"BTCUSDT"},
        trailing=trailing,
        clock=h.clock,
    )
    h.governor.portfolio_check = pipe.recheck
    history.seed(ks[:WARMUP])
    # Binance's tick, so venue rounding cannot move a stop relative to the research level.
    h.venue._instruments["BTCUSDT"] = replace(h.venue._instruments["BTCUSDT"], tick_size=D("0.1"))
    tick = D("0.1")

    def q(x) -> Decimal:
        return (D(repr(float(x))) / tick).quantize(D("1")) * tick

    async def book(px, kind="mid"):
        """Put the bar's traded extreme exactly at the touch the venue's stop checks."""
        p = q(px)
        bid, ask = {"low": (p, p + tick), "high": (p - tick, p)}.get(kind, (p - tick, p + tick))
        await h.venue.move_market("BTCUSD", bid, ask)
        await h.drain()

    prev_close = ks[WARMUP - 1].close
    for k in ks[WARMUP : WARMUP + bars]:
        # Signals decided at the previous close execute seconds later, at that price
        # (production timing), then the bar trades: open, extremes, close.
        if not signals.empty():
            await book(prev_close)
            while not signals.empty():
                await h.engine.process_signal(signals.get_nowait())
                await h.drain()
        h.clock.set(k.open_time + timedelta(seconds=1))
        await book(k.open)
        pos = h.venue.position("BTCUSD")
        path = [(k.low, "low"), (k.high, "high")]
        if pos is not None and pos.size < 0:
            path.reverse()
        for px, kind in [*path, (k.close, "mid")]:
            await book(px, kind)
        h.clock.set(k.open_time + STEP + timedelta(seconds=1))
        await pipe.handle_candle(k)
        await h.drain()
        prev_close = k.close
    async with h.raw_factory() as session:
        rows = (
            await session.execute(
                select(TradeResult, Order, Signal)
                .join(Order, Order.signal_id == TradeResult.signal_id)
                .join(Signal, Signal.id == TradeResult.signal_id)
                .order_by(TradeResult.closed_at)
            )
        ).all()
    await h.close()
    return rows


def bar_of(ts, index: pd.DatetimeIndex) -> int:
    """Index of the bar whose open <= ts (SQLite returns naive UTC datetimes)."""
    return int(index.searchsorted(pd.Timestamp(utc(ts)), side="right")) - 1


async def test_live_stack_replay_takes_the_research_trades(tmp_path):
    frame = real_frame()
    bars = 1500
    rows = await replay(tmp_path, frame, bars)
    research_frame = frame.iloc[: WARMUP + bars]
    strat = TrendBreakout(**frozen_params().to_dict())
    intents = [
        i
        for i in strat.generate(Dataset("BTCUSDT", "4h", research_frame, "fixture"))
        if i.bar >= WARMUP - 1
    ]
    trades, _ = simulate(
        research_frame, intents, CostModel(), Sizing(10_000, 0.005, 1e9), "BTCUSDT"
    )
    idx = research_frame.index
    research = [
        (
            idx.get_loc(t.signal_time),
            idx.get_loc(t.exit_time),
            "trail" if t.exit_reason == "trail" else t.exit_reason,
        )
        for t in trades
        if t.filled and t.exit_time is not None and t.exit_time < idx[-1]
    ]
    kind = {"trail_hit": "trail", "sl_hit": "sl", "time_stop": "time"}
    live = []
    for result, _order, sig in rows:
        signal_bar = idx.get_loc(pd.Timestamp(utc(sig.trail_from)) - STEP)
        live.append(
            (
                signal_bar,
                bar_of(result.closed_at, idx),
                kind.get(result.exit_reason, result.exit_reason),
            )
        )
    assert len(research) >= 8
    assert live == research
