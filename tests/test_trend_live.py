"""Replay parity: live 4h trend decisions equal the research implementation's.

The live generator is fed candles one at a time, exactly as the WebSocket fanout
delivers them, and must reproduce ``research.strategies.TrendBreakout.generate``
bar for bar: same bars, sides, entries, stops and trail distances, bit for bit.
"""

from __future__ import annotations

import asyncio
import tomllib
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.core.clock import ManualClock
from signal_bot.db.models import Base, Signal
from signal_bot.events import EventBus, InMemoryJournal
from signal_bot.events.envelope import EventType
from signal_bot.execution.adapter import InstrumentSpec
from signal_bot.execution.order_store import OrderStore
from signal_bot.features.candle_history import CandleHistory
from signal_bot.ingest.binance_ws import Kline
from signal_bot.research.data import Dataset, resample, synthetic
from signal_bot.research.strategies import TrendBreakout
from signal_bot.risk.portfolio import PortfolioLimits
from signal_bot.signals.trend import TrendParams, trend_decisions
from signal_bot.signals.trend_live import LivePortfolio, TrendPipeline, TrendSignalGenerator
from signal_bot.venues.simulated import SimulatedVenue

FIXTURE = Path(__file__).parent / "fixtures" / "binance_btcusdt_4h_2024-01_2025-06.csv.gz"
CONFIG = "signal_bot/research/strategies/trend_breakout.toml"
STEP = timedelta(hours=4)


def frozen_params() -> TrendParams:
    with open(CONFIG, "rb") as fh:
        return TrendParams.from_mapping(tomllib.load(fh)["params"])


def real_frame() -> pd.DataFrame:
    frame = pd.read_csv(FIXTURE, index_col="open_time", parse_dates=True)
    frame.index = pd.DatetimeIndex(frame.index, tz="UTC") if frame.index.tz is None else frame.index
    return frame


def klines(frame: pd.DataFrame, symbol: str = "BTCUSDT", closed: bool = True) -> list[Kline]:
    """Rows as the exchange delivers them: decimal strings, parsed to Decimal."""
    out = []
    for ts, r in frame.iterrows():
        out.append(
            Kline(
                symbol=symbol,
                interval="4h",
                open_time=ts.to_pydatetime(),
                open=Decimal(repr(float(r["open"]))),
                high=Decimal(repr(float(r["high"]))),
                low=Decimal(repr(float(r["low"]))),
                close=Decimal(repr(float(r["close"]))),
                volume=Decimal(repr(float(r["volume"]))),
                quote_volume=Decimal(repr(float(r["quote_volume"]))),
                trade_count=int(r.get("trade_count", 0)),
                taker_buy_volume=Decimal(repr(float(r.get("taker_buy_volume", 0.0)))),
                closed=closed,
            )
        )
    return out


def research_intents(frame: pd.DataFrame, params: TrendParams):
    strat = TrendBreakout(**params.to_dict())
    return strat.generate(Dataset("BTCUSDT", "4h", frame, "fixture"))


def key(time, side, entry, stop, trail):
    return (pd.Timestamp(time), side, entry, stop, trail)


# ---------------------------------------------------------------------------
# parity
# ---------------------------------------------------------------------------


def test_live_decisions_equal_research_on_real_binance_bars():
    frame = real_frame()
    params = frozen_params()
    research = [
        key(frame.index[i.bar], i.side, i.entry, i.stop, i.trail_distance)
        for i in research_intents(frame, params)
    ]
    gen = TrendSignalGenerator(params, CandleHistory("4h"), min_history_bars=0)
    live = []
    for k in klines(frame):
        outcome, d = gen.on_candle(k)
        assert outcome == "appended"
        if d is not None:
            live.append(key(d.time, d.side, d.entry, d.stop, d.trail_distance))
    assert len(research) >= 20  # the fixture actually exercises the rule
    assert {r[1] for r in research} == {"long", "short"}
    assert live == research  # bit-for-bit: same bars, sides, prices


def test_production_warm_up_reproduces_long_running_research():
    """Live seeds 1000 bars from REST; research has run since the first bar of data."""
    frame = real_frame()
    params = frozen_params()
    cut = 2000
    research = [
        key(frame.index[i.bar], i.side, i.entry, i.stop, i.trail_distance)
        for i in research_intents(frame, params)
        if i.bar >= cut
    ]
    history = CandleHistory("4h")
    gen = TrendSignalGenerator(params, history, min_history_bars=1000)
    seed = klines(frame.iloc[cut - 1000 : cut])
    assert history.seed(seed) == 1000 and gen.ready("BTCUSDT")
    live = []
    for k in klines(frame.iloc[cut:]):
        _, d = gen.on_candle(k)
        if d is not None:
            live.append((pd.Timestamp(d.time), d.side, d.entry, d.stop, d.trail_distance))
    # Decisions match; prices are exact except the ATR/EMA residual from the shorter start.
    assert [(r[0], r[1], r[2]) for r in live] == [(r[0], r[1], r[2]) for r in research]
    for a, b in zip(live, research, strict=True):
        assert a[3] == pytest.approx(b[3], rel=1e-9)
        assert a[4] == pytest.approx(b[4], rel=1e-9)


def test_resampled_research_bars_and_native_4h_bars_give_the_same_decisions():
    """Research resamples 1h files; live receives native 4h klines (OHLC identical)."""
    params = TrendParams.from_mapping({**frozen_params().to_dict(), "trend_ema": 50})
    hourly = synthetic(interval="1h", bars=7_000, seed=5)
    four = resample(hourly.frame, "4h")
    research = [
        (i.bar, i.side, i.entry, i.stop) for i in TrendBreakout(**params.to_dict()).generate(hourly)
    ]
    gen = TrendSignalGenerator(params, CandleHistory("4h"), min_history_bars=0)
    live = []
    for n, k in enumerate(klines(four, "SYNTHUSDT")):
        _, d = gen.on_candle(k)
        if d is not None:
            live.append((n, d.side, d.entry, d.stop))
    assert research and live == research


# ---------------------------------------------------------------------------
# completed candles only
# ---------------------------------------------------------------------------


def test_history_accepts_only_completed_in_order_candles():
    frame = real_frame().iloc[:5]
    history = CandleHistory("4h")
    ks = klines(frame)
    forming = klines(frame.iloc[3:4], closed=False)[0]
    assert history.append(forming) == "not_closed"
    assert history.append(ks[0]) == "appended"
    assert history.append(ks[0]) == "duplicate"
    assert history.append(ks[2]) == "appended"  # a gap is allowed (and counted)
    assert history.gaps["BTCUSDT"] == 1
    assert history.append(ks[1]) == "out_of_order"
    from dataclasses import replace

    assert history.append(replace(ks[3], interval="1h")) == "wrong_interval"
    assert history.bars("BTCUSDT") == 2


def test_a_decision_never_changes_when_later_bars_arrive():
    frame = real_frame()
    params = frozen_params()
    full = {(d.bar, d.side, d.entry, d.stop) for d in trend_decisions(frame, params)}
    for end in (1200, 1900, 2600):
        prefix = {
            (d.bar, d.side, d.entry, d.stop) for d in trend_decisions(frame.iloc[:end], params)
        }
        assert prefix == {d for d in full if d[0] < end}


# ---------------------------------------------------------------------------
# pipeline: batching, ranking, portfolio admission, persistence
# ---------------------------------------------------------------------------

SPECS = {
    "AAAUSDT": InstrumentSpec("AAAUSD", 1, Decimal("0.01"), Decimal("1"), Decimal("0.001")),
    "BBBUSDT": InstrumentSpec("BBBUSD", 2, Decimal("0.01"), Decimal("1"), Decimal("0.001")),
}


def breakout_frames(n: int = 400) -> dict[str, pd.DataFrame]:
    """Two symbols whose final bar breaks out upward on high volume (simultaneous entries)."""
    idx = pd.date_range("2025-01-01", periods=n, freq="4h", tz="UTC")
    out = {}
    for sym, jump in (("AAAUSDT", 0.04), ("BBBUSDT", 0.06)):
        base = 1000.0 + pd.Series(range(n), index=idx, dtype=float) * 0.5
        wiggle = pd.Series([(i % 7 - 3) * 2.0 for i in range(n)], index=idx)
        close = base + wiggle
        close.iloc[-1] = close.iloc[-60:-1].max() * (1 + jump)
        f = pd.DataFrame(
            {
                "open": close.shift(1).fillna(close.iloc[0]),
                "high": close + 3.0,
                "low": close - 3.0,
                "close": close,
                "volume": 100.0,
                "quote_volume": close * 100.0,
            }
        )
        f.loc[idx[-1], "high"] = close.iloc[-1] + 1.0
        f.loc[idx[-1], "volume"] = 400.0
        out[sym] = f
    return out


@pytest.fixture
async def pipeline_env(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 't.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    frames = breakout_frames()
    last_close = frames["AAAUSDT"].index[-1].to_pydatetime() + STEP
    clock = ManualClock(last_close - timedelta(hours=8))
    bus = EventBus(InMemoryJournal())
    venue = SimulatedVenue(SPECS, bus, clock=clock, initial_balance=Decimal("10000"))
    for spec in SPECS.values():
        venue.set_book(spec.symbol, Decimal("1100"), Decimal("1100.5"))
    store = OrderStore(factory, bus, clock=clock)
    history = CandleHistory("4h")
    params = TrendParams.from_mapping({**frozen_params().to_dict(), "trend_ema": 50})
    gen = TrendSignalGenerator(params, history, min_history_bars=300)
    limits = PortfolioLimits()
    portfolio = LivePortfolio(venue, store, history, factory, limits, clock=clock)
    candle_q: asyncio.Queue = asyncio.Queue()
    signal_q: asyncio.Queue = asyncio.Queue()
    sub = bus.subscribe("test", [EventType.SIGNAL_GENERATED, EventType.RISK_DENIED])
    pipe = TrendPipeline(
        gen,
        portfolio,
        candle_q,
        signal_q,
        bus,
        factory,
        universe=list(SPECS),
        tradable=set(SPECS),
        clock=clock,
        batch_timeout=120,
    )
    for sym, f in frames.items():
        history.seed(klines(f.iloc[:-1], sym))
    yield {
        "pipe": pipe,
        "frames": frames,
        "clock": clock,
        "factory": factory,
        "signal_q": signal_q,
        "sub": sub,
        "last_close": last_close,
        "limits": limits,
    }
    await engine.dispose()


async def test_simultaneous_breakouts_are_batched_ranked_and_sized_on_shared_equity(pipeline_env):
    env = pipeline_env
    pipe, frames, clock = env["pipe"], env["frames"], env["clock"]
    clock.set(env["last_close"] + timedelta(seconds=2))
    a = klines(frames["AAAUSDT"].iloc[-1:], "AAAUSDT")[0]
    b = klines(frames["BBBUSDT"].iloc[-1:], "BBBUSDT")[0]
    await pipe.handle_candle(a)
    assert env["signal_q"].empty()  # waits for every tradable symbol's bar
    await pipe.handle_candle(b)
    sigs = [env["signal_q"].get_nowait() for _ in range(env["signal_q"].qsize())]
    # BBB broke out further (in ATRs), so it ranks first.
    assert [s.symbol for s in sigs] == ["BBBUSDT", "AAAUSDT"]
    for s in sigs:
        assert s.strategy == "trend_breakout" and s.take_profit is None
        assert s.trail_distance > 0 and s.planned_contracts >= 1
        assert s.trail_from == env["last_close"]
        assert s.time_stop_at == env["last_close"] + STEP * 181
        risk = float(s.planned_contracts) * 0.001 * float(s.entry - s.stop_loss)
        assert risk <= 10000 * env["limits"].risk_per_trade + 1e-6
    assert pipe.last_batch["decisions"][0]["symbol"] == "BBBUSDT"
    async with env["factory"]() as session:
        rows = (await session.execute(select(Signal))).scalars().all()
    assert {r.status for r in rows} == {"pending"} and len(rows) == 2


async def test_late_candles_update_history_but_never_trade(pipeline_env):
    env = pipeline_env
    pipe, frames, clock = env["pipe"], env["frames"], env["clock"]
    clock.set(env["last_close"] + timedelta(minutes=30))  # e.g. a REST backfill after an outage
    for sym in SPECS:
        await pipe.handle_candle(klines(frames[sym].iloc[-1:], sym)[0])
    await pipe.flush_due()
    assert env["signal_q"].empty()
    assert pipe.history.bars("AAAUSDT") == len(frames["AAAUSDT"])


async def test_portfolio_limits_refuse_and_record_the_reason(pipeline_env):
    env = pipeline_env
    pipe, frames, clock = env["pipe"], env["frames"], env["clock"]
    pipe.limits = pipe.portfolio.limits = PortfolioLimits(max_entries_per_bar=1)
    clock.set(env["last_close"] + timedelta(seconds=2))
    for sym in SPECS:
        await pipe.handle_candle(klines(frames[sym].iloc[-1:], sym)[0])
    assert [s.symbol for s in [env["signal_q"].get_nowait()]] == ["BBBUSDT"]
    async with env["factory"]() as session:
        refused = (
            await session.execute(select(Signal).where(Signal.status == "cancelled"))
        ).scalar_one()
    assert refused.symbol == "AAAUSDT"
    assert refused.cancelled_reason == "portfolio:max_entries_per_bar"
    kinds = []
    while not env["sub"].queue.empty():
        kinds.append(env["sub"].queue.get_nowait().event_type)
    assert kinds.count(EventType.SIGNAL_GENERATED) == 1 and kinds.count(EventType.RISK_DENIED) == 1


async def test_batch_times_out_when_a_symbol_never_reports(pipeline_env):
    env = pipeline_env
    pipe, frames, clock = env["pipe"], env["frames"], env["clock"]
    clock.set(env["last_close"] + timedelta(seconds=2))
    await pipe.handle_candle(klines(frames["AAAUSDT"].iloc[-1:], "AAAUSDT")[0])
    await pipe.flush_due()
    assert env["signal_q"].empty()
    clock.advance(130)
    await pipe.flush_due()
    assert env["signal_q"].get_nowait().symbol == "AAAUSDT"


async def test_governor_recheck_uses_the_planned_size(pipeline_env):
    env = pipeline_env
    pipe, frames, clock = env["pipe"], env["frames"], env["clock"]
    clock.set(env["last_close"] + timedelta(seconds=2))
    for sym in SPECS:
        await pipe.handle_candle(klines(frames[sym].iloc[-1:], sym)[0])
    sig = env["signal_q"].get_nowait()
    assert await pipe.recheck(sig) == (True, None)
    huge = Signal(**{c: getattr(sig, c) for c in ("symbol", "side", "entry", "stop_loss")})
    huge.planned_contracts = Decimal("100000")
    ok, reason = await pipe.recheck(huge)
    assert not ok and reason in ("portfolio:max_open_risk", "portfolio:correlated_risk")
