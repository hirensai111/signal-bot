"""Backtest runner: replays history through the *live* signal modules.

``replay_live_signals`` feeds candles through the production HTF scanner,
microstructure engine, sweep detector and risk sizer (backed by an isolated
in-memory SQLite database) and returns the signals they emit.  Execution is
then simulated by the deterministic fill model in
:mod:`signal_bot.research.fill_model` — there is no randomness anywhere, so a
run is reproducible from its data snapshot and configuration.

For full research runs (walk-forward, cost sensitivity, multi-symbol, versioned
reports) use ``signal-bot-research`` (signal_bot/research/cli.py); this module
keeps a single-symbol CLI for quick checks.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.db.models import AggTrade, Base, Candle1m, Candle5m
from signal_bot.features.htf_levels import HtfLevelScanner
from signal_bot.features.microstructure import MicrostructureEngine
from signal_bot.ingest.binance_ws import Kline
from signal_bot.research.data import Dataset, resample
from signal_bot.signals.risk_sizer import RiskSizer
from signal_bot.signals.sweep_detector import SweepDetector

logger = logging.getLogger(__name__)


def _kline(symbol: str, interval: str, ts: pd.Timestamp, row) -> Kline:
    from decimal import Decimal

    return Kline(
        symbol=symbol,
        interval=interval,
        open_time=ts.to_pydatetime(),
        open=Decimal(str(row.open)),
        high=Decimal(str(row.high)),
        low=Decimal(str(row.low)),
        close=Decimal(str(row.close)),
        volume=Decimal(str(row.volume)),
        quote_volume=Decimal(str(row.quote_volume)),
        trade_count=int(row.trade_count),
        taker_buy_volume=Decimal(str(row.taker_buy_volume)),
        closed=True,
    )


def synthesize_agg_trades(symbol: str, frame_1m: pd.DataFrame) -> list[dict]:
    """Two aggregate trades per 1m candle (taker buys / taker sells).

    Gives the microstructure engine the buyer/seller split it needs without
    downloading every individual trade.
    """
    rows = []
    trade_id = 0
    for ts, r in frame_1m.iterrows():
        buy = float(r.taker_buy_volume)
        sell = float(r.volume) - buy
        for qty, maker in ((buy, False), (sell, True)):
            if qty > 0:
                trade_id += 1
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_id": trade_id,
                        "price": float(r.close),
                        "qty": qty,
                        "quote_qty": qty * float(r.close),
                        "ts": ts.to_pydatetime(),
                        "is_buyer_maker": maker,
                    }
                )
    return rows


async def replay_live_signals(
    symbol: str, frame_1m: pd.DataFrame, timeframe: str = "5m"
) -> list[dict[str, Any]]:
    """Return the signals the production modules generate on this history."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    ltf = frame_1m if timeframe == "1m" else resample(frame_1m, timeframe)
    htf = {"1h": resample(frame_1m, "1h"), "4h": resample(frame_1m, "4h")}
    model = Candle1m if timeframe == "1m" else Candle5m
    candle_rows = [
        {
            "symbol": symbol,
            "open_time": ts.to_pydatetime(),
            "open": float(r.open),
            "high": float(r.high),
            "low": float(r.low),
            "close": float(r.close),
            "volume": float(r.volume),
            "quote_volume": float(r.quote_volume),
            "trade_count": int(r.trade_count),
            "taker_buy_volume": float(r.taker_buy_volume),
            "closed": True,
        }
        for ts, r in ltf.iterrows()
    ]
    trades = synthesize_agg_trades(symbol, frame_1m)
    async with factory() as session:
        for i in range(0, len(candle_rows), 5000):
            await session.execute(insert(model), candle_rows[i : i + 5000])
        for i in range(0, len(trades), 5000):
            await session.execute(insert(AggTrade), trades[i : i + 5000])
        await session.commit()

    scanner = HtfLevelScanner(asyncio.Queue(), lookback=3, session_factory=factory)
    micro = MicrostructureEngine(asyncio.Queue(), session_factory=factory, settle_delay=0)
    detector = SweepDetector(asyncio.Queue(), session_factory=factory)
    sizer = RiskSizer(session_factory=factory)

    step = pd.Timedelta(minutes=1 if timeframe == "1m" else 5)
    htf_by_close = {
        name: {
            ts + pd.Timedelta(hours=1 if name == "1h" else 4): (ts, row) for ts, row in f.iterrows()
        }
        for name, f in htf.items()
    }
    signals: list[dict[str, Any]] = []
    for ts, row in ltf.iterrows():
        close_time = ts + step
        for name in ("1h", "4h"):
            hit = htf_by_close[name].get(close_time)
            if hit is not None:
                await scanner._process_candle(_kline(symbol, name, hit[0], hit[1]))
        kline = _kline(symbol, timeframe, ts, row)
        await micro._process_candle(kline)
        candidate = await detector._evaluate(kline)
        if candidate is None:
            continue
        sig = await sizer.size_and_persist(candidate, timeframe, created_at=kline.open_time)
        if sig is None:
            continue
        signals.append(
            {
                "open_time": ts,
                "side": sig.side,
                "entry": sig.entry,
                "stop": sig.stop_loss,
                "take_profit": sig.take_profit,
                "leverage": sig.leverage,
                "features": candidate.pattern_features,
            }
        )
    await engine.dispose()
    logger.info(
        "%s: %d live-module signals over %d %s bars", symbol, len(signals), len(ltf), timeframe
    )
    return signals


class BacktestRunner:
    """Single-symbol backtest of the production sweep strategy."""

    def __init__(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "5m",
        cache_dir: str = "data/cache",
    ) -> None:
        self.symbol = symbol.upper()
        self.start = start
        self.end = end
        self.timeframe = timeframe
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dataset:
        from signal_bot.research.data import load_binance_vision

        ds = load_binance_vision(
            self.symbol,
            "1m",
            self.start.strftime("%Y-%m"),
            (self.end - timedelta(days=1)).strftime("%Y-%m"),
            cache_dir=str(self.cache_dir),
        )
        return ds.slice(self.start, self.end)

    def run(self, data: Dataset | None = None) -> dict[str, Any]:
        from signal_bot.research.fill_model import CostModel, Sizing, simulate, trades_frame
        from signal_bot.research.metrics import performance
        from signal_bot.research.strategies import LiquiditySweepBaseline, frame_for

        data = data or self.load()
        strategy = LiquiditySweepBaseline(timeframe=self.timeframe)
        intents = strategy.generate(data)
        frame = frame_for(data, self.timeframe)
        sizing = Sizing()
        trades, equity = simulate(frame, intents, CostModel(), sizing, self.symbol)
        metrics = performance(trades, equity, sizing.initial_equity)
        stem = f"backtest_{self.symbol}_{self.timeframe}_{self.start.date()}_{self.end.date()}"
        trades_frame(trades).to_csv(self.cache_dir / f"{stem}.csv", index=False)
        out = {"symbol": self.symbol, "snapshot_id": data.snapshot_id, "metrics": metrics}
        (self.cache_dir / f"{stem}.json").write_text(json.dumps(out, indent=2, default=str))
        print(json.dumps(metrics, indent=2, default=str))
        return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest the production sweep strategy")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--timeframe", default="5m", choices=["1m", "5m"])
    args = parser.parse_args()
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    BacktestRunner(args.symbol, start, end, args.timeframe).run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    main()
