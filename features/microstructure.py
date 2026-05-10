"""Microstructure features: CVD, volume z-score, and volume ratio."""

import asyncio
import logging
from datetime import timedelta
from decimal import Decimal

import pandas as pd
from sqlalchemy import func, select

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import (
    AggTrade as AggTradeModel,
    Candle1m,
    Candle5m,
    CandleFeature,
)
from signal_bot.ingest.binance_ws import Kline

logger = logging.getLogger(__name__)


class MicrostructureEngine:
    """Computes CVD and volume metrics per closed candle."""

    def __init__(
        self,
        candle_queue: asyncio.Queue,
        session_factory=None,
        settle_delay: float = 1.5,
    ) -> None:
        self.candle_queue = candle_queue
        self.session_factory = session_factory or AsyncSessionLocal
        self.settle_delay = settle_delay
        self._shutdown = asyncio.Event()
        self._cvd_state: dict[tuple[str, str], Decimal] = {}

    def shutdown(self) -> None:
        logger.info("Microstructure engine shutdown requested.")
        self._shutdown.set()

    async def run(self) -> None:
        logger.info("Microstructure engine started.")
        while not self._shutdown.is_set():
            try:
                candle = await asyncio.wait_for(
                    self.candle_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            if candle.interval not in ("1m", "5m"):
                continue

            await self._process_candle(candle)

    async def _process_candle(self, candle: Kline) -> None:
        if self.settle_delay > 0:
            await asyncio.sleep(self.settle_delay)

        async with self.session_factory() as session:
            cvd_delta = await self._compute_cvd_delta(session, candle)

            key = (candle.symbol, candle.interval)
            if key not in self._cvd_state:
                self._cvd_state[key] = Decimal("0")
            self._cvd_state[key] += cvd_delta

            vol_mean, vol_std = await self._compute_volume_stats(session, candle)
            if vol_std > 0:
                vol_zscore = (candle.volume - vol_mean) / vol_std
            else:
                vol_zscore = Decimal("0")

            if vol_mean > 0:
                vol_ratio = candle.volume / vol_mean
            else:
                vol_ratio = Decimal("0")

            await self._persist_features(
                session,
                candle,
                cvd_delta,
                self._cvd_state[key],
                vol_zscore,
                vol_ratio,
            )
            await session.commit()

    async def _compute_cvd_delta(self, session, candle: Kline) -> Decimal:
        delta = timedelta(minutes=1) if candle.interval == "1m" else timedelta(minutes=5)

        buy_vol = await session.scalar(
            select(func.coalesce(func.sum(AggTradeModel.qty), Decimal("0"))).where(
                AggTradeModel.symbol == candle.symbol,
                AggTradeModel.ts >= candle.open_time,
                AggTradeModel.ts < candle.open_time + delta,
                AggTradeModel.is_buyer_maker.is_(False),
            )
        )

        sell_vol = await session.scalar(
            select(func.coalesce(func.sum(AggTradeModel.qty), Decimal("0"))).where(
                AggTradeModel.symbol == candle.symbol,
                AggTradeModel.ts >= candle.open_time,
                AggTradeModel.ts < candle.open_time + delta,
                AggTradeModel.is_buyer_maker.is_(True),
            )
        )

        return buy_vol - sell_vol

    async def _compute_volume_stats(self, session, candle: Kline) -> tuple[Decimal, Decimal]:
        table = Candle1m if candle.interval == "1m" else Candle5m

        result = await session.execute(
            select(table)
            .where(
                table.symbol == candle.symbol,
                table.open_time < candle.open_time,
            )
            .order_by(table.open_time.desc())
            .limit(19)
        )
        rows = result.scalars().all()

        volumes = [candle.volume] + [r.volume for r in rows]
        n = len(volumes)

        if n < 2:
            return Decimal("0"), Decimal("0")

        mean = sum(volumes) / n
        variance = sum((v - mean) ** 2 for v in volumes) / n
        std = variance.sqrt()

        return mean, std

    async def _persist_features(
        self,
        session,
        candle: Kline,
        cvd_delta: Decimal,
        cvd_cumulative: Decimal,
        vol_zscore: Decimal,
        vol_ratio: Decimal,
    ) -> None:
        feature = CandleFeature(
            symbol=candle.symbol,
            timeframe=candle.interval,
            open_time=candle.open_time,
            cvd_delta=cvd_delta,
            cvd_cumulative=cvd_cumulative,
            vol_zscore=vol_zscore,
            vol_ratio_20=vol_ratio,
        )
        session.add(feature)
        logger.debug(
            "Persisted features for %s %s @ %s: cvd=%s vol_z=%s vol_r=%s",
            candle.symbol,
            candle.interval,
            candle.open_time,
            cvd_cumulative,
            vol_zscore,
            vol_ratio,
        )


async def get_recent_features(
    symbol: str, timeframe: str, n: int = 100, session_factory=None
) -> pd.DataFrame:
    """Return the most recent n feature rows as a pandas DataFrame."""
    session_factory = session_factory or AsyncSessionLocal
    async with session_factory() as session:
        result = await session.execute(
            select(CandleFeature)
            .where(
                CandleFeature.symbol == symbol,
                CandleFeature.timeframe == timeframe,
            )
            .order_by(CandleFeature.open_time.desc())
            .limit(n)
        )
        rows = result.scalars().all()

    if not rows:
        return pd.DataFrame(
            columns=[
                "symbol",
                "timeframe",
                "open_time",
                "cvd_delta",
                "cvd_cumulative",
                "vol_zscore",
                "vol_ratio_20",
            ]
        )

    data = [
        {
            "symbol": r.symbol,
            "timeframe": r.timeframe,
            "open_time": r.open_time,
            "cvd_delta": r.cvd_delta,
            "cvd_cumulative": r.cvd_cumulative,
            "vol_zscore": r.vol_zscore,
            "vol_ratio_20": r.vol_ratio_20,
        }
        for r in rows
    ]

    df = pd.DataFrame(data)
    df = df.sort_values("open_time").reset_index(drop=True)
    return df
