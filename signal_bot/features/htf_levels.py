"""HTF level detection: fractal swing highs / lows on 1H and 4H candles."""

import argparse
import asyncio
import logging
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import HtfLevel
from signal_bot.ingest.binance_ws import Kline

logger = logging.getLogger(__name__)


class HtfLevelScanner:
    """Detects swing highs/lows using a fractal definition and tracks breakouts."""

    def __init__(
        self,
        candle_queue: asyncio.Queue,
        lookback: int = 3,
        session_factory=None,
    ) -> None:
        self.candle_queue = candle_queue
        self.lookback = lookback
        self.session_factory = session_factory or AsyncSessionLocal
        self._shutdown = asyncio.Event()
        # In-memory candle buffers per (symbol, interval)
        self._candles: dict[tuple[str, str], list[Kline]] = {}

    def shutdown(self) -> None:
        logger.info("HTF scanner shutdown requested.")
        self._shutdown.set()

    async def run(self) -> None:
        logger.info("HTF scanner started (lookback=%d).", self.lookback)
        while not self._shutdown.is_set():
            try:
                candle = await asyncio.wait_for(self.candle_queue.get(), timeout=1.0)
            except TimeoutError:
                continue

            if candle.interval not in ("1h", "4h"):
                continue

            await self._process_candle(candle)

    async def _process_candle(self, candle: Kline) -> None:
        key = (candle.symbol, candle.interval)
        if key not in self._candles:
            self._candles[key] = []
        self._candles[key].append(candle)

        n = self.lookback
        candles = self._candles[key]

        # Need 2n+1 candles to evaluate a swing point
        if len(candles) < 2 * n + 1:
            return

        # Candidate is the candle that now has n neighbours on each side
        idx = len(candles) - n - 1
        candidate = candles[idx]

        is_swing_high = all(
            candidate.high > candles[i].high for i in range(idx - n, idx + n + 1) if i != idx
        )
        is_swing_low = all(
            candidate.low < candles[i].low for i in range(idx - n, idx + n + 1) if i != idx
        )

        async with self.session_factory() as session:
            if is_swing_high:
                await self._save_level(
                    session,
                    candle.symbol,
                    candle.interval,
                    "swing_high",
                    candidate.high,
                    candidate.open_time,
                )
            if is_swing_low:
                await self._save_level(
                    session,
                    candle.symbol,
                    candle.interval,
                    "swing_low",
                    candidate.low,
                    candidate.open_time,
                )

            await session.flush()
            await self._update_broken_levels(
                session,
                candle.symbol,
                candle.interval,
                candle,
            )
            await self._prune_levels(session, candle.symbol, candle.interval)
            await session.commit()

    async def _save_level(
        self,
        session,
        symbol: str,
        interval: str,
        level_type: str,
        price: Decimal,
        formed_at: datetime,
    ) -> None:
        existing = await session.scalar(
            select(HtfLevel).where(
                HtfLevel.symbol == symbol,
                HtfLevel.timeframe == interval,
                HtfLevel.level_type == level_type,
                HtfLevel.price == price,
                HtfLevel.formed_at == formed_at,
            )
        )
        if existing:
            return

        level = HtfLevel(
            symbol=symbol,
            timeframe=interval,
            level_type=level_type,
            price=price,
            formed_at=formed_at,
            active=True,
        )
        session.add(level)
        logger.info(
            "New %s level: %s %s @ %s (formed %s)",
            level_type,
            symbol,
            interval,
            price,
            formed_at,
        )

    async def _update_broken_levels(
        self,
        session,
        symbol: str,
        interval: str,
        candle: Kline,
    ) -> None:
        # Swing high broken when close moves above the level
        result = await session.execute(
            select(HtfLevel).where(
                HtfLevel.symbol == symbol,
                HtfLevel.timeframe == interval,
                HtfLevel.level_type == "swing_high",
                HtfLevel.active.is_(True),
                HtfLevel.price < candle.close,
            )
        )
        for level in result.scalars():
            level.active = False
            level.broken_at = candle.open_time
            logger.info(
                "Swing high broken: %s %s @ %s by close %s",
                symbol,
                interval,
                level.price,
                candle.close,
            )

        # Swing low broken when close moves below the level
        result = await session.execute(
            select(HtfLevel).where(
                HtfLevel.symbol == symbol,
                HtfLevel.timeframe == interval,
                HtfLevel.level_type == "swing_low",
                HtfLevel.active.is_(True),
                HtfLevel.price > candle.close,
            )
        )
        for level in result.scalars():
            level.active = False
            level.broken_at = candle.open_time
            logger.info(
                "Swing low broken: %s %s @ %s by close %s",
                symbol,
                interval,
                level.price,
                candle.close,
            )

    async def _prune_levels(self, session, symbol: str, interval: str) -> None:
        """Keep only the 20 most-recent active levels per symbol/timeframe."""
        result = await session.execute(
            select(HtfLevel)
            .where(
                HtfLevel.symbol == symbol,
                HtfLevel.timeframe == interval,
                HtfLevel.active.is_(True),
            )
            .order_by(HtfLevel.formed_at.desc())
            .offset(20)
        )
        for level in result.scalars():
            level.active = False


async def print_active_levels(symbol: str, timeframe: str) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(HtfLevel)
            .where(
                HtfLevel.symbol == symbol.upper(),
                HtfLevel.timeframe == timeframe.lower(),
                HtfLevel.active.is_(True),
            )
            .order_by(HtfLevel.formed_at.desc())
        )
        levels = result.scalars().all()
        print(f"Active HTF levels for {symbol.upper()} {timeframe.lower()}: {len(levels)}")
        for lvl in levels:
            print(f"  [{lvl.level_type:12}] @ {lvl.price:>16}  formed {lvl.formed_at.isoformat()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="HTF levels CLI")
    parser.add_argument("--symbol", required=True, help="e.g. BTCUSDT")
    parser.add_argument(
        "--timeframe",
        required=True,
        choices=["1h", "4h", "1H", "4H"],
        help="Timeframe to inspect",
    )
    args = parser.parse_args()
    asyncio.run(print_active_levels(args.symbol, args.timeframe))


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    main()
