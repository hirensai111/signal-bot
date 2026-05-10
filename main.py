"""Startup script: initialise schema and seed instruments."""

import asyncio
import logging

from sqlalchemy import select

from config import settings
from db.connection import async_engine
from db.models import Base, Instrument

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Placeholder instrument defaults — these will be overwritten by a future
# exchange-sync module that pulls live contract specs from Binance.
_DEFAULT_INSTRUMENTS = [
    {"symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT", "tick_size": "0.10", "min_qty": "0.001"},
    {"symbol": "ETHUSDT", "base_asset": "ETH", "quote_asset": "USDT", "tick_size": "0.01", "min_qty": "0.001"},
    {"symbol": "SOLUSDT", "base_asset": "SOL", "quote_asset": "USDT", "tick_size": "0.001", "min_qty": "0.01"},
    {"symbol": "LINKUSDT", "base_asset": "LINK", "quote_asset": "USDT", "tick_size": "0.001", "min_qty": "0.01"},
    {"symbol": "AVAXUSDT", "base_asset": "AVAX", "quote_asset": "USDT", "tick_size": "0.001", "min_qty": "0.01"},
    {"symbol": "ADAUSDT", "base_asset": "ADA", "quote_asset": "USDT", "tick_size": "0.0001", "min_qty": "1.0"},
]


async def init_schema() -> None:
    """Create all tables if they do not already exist."""
    logger.info("Creating tables (if missing)…")
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Schema initialised.")


async def seed_instruments() -> None:
    """Seed the instruments table with the default watch-list."""
    from db.connection import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        for inst in _DEFAULT_INSTRUMENTS:
            existing = await session.scalar(
                select(Instrument.symbol).where(Instrument.symbol == inst["symbol"])
            )
            if existing:
                logger.info("Instrument %s already present — skipping.", inst["symbol"])
                continue

            instrument = Instrument(
                symbol=inst["symbol"],
                base_asset=inst["base_asset"],
                quote_asset=inst["quote_asset"],
                tick_size=inst["tick_size"],
                min_qty=inst["min_qty"],
                active=True,
            )
            session.add(instrument)
            logger.info("Seeding instrument %s.", inst["symbol"])

        await session.commit()
    logger.info("Instrument seeding complete.")


async def main() -> None:
    await init_schema()
    await seed_instruments()


if __name__ == "__main__":
    asyncio.run(main())
