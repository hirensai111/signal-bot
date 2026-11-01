"""Async queue consumer that batches inserts to candles_1m, candles_5m and agg_trades."""

import asyncio
import logging

from sqlalchemy.dialects.postgresql import insert

from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import AggTrade as AggTradeModel
from signal_bot.db.models import Candle1m, Candle5m
from signal_bot.ingest.binance_ws import AggTrade, Kline

logger = logging.getLogger(__name__)


class QueueWriter:
    """Consumes the ingest queue, batches DB inserts, and emits closed-candle events."""

    def __init__(
        self,
        source_queue: asyncio.Queue,
        session_factory=None,
        flush_interval: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        self.source_queue = source_queue
        self.session_factory = session_factory or AsyncSessionLocal
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self._shutdown = asyncio.Event()
        self._last_flush = 0.0
        self._batches: dict[str, list[dict]] = {
            "candles_1m": [],
            "candles_5m": [],
            "agg_trades": [],
        }
        self.closed_candle_queue: asyncio.Queue = asyncio.Queue()
        self._subscribers: list[asyncio.Queue] = []

    def subscribe(self, queue: asyncio.Queue) -> None:
        """Register an additional subscriber queue for closed-candle events."""
        self._subscribers.append(queue)

    def shutdown(self) -> None:
        logger.info("Writer shutdown requested.")
        self._shutdown.set()

    async def run(self) -> None:
        self._last_flush = asyncio.get_event_loop().time()
        async with self.session_factory() as session:
            while not self._shutdown.is_set():
                try:
                    item = await asyncio.wait_for(self.source_queue.get(), timeout=0.1)
                except TimeoutError:
                    await self._flush_if_needed(session)
                    continue

                await self._handle_item(item)
                await self._flush_if_needed(session)

            await self._flush(session)

    async def _handle_item(self, item: object) -> None:
        if isinstance(item, Kline) and item.closed:
            if item.interval == "1m":
                self._batches["candles_1m"].append(self._kline_to_dict(item))
            elif item.interval == "5m":
                self._batches["candles_5m"].append(self._kline_to_dict(item))

            # Emit closed-candle event for downstream consumers (e.g. HTF levels, microstructure)
            await self.closed_candle_queue.put(item)
            for sub in self._subscribers:
                await sub.put(item)

        elif isinstance(item, AggTrade):
            self._batches["agg_trades"].append(self._agg_trade_to_dict(item))

    async def _flush_if_needed(self, session) -> None:
        now = asyncio.get_event_loop().time()
        size_reached = any(len(batch) >= self.batch_size for batch in self._batches.values())
        if size_reached or (now - self._last_flush >= self.flush_interval):
            await self._flush(session)
            self._last_flush = now

    async def _flush(self, session) -> None:
        for table_name, batch in self._batches.items():
            if not batch:
                continue

            if table_name == "candles_1m":
                stmt = (
                    insert(Candle1m)
                    .values(batch)
                    .on_conflict_do_nothing(index_elements=["symbol", "open_time"])
                )
            elif table_name == "candles_5m":
                stmt = (
                    insert(Candle5m)
                    .values(batch)
                    .on_conflict_do_nothing(index_elements=["symbol", "open_time"])
                )
            elif table_name == "agg_trades":
                stmt = (
                    insert(AggTradeModel)
                    .values(batch)
                    .on_conflict_do_nothing(index_elements=["symbol", "trade_id"])
                )
            else:
                continue

            await session.execute(stmt)
            logger.info("Flushed %d rows to %s", len(batch), table_name)
            batch.clear()

        await session.commit()

    @staticmethod
    def _kline_to_dict(k: Kline) -> dict:
        return {
            "symbol": k.symbol,
            "open_time": k.open_time,
            "open": k.open,
            "high": k.high,
            "low": k.low,
            "close": k.close,
            "volume": k.volume,
            "quote_volume": k.quote_volume,
            "trade_count": k.trade_count,
            "taker_buy_volume": k.taker_buy_volume,
            "closed": k.closed,
        }

    @staticmethod
    def _agg_trade_to_dict(t: AggTrade) -> dict:
        return {
            "symbol": t.symbol,
            "trade_id": t.trade_id,
            "price": t.price,
            "qty": t.qty,
            "quote_qty": t.quote_qty,
            "ts": t.ts,
            "is_buyer_maker": t.is_buyer_maker,
        }
