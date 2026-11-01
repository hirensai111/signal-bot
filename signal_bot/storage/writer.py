"""Async queue consumer that batches inserts and fans closed candles out to subscribers.

Two correctness rules live here:

* **Independent subscriptions.**  Every feature engine gets its own queue
  (optionally filtered by interval).  Previously the HTF scanner and the
  microstructure engine competed for one queue and each discarded the other's
  candles, so valid candles were silently lost.
* **Persist, then publish, in order.**  A closed candle is forwarded only after
  the flush that wrote it (downstream engines query the DB for it), and only if
  it is newer than the last candle forwarded for the same symbol/interval.
  Duplicates (backfill/live overlap) and out-of-order candles are persisted but
  not forwarded, because the feature engines keep incremental state.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from datetime import datetime, timedelta

from sqlalchemy.dialects.postgresql import insert

from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import AggTrade as AggTradeModel
from signal_bot.db.models import Candle1m, Candle5m
from signal_bot.ingest.binance_ws import AggTrade, Kline
from signal_bot.observability import metrics

logger = logging.getLogger(__name__)

MAX_BUFFERED_TRADES = 200_000

INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}


def close_time(open_time: datetime, interval: str) -> datetime:
    return open_time + timedelta(seconds=INTERVAL_SECONDS.get(interval, 60))


def candle_close_time(kline: Kline) -> datetime:
    return close_time(kline.open_time, kline.interval)


class CandleFanout:
    """Broadcasts each closed candle to every subscriber whose interval filter matches."""

    def __init__(self) -> None:
        self._subs: dict[str, tuple[asyncio.Queue, frozenset[str] | None]] = {}
        self._last_forwarded: dict[tuple[str, str], datetime] = {}
        self.forwarded = 0
        self.dropped = 0

    def subscribe(
        self,
        name: str,
        intervals: Iterable[str] | None = None,
        queue: asyncio.Queue | None = None,
    ) -> asyncio.Queue:
        if name in self._subs:
            raise ValueError(f"candle subscriber {name!r} already registered")
        q = queue if queue is not None else asyncio.Queue()
        self._subs[name] = (q, frozenset(intervals) if intervals is not None else None)
        return q

    @property
    def subscriber_lag(self) -> dict[str, int]:
        return {name: q.qsize() for name, (q, _) in self._subs.items()}

    async def publish(self, kline: Kline) -> int:
        key = (kline.symbol, kline.interval)
        last = self._last_forwarded.get(key)
        if last is not None and kline.open_time <= last:
            self.dropped += 1
            reason = "duplicate" if kline.open_time == last else "out_of_order"
            metrics.CANDLES_DROPPED.labels(reason).inc()
            logger.debug("Not forwarding %s candle %s %s @ %s", reason, *key, kline.open_time)
            return 0
        self._last_forwarded[key] = kline.open_time
        metrics.MARKET_DATA_AGE.labels(kline.symbol, kline.interval).set(0)
        delivered = 0
        for q, intervals in self._subs.values():
            if intervals is None or kline.interval in intervals:
                await q.put(kline)
                delivered += 1
        self.forwarded += 1
        return delivered

    def last_candle_time(self, symbol: str, interval: str) -> datetime | None:
        return self._last_forwarded.get((symbol, interval))


class QueueWriter:
    """Consumes the ingest queue, batches DB inserts, and emits closed-candle events."""

    def __init__(
        self,
        source_queue: asyncio.Queue,
        session_factory=None,
        flush_interval: float = 1.0,
        batch_size: int = 100,
        fanout: CandleFanout | None = None,
    ) -> None:
        self.source_queue = source_queue
        self.session_factory = session_factory or AsyncSessionLocal
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self.fanout = fanout or CandleFanout()
        self._shutdown = asyncio.Event()
        self._last_flush = 0.0
        self._batches: dict[str, list[dict]] = {
            "candles_1m": [],
            "candles_5m": [],
            "agg_trades": [],
        }
        self._pending_emit: list[Kline] = []
        self.healthy = True
        self._retry_delay = 0.0
        self._retry_at = 0.0

    def subscribe(
        self,
        name: str,
        intervals: Iterable[str] | None = None,
        queue: asyncio.Queue | None = None,
    ) -> asyncio.Queue:
        """Register an independent closed-candle subscriber; returns its queue."""
        return self.fanout.subscribe(name, intervals, queue)

    def shutdown(self) -> None:
        logger.info("Writer shutdown requested.")
        self._shutdown.set()

    async def run(self) -> None:
        self._last_flush = asyncio.get_running_loop().time()
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
            self._pending_emit.append(item)
        elif isinstance(item, AggTrade):
            self._batches["agg_trades"].append(self._agg_trade_to_dict(item))

    async def _flush_if_needed(self, session) -> None:
        now = asyncio.get_running_loop().time()
        if now < self._retry_at:
            return  # backing off after a failed flush
        size_reached = any(len(batch) >= self.batch_size for batch in self._batches.values())
        # A closed candle forces a flush so downstream engines see it promptly.
        if size_reached or self._pending_emit or (now - self._last_flush >= self.flush_interval):
            await self._flush(session)
            self._last_flush = now

    async def _flush(self, session) -> None:
        try:
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
                else:
                    stmt = (
                        insert(AggTradeModel)
                        .values(batch)
                        .on_conflict_do_nothing(index_elements=["symbol", "trade_id"])
                    )
                await session.execute(stmt)
                logger.debug("Flushed %d rows to %s", len(batch), table_name)
            await session.commit()
        except Exception:
            # Keep batches for the next attempt; do not forward candles that aren't persisted.
            await session.rollback()
            if self.healthy:
                logger.exception("Candle/trade flush failed; will retry")
            self.healthy = False
            self._retry_delay = min(max(self._retry_delay * 2, 0.5), 30.0)
            self._retry_at = asyncio.get_running_loop().time() + self._retry_delay
            # Bound memory during long outages: trades are the bulk and least critical.
            trades = self._batches["agg_trades"]
            if len(trades) > MAX_BUFFERED_TRADES:
                dropped = len(trades) - MAX_BUFFERED_TRADES
                del trades[:dropped]
                metrics.CANDLES_DROPPED.labels("trade_buffer_overflow").inc(dropped)
            return
        self.healthy = True
        self._retry_delay = 0.0
        self._retry_at = 0.0
        for batch in self._batches.values():
            batch.clear()
        pending, self._pending_emit = self._pending_emit, []
        for kline in pending:
            await self.fanout.publish(kline)

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
