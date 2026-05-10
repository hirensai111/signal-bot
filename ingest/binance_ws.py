"""Binance Futures USDT-M data ingestion via REST backfill + multiplexed websocket."""

import asyncio
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from binance import AsyncClient, BinanceSocketManager

from signal_bot.config import settings

logger = logging.getLogger(__name__)

# Placeholder for prompt 3 — actual storage layer will replace this
# from signal_bot.storage import persist_klines, persist_agg_trades


@dataclass(frozen=True, slots=True)
class Kline:
    symbol: str
    interval: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    taker_buy_volume: Decimal
    closed: bool


@dataclass(frozen=True, slots=True)
class AggTrade:
    symbol: str
    trade_id: int
    price: Decimal
    qty: Decimal
    quote_qty: Decimal
    ts: datetime
    is_buyer_maker: bool


class DataIngestor:
    """Backfills historical candles and subscribes to live futures streams."""

    BACKFILL_INTERVALS = ("1m", "5m", "1h", "4h")
    LIVE_STREAMS = ("kline_1m", "kline_5m", "aggTrade")

    def __init__(self, queue: asyncio.Queue, message_limit: Optional[int] = None) -> None:
        self.queue = queue
        self.message_limit = message_limit
        self._counts: dict[str, int] = {}
        self._shutdown = asyncio.Event()
        self._total_received = 0

    @property
    def counts(self) -> dict[str, int]:
        return self._counts.copy()

    def shutdown(self) -> None:
        logger.info("Shutdown requested.")
        self._shutdown.set()

    async def run(self) -> None:
        client = await AsyncClient.create(
            api_key=settings.binance_api_key or None,
            api_secret=settings.binance_api_secret or None,
            testnet=settings.binance_testnet,
        )
        try:
            logger.info("Starting backfill for %d symbols...", len(settings.symbols))
            await self._backfill(client)
            logger.info("Backfill complete. Starting websocket...")
            await self._websocket_loop(client)
        finally:
            await client.close_connection()
            logger.info("Client connection closed.")

    async def _backfill(self, client: AsyncClient) -> None:
        for symbol in settings.symbols:
            for interval in self.BACKFILL_INTERVALS:
                logger.debug("Backfilling %s %s...", symbol, interval)
                try:
                    klines = await client.futures_klines(
                        symbol=symbol, interval=interval, limit=500
                    )
                except Exception:
                    logger.exception("Failed to backfill %s %s", symbol, interval)
                    continue

                for raw in klines:
                    kline = self._rest_kline_to_dataclass(symbol, interval, raw)
                    await self.queue.put(kline)

                logger.info("Backfilled %d %s candles for %s", len(klines), interval, symbol)

    async def _websocket_loop(self, client: AsyncClient) -> None:
        bsm = BinanceSocketManager(client)
        streams = [
            f"{sym.lower()}@{stream}"
            for sym in settings.symbols
            for stream in self.LIVE_STREAMS
        ]

        backoff = 1.0
        while not self._shutdown.is_set():
            try:
                async with bsm.futures_multiplex_socket(streams) as ws:
                    logger.info("WebSocket connected (%d streams).", len(streams))
                    backoff = 1.0
                    while not self._shutdown.is_set():
                        msg = await ws.recv()
                        await self._handle_message(msg)
                        if self.message_limit and self._total_received >= self.message_limit:
                            self._shutdown.set()
                            break
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("WebSocket error. Reconnecting in %.0fs...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _handle_message(self, msg: dict) -> None:
        stream = msg.get("stream", "unknown")
        data = msg.get("data", {})
        self._counts[stream] = self._counts.get(stream, 0) + 1
        self._total_received += 1

        if "kline" in stream:
            item = self._ws_kline_to_dataclass(stream, data)
        elif "aggTrade" in stream:
            item = self._ws_agg_trade_to_dataclass(stream, data)
        else:
            logger.warning("Unknown stream: %s", stream)
            return

        await self.queue.put(item)

    @staticmethod
    def _rest_kline_to_dataclass(symbol: str, interval: str, raw: list) -> Kline:
        # raw: [open_time, open, high, low, close, volume, close_time,
        #       quote_volume, trades, taker_buy_base, taker_buy_quote, ignore]
        return Kline(
            symbol=symbol,
            interval=interval,
            open_time=datetime.fromtimestamp(raw[0] / 1000.0, tz=timezone.utc),
            open=Decimal(raw[1]),
            high=Decimal(raw[2]),
            low=Decimal(raw[3]),
            close=Decimal(raw[4]),
            volume=Decimal(raw[5]),
            quote_volume=Decimal(raw[7]),
            trade_count=int(raw[8]),
            taker_buy_volume=Decimal(raw[9]),
            closed=True,
        )

    @staticmethod
    def _ws_kline_to_dataclass(stream: str, data: dict) -> Kline:
        k = data["k"]
        return Kline(
            symbol=k["s"],
            interval=k["i"],
            open_time=datetime.fromtimestamp(k["t"] / 1000.0, tz=timezone.utc),
            open=Decimal(k["o"]),
            high=Decimal(k["h"]),
            low=Decimal(k["l"]),
            close=Decimal(k["c"]),
            volume=Decimal(k["v"]),
            quote_volume=Decimal(k["q"]),
            trade_count=int(k["n"]),
            taker_buy_volume=Decimal(k["V"]),
            closed=k["x"],
        )

    @staticmethod
    def _ws_agg_trade_to_dataclass(stream: str, data: dict) -> AggTrade:
        price = Decimal(data["p"])
        qty = Decimal(data["q"])
        return AggTrade(
            symbol=data["s"],
            trade_id=int(data["a"]),
            price=price,
            qty=qty,
            quote_qty=(price * qty),
            ts=datetime.fromtimestamp(data["T"] / 1000.0, tz=timezone.utc),
            is_buyer_maker=data["m"],
        )


async def _stats_printer(ingestor: DataIngestor, interval: int = 30) -> None:
    """Periodically print per-stream message counts."""
    while True:
        await asyncio.sleep(interval)
        counts = ingestor.counts
        if counts:
            total = sum(counts.values())
            logger.info("[STATS] total=%d %s", total, counts)
        else:
            logger.info("[STATS] No messages yet.")


async def _queue_drain(queue: asyncio.Queue) -> None:
    """Prevent unbounded queue growth until a consumer is wired in prompt 3."""
    while True:
        try:
            await queue.get()
        except Exception:
            logger.exception("Queue drain error")


async def main() -> None:
    queue: asyncio.Queue = asyncio.Queue()
    ingestor = DataIngestor(queue)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, ingestor.shutdown)
        except (ValueError, OSError, NotImplementedError):
            # Windows lacks SIGTERM support in asyncio.add_signal_handler
            pass

    await asyncio.gather(
        ingestor.run(),
        _stats_printer(ingestor),
        _queue_drain(queue),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    asyncio.run(main())
