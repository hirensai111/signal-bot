"""Startup script: initialise schema, seed instruments, and run the full pipeline."""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

from sqlalchemy import func, select

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal, async_engine
from signal_bot.db.models import Base, Instrument, Signal
from signal_bot.delta.client import DeltaClient, DeltaWebSocket
from signal_bot.delivery.telegram_bot import TelegramNotifier
from signal_bot.execution.engine import EXEC_MODE, ExecutionEngine
from signal_bot.execution.governor import BaseGovernor
from signal_bot.features.htf_levels import HtfLevelScanner
from signal_bot.features.microstructure import MicrostructureEngine
from signal_bot.ingest.binance_ws import DataIngestor
from signal_bot.signals.risk_sizer import RiskSizer
from signal_bot.signals.sweep_detector import SweepDetector
from signal_bot.storage.writer import QueueWriter
from signal_bot.tracking.outcome_monitor import OutcomeMonitor
from signal_bot.tracking.reconciliation import Reconciler

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Known instrument defaults keyed by symbol.
_INSTRUMENT_DEFAULTS = {
    "BTCUSDT": {"base_asset": "BTC", "quote_asset": "USDT", "tick_size": "0.10", "min_qty": "0.001"},
    "ETHUSDT": {"base_asset": "ETH", "quote_asset": "USDT", "tick_size": "0.01", "min_qty": "0.001"},
    "SOLUSDT": {"base_asset": "SOL", "quote_asset": "USDT", "tick_size": "0.001", "min_qty": "0.01"},
    "LINKUSDT": {"base_asset": "LINK", "quote_asset": "USDT", "tick_size": "0.001", "min_qty": "0.01"},
    "AVAXUSDT": {"base_asset": "AVAX", "quote_asset": "USDT", "tick_size": "0.001", "min_qty": "0.01"},
    "ADAUSDT": {"base_asset": "ADA", "quote_asset": "USDT", "tick_size": "0.0001", "min_qty": "1.0"},
    "XRPUSDT": {"base_asset": "XRP", "quote_asset": "USDT", "tick_size": "0.0001", "min_qty": "1.0"},
    "BNBUSDT": {"base_asset": "BNB", "quote_asset": "USDT", "tick_size": "0.01", "min_qty": "0.001"},
}


async def init_schema() -> None:
    """Create all tables if they do not already exist."""
    logger.info("Creating tables (if missing)…")
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Schema initialised.")


async def seed_instruments() -> None:
    """Seed the instruments table from settings.symbols."""
    async with AsyncSessionLocal() as session:
        for symbol in settings.symbols:
            existing = await session.scalar(
                select(Instrument.symbol).where(Instrument.symbol == symbol)
            )
            if existing:
                logger.info("Instrument %s already present — skipping.", symbol)
                continue

            defaults = _INSTRUMENT_DEFAULTS.get(symbol, {
                "base_asset": symbol.replace("USDT", ""),
                "quote_asset": "USDT",
                "tick_size": "0.01",
                "min_qty": "0.001",
            })
            instrument = Instrument(
                symbol=symbol,
                base_asset=defaults["base_asset"],
                quote_asset=defaults["quote_asset"],
                tick_size=defaults["tick_size"],
                min_qty=defaults["min_qty"],
                active=True,
            )
            session.add(instrument)
            logger.info("Seeding instrument %s.", symbol)

        await session.commit()
    logger.info("Instrument seeding complete.")


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------

async def _signal_pipeline(
    detector: SweepDetector,
    sizer: RiskSizer,
    signal_queue: asyncio.Queue,
    candle_queue: asyncio.Queue,
    shutdown_event: asyncio.Event,
    counter: list,
) -> None:
    """Bridge: consumes closed candles, runs sweep detection, sizes signals, queues them."""
    logger.info("Signal pipeline started.")
    while not shutdown_event.is_set():
        try:
            candle = await asyncio.wait_for(candle_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        if candle.interval not in ("1m", "5m"):
            continue

        candidate = await detector._evaluate(candle)
        if candidate is None:
            continue

        signal = await sizer.size_and_persist(
            candidate, candle.interval, created_at=candle.open_time
        )
        if signal is not None:
            await signal_queue.put(signal)
            counter[0] += 1
            logger.info(
                "Signal queued: %s %s entry=%s stop=%s tp=%s leverage=%dx",
                signal.symbol,
                signal.side,
                signal.entry,
                signal.stop_loss,
                signal.take_profit,
                signal.leverage,
            )


async def _run_engine(
    engine: ExecutionEngine,
    reconciler: Reconciler,
    shutdown_event: asyncio.Event,
) -> None:
    """Start execution engine after first reconciliation, keep alive until shutdown."""
    logger.info("Waiting for first reconciliation before starting execution engine...")
    await reconciler.wait_for_first_run()
    logger.info("First reconciliation complete; starting execution engine.")
    await engine.start()
    while not shutdown_event.is_set():
        await asyncio.sleep(1)


async def _run_telegram(
    telegram: TelegramNotifier,
    shutdown_event: asyncio.Event,
) -> None:
    """Start Telegram polling, keep alive until shutdown."""
    await telegram.start()
    while not shutdown_event.is_set():
        await asyncio.sleep(1)
    await telegram.stop()


async def _run_delta_ws(
    ws: DeltaWebSocket,
    shutdown_event: asyncio.Event,
) -> None:
    """Start Delta private WebSocket, keep alive until shutdown."""
    ws.start()
    while not shutdown_event.is_set():
        await asyncio.sleep(1)
    await ws.stop()


async def _stats_logger(
    ingestor: DataIngestor,
    queues: dict[str, asyncio.Queue],
    counter: list,
    shutdown_event: asyncio.Event,
) -> None:
    """Print pipeline stats every 30 seconds."""
    while not shutdown_event.is_set():
        await asyncio.sleep(30)
        counts = ingestor.counts
        async with AsyncSessionLocal() as session:
            open_positions = await session.scalar(
                select(func.count()).where(Signal.status == "triggered")
            )
        logger.info(
            "stats | ingest=%s | queues=(ingest=%d, candles=%d, signals=%d, delta=%d) "
            "| signals_fired=%d | open_positions=%d",
            counts,
            queues["ingest"].qsize(),
            queues["candles"].qsize(),
            queues["signals"].qsize(),
            queues["delta"].qsize(),
            counter[0],
            open_positions or 0,
        )


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main() -> None:
    logger.info(
        "Bot starting | mode=%s | delta=%s | symbols=%s | risk_pct=%s%%",
        settings.exec_mode,
        settings.delta_mode,
        settings.symbols,
        settings.risk_pct_per_trade,
    )

    await init_schema()
    await seed_instruments()

    # Shared event for graceful shutdown
    shutdown_event = asyncio.Event()

    def _request_shutdown() -> None:
        if not shutdown_event.is_set():
            logger.info("Shutdown signal received.")
            shutdown_event.set()

    # Register signal handlers (SIGINT works on Windows; SIGTERM may not)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except (NotImplementedError, ValueError):
            pass

    # Shared queues
    ingest_queue: asyncio.Queue = asyncio.Queue()
    signal_queue: asyncio.Queue = asyncio.Queue()
    delta_events: asyncio.Queue = asyncio.Queue()

    # Delta client (enter async context so WS feed can run)
    delta_client = DeltaClient(
        api_key=settings.delta_api_key,
        api_secret=settings.delta_api_secret,
        mode=settings.delta_mode,
    )

    # Telegram notifier
    telegram = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
    )

    # Risk governor
    governor = BaseGovernor()

    # Reconciler (started immediately so it can run before engine)
    reconciler = Reconciler(
        delta_client=delta_client,
        session_factory=AsyncSessionLocal,
        telegram=telegram,
        governor=governor,
    )

    # Delta WebSocket feeds private order/position events into delta_events
    delta_ws = DeltaWebSocket(delta_client, delta_events)

    # Ingestor: Binance klines + aggTrades
    ingestor = DataIngestor(queue=ingest_queue)

    # Writer: batches DB inserts, emits closed-candle events
    writer = QueueWriter(source_queue=ingest_queue)

    # Feature engines subscribe to closed candles
    htf_scanner = HtfLevelScanner(candle_queue=writer.closed_candle_queue)
    micro_engine = MicrostructureEngine(candle_queue=writer.closed_candle_queue)

    # Sweep detector gets its own subscriber queue so we can bridge to sizer
    sweep_candle_queue: asyncio.Queue = asyncio.Queue()
    writer.subscribe(sweep_candle_queue)
    sweep_detector = SweepDetector(candle_queue=sweep_candle_queue)

    # Risk sizer (one-shot per candidate)
    risk_sizer = RiskSizer()

    # Execution engine
    exec_mode = EXEC_MODE.LIVE if settings.exec_mode == "live" else EXEC_MODE.DRY_RUN
    execution_engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=signal_queue,
        delta_events=delta_events,
        governor=governor,
        exec_mode=exec_mode,
        reconciler=reconciler,
    )

    # Outcome monitor
    outcome_monitor = OutcomeMonitor(
        delta_events=delta_events,
        telegram_notifier=telegram,
        governor=governor,
        delta_client=delta_client,
    )

    # Counter for signals fired since startup
    signals_fired: list = [0]

    queues = {
        "ingest": ingest_queue,
        "candles": writer.closed_candle_queue,
        "signals": signal_queue,
        "delta": delta_events,
    }

    async with delta_client:
        # Start reconciler immediately; engine will wait for first run
        reconciler.start()

        # Start all concurrent tasks
        tasks = [
            asyncio.create_task(ingestor.run(), name="ingestor"),
            asyncio.create_task(writer.run(), name="writer"),
            asyncio.create_task(htf_scanner.run(), name="htf_scanner"),
            asyncio.create_task(micro_engine.run(), name="micro_engine"),
            asyncio.create_task(
                _signal_pipeline(
                    sweep_detector, risk_sizer, signal_queue,
                    sweep_candle_queue, shutdown_event, signals_fired,
                ),
                name="signal_pipeline",
            ),
            asyncio.create_task(
                _run_engine(execution_engine, reconciler, shutdown_event),
                name="execution_engine",
            ),
            asyncio.create_task(outcome_monitor.run(), name="outcome_monitor"),
            asyncio.create_task(
                _run_delta_ws(delta_ws, shutdown_event),
                name="delta_ws",
            ),
            asyncio.create_task(
                _run_telegram(telegram, shutdown_event),
                name="telegram",
            ),
            asyncio.create_task(
                _stats_logger(ingestor, queues, signals_fired, shutdown_event),
                name="stats_logger",
            ),
        ]

        try:
            # return_exceptions=False => one crash brings down the whole bot
            await asyncio.gather(*tasks, return_exceptions=False)
        except asyncio.CancelledError:
            logger.info("Main gather cancelled.")
        except Exception:
            logger.exception("Fatal error in pipeline — shutting down.")
        finally:
            # Graceful shutdown sequence
            logger.info("Shutting down pipeline...")
            shutdown_event.set()

            ingestor.shutdown()
            writer.shutdown()
            htf_scanner.shutdown()
            micro_engine.shutdown()
            sweep_detector.shutdown()
            outcome_monitor.shutdown()

            await execution_engine.stop()
            await reconciler.stop()
            await delta_ws.stop()
            await telegram.stop()

            # Cancel any lingering tasks
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            # Flush pending DB writes
            logger.info("Closing database engine.")
            await async_engine.dispose()
            logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
