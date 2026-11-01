"""Startup: schema, instruments, and the full event-driven pipeline.

Wiring summary (see docs/architecture.md):

    Binance WS ─► ingest queue ─► QueueWriter ─(persist, then fan out)─► HTF scanner   {1h,4h}
                                                                    ├─► microstructure {1m,5m}
                                                                    └─► signal pipeline{1m,5m}
    signal pipeline ─► RiskSizer ─► signal queue ─► ExecutionEngine ─► ExchangeAdapter
                                                        ▲   (RiskGovernor gates every submit)
    venue stream ─► EventBus (journal first) ─┬─► execution   (own queue)
                                              ├─► outcomes    (own queue)
                                              └─► position view (own queue, DB-independent)
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import tomllib
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal, async_engine
from signal_bot.db.models import Base, Instrument
from signal_bot.delivery.telegram_bot import TelegramNotifier
from signal_bot.delta.client import DeltaClient, DeltaWebSocket
from signal_bot.events import Event, EventBus, EventType, SqlEventJournal
from signal_bot.execution.delta_adapter import DeltaExchangeAdapter, DeltaStreamBridge
from signal_bot.execution.engine import EXEC_MODE, ExecutionConfig, ExecutionEngine
from signal_bot.execution.order_store import OrderStore
from signal_bot.features.htf_levels import HtfLevelScanner
from signal_bot.features.microstructure import MicrostructureEngine
from signal_bot.ingest.binance_ws import DataIngestor, Kline
from signal_bot.observability import metrics
from signal_bot.observability.logging import bind, configure_logging
from signal_bot.observability.positions import PositionView
from signal_bot.observability.server import OpsServer
from signal_bot.observability.status import StatusService
from signal_bot.provenance import git_info, package_version
from signal_bot.risk.governor import RiskGovernor
from signal_bot.signals.risk_sizer import RiskSizer
from signal_bot.signals.sweep_detector import SweepDetector
from signal_bot.storage.writer import QueueWriter, candle_close_time
from signal_bot.tracking.outcome_monitor import OutcomeMonitor
from signal_bot.tracking.reconciliation import Reconciler
from signal_bot.venues.paper import PaperBroker
from signal_bot.venues.simulated import CostModel

logger = logging.getLogger(__name__)

_INSTRUMENT_DEFAULTS = {
    "BTCUSDT": {
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "tick_size": "0.10",
        "min_qty": "0.001",
    },
    "ETHUSDT": {
        "base_asset": "ETH",
        "quote_asset": "USDT",
        "tick_size": "0.01",
        "min_qty": "0.001",
    },
    "SOLUSDT": {
        "base_asset": "SOL",
        "quote_asset": "USDT",
        "tick_size": "0.001",
        "min_qty": "0.01",
    },
    "LINKUSDT": {
        "base_asset": "LINK",
        "quote_asset": "USDT",
        "tick_size": "0.001",
        "min_qty": "0.01",
    },
    "AVAXUSDT": {
        "base_asset": "AVAX",
        "quote_asset": "USDT",
        "tick_size": "0.001",
        "min_qty": "0.01",
    },
    "ADAUSDT": {
        "base_asset": "ADA",
        "quote_asset": "USDT",
        "tick_size": "0.0001",
        "min_qty": "1.0",
    },
    "XRPUSDT": {
        "base_asset": "XRP",
        "quote_asset": "USDT",
        "tick_size": "0.0001",
        "min_qty": "1.0",
    },
    "BNBUSDT": {
        "base_asset": "BNB",
        "quote_asset": "USDT",
        "tick_size": "0.01",
        "min_qty": "0.001",
    },
}


async def init_schema() -> None:
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Schema initialised.")


async def seed_instruments() -> None:
    async with AsyncSessionLocal() as session:
        for symbol in settings.symbols:
            if await session.scalar(select(Instrument.symbol).where(Instrument.symbol == symbol)):
                continue
            d = _INSTRUMENT_DEFAULTS.get(
                symbol,
                {
                    "base_asset": symbol.replace("USDT", ""),
                    "quote_asset": "USDT",
                    "tick_size": "0.01",
                    "min_qty": "0.001",
                },
            )
            session.add(Instrument(symbol=symbol, active=True, **d))
        await session.commit()


def load_strategy_release() -> dict:
    path = Path(settings.strategy_config)
    if not path.exists():
        return {"name": "liquidity_sweep", "version": "unversioned"}
    with path.open("rb") as fh:
        doc = tomllib.load(fh)
    return {"name": doc.get("name"), "version": doc.get("version"), "config_path": str(path)}


# ---------------------------------------------------------------------------
# pipeline stages
# ---------------------------------------------------------------------------


def candle_is_fresh(candle: Kline, now: datetime, max_lag_seconds: float) -> bool:
    """Late or backfilled candles may update features but must never generate trades."""
    return (now - candle_close_time(candle)).total_seconds() <= max_lag_seconds


async def signal_pipeline(
    detector: SweepDetector,
    sizer: RiskSizer,
    candle_queue: asyncio.Queue,
    signal_queue: asyncio.Queue,
    bus: EventBus,
    telegram: TelegramNotifier,
    shutdown: asyncio.Event,
    strategy: dict,
) -> None:
    logger.info("Signal pipeline started.")
    while not shutdown.is_set():
        try:
            candle: Kline = await asyncio.wait_for(candle_queue.get(), timeout=1.0)
        except TimeoutError:
            continue
        if candle.interval not in ("1m", "5m"):
            continue
        if not candle_is_fresh(candle, datetime.now(timezone.utc), settings.max_candle_lag_seconds):
            metrics.CANDLES_DROPPED.labels("stale_for_signals").inc()
            continue
        try:
            candidate = await detector._evaluate(candle)
            if candidate is None:
                continue
            sig = await sizer.size_and_persist(
                candidate, candle.interval, created_at=candle.open_time
            )
        except Exception:
            logger.exception("Signal evaluation failed for %s %s", candle.symbol, candle.interval)
            continue
        if sig is None:
            continue
        with bind(correlation_id=str(sig.id), signal_id=str(sig.id)):
            await bus.publish(
                Event.create(
                    EventType.SIGNAL_GENERATED,
                    {
                        "signal_id": str(sig.id),
                        "symbol": sig.symbol,
                        "side": sig.side,
                        "entry": sig.entry,
                        "stop": sig.stop_loss,
                        "take_profit": sig.take_profit,
                        "strategy": strategy,
                    },
                    source="signal_pipeline",
                    correlation_id=str(sig.id),
                )
            )
            await signal_queue.put(sig)
            logger.info("Signal queued: %s %s entry=%s", sig.symbol, sig.side, sig.entry)
        try:
            await telegram.notify_signal(sig)
        except Exception:
            logger.exception("Telegram signal notification failed")


async def housekeeping(bus: EventBus, writer: QueueWriter, shutdown: asyncio.Event) -> None:
    """Retry buffered journal writes and refresh lag/age gauges."""
    while not shutdown.is_set():
        await asyncio.sleep(5)
        if bus.pending_journal_writes:
            try:
                written = await bus.flush_pending()
                logger.info("Flushed %d buffered journal events", written)
            except Exception:
                logger.warning(
                    "Journal still unavailable (%d buffered)", bus.pending_journal_writes
                )
        for (symbol, interval), age in writer.fanout.ages(datetime.now(timezone.utc)).items():
            metrics.MARKET_DATA_AGE.labels(symbol, interval).set(age)
        for name, sub in bus.subscriptions.items():
            metrics.EVENT_QUEUE_LAG.labels(name).set(sub.lag)


async def keep_alive(coro_fn, shutdown: asyncio.Event) -> None:
    await coro_fn()
    while not shutdown.is_set():
        await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# orchestrator
# ---------------------------------------------------------------------------


async def main() -> None:
    code = git_info()
    configure_logging(
        settings.log_level,
        settings.log_format,
        static={
            "env": settings.exec_mode,
            "version": package_version(),
            "commit": code["commit"][:12],
        },
    )
    problems = settings.validate_for_startup()
    if problems:
        for p in problems:
            logger.critical("Configuration error: %s", p)
        raise SystemExit(2)

    live = settings.exec_mode == "live"
    strategy = load_strategy_release()
    logger.info(
        "Bot starting | mode=%s delta=%s symbols=%s strategy=%s@%s settings=%s",
        settings.exec_mode,
        settings.delta_mode,
        settings.symbols,
        strategy.get("name"),
        strategy.get("version"),
        settings.fingerprint(),
    )
    await init_schema()
    await seed_instruments()

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except (NotImplementedError, ValueError):
            pass

    bus = EventBus(SqlEventJournal(AsyncSessionLocal))
    telegram = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
    delta_client = DeltaClient(
        settings.delta_api_key, settings.delta_api_secret, settings.delta_mode
    )

    async with delta_client:  # loads Delta products (public) for symbol mapping
        delta_adapter = DeltaExchangeAdapter(delta_client)
        if live:
            adapter = delta_adapter
            mode = EXEC_MODE.LIVE
        else:
            adapter = PaperBroker(
                {s: delta_adapter.instrument(s) for s in settings.symbols},
                bus,
                book_source=delta_adapter.order_book,
                session_factory=AsyncSessionLocal,
                costs=CostModel(
                    taker_fee=Decimal(str(settings.paper_taker_fee)),
                    maker_fee=Decimal(str(settings.paper_maker_fee)),
                    latency_seconds=settings.paper_latency_ms / 1000,
                    funding_rate=Decimal(str(settings.paper_funding_rate)),
                ),
                initial_balance=Decimal(
                    str(settings.paper_initial_equity or settings.account_size_usdt)
                ),
                state_path=settings.paper_state_path,
            )
            adapter.restore()
            mode = EXEC_MODE.PAPER

        governor = RiskGovernor(AsyncSessionLocal, exchange=adapter, telegram=telegram, bus=bus)
        store = OrderStore(AsyncSessionLocal, bus)
        signal_queue: asyncio.Queue = asyncio.Queue()
        engine = ExecutionEngine(
            adapter,
            governor,
            bus,
            store,
            signal_queue,
            config=ExecutionConfig(
                order_type=settings.order_type,
                no_fill_timeout=settings.no_fill_timeout_seconds,
                max_book_age_seconds=settings.max_book_age_seconds,
                require_stream_on_start=live,
            ),
            alerts=telegram,
            mode=mode,
        )
        monitor = OutcomeMonitor(bus, store, adapter, telegram_notifier=telegram, governor=governor)
        reconciler = Reconciler(
            adapter,
            store,
            governor=governor,
            telegram=telegram,
            outcome_monitor=monitor,
            engine=engine,
            bus=bus,
            interval=settings.reconcile_interval_seconds,
            unknown_grace_seconds=settings.unknown_order_grace_seconds,
            block_on_orphans=settings.block_on_orphan_positions,
        )
        engine.reconciler = reconciler
        position_view = PositionView(bus)

        # Subscribe every durable consumer, then replay history before any stream starts.
        replay = {}
        for sub in (engine.subscribe(), monitor.subscribe()):
            replay[sub.name] = await bus.replay(sub)

        ingest_queue: asyncio.Queue = asyncio.Queue()
        ingestor = DataIngestor(queue=ingest_queue)
        writer = QueueWriter(source_queue=ingest_queue)
        htf_scanner = HtfLevelScanner(candle_queue=writer.subscribe("htf_levels", {"1h", "4h"}))
        micro_engine = MicrostructureEngine(
            candle_queue=writer.subscribe("microstructure", {"1m", "5m"})
        )
        sweep_queue = writer.subscribe("signals", {"1m", "5m"})
        detector = SweepDetector(candle_queue=sweep_queue)
        sizer = RiskSizer()

        stream = None
        if live:
            bridge = DeltaStreamBridge(bus)
            stream = DeltaWebSocket(
                delta_client,
                bridge.handle_raw,
                on_connect=bridge.on_connect,
                on_disconnect=bridge.on_disconnect,
            )

        status = StatusService(
            environment={
                "exec_mode": settings.exec_mode,
                "venue": adapter.venue,
                "delta_mode": settings.delta_mode,
                "version": package_version(),
                "commit": code["commit"],
                "dirty": code["dirty"],
                "strategy": strategy,
                "settings_fingerprint": settings.fingerprint(),
            },
            governor=governor,
            bus=bus,
            order_store=store,
            session_factory=AsyncSessionLocal,
            reconciler=reconciler,
            adapter=adapter,
            position_view=position_view,
            fanout=writer.fanout,
            writer=writer,
            engine=engine,
            stream=stream,
            ingestor=ingestor,
            replay_counts=replay,
            max_candle_lag_seconds=settings.max_candle_lag_seconds,
        )
        ops = OpsServer(
            status,
            governor,
            AsyncSessionLocal,
            operator_token=settings.operator_token,
            host=settings.ops_host,
            port=settings.ops_port,
        )
        await ops.start()

        governor.start()
        reconciler.start()

        tasks = [
            asyncio.create_task(ingestor.run(), name="ingestor"),
            asyncio.create_task(writer.run(), name="writer"),
            asyncio.create_task(htf_scanner.run(), name="htf_scanner"),
            asyncio.create_task(micro_engine.run(), name="micro_engine"),
            asyncio.create_task(
                signal_pipeline(
                    detector, sizer, sweep_queue, signal_queue, bus, telegram, shutdown, strategy
                ),
                name="signal_pipeline",
            ),
            asyncio.create_task(keep_alive(engine.start, shutdown), name="execution_engine"),
            asyncio.create_task(monitor.run(), name="outcome_monitor"),
            asyncio.create_task(position_view.run(), name="position_view"),
            asyncio.create_task(keep_alive(telegram.start, shutdown), name="telegram"),
            asyncio.create_task(housekeeping(bus, writer, shutdown), name="housekeeping"),
        ]
        if isinstance(adapter, PaperBroker):
            tasks.append(asyncio.create_task(adapter.run(), name="paper_broker"))
        if stream is not None:
            stream.start()

        waiter = asyncio.create_task(shutdown.wait(), name="shutdown-wait")
        try:
            done, _ = await asyncio.wait([waiter, *tasks], return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t is not waiter and t.exception() is not None:
                    logger.critical("Task %s crashed: %r — halting", t.get_name(), t.exception())
                    governor.set_breaker("task_crashed", t.get_name())
        finally:
            logger.info("Shutting down pipeline...")
            shutdown.set()
            for component in (ingestor, writer, htf_scanner, micro_engine, detector, monitor):
                component.shutdown()
            position_view.stop()
            if isinstance(adapter, PaperBroker):
                adapter.stop()
                await adapter.checkpoint()
            await engine.stop()
            await reconciler.stop()
            await governor.stop()
            if stream is not None:
                await stream.stop()
            await telegram.stop()
            await ops.stop()
            for t in [waiter, *tasks]:
                if not t.done():
                    t.cancel()
            await asyncio.gather(waiter, *tasks, return_exceptions=True)
            if bus.pending_journal_writes:
                try:
                    await bus.flush_pending()
                except Exception:
                    logger.error(
                        "%d journal events could not be flushed", bus.pending_journal_writes
                    )
            await async_engine.dispose()
            logger.info("Shutdown complete.")


def cli() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    cli()
