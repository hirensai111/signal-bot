"""Wires the real execution stack against the fault-injecting simulated venue.

Everything is production code except the venue and the database (SQLite file).
Consumers are driven deterministically with :meth:`Harness.drain` instead of
background loops, so scenario tests are reproducible.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.core.clock import ManualClock
from signal_bot.db.models import Base, Order, RiskState, Signal, SignalOutcome, TradeResult
from signal_bot.events import EventBus, InMemoryJournal, SqlEventJournal
from signal_bot.execution.adapter import InstrumentSpec
from signal_bot.execution.engine import EXEC_MODE, ExecutionConfig, ExecutionEngine
from signal_bot.execution.order_store import OrderStore
from signal_bot.risk.governor import RiskGovernor
from signal_bot.tracking.outcome_monitor import OutcomeMonitor
from signal_bot.tracking.reconciliation import Reconciler
from signal_bot.venues.simulated import CostModel, SimulatedVenue

BTC = InstrumentSpec(
    symbol="BTCUSD",
    product_id=27,
    tick_size=Decimal("0.5"),
    lot_size=Decimal("1"),
    contract_value=Decimal("0.001"),
)
ETH = InstrumentSpec(
    symbol="ETHUSD",
    product_id=3136,
    tick_size=Decimal("0.05"),
    lot_size=Decimal("1"),
    contract_value=Decimal("0.01"),
)


class FlakySessionFactory:
    """Session factory whose database can be 'unplugged' to simulate an outage."""

    def __init__(self, factory) -> None:
        self.factory = factory
        self.down = False

    def __call__(self):
        if self.down:
            return _DownSession()
        return self.factory()


class _DownSession:
    async def __aenter__(self):
        raise ConnectionError("database unavailable (injected)")

    async def __aexit__(self, *exc):
        return False


class RecordingAlerts:
    def __init__(self) -> None:
        self.messages: list[str] = []
        self.application = None
        self.chat_id = None

    async def send_text(self, text: str) -> None:
        self.messages.append(text)

    async def notify_outcome(self, signal, outcome) -> None:
        self.messages.append(f"outcome {signal.symbol} {outcome.exit_reason}")


class Harness:
    def __init__(self) -> None:
        self.tasks: list[asyncio.Task] = []

    @classmethod
    async def create(
        cls,
        tmp_path,
        *,
        sql_journal: bool = True,
        config: ExecutionConfig | None = None,
        costs: CostModel | None = None,
        governor_kwargs: dict | None = None,
        unknown_grace_seconds: float = 300.0,
        book: bool = True,
        db_name: str = "harness.db",
    ) -> Harness:
        h = cls()
        h.tmp_path = tmp_path
        h.db_engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / db_name}")
        async with h.db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        raw = async_sessionmaker(h.db_engine, class_=AsyncSession, expire_on_commit=False)
        h.raw_factory = raw
        h.factory = FlakySessionFactory(raw)
        h.clock = ManualClock(datetime.now(timezone.utc).replace(microsecond=0))
        h.journal = SqlEventJournal(h.factory) if sql_journal else InMemoryJournal()
        h.bus = EventBus(h.journal)
        h.venue = SimulatedVenue(
            {"BTCUSDT": BTC, "ETHUSDT": ETH},
            h.bus,
            clock=h.clock,
            costs=costs or CostModel(funding_rate=Decimal("0")),
        )
        h.alerts = RecordingAlerts()
        gk = dict(
            daily_loss_limit_r=-3.0,
            max_trades_per_day=10,
            max_concurrent_positions=3,
            max_consecutive_losses=5,
            min_balance_usdt=50.0,
        )
        gk.update(governor_kwargs or {})
        h.governor = RiskGovernor(
            session_factory=h.factory,
            exchange=h.venue,
            telegram=h.alerts,
            bus=h.bus,
            clock=h.clock,
            **gk,
        )
        h.store = OrderStore(h.factory, h.bus, clock=h.clock)
        h.signal_queue: asyncio.Queue = asyncio.Queue()
        h.config = config or ExecutionConfig(
            unknown_lookup_attempts=3,
            unknown_lookup_backoff=0.001,
            protective_confirm_timeout=0.01,
            protective_poll_interval=0.005,
            protective_retry_attempts=2,
            no_fill_timeout=30,
        )
        h.engine = ExecutionEngine(
            h.venue,
            h.governor,
            h.bus,
            h.store,
            h.signal_queue,
            session_factory=h.factory,
            config=h.config,
            alerts=h.alerts,
            clock=h.clock,
            mode=EXEC_MODE.SIM,
        )
        h.monitor = OutcomeMonitor(
            h.bus,
            h.store,
            adapter=h.venue,
            session_factory=h.factory,
            telegram_notifier=h.alerts,
            governor=h.governor,
            clock=h.clock,
            exit_fill_retries=1,
            exit_fill_retry_delay=0,
        )
        h.reconciler = Reconciler(
            h.venue,
            h.store,
            governor=h.governor,
            telegram=h.alerts,
            outcome_monitor=h.monitor,
            engine=h.engine,
            bus=h.bus,
            clock=h.clock,
            unknown_grace_seconds=unknown_grace_seconds,
        )
        h.engine.reconciler = h.reconciler
        h.exec_sub = h.engine.subscribe()
        h.outcome_sub = h.monitor.subscribe()
        if book:
            h.venue.set_book("BTCUSD", Decimal("64999.5"), Decimal("65000"))
            h.venue.set_book("ETHUSD", Decimal("3499.95"), Decimal("3500"))
        return h

    # ------------------------------------------------------------------
    async def make_signal(
        self,
        symbol: str = "BTCUSDT",
        side: str = "long",
        entry: str = "65000",
        stop: str = "64000",
        tp: str = "67000",
        created_at: datetime | None = None,
    ) -> Signal:
        sig = Signal(
            id=uuid.uuid4(),
            symbol=symbol,
            side=side,
            entry=Decimal(entry),
            stop_loss=Decimal(stop),
            take_profit=Decimal(tp),
            leverage=10,
            risk_pct=Decimal("1.0"),
            account_size_assumed=Decimal("10000"),
            est_hold_minutes=30,
            status="pending",
            created_at=created_at or self.clock.now(),
        )
        async with self.raw_factory() as session:
            session.add(sig)
            await session.commit()
        return sig

    async def submit(self, sig: Signal) -> Order | None:
        order = await self.engine.process_signal(sig)
        await self.drain()
        return order

    async def drain(self, rounds: int = 20) -> None:
        """Deliver every queued event to the execution engine and outcome monitor."""
        for _ in range(rounds):
            progressed = False
            for sub, handler in (
                (self.exec_sub, self.engine.handle_event),
                (self.outcome_sub, self.monitor.handle_event),
            ):
                while not sub.queue.empty():
                    event = sub.queue.get_nowait()
                    await handler(event)
                    await sub.ack(event)
                    progressed = True
            pending = list(self.engine._background)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
                progressed = True
            if not progressed:
                return

    # ------------------------------------------------------------------
    async def order_for(self, sig: Signal) -> Order | None:
        return await self.store.get_for_signal(sig.id)

    async def signal(self, sig: Signal) -> Signal:
        async with self.raw_factory() as session:
            return await session.get(Signal, sig.id)

    async def outcomes(self) -> list[SignalOutcome]:
        async with self.raw_factory() as session:
            return list((await session.execute(select(SignalOutcome))).scalars())

    async def trade_results(self) -> list[TradeResult]:
        async with self.raw_factory() as session:
            return list((await session.execute(select(TradeResult))).scalars())

    async def risk_state(self) -> RiskState:
        async with self.raw_factory() as session:
            return await session.get(RiskState, 1)

    async def close(self) -> None:
        await self.engine.stop()
        await self.reconciler.stop()
        await self.db_engine.dispose()
