"""Run the real execution stack against a synthetic market and serve the control room.

Everything except the market is production code: the risk governor, execution
engine, order state machine, outcome monitor, reconciler, event bus/journal,
paper broker and ops server.  Prices come from a seeded random walk, so this is
a *simulation* — useful to see the control room populated, to rehearse the halt
procedure, and to inject faults, without exchange credentials or market data.

    python scripts/control_room_demo.py --port 8081 --token demo-token

Then open http://127.0.0.1:8081/ .  Ctrl-C to stop.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import tempfile
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from signal_bot.db.models import Base, Signal
from signal_bot.events import EventBus, SqlEventJournal
from signal_bot.execution.adapter import BookLevel, BookSnapshot, InstrumentSpec
from signal_bot.execution.engine import EXEC_MODE, ExecutionConfig, ExecutionEngine
from signal_bot.execution.order_store import OrderStore
from signal_bot.observability.positions import PositionView
from signal_bot.observability.server import OpsServer
from signal_bot.observability.status import StatusService
from signal_bot.provenance import git_info, package_version
from signal_bot.risk.governor import RiskGovernor
from signal_bot.tracking.outcome_monitor import OutcomeMonitor
from signal_bot.tracking.reconciliation import Reconciler
from signal_bot.venues.paper import PaperBroker
from signal_bot.venues.simulated import CostModel

INSTRUMENTS = {
    "BTCUSDT": InstrumentSpec("BTCUSD", 27, Decimal("0.5"), Decimal("1"), Decimal("0.001")),
    "ETHUSDT": InstrumentSpec("ETHUSD", 3136, Decimal("0.05"), Decimal("1"), Decimal("0.01")),
}


class RandomWalkMarket:
    """Seeded GBM-ish mid prices with a fixed spread and depth (venue prices)."""

    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.mid = {"BTCUSD": 65000.0, "ETHUSD": 3500.0}
        self.tick = {"BTCUSD": 0.5, "ETHUSD": 0.05}

    def step(self) -> None:
        for sym in self.mid:
            self.mid[sym] *= 1 + self.rng.gauss(0, 0.0015)

    async def book(self, symbol: str) -> BookSnapshot:
        tick = Decimal(str(self.tick[symbol]))
        mid = Decimal(str(round(self.mid[symbol] / self.tick[symbol]) * self.tick[symbol]))
        size = Decimal(str(self.rng.randint(60, 200)))
        return BookSnapshot(
            symbol,
            tuple(BookLevel(mid - tick * (i + 1), size) for i in range(5)),
            tuple(BookLevel(mid + tick * i, size) for i in range(5)),
            datetime.now(timezone.utc),
        )


async def main(port: int, token: str, seed: int, step_seconds: float) -> None:
    workdir = Path(tempfile.mkdtemp(prefix="signalbot-demo-"))
    db = create_async_engine(f"sqlite+aiosqlite:///{workdir / 'demo.db'}")
    async with db.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(db, class_=AsyncSession, expire_on_commit=False)

    market = RandomWalkMarket(seed)
    bus = EventBus(SqlEventJournal(factory))
    venue = PaperBroker(
        INSTRUMENTS,
        bus,
        market.book,
        session_factory=factory,
        costs=CostModel(funding_rate=Decimal("0.0001")),
        poll_interval=step_seconds,
        equity_snapshot_interval=step_seconds * 4,
    )
    governor = RiskGovernor(factory, exchange=venue, bus=bus, max_trades_per_day=1000)
    store = OrderStore(factory, bus)
    queue: asyncio.Queue = asyncio.Queue()
    engine = ExecutionEngine(
        venue, governor, bus, store, queue, session_factory=factory, mode=EXEC_MODE.SIM,
        config=ExecutionConfig(max_book_age_seconds=30),
    )
    monitor = OutcomeMonitor(bus, store, venue, session_factory=factory, governor=governor)
    reconciler = Reconciler(
        venue, store, governor=governor, outcome_monitor=monitor, engine=engine, bus=bus,
        interval=20,
    )
    engine.reconciler = reconciler
    view = PositionView(bus)
    replay = {s.name: await bus.replay(s) for s in (engine.subscribe(), monitor.subscribe())}
    status = StatusService(
        environment={
            "exec_mode": "simulation",
            "venue": "synthetic market",
            "version": package_version(),
            "commit": git_info()["commit"],
            "dirty": git_info()["dirty"],
            "strategy": {"name": "demo random entries", "version": f"seed {seed}"},
        },
        governor=governor,
        bus=bus,
        order_store=store,
        session_factory=factory,
        reconciler=reconciler,
        adapter=venue,
        position_view=view,
        replay_counts=replay,
    )
    ops = OpsServer(status, governor, factory, operator_token=token, port=port)
    await ops.start()
    governor.start()
    reconciler.start()
    tasks = [
        asyncio.create_task(engine.start()),
        asyncio.create_task(monitor.run()),
        asyncio.create_task(view.run()),
        asyncio.create_task(venue.run()),
    ]
    print(f"Control room (simulation): http://127.0.0.1:{port}/  token={token!r}  data={workdir}")
    rng = random.Random(seed + 1)
    try:
        step = 0
        while True:
            market.step()
            step += 1
            if step % 6 == 0:
                binance = rng.choice(list(INSTRUMENTS))
                venue_symbol = INSTRUMENTS[binance].symbol
                mid = Decimal(str(market.mid[venue_symbol]))
                side = rng.choice(["long", "short"])
                risk = mid * Decimal("0.004")
                stop = mid - risk if side == "long" else mid + risk
                tp = mid + 2 * risk if side == "long" else mid - 2 * risk
                sig = Signal(
                    id=uuid.uuid4(), symbol=binance, side=side, entry=mid, stop_loss=stop,
                    take_profit=tp, leverage=5, risk_pct=Decimal("0.5"),
                    account_size_assumed=Decimal("10000"), est_hold_minutes=30, status="pending",
                    created_at=datetime.now(timezone.utc),
                )
                async with factory() as session:
                    session.add(sig)
                    await session.commit()
                await queue.put(sig)
            await asyncio.sleep(step_seconds)
    finally:
        for t in tasks:
            t.cancel()
        await engine.stop()
        await reconciler.stop()
        await governor.stop()
        await ops.stop()
        await db.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--token", default="demo-token")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--step-seconds", type=float, default=0.5)
    args = parser.parse_args()
    try:
        asyncio.run(main(args.port, args.token, args.seed, args.step_seconds))
    except KeyboardInterrupt:
        pass
