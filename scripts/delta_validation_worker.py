"""Read-only-exchange bot process used by guarded Delta integration scenarios.

The guardian owns all exchange mutations and cleanup. This process runs the real
engine, reconciler, stream bridge, order store, event journal and outcome monitor
against an isolated SQLite database. It cannot place or cancel exchange orders.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from decimal import Decimal
from pathlib import Path


async def seed_database(db_path, plan, signal_id, contract_value):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from signal_bot.db.models import Base, Signal
    from signal_bot.execution.order_store import OrderStore
    from signal_bot.execution.orders import OrderState

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = create_async_engine("sqlite+aiosqlite:///" + db_path.as_posix())
    try:
        async with db.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(db, expire_on_commit=False)
        signal = Signal(
            id=signal_id,
            symbol="BTCUSDT",
            side="long",
            entry=Decimal(plan["limit_price"]),
            stop_loss=Decimal(plan["bracket_stop_loss_price"]),
            take_profit=Decimal(plan["bracket_take_profit_price"]),
            leverage=1,
            risk_pct=Decimal("1"),
            account_size_assumed=Decimal("100"),
            est_hold_minutes=1,
        )
        async with factory() as session:
            session.add(signal)
            await session.commit()
        store = OrderStore(factory)
        await store.create(
            signal,
            venue="delta",
            client_order_id=plan["client_order_id"],
            symbol="BTCUSD",
            side="buy",
            order_type="limit_order",
            size=Decimal(1),
            contract_value=Decimal(contract_value),
            limit_price=Decimal(plan["limit_price"]),
            intended_entry=Decimal(plan["limit_price"]),
            stop_loss_price=Decimal(plan["bracket_stop_loss_price"]),
            take_profit_price=Decimal(plan["bracket_take_profit_price"]),
        )
        await store.transition(
            plan["client_order_id"],
            OrderState.SUBMITTING,
            event_id="validation-intent-" + signal_id.hex,
        )
    finally:
        await db.dispose()


async def worker(db_path, status_path):
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from scripts.test_delta_fill_guarded import validate_environment
    from scripts.test_delta_no_fill import TESTNET_REST, TESTNET_WS, require
    from signal_bot.config import settings
    from signal_bot.db.models import Signal, SignalOutcome, TradeResult
    from signal_bot.delta.client import DeltaClient, DeltaWebSocket
    from signal_bot.events import EventBus, SqlEventJournal
    from signal_bot.events.envelope import EventType
    from signal_bot.execution.adapter import ExchangeUnavailable
    from signal_bot.execution.delta_adapter import DeltaExchangeAdapter, DeltaStreamBridge
    from signal_bot.execution.engine import ExecutionConfig, ExecutionEngine
    from signal_bot.execution.order_store import OrderStore
    from signal_bot.risk.governor import RiskGovernor
    from signal_bot.tracking.outcome_monitor import OutcomeMonitor
    from signal_bot.tracking.reconciliation import Reconciler

    validate_environment(settings.delta_mode, settings.exec_mode, settings.delta_ws_url)

    class ReadOnlyClient(DeltaClient):
        mutation_attempts = 0

        async def _request(self, method, path, params=None, data=None):
            require(self.mode == "testnet" and self.rest_url == TESTNET_REST, "testnet_required")
            if method != "GET":
                self.mutation_attempts += 1
                raise ExchangeUnavailable("validation worker forbids exchange mutations")
            return await super()._request(method, path, params=params, data=data)

    db = create_async_engine("sqlite+aiosqlite:///" + db_path.as_posix())
    factory = async_sessionmaker(db, expire_on_commit=False)
    client = ReadOnlyClient(mode="testnet", request_timeout=3)
    engine = None
    reconciler = None
    socket = None
    monitor_task = None
    reader = None
    state = {}
    try:
        await client.initialize()
        bus = EventBus(SqlEventJournal(factory))
        store = OrderStore(factory, bus)
        adapter = DeltaExchangeAdapter(client)
        governor = RiskGovernor(session_factory=factory, exchange=adapter, bus=bus)
        engine = ExecutionEngine(
            adapter,
            governor,
            bus,
            store,
            asyncio.Queue(),
            session_factory=factory,
            config=ExecutionConfig(
                protective_confirm_timeout=0.5,
                protective_poll_interval=0.1,
                protective_retry_attempts=0,
                watch_interval=0.5,
                require_stream_on_start=True,
            ),
        )
        monitor = OutcomeMonitor(
            bus,
            store,
            adapter=adapter,
            session_factory=factory,
            governor=governor,
            exit_fill_retries=3,
            exit_fill_retry_delay=0.2,
        )
        reconciler = Reconciler(
            adapter,
            store,
            governor=governor,
            outcome_monitor=monitor,
            engine=engine,
            bus=bus,
            interval=3,
            retry_delay=0.3,
        )
        engine.reconciler = reconciler
        exec_sub, monitor_sub = engine.subscribe(), monitor.subscribe()
        await bus.replay(exec_sub)
        await bus.replay(monitor_sub)
        monitor_task = asyncio.create_task(monitor.run())
        bridge = DeltaStreamBridge(bus)
        socket = DeltaWebSocket(
            client,
            bridge.handle_raw,
            on_connect=bridge.on_connect,
            on_disconnect=bridge.on_disconnect,
            symbols=["BTCUSD"],
            ws_url=TESTNET_WS,
            auth_path="/live",
        )
        socket.start()
        reconciler.start()
        # The actual engine waits for first reconciliation before it starts its
        # signal consumer. No replacement startup gate is used by the test.
        await asyncio.wait_for(engine.start(), timeout=25)
        local = (await store.recent(1))[0]
        entry = [f for f in await store.fills_for(local.client_order_id) if f.purpose == "entry"]
        state.update(
            startup_state=local.state,
            startup_entry_qty=str(sum((f.qty for f in entry), Decimal(0))),
            startup_protection=local.protective_status,
            reconciled_before_signals=reconciler.last_ok and engine._running,
        )

        async def snapshot():
            local = (await store.recent(1))[0]
            fills = await store.fills_for(local.client_order_id)
            async with factory() as session:
                outcomes = list((await session.execute(select(SignalOutcome))).scalars())
                trades = list((await session.execute(select(TradeResult))).scalars())
            closes = await bus.journal.read(0, [EventType.POSITION_CLOSED])
            state.update(
                ready=socket.connected and engine._running,
                state=local.state,
                protection=local.protective_status,
                entry_qty=str(sum((f.qty for f in fills if f.purpose == "entry"), Decimal(0))),
                exit_qty=str(sum((f.qty for f in fills if f.purpose != "entry"), Decimal(0))),
                exit_purposes=sorted({f.purpose for f in fills if f.purpose != "entry"}),
                fill_count=len(fills),
                outcomes=len(outcomes),
                trade_results=len(trades),
                exit_reason=outcomes[0].exit_reason if outcomes else None,
                close_events=len(closes),
                mutation_attempts=client.mutation_attempts,
                first_reconciliation_complete=reconciler.last_ok,
            )
            temporary = status_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(state), encoding="utf-8")
            temporary.replace(status_path)

        reader = asyncio.create_task(asyncio.to_thread(sys.stdin.readline))
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if reader.done():
                command = reader.result().strip()
                if command in ("", "stop"):
                    break
                if command == "reconcile":
                    await reconciler.reconcile_now(reason="validation")
                elif command == "duplicate":
                    for event in await bus.journal.read(
                        0, [EventType.EXCHANGE_FILL, EventType.EXCHANGE_POSITION_UPDATE]
                    ):
                        await engine.handle_event(event)
                        await monitor.handle_event(event)
                    local = (await store.recent(1))[0]
                    if local.state == "closed":
                        await monitor.close_trade(local)
                        await monitor.close_trade(local)
                    state["duplicate_replay_complete"] = True
                elif command == "duplicate_signal":
                    async with factory() as session:
                        signal = await session.scalar(select(Signal))
                    state["duplicate_signal_rejected"] = await engine.process_signal(signal) is None
                reader = asyncio.create_task(asyncio.to_thread(sys.stdin.readline))
            await snapshot()
            await asyncio.sleep(0.25)
    finally:
        if socket:
            await socket.stop()
        if engine:
            await engine.stop()
        if reconciler:
            await reconciler.stop()
        if monitor_task:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        await client.close()
        await db.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--status", required=True)
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        asyncio.run(worker(Path(args.database), Path(args.status)))
    except Exception as exc:
        from scripts.test_delta_no_fill import safe_error

        Path(args.status).write_text(
            json.dumps({"worker_error": safe_error(exc)}), encoding="utf-8"
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
