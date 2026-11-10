"""Convert a consistent paper snapshot into the first TP bot checkpoint."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial
from pathlib import Path

import aiohttp
from sqlalchemy import func, select

from signal_bot.config import settings
from signal_bot.core.clock import ManualClock, utc
from signal_bot.db.connection import AsyncSessionLocal, async_engine
from signal_bot.db.models import Fill, Order, PaperLedgerEntry, Signal
from signal_bot.delta.client import ENDPOINTS
from signal_bot.events import EventBus, SqlEventJournal
from signal_bot.execution.adapter import FillPurpose, InstrumentSpec, VenueOrderState
from signal_bot.execution.engine import ExecutionEngine
from signal_bot.execution.order_store import OrderStore
from signal_bot.execution.orders import TERMINAL_STATES, OrderState
from signal_bot.tracking.outcome_monitor import OutcomeMonitor
from signal_bot.venues.paper import PaperBroker
from signal_bot.venues.simulated import CostModel

TAKE_PROFIT_R = Decimal("1.5")
UNIVERSE = Path(__file__).resolve().parents[1] / "research" / "delta_universe.json"


@dataclass(frozen=True)
class HistoricalExit:
    purpose: FillPurpose
    price: Decimal
    minute_start: datetime
    minute_end: datetime


async def historical_exit(
    session: aiohttp.ClientSession,
    symbol: str,
    long: bool,
    stop: Decimal,
    target: Decimal,
    filled_at: datetime,
    cutoff: datetime,
) -> HistoricalExit | None:
    """Find the first bracket touch on completed Delta trade candles."""
    url = f"{ENDPOINTS['production']['rest']}/v2/history/candles"
    start = int(filled_at.timestamp()) // 60 * 60
    end = int(cutoff.timestamp()) // 60 * 60
    for left in range(start, end, 60_000):
        right = min(end, left + 60_000)
        params = {"resolution": "1m", "symbol": symbol, "start": left, "end": right}
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            payload = await response.json()
        candles = sorted(
            (row for row in payload.get("result", []) if left <= int(row["time"]) < right),
            key=lambda row: int(row["time"]),
        )
        if not payload.get("success") or not candles:
            raise ValueError(f"{symbol}: Delta returned no historical data for {left}–{right}")
        for row in candles:
            high, low = Decimal(str(row["high"])), Decimal(str(row["low"]))
            stop_hit = low <= stop if long else high >= stop
            target_hit = high >= target if long else low <= target
            if not (stop_hit or target_hit):
                continue
            stamp = int(row["time"])
            if stamp < filled_at.timestamp():
                raise ValueError(f"{symbol}: a bracket touch in the entry minute is ambiguous")
            # Minute candles cannot establish order when both levels were crossed.
            # Use the same stop-first assumption as the research fill model.
            opening = Decimal(str(row["open"]))
            stop_fill = min(opening, stop) if long else max(opening, stop)
            return HistoricalExit(
                FillPurpose.STOP_LOSS if stop_hit else FillPurpose.TAKE_PROFIT,
                stop_fill if stop_hit else target,
                datetime.fromtimestamp(stamp, timezone.utc),
                datetime.fromtimestamp(stamp + 60, timezone.utc),
            )
    return None


def convert_positions(state: dict, open_orders: list, products: dict, at: datetime) -> list[dict]:
    """Change exit rules on the copy, keeping the original fills and timers."""
    positions = state["positions"]
    db_orders = {order.client_order_id: (order, signal) for order, signal in open_orders}
    broker_orders = {order["request"]["client_order_id"]: order for order in state["orders"]}
    owners = {position["owner"] for position in positions}
    if not positions or owners != db_orders.keys() or len(owners) != len(positions):
        raise ValueError("The paper checkpoint and database must agree on all open positions")
    if len({p["symbol"] for p in positions}) != len(positions):
        raise ValueError("Duplicate paper positions in the checkpoint")
    if any(
        order["state"] in (VenueOrderState.OPEN, VenueOrderState.PARTIALLY_FILLED)
        for order in broker_orders.values()
    ):
        raise ValueError("Wait for outstanding paper orders to settle before forking")

    summary = []
    for position in positions:
        order, signal = db_orders[position["owner"]]
        broker_order = broker_orders.get(order.client_order_id)
        symbol = position["symbol"]
        size = Decimal(position["size"])
        entry = Decimal(position["entry_price"])
        direction = Decimal(1) if size > 0 else Decimal(-1)
        if (
            order.venue != "paper"
            or order.symbol != symbol
            or order.side != ("buy" if size > 0 else "sell")
            or not size
            or order.filled_size != abs(size)
            or order.avg_fill_price is None
            or abs(order.avg_fill_price - entry) > Decimal("0.0000000000000001")
            or order.protective_status != "confirmed"
            or position["stop_price"] is None
            or Decimal(position["stop_price"])
            != (order.current_stop_price or order.stop_loss_price)
            or broker_order is None
            or Decimal(broker_order["filled"]) != abs(size)
        ):
            raise ValueError(
                f"{symbol}: open fill or protection differs between checkpoint and database"
            )

        product = products[symbol]
        tick = Decimal(product["tick_size"])
        if tick <= 0 or order.contract_value != Decimal(product["contract_value"]):
            raise ValueError(f"{symbol}: fork contract metadata does not match the original order")
        # Same calculation as the TP strategy at the original signal, including
        # its saved Binance-to-Delta price translation and Delta tick rounding.
        risk = abs(signal.entry - signal.stop_loss)
        signal_target = signal.entry + direction * TAKE_PROFIT_R * risk
        target = ExecutionEngine.round_to_tick(signal_target + (order.price_basis or 0), tick)
        stop = order.stop_loss_price
        if min(stop, entry, target) <= 0 or not (
            direction * (entry - stop) > 0 and direction * (target - entry) > 0
        ):
            raise ValueError(
                f"{symbol}: the initial stop and target must bracket the original fill"
            )

        position.update(
            stop_price=str(stop), take_profit_price=str(target), trail_amount=None, trail_best=None
        )
        broker_order["request"].update(
            stop_loss_price=str(stop), take_profit_price=str(target), trail_amount=None
        )
        order.current_stop_price = stop
        order.take_profit_price = target
        order.trail_mode = "none"
        order.trail_distance = None
        order.trail_from = None
        order.reference_stop = None
        order.best_price = None
        order.best_price_bar = None
        order.stop_updated_at = at
        order.exit_reason_hint = None
        signal.take_profit = signal_target
        signal.trail_distance = None
        summary.append(
            {
                "symbol": symbol,
                "size": str(size),
                "entry": str(entry),
                "stop": str(stop),
                "target": str(target),
            }
        )
    return summary


async def remove_post_exit_funding(session, state: dict, exits: dict[str, HistoricalExit]) -> None:
    """Undo funding the main account accrued after each simulated TP exit."""
    adjustment = Decimal("0")
    rows = await session.scalars(select(PaperLedgerEntry).order_by(PaperLedgerEntry.id))
    for row in rows:
        exit_ = exits.get(row.symbol)
        if exit_ and row.kind == "funding" and utc(row.occurred_at) >= exit_.minute_end:
            adjustment += row.amount
            await session.delete(row)
        elif adjustment:
            row.balance_after -= adjustment
    state["cash"] = str(Decimal(state["cash"]) - adjustment)


async def replay_exits(
    checkpoint: Path, session_factory, products: dict, exits: dict[str, HistoricalExit]
) -> None:
    """Send simulated exit fills through the normal paper broker and outcome monitor."""
    ordered = sorted(exits.items(), key=lambda item: item[1].minute_end)
    clock = ManualClock(ordered[0][1].minute_end)
    bus = EventBus(SqlEventJournal(session_factory))
    store = OrderStore(session_factory, bus, clock=clock)

    async def no_live_books(symbol: str):
        raise RuntimeError(f"Unexpected live order book request for {symbol} during fork replay")

    specs = {
        p["binance_symbol"]: InstrumentSpec(
            symbol,
            p["product_id"],
            Decimal(p["tick_size"]),
            Decimal("1"),
            Decimal(p["contract_value"]),
        )
        for symbol, p in products.items()
    }
    broker = PaperBroker(
        specs,
        bus,
        no_live_books,
        session_factory=session_factory,
        clock=clock,
        costs=CostModel(
            maker_fee=Decimal(str(settings.paper_maker_fee)),
            taker_fee=Decimal(str(settings.paper_taker_fee)),
            funding_rate=Decimal("0"),
        ),
        state_path=str(checkpoint),
    )
    if not broker.restore():
        raise ValueError("The temporary TP checkpoint is missing")
    await broker.restore_ledger()
    monitor = OutcomeMonitor(
        bus, store, adapter=broker, session_factory=session_factory, clock=clock
    )
    subscriber = monitor.subscribe()

    for symbol, exit_ in ordered:
        clock.set(exit_.minute_end)
        position = broker.position(symbol)
        fills_before = len(broker.fills)
        await broker.move_market(symbol, exit_.price, exit_.price, size=abs(position.size))
        if len(broker.fills) != fills_before + 1 or broker.position(symbol) is not None:
            raise ValueError(f"{symbol}: replay did not close the full position")
        fill = broker.fills[-1]
        if fill.purpose != exit_.purpose:
            raise ValueError(f"{symbol}: replay closed with the wrong exit reason")
        while not subscriber.queue.empty():
            event = subscriber.queue.get_nowait()
            await monitor.handle_event(event)
            await subscriber.ack(event)
        order = await store.get_by_client_id(fill.client_order_id)
        if order is None or order.state != OrderState.CLOSED:
            raise ValueError(f"{symbol}: replay outcome was not recorded")
    await bus.flush_pending()
    await broker.checkpoint()


async def seed(
    state: dict, destination: Path, session_factory, products: dict, at: datetime, lookup_exit=None
) -> list[dict]:
    if await asyncio.to_thread(destination.exists):
        raise ValueError("The TP bot already has a paper checkpoint")
    # Write the checkpoint beside its destination and publish it only when the
    # SQL conversion has committed. Deployment leaves the TP bot off on failure.
    fd, temporary = tempfile.mkstemp(prefix=".fork-tp-", dir=destination.parent)
    exits: dict[str, HistoricalExit] = {}
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as checkpoint:
            async with session_factory.begin() as session:
                rows = (
                    await session.execute(
                        select(Order, Signal)
                        .join(Signal, Order.signal_id == Signal.id)
                        .where(Order.state.not_in(TERMINAL_STATES))
                    )
                ).all()
                if any(order.state != OrderState.FILLED for order, _ in rows):
                    raise ValueError("Wait for unresolved paper orders to settle before forking")
                summary = convert_positions(state, rows, products, at)
                if lookup_exit is not None:
                    opened = dict(
                        (
                            await session.execute(
                                select(Fill.symbol, func.max(Fill.occurred_at))
                                .where(
                                    Fill.purpose == FillPurpose.ENTRY,
                                    Fill.order_id.in_([order.id for order, _ in rows]),
                                )
                                .group_by(Fill.symbol)
                            )
                        ).all()
                    )
                    if set(opened) != {row["symbol"] for row in summary}:
                        raise ValueError("An inherited position is missing its original entry fill")
                    results = await asyncio.gather(
                        *(
                            lookup_exit(
                                row["symbol"],
                                Decimal(row["size"]) > 0,
                                Decimal(row["stop"]),
                                Decimal(row["target"]),
                                utc(opened[row["symbol"]]),
                                at,
                            )
                            for row in summary
                        )
                    )
                    for row, exit_ in zip(summary, results, strict=True):
                        row["historical_exit"] = None
                        if exit_ is not None:
                            exits[row["symbol"]] = exit_
                            row["historical_exit"] = {
                                "source": "delta_trade_candle_1m_replay",
                                "purpose": exit_.purpose,
                                "price": str(exit_.price),
                                "minute_start": exit_.minute_start.isoformat(),
                                "recorded_exit_time": exit_.minute_end.isoformat(),
                            }
                    if exits:
                        await remove_post_exit_funding(session, state, exits)
                json.dump(state, checkpoint, separators=(",", ":"))
        if exits:
            await replay_exits(Path(temporary), session_factory, products, exits)
        os.replace(temporary, destination)
        return summary
    finally:
        await asyncio.to_thread(Path(temporary).unlink, missing_ok=True)


async def main() -> None:
    if (
        settings.exec_mode != "paper"
        or not settings.paper_only
        or async_engine.url.database != "signal_bot_tp"
        or Path(settings.strategy_config).name != "trend_portfolio_tp15.toml"
    ):
        raise SystemExit("Run the fork only in the bot-tp paper service")
    products = {
        p["delta_symbol"]: p for p in json.loads(UNIVERSE.read_text(encoding="utf-8"))["products"]
    }
    at = datetime.now(timezone.utc)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as history:
            summary = await seed(
                json.load(sys.stdin),
                Path(settings.paper_state_path),
                AsyncSessionLocal,
                products,
                at,
                lookup_exit=partial(historical_exit, history),
            )
    finally:
        await async_engine.dispose()
    print(json.dumps({"forked_at": at.isoformat(), "positions": summary}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
