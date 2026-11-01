"""Paper broker: the simulated venue driven by *real* order books.

Unlike the old DRY_RUN path (instant fill at the signal price, zero fees), the
paper broker:

* refreshes the live venue order book before matching, so entries pay the
  actual spread and walk actual depth (partial fills when liquidity is thin);
* adds configurable latency between decision and match;
* charges maker/taker fees and periodic funding;
* runs bracket legs against subsequent books (stops gap to the book);
* keeps a virtual ledger persisted to ``paper_ledger`` (exactly-once by entry
  id) and snapshots mark-to-market equity for the control room.

Its state (positions, orders, cash) is checkpointed to a JSON file so a restart
does not silently "lose" paper positions that the order store still holds.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from sqlalchemy.exc import IntegrityError

from signal_bot.core.clock import SYSTEM_CLOCK, Clock
from signal_bot.db.models import PaperLedgerEntry
from signal_bot.events.bus import EventBus
from signal_bot.execution.adapter import (
    BookSnapshot,
    ExchangeOrder,
    ExchangeUnavailable,
    FillPurpose,
    InstrumentSpec,
    OrderRequest,
    VenueOrderState,
)
from signal_bot.observability import metrics
from signal_bot.venues.simulated import CostModel, SimulatedVenue, _Order, _Position

logger = logging.getLogger(__name__)

BookSource = Callable[[str], Awaitable[BookSnapshot]]


class PaperBroker(SimulatedVenue):
    def __init__(
        self,
        instruments: dict[str, InstrumentSpec],
        bus: EventBus,
        book_source: BookSource,
        *,
        session_factory=None,
        costs: CostModel | None = None,
        initial_balance: Decimal = Decimal("10000"),
        clock: Clock = SYSTEM_CLOCK,
        poll_interval: float = 2.0,
        state_path: str | None = None,
        equity_snapshot_interval: float = 60.0,
    ) -> None:
        super().__init__(
            instruments,
            bus,
            venue="paper",
            clock=clock,
            costs=costs,
            initial_balance=initial_balance,
        )
        self.book_source = book_source
        self.session_factory = session_factory
        self.poll_interval = poll_interval
        self.state_path = Path(state_path) if state_path else None
        self.equity_snapshot_interval = equity_snapshot_interval
        self.equity_curve: deque[dict] = deque(maxlen=5000)
        self._persisted_ledger = 0
        self._running = False
        self.last_book_error: str | None = None

    # ------------------------------------------------------------------
    async def refresh_book(self, symbol: str) -> BookSnapshot:
        try:
            snap = await self.book_source(symbol)
        except Exception as exc:
            self.last_book_error = f"{type(exc).__name__}: {exc}"
            raise ExchangeUnavailable(f"paper book source failed for {symbol}: {exc}") from exc
        self.last_book_error = None
        await self.on_book(snap)
        return snap

    async def order_book(self, symbol: str) -> BookSnapshot:
        await self.refresh_book(symbol)
        return await super().order_book(symbol)

    async def _before_match(self, symbol: str) -> None:
        # Runs after the simulated latency, so orders match the book as it is *then*.
        await self.refresh_book(symbol)

    async def place_order(self, request: OrderRequest) -> ExchangeOrder:
        try:
            return await super().place_order(request)
        finally:
            await self.checkpoint()

    async def close_position(self, symbol: str, client_order_id: str) -> ExchangeOrder:
        try:
            return await super().close_position(symbol, client_order_id)
        finally:
            await self.checkpoint()

    # ------------------------------------------------------------------
    async def run(self) -> None:
        """Poll live books for symbols with exposure; trigger legs; persist the ledger."""
        self._running = True
        last_snapshot = 0.0
        while self._running:
            for symbol in self._active_symbols():
                try:
                    await self.refresh_book(symbol)
                except ExchangeUnavailable as exc:
                    logger.warning("%s", exc)
            await self.checkpoint()
            mono = self.clock.monotonic()
            if mono - last_snapshot >= self.equity_snapshot_interval:
                last_snapshot = mono
                self.snapshot_equity()
            await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        self._running = False

    def _active_symbols(self) -> list[str]:
        symbols = {p.symbol for p in self._positions.values() if p.size != 0}
        symbols |= {
            o.request.symbol
            for o in self._orders.values()
            if o.state in (VenueOrderState.OPEN, VenueOrderState.PARTIALLY_FILLED)
        }
        return sorted(symbols)

    def snapshot_equity(self) -> dict:
        point = {
            "ts": self.clock.now().isoformat(),
            "equity": float(self.equity()),
            "cash": float(self.cash),
            "unrealized": float(self.unrealized_pnl()),
        }
        self.equity_curve.append(point)
        metrics.PAPER_EQUITY.set(point["equity"])
        return point

    def account_summary(self) -> dict:
        equity = self.equity()
        fees = sum((-e.amount for e in self._ledger if e.kind == "fee"), Decimal("0"))
        funding = sum((e.amount for e in self._ledger if e.kind == "funding"), Decimal("0"))
        realized = sum((e.amount for e in self._ledger if e.kind == "realized_pnl"), Decimal("0"))
        return {
            "initial": float(self.initial_balance),
            "equity": float(equity),
            "cash": float(self.cash),
            "unrealized": float(self.unrealized_pnl()),
            "realized_pnl": float(realized),
            "fees_paid": float(fees),
            "funding": float(funding),
            "return_pct": float((equity / self.initial_balance - 1) * 100)
            if self.initial_balance
            else 0.0,
            "book_source_error": self.last_book_error,
        }

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------
    async def checkpoint(self) -> None:
        await self._persist_ledger()
        self._save_state()

    async def _persist_ledger(self) -> None:
        if self.session_factory is None:
            return
        pending = self._ledger[self._persisted_ledger :]
        if not pending:
            return
        try:
            async with self.session_factory() as session:
                session.add_all([_ledger_row(e) for e in pending])
                await session.commit()
        except IntegrityError:
            # Some entries already persisted (e.g. after restore): insert one by one.
            for entry in pending:
                try:
                    async with self.session_factory() as session:
                        session.add(_ledger_row(entry))
                        await session.commit()
                except IntegrityError:
                    pass
        except Exception:
            logger.warning("paper ledger persist deferred (database unavailable)")
            return
        self._persisted_ledger = len(self._ledger)

    def _save_state(self) -> None:
        if self.state_path is None:
            return
        state = {
            "cash": str(self.cash),
            "initial_balance": str(self.initial_balance),
            "order_counter": self._order_counter,
            "fill_counter": self._fill_counter,
            "positions": [
                {k: (str(v) if isinstance(v, Decimal) else v) for k, v in asdict(p).items()}
                for p in self._positions.values()
                if p.size != 0
            ],
            "orders": [
                {
                    "request": {
                        k: (str(v) if isinstance(v, Decimal) else v)
                        for k, v in asdict(o.request).items()
                    },
                    "exchange_order_id": o.exchange_order_id,
                    "state": str(o.state),
                    "filled": str(o.filled),
                    "notional": str(o.notional),
                    "fees": str(o.fees),
                    "purpose": str(o.purpose),
                }
                for o in self._orders.values()
            ][-500:],
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.state_path.parent, prefix=".paper-")
        with os.fdopen(fd, "w") as fh:
            json.dump(state, fh)
        os.replace(tmp, self.state_path)

    def restore(self) -> bool:
        if self.state_path is None or not self.state_path.exists():
            return False
        state = json.loads(self.state_path.read_text())
        self.cash = Decimal(state["cash"])
        self.initial_balance = Decimal(state.get("initial_balance", self.initial_balance))
        self._order_counter = state.get("order_counter", 0)
        self._fill_counter = state.get("fill_counter", 0)
        dec_fields = {"size", "entry_price", "stop_price", "take_profit_price"}
        for raw in state.get("positions", []):
            kw = {
                k: (Decimal(v) if k in dec_fields and v is not None else v) for k, v in raw.items()
            }
            self._positions[kw["symbol"]] = _Position(**kw)
        req_dec = {"size", "limit_price", "stop_loss_price", "take_profit_price"}
        for raw in state.get("orders", []):
            req = {
                k: (Decimal(v) if k in req_dec and v is not None else v)
                for k, v in raw["request"].items()
            }
            order = _Order(
                request=OrderRequest(**req),
                exchange_order_id=raw["exchange_order_id"],
                state=VenueOrderState(raw["state"]),
                filled=Decimal(raw["filled"]),
                notional=Decimal(raw["notional"]),
                fees=Decimal(raw["fees"]),
                updated_at=self.clock.now(),
                purpose=FillPurpose(raw.get("purpose", FillPurpose.ENTRY)),
            )
            self._orders[order.request.client_order_id] = order
        self._persisted_ledger = len(self._ledger)
        logger.info(
            "Paper broker restored: cash=%s positions=%d orders=%d",
            self.cash,
            len(self._positions),
            len(self._orders),
        )
        return True


def _ledger_row(entry) -> PaperLedgerEntry:
    return PaperLedgerEntry(
        entry_id=entry.entry_id,
        kind=entry.kind,
        symbol=entry.symbol,
        amount=entry.amount,
        balance_after=entry.balance_after,
        reference=entry.reference,
        occurred_at=entry.occurred_at,
    )
