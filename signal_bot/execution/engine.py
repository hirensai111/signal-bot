"""Execution engine: turns sized signals into venue orders through the state machine.

Safety properties (each covered by tests/scenarios):

1. **Risk-gated, fail-closed.**  No order is sent unless a healthy risk governor
   approves it *and* the synchronous gate is open immediately before the call.
2. **Intent before network.**  The order is persisted as ``submitting`` before
   the venue is contacted, so a crash mid-call is recoverable from the
   deterministic ``client_order_id``.
3. **Ambiguity is not failure.**  A timeout / 5xx / reset moves the order to
   ``submission_unknown``; the engine resolves it by looking the client id up
   and never resubmits.  While unresolved, all new submissions are blocked.
4. **Protected or flat.**  After an entry fills, the protective stop must be
   confirmed on the venue.  If it is not, the kill switch is engaged and the
   recovery procedure runs: attach a standalone stop, else flatten.
5. **Stream loss blocks trading** until the stream is back *and* a
   reconciliation pass has succeeded.
"""

from __future__ import annotations

import asyncio
import enum
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal

from sqlalchemy import select

from signal_bot.core.clock import SYSTEM_CLOCK, Clock, utc
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import Order, Signal
from signal_bot.events.bus import EventBus, Subscription
from signal_bot.events.envelope import Event, EventType, deterministic_event_id
from signal_bot.execution.adapter import (
    AuthenticationFailed,
    ExchangeAdapter,
    ExchangeError,
    ExchangeOrder,
    FillPurpose,
    OrderRejected,
    OrderRequest,
    fill_from_payload,
    order_from_payload,
)
from signal_bot.execution.order_store import OrderStore, local_event_id
from signal_bot.execution.orders import EXPOSURE_STATES, OrderState
from signal_bot.observability import metrics
from signal_bot.observability.logging import bind

logger = logging.getLogger(__name__)

S = OrderState

BASIS_ABORT_PCT = Decimal("0.0015")  # 0.15 %
NO_FILL_TIMEOUT = 60.0
CLIENT_ORDER_PREFIX = "sig_"


class EXEC_MODE(enum.Enum):
    LIVE = "live"
    PAPER = "paper"
    SIM = "sim"


def client_order_id_for(signal_id) -> str:
    """Deterministic, venue-safe (32 chars) client order id for a signal."""
    hex_id = signal_id.hex if hasattr(signal_id, "hex") else str(signal_id).replace("-", "")
    return f"{CLIENT_ORDER_PREFIX}{hex_id[:28]}"


@dataclass
class ExecutionConfig:
    order_type: str = "limit_ioc"
    basis_abort_pct: Decimal = BASIS_ABORT_PCT
    max_book_age_seconds: float = 10.0
    max_signal_age_seconds: float = 420.0
    no_fill_timeout: float = NO_FILL_TIMEOUT
    fill_poll_interval: float = 5.0
    cancel_confirm_timeout: float = 30.0
    unknown_lookup_attempts: int = 5
    unknown_lookup_backoff: float = 1.0
    protective_confirm_timeout: float = 5.0
    protective_poll_interval: float = 0.25
    protective_retry_attempts: int = 3
    watch_interval: float = 1.0
    require_stream_on_start: bool = False
    # Trailing stops (signals with trail_distance): "stepped" = exchange-resident stop moved
    # by execution/trailing.py at each completed bar; "native" = venue trailing stop.
    trail_mode: str = "stepped"


class ExecutionEngine:
    def __init__(
        self,
        adapter: ExchangeAdapter,
        governor,
        bus: EventBus,
        order_store: OrderStore,
        signal_queue: asyncio.Queue,
        session_factory=None,
        config: ExecutionConfig | None = None,
        reconciler=None,
        alerts=None,
        clock: Clock = SYSTEM_CLOCK,
        mode: EXEC_MODE = EXEC_MODE.PAPER,
    ) -> None:
        self.adapter = adapter
        self.governor = governor
        self.bus = bus
        self.store = order_store
        self.signal_queue = signal_queue
        self.session_factory = session_factory or AsyncSessionLocal
        self.config = config or ExecutionConfig()
        self.reconciler = reconciler
        self.alerts = alerts
        self.clock = clock
        self.mode = mode

        self._leverage_cache: dict[str, int] = {}
        self._awaiting: dict[str, datetime] = {}  # coid -> fill deadline
        self._last_poll: dict[str, float] = {}
        self._protecting: set[str] = set()
        self._subscription: Subscription | None = None
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._background: set[asyncio.Task] = set()
        self.last_processed_at: datetime | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def subscribe(self) -> Subscription:
        """Register the bus subscription (call before streams start, then replay)."""
        if self._subscription is None:
            self._subscription = self.bus.subscribe(
                "execution",
                [
                    EventType.EXCHANGE_ORDER_UPDATE,
                    EventType.EXCHANGE_FILL,
                    EventType.STREAM_CONNECTED,
                    EventType.STREAM_DISCONNECTED,
                ],
            )
        return self._subscription

    async def start(self) -> None:
        if self._running:
            return
        self.subscribe()
        if self.config.require_stream_on_start and self.governor is not None:
            self.governor.set_breaker("exchange_stream", "private stream not yet connected")
        if self.reconciler is not None:
            logger.info("Waiting for first reconciliation before starting engine...")
            await self.reconciler.wait_for_first_run()
        await self._expire_stale_pending_signals()
        await self._resume_open_orders()
        self._running = True
        self._tasks = [
            asyncio.create_task(self._consume_signals(), name="exec-signals"),
            asyncio.create_task(self._consume_events(), name="exec-events"),
            asyncio.create_task(self._watch_orders(), name="exec-watch"),
        ]
        logger.info(
            "ExecutionEngine started (mode=%s, venue=%s)", self.mode.value, self.adapter.venue
        )

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks + list(self._background):
            t.cancel()
        for t in self._tasks + list(self._background):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        logger.info("ExecutionEngine stopped")

    def _spawn(self, coro, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    # ------------------------------------------------------------------
    # signals
    # ------------------------------------------------------------------
    async def _consume_signals(self) -> None:
        while self._running:
            try:
                signal: Signal = await asyncio.wait_for(self.signal_queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            try:
                await self.process_signal(signal)
            except Exception:
                logger.exception("Unhandled error processing signal %s", signal.id)
            self.last_processed_at = self.clock.now()

    async def process_signal(self, signal: Signal) -> Order | None:
        coid = client_order_id_for(signal.id)
        with bind(
            correlation_id=str(signal.id),
            signal_id=str(signal.id),
            order_id=coid,
            symbol=signal.symbol,
        ):
            return await self._process_signal(signal, coid)

    async def _process_signal(self, signal: Signal, coid: str) -> Order | None:
        try:
            async with self.session_factory() as session:
                fresh: Signal | None = await session.get(Signal, signal.id)
        except Exception:
            # Without the database we cannot persist intent: never trade blind.
            logger.exception("Database unavailable; signal %s not executed", signal.id)
            metrics.RISK_DENIALS.labels("database_unavailable").inc()
            return None
        if fresh is None:
            logger.warning("Signal %s not found in DB", signal.id)
            return None
        if fresh.status != "pending" or fresh.client_order_id is not None:
            logger.debug("Signal %s already at status=%s — skipping", fresh.id, fresh.status)
            return None
        if await self.store.get_for_signal(fresh.id) is not None:
            return None

        created = utc(fresh.created_at)
        if (
            created
            and (self.clock.now() - created).total_seconds() > self.config.max_signal_age_seconds
        ):
            await self._cancel_signal(fresh, "signal_stale")
            return None

        # 1. risk gate (fail closed)
        approved, reason = await self._risk_check(fresh)
        if not approved:
            await self._cancel_signal(fresh, reason or "governor_denied")
            await self._publish(
                EventType.RISK_DENIED,
                {"signal_id": str(fresh.id), "symbol": fresh.symbol, "reason": reason},
                correlation_id=str(fresh.id),
            )
            logger.info("Signal %s denied by risk governor: %s", fresh.id, reason)
            return None

        # 2. instrument + live book
        try:
            spec = self.adapter.instrument(fresh.symbol)
        except (KeyError, RuntimeError):
            await self._cancel_signal(fresh, "symbol_unmapped")
            return None
        # Venues net positions per product: a second order on the same symbol would
        # silently reduce/flip the first trade instead of opening a new one.
        exposed = await self.store.in_states(EXPOSURE_STATES)
        if any(o.symbol == spec.symbol for o in exposed):
            await self._cancel_signal(fresh, "symbol_already_exposed")
            return None
        try:
            book = await self.adapter.order_book(spec.symbol)
        except ExchangeError as exc:
            await self._cancel_signal(fresh, f"order_book_unavailable: {exc}")
            return None
        if book.best_bid is None or book.best_ask is None:
            await self._cancel_signal(fresh, "empty_order_book")
            return None
        book_age = (self.clock.now() - utc(book.ts)).total_seconds()
        if book_age > self.config.max_book_age_seconds:
            await self._cancel_signal(fresh, "stale_order_book")
            logger.warning("Signal %s aborted: order book %.1fs old", fresh.id, book_age)
            return None

        # 3. cross-venue translation
        mid = (book.best_bid + book.best_ask) / 2
        basis = mid - fresh.entry
        basis_pct = abs(basis) / fresh.entry if fresh.entry else Decimal("0")
        if basis_pct > self.config.basis_abort_pct:
            await self._cancel_signal(fresh, "cross_venue_basis_too_wide", basis=basis)
            logger.warning("Signal %s aborted: basis=%.4f%%", fresh.id, float(basis_pct) * 100)
            return None

        tick = spec.tick_size
        entry = self.round_to_tick(fresh.entry + basis, tick)
        stop = self.round_to_tick(fresh.stop_loss + basis, tick)
        target = (
            None
            if fresh.take_profit is None
            else self.round_to_tick(fresh.take_profit + basis, tick)
        )
        trail = Decimal(str(fresh.trail_distance)) if fresh.trail_distance else None
        trail_mode = self.config.trail_mode if trail else "none"
        if fresh.planned_contracts:
            # The portfolio rules already sized this trade in whole contracts on shared equity.
            size = Decimal(str(fresh.planned_contracts))
        else:
            size = self.compute_size(fresh, spec.contract_value)
        size = (size / spec.lot_size).to_integral_value(rounding=ROUND_DOWN) * spec.lot_size
        if size < spec.lot_size:
            await self._cancel_signal(fresh, "size_below_min", basis=basis)
            return None

        side = "buy" if fresh.side == "long" else "sell"
        if self.config.order_type == "limit_ioc":
            limit = book.best_ask + tick if side == "buy" else book.best_bid - tick
            limit = self.round_to_tick(limit, tick)
        else:
            limit = None

        # 4. persist intent
        order = await self.store.create(
            fresh,
            venue=self.adapter.venue,
            client_order_id=coid,
            symbol=spec.symbol,
            side=side,
            order_type=self.config.order_type,
            size=size,
            contract_value=spec.contract_value,
            limit_price=limit,
            intended_entry=entry,
            stop_loss_price=stop,
            take_profit_price=target,
            correlation_id=str(fresh.id),
            trail_mode=trail_mode,
            trail_distance=trail,
            trail_from=utc(fresh.trail_from),
            reference_stop=fresh.stop_loss if trail else None,
            price_basis=basis,
            time_stop_at=utc(fresh.time_stop_at),
        )
        if order is None:
            return None
        await self._set_basis(fresh.id, basis)

        try:
            if self._leverage_cache.get(spec.symbol) != fresh.leverage:
                await self.adapter.set_leverage(spec.symbol, fresh.leverage)
                self._leverage_cache[spec.symbol] = fresh.leverage
        except AuthenticationFailed as exc:
            self._auth_failed(exc)
            await self._transition(coid, S.CANCELLED, "leverage_setup_auth_failed")
            return order
        except ExchangeError as exc:
            await self._transition(coid, S.CANCELLED, f"leverage_setup_failed: {exc}")
            return order

        open_, why = self.governor.gate_open()
        if not open_:
            await self._transition(coid, S.CANCELLED, f"gate_closed: {why}")
            return order

        await self._transition(coid, S.SUBMITTING, "intent persisted")
        try:
            await self.governor.record_submission(fresh.id)
        except Exception:
            logger.exception("Could not record submission; aborting before network call")
            await self._transition(coid, S.CANCELLED, "risk_state_unavailable")
            return order

        # 5. the one network call that can create exposure
        request = OrderRequest(
            client_order_id=coid,
            symbol=spec.symbol,
            side=side,
            size=size,
            order_type=self.config.order_type,
            limit_price=limit,
            stop_loss_price=None if trail_mode == "native" else stop,
            take_profit_price=target,
            trail_amount=self.round_to_tick(trail, tick) if trail_mode == "native" else None,
        )
        try:
            ack = await self.adapter.place_order(request)
        except OrderRejected as exc:
            metrics.ORDERS_REJECTED.labels(self.adapter.venue).inc()
            await self._transition(coid, S.REJECTED, f"rejected: {exc}")
            return order
        except AuthenticationFailed as exc:
            self._auth_failed(exc)
            await self._transition(coid, S.REJECTED, "authentication_failed")
            return order
        except Exception as exc:  # AmbiguousOrderOutcome and anything unexpected
            metrics.SUBMISSIONS_UNKNOWN.labels(self.adapter.venue).inc()
            await self._mark_unknown(coid, f"{type(exc).__name__}: {exc}")
            await self.resolve_unknown(coid)
            return order

        await self._apply_update(ack, source="rest")
        return order

    async def _risk_check(self, signal: Signal) -> tuple[bool, str | None]:
        if self.governor is None:
            return False, "no_risk_governor"
        if not self.governor.healthy:
            return False, "risk_governor_unhealthy"
        try:
            return await self.governor.check(signal)
        except Exception:
            logger.exception("Risk governor raised; denying")
            return False, "risk_governor_error"

    # ------------------------------------------------------------------
    # unknown submission resolution
    # ------------------------------------------------------------------
    async def _mark_unknown(self, coid: str, reason: str) -> None:
        self.governor.set_breaker(f"unresolved_order:{coid}", reason)
        await self._transition(coid, S.SUBMISSION_UNKNOWN, reason)
        await self._alert(
            f"Order {coid} submission outcome UNKNOWN ({reason}); resolving by lookup"
        )

    async def resolve_unknown(self, coid: str) -> OrderState:
        """Look the order up by client id; never resubmit.  Returns the resulting state."""
        delay = self.config.unknown_lookup_backoff
        for attempt in range(1, self.config.unknown_lookup_attempts + 1):
            try:
                found = await self.adapter.get_order(coid)
            except AuthenticationFailed as exc:
                self._auth_failed(exc)
                found = None
            except ExchangeError as exc:
                logger.warning("Lookup %d for %s failed: %s", attempt, coid, exc)
                found = None
            if found is not None:
                await self._apply_update(found, source="lookup")
                state = await self._state(coid)
                if state not in (S.SUBMISSION_UNKNOWN, S.RECONCILIATION_REQUIRED):
                    self.governor.clear_breaker(f"unresolved_order:{coid}")
                    logger.info("Resolved unknown submission %s -> %s", coid, state)
                return state
            await asyncio.sleep(delay)
            delay *= 2
        await self._transition(
            coid, S.RECONCILIATION_REQUIRED, "not found after lookups; reconciler will resolve"
        )
        await self._alert(f"Order {coid} still unresolved; trading blocked until reconciled")
        return S.RECONCILIATION_REQUIRED

    # ------------------------------------------------------------------
    # venue events
    # ------------------------------------------------------------------
    async def _consume_events(self) -> None:
        sub = self.subscribe()
        while self._running:
            event = await sub.get(timeout=1.0)
            if event is None:
                continue
            try:
                with bind(event_id=event.event_id, correlation_id=event.correlation_id):
                    await self.handle_event(event)
            except Exception:
                metrics.EVENT_PROCESSING_FAILURES.labels("execution").inc()
                logger.exception("execution failed to process %s", event.event_type)
            finally:
                await sub.ack(event)

    async def handle_event(self, event: Event) -> None:
        etype = event.event_type
        if etype == EventType.EXCHANGE_ORDER_UPDATE:
            update = order_from_payload(event.payload)
            if update.purpose != FillPurpose.ENTRY:
                return  # protective legs / closes are the outcome monitor's concern
            if not update.client_order_id.startswith(CLIENT_ORDER_PREFIX):
                return
            await self._apply_update(update, event_id=event.event_id)
        elif etype == EventType.EXCHANGE_FILL:
            fill = fill_from_payload(event.payload)
            # UNKNOWN fills are classified by the store (same side as our order = entry).
            if fill.purpose in (FillPurpose.ENTRY, FillPurpose.UNKNOWN):
                await self.store.record_fill(fill, event.payload.get("venue", self.adapter.venue))
        elif etype == EventType.STREAM_DISCONNECTED:
            self.governor.set_breaker(
                "exchange_stream", f"private stream down: {event.payload.get('reason')}"
            )
            await self._alert("Exchange stream DISCONNECTED; new orders blocked")
        elif etype == EventType.STREAM_CONNECTED:
            self._spawn(self._recover_stream(), "exec-stream-recover")

    async def _recover_stream(self) -> None:
        """Events may have been missed while disconnected: reconcile before resuming."""
        if self.reconciler is not None:
            ok = await self.reconciler.reconcile_now(reason="stream_reconnected")
            if not ok:
                logger.error("Reconciliation after reconnect failed; trading stays blocked")
                return
        self.governor.clear_breaker("exchange_stream")
        logger.info("Exchange stream recovered and reconciled; trading unblocked")

    async def _apply_update(
        self, update: ExchangeOrder, *, event_id: str | None = None, source: str = "stream"
    ) -> None:
        if event_id is None:
            event_id = deterministic_event_id(
                source,
                update.client_order_id,
                update.state,
                update.filled_size,
                update.updated_at.isoformat(),
            )
        result = await self.store.apply_venue_update(update, event_id=event_id)
        if result.outcome == "unknown_order":
            metrics.RECONCILIATION_DISCREPANCIES.labels("unknown_order_event").inc()
            logger.warning("Venue update for unknown order %s", update.client_order_id)
            return
        state = OrderState(result.order.state) if result.order is not None else None
        if result.applied and result.from_state in (
            S.SUBMISSION_UNKNOWN,
            S.RECONCILIATION_REQUIRED,
        ):
            self.governor.clear_breaker(f"unresolved_order:{update.client_order_id}")
        if state in (S.ACKNOWLEDGED, S.PARTIALLY_FILLED):
            self._awaiting.setdefault(
                update.client_order_id,
                utc(result.order.submitted_at or result.order.created_at)
                + timedelta(seconds=self.config.no_fill_timeout),
            )
        if state in (S.PARTIALLY_FILLED, S.FILLED):
            self._spawn(self.ensure_protection(update.client_order_id), "exec-protect")
        if state not in (S.ACKNOWLEDGED, S.PARTIALLY_FILLED, S.CANCEL_PENDING):
            self._awaiting.pop(update.client_order_id, None)

    # ------------------------------------------------------------------
    # protective orders
    # ------------------------------------------------------------------
    async def ensure_protection(self, coid: str, recheck: bool = False) -> str:
        """Confirm the stop exists on the venue; otherwise halt and recover.

        ``recheck=True`` re-verifies an order previously confirmed (used by the
        reconciler when the venue position no longer shows a stop).
        """
        if coid in self._protecting:
            return "in_progress"
        self._protecting.add(coid)
        try:
            with bind(order_id=coid):
                return await self._ensure_protection(coid, recheck)
        finally:
            self._protecting.discard(coid)

    async def _ensure_protection(self, coid: str, recheck: bool) -> str:
        order = await self.store.get_by_client_id(coid)
        if order is None:
            return "unknown_order"
        done = ("flattened",) if recheck else ("confirmed", "recovered", "flattened")
        if order.protective_status in done:
            return order.protective_status

        poll = max(self.config.protective_poll_interval, 1e-3)
        attempts = max(1, int(self.config.protective_confirm_timeout / poll))
        for attempt in range(attempts):
            try:
                position = next(
                    (p for p in await self.adapter.positions() if p.symbol == order.symbol), None
                )
            except ExchangeError:
                position = None
            if position is not None and position.has_stop_loss:
                await self.store.set_protective_status(coid, "confirmed")
                return "confirmed"
            if position is None and await self._already_exited(order):
                await self.store.set_protective_status(coid, "not_required")
                return "not_required"
            if attempt < attempts - 1:
                await asyncio.sleep(poll)

        if position is None:
            # No exposure visible on the venue, so nothing is unprotected.  The
            # reconciler compares filled orders with venue positions and will
            # either record the missed close or re-run this check if exposure appears.
            logger.warning("Filled order %s shows no venue position yet; deferring", coid)
            await self.store.set_protective_status(coid, "unverified")
            return "unverified"

        # ---- critical: exposure without a confirmed stop ----
        metrics.PROTECTIVE_FAILURES.inc()
        reason = f"protective_order_failed:{coid}"
        logger.critical("No protective stop confirmed for %s — entering critical state", coid)
        await self.store.set_protective_status(coid, "failed")
        await self._publish(
            EventType.PROTECTIVE_ORDER_FAILED,
            {"client_order_id": coid, "symbol": order.symbol},
            correlation_id=order.correlation_id,
        )
        try:
            await self.governor.set_kill_switch(reason, actor="execution_engine")
        except Exception:
            logger.exception("Kill switch persist failed (engaged in memory)")
        await self._alert(f"CRITICAL: no stop on {order.symbol} for {coid}. Kill switch engaged.")

        exit_side = "sell" if order.side == "buy" else "buy"
        size = order.filled_size or order.size
        # A trailed stop may have moved since entry: never re-protect at a looser level.
        stop_price = order.current_stop_price or order.stop_loss_price
        for attempt in range(1, self.config.protective_retry_attempts + 1):
            try:
                await self.adapter.place_protective_stop(
                    order.symbol, exit_side, size, stop_price, coid
                )
                pos = next(
                    (p for p in await self.adapter.positions() if p.symbol == order.symbol), None
                )
                if pos is None or pos.has_stop_loss:
                    await self.store.set_protective_status(coid, "recovered")
                    await self._alert(f"Recovery: standalone stop attached for {coid}")
                    return "recovered"
            except ExchangeError as exc:
                logger.error("Protective stop attempt %d for %s failed: %s", attempt, coid, exc)
            await asyncio.sleep(self.config.protective_poll_interval)

        try:
            await self.adapter.close_position(order.symbol, coid)
            await self.store.set_protective_status(coid, "flattened")
            await self._alert(f"Recovery: position {order.symbol} FLATTENED for {coid}")
            return "flattened"
        except ExchangeError as exc:
            logger.critical("Flatten failed for %s: %s", coid, exc)
            await self._transition(
                coid, S.RECONCILIATION_REQUIRED, f"unprotected position, flatten failed: {exc}"
            )
            await self._alert(f"CRITICAL: could not protect OR flatten {order.symbol} ({coid})")
            return "failed"

    async def _already_exited(self, order: Order) -> bool:
        """Did the position close before we could check it (fast stop-out)?  Ask the venue."""
        fills = await self.store.fills_for(order.client_order_id)
        if any(f.purpose != str(FillPurpose.ENTRY) for f in fills):
            return True
        try:
            venue_exits = await self.adapter.exit_fills(
                order.symbol, order.client_order_id, utc(order.submitted_at)
            )
        except ExchangeError:
            return False
        for fill in venue_exits:
            await self.store.record_fill(fill, order.venue)
        return bool(venue_exits)

    # ------------------------------------------------------------------
    # watcher: polling fallback + no-fill timeout
    # ------------------------------------------------------------------
    async def _watch_orders(self) -> None:
        while self._running:
            try:
                await self.watch_once()
            except Exception:
                logger.exception("order watch iteration failed")
            await asyncio.sleep(self.config.watch_interval)

    async def watch_once(self) -> None:
        now = self.clock.now()
        for coid, deadline in list(self._awaiting.items()):
            order = await self.store.get_by_client_id(coid)
            if order is None:
                self._awaiting.pop(coid, None)
                continue
            state = OrderState(order.state)
            mono = self.clock.monotonic()
            last = self._last_poll.get(coid)
            if last is None or mono - last >= self.config.fill_poll_interval:
                self._last_poll[coid] = mono
                try:
                    found = await self.adapter.get_order(coid)
                    if found is not None:
                        await self._apply_update(found, source="poll")
                        state = await self._state(coid)
                except ExchangeError:
                    pass
            if state in (S.ACKNOWLEDGED, S.PARTIALLY_FILLED) and now >= deadline:
                await self._transition(coid, S.CANCEL_PENDING, "no_fill_timeout")
                try:
                    await self.adapter.cancel_order(coid, order.exchange_order_id)
                except ExchangeError as exc:
                    logger.warning("Cancel for %s failed (%s); will poll", coid, exc)
            elif state == S.CANCEL_PENDING and now >= deadline + timedelta(
                seconds=self.config.cancel_confirm_timeout
            ):
                await self._transition(coid, S.RECONCILIATION_REQUIRED, "cancel not confirmed")
                self.governor.set_breaker(f"unresolved_order:{coid}", "cancel not confirmed")
                self._awaiting.pop(coid, None)
            elif state not in (S.ACKNOWLEDGED, S.PARTIALLY_FILLED, S.CANCEL_PENDING):
                self._awaiting.pop(coid, None)

    async def _resume_open_orders(self) -> None:
        for order in await self.store.in_states(
            [S.ACKNOWLEDGED, S.PARTIALLY_FILLED, S.CANCEL_PENDING]
        ):
            start = utc(order.submitted_at or order.created_at)
            self._awaiting[order.client_order_id] = start + timedelta(
                seconds=self.config.no_fill_timeout
            )
        for order in await self.store.in_states([S.FILLED, S.PARTIALLY_FILLED]):
            if order.protective_status not in (
                "confirmed",
                "recovered",
                "flattened",
                "not_required",
            ):
                self._spawn(self.ensure_protection(order.client_order_id), "exec-protect-resume")

    async def _expire_stale_pending_signals(self) -> None:
        """Signals that were queued in memory when the process died are now stale."""
        cutoff = self.clock.now() - timedelta(seconds=self.config.max_signal_age_seconds)
        async with self.session_factory() as session:
            rows = await session.execute(
                select(Signal).where(Signal.status == "pending", Signal.client_order_id.is_(None))
            )
            expired = 0
            for sig in rows.scalars():
                if utc(sig.created_at) < cutoff:
                    sig.status = "expired"
                    sig.cancelled_reason = "stale_on_restart"
                    expired += 1
            await session.commit()
        if expired:
            logger.info("Expired %d stale pending signals on startup", expired)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    async def _transition(self, coid: str, state: OrderState, reason: str):
        return await self.store.transition(
            coid, state, event_id=local_event_id(coid, state, reason), reason=reason
        )

    async def _state(self, coid: str) -> OrderState | None:
        order = await self.store.get_by_client_id(coid)
        return OrderState(order.state) if order else None

    async def _cancel_signal(self, signal: Signal, reason: str, basis: Decimal | None = None):
        async with self.session_factory() as session:
            fresh = await session.get(Signal, signal.id)
            if fresh is None:
                return
            fresh.status = "cancelled"
            fresh.cancelled_reason = reason[:50]
            if basis is not None:
                fresh.delta_basis = basis
            await session.commit()

    async def _set_basis(self, signal_id, basis: Decimal) -> None:
        async with self.session_factory() as session:
            fresh = await session.get(Signal, signal_id)
            if fresh is not None:
                fresh.delta_basis = basis
                await session.commit()

    def _auth_failed(self, exc: Exception) -> None:
        self.governor.set_breaker("exchange_auth", f"authentication failed: {exc}")
        self._spawn(self._alert(f"CRITICAL: exchange authentication failed: {exc}"), "alert")

    async def _alert(self, text: str) -> None:
        if self.alerts is None:
            return
        try:
            await self.alerts.send_text(text)
        except Exception:
            logger.exception("alert delivery failed")

    async def _publish(self, event_type, payload, correlation_id=None) -> None:
        try:
            await self.bus.publish(
                Event.create(event_type, payload, "execution", correlation_id=correlation_id)
            )
        except Exception:
            logger.exception("publish %s failed", event_type)

    @staticmethod
    def round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
        if tick == 0:
            return value
        return (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick

    @staticmethod
    def compute_size(signal: Signal, contract_value: Decimal) -> Decimal:
        risk_amount = signal.account_size_assumed * signal.risk_pct / Decimal("100")
        stop_distance_pct = abs(signal.entry - signal.stop_loss) / signal.entry
        if stop_distance_pct == 0 or contract_value == 0:
            return Decimal("0")
        position_notional = risk_amount / stop_distance_pct
        return position_notional / (contract_value * signal.entry)
