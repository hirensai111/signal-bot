"""Reconciles venue truth with the local order store.

Runs at startup (the execution engine waits for the first successful pass),
periodically, and on demand after the private stream reconnects.  The venue is
the source of truth for *exposure*; the local store is the source of truth for
*intent*.  Rules:

* ``submitting`` / ``submission_unknown`` / ``reconcile_required``: look the
  order up by client id.  Found → adopt the venue state.  Not found → keep the
  order unresolved (trading blocked) until ``unknown_grace_seconds`` have
  elapsed *and* the venue shows no position on that symbol; only then mark it
  ``cancelled`` (``not_found_on_exchange``).  Unknown is never treated as
  cancelled immediately.
* ``pending`` orders older than the grace window were never sent (the
  ``submitting`` transition is persisted before any network call) → cancelled.
* open orders: refresh from the venue (catches missed fills/cancels).
* ``filled`` orders without a venue position: the close was missed →
  reconstruct it from venue fills.
* ``filled`` orders whose venue position lacks a stop → protective recovery.
* venue positions with no local owner → orphan exposure: alert and (by default)
  block new orders until resolved by an operator.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import timedelta

from signal_bot.core.clock import SYSTEM_CLOCK, Clock, utc
from signal_bot.events.envelope import Event, EventType, deterministic_event_id
from signal_bot.execution.adapter import AuthenticationFailed, ExchangeError
from signal_bot.execution.engine import CLIENT_ORDER_PREFIX
from signal_bot.execution.order_store import OrderStore, local_event_id
from signal_bot.execution.orders import OrderState
from signal_bot.observability import metrics

logger = logging.getLogger(__name__)

S = OrderState
RECONCILE_INTERVAL_SECONDS = 300


class Reconciler:
    def __init__(
        self,
        adapter,
        order_store: OrderStore,
        governor=None,
        telegram=None,
        outcome_monitor=None,
        engine=None,
        bus=None,
        clock: Clock = SYSTEM_CLOCK,
        interval: float = RECONCILE_INTERVAL_SECONDS,
        unknown_grace_seconds: float = 300.0,
        block_on_orphans: bool = True,
        retry_delay: float = 10.0,
        trailing=None,
    ) -> None:
        self.adapter = adapter
        self.store = order_store
        self.governor = governor
        self.telegram = telegram
        self.outcome_monitor = outcome_monitor
        self.engine = engine
        self.bus = bus
        self.clock = clock
        self.interval = interval
        self.unknown_grace = timedelta(seconds=unknown_grace_seconds)
        self.block_on_orphans = block_on_orphans
        self.retry_delay = retry_delay
        # TrailingStopManager: re-derives trailing stops and runs overdue time stops after
        # every pass (on restart this restores stops the process missed while down).
        self.trailing = trailing

        self._running = False
        self._task: asyncio.Task | None = None
        self._has_run_once = asyncio.Event()
        self._lock = asyncio.Lock()
        self.last_run_at = None
        self.last_ok = False
        self.last_discrepancies: list[dict] = []
        self.runs = 0

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="reconciler")
        logger.info("Reconciler started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def wait_for_first_run(self) -> None:
        await self._has_run_once.wait()

    async def _run_loop(self) -> None:
        while self._running and not self._has_run_once.is_set():
            if await self.reconcile_now(reason="startup"):
                break
            await asyncio.sleep(self.retry_delay)
        while self._running:
            await asyncio.sleep(self.interval)
            if self._running:
                await self.reconcile_now(reason="periodic")

    async def reconcile_now(self, reason: str = "manual") -> bool:
        async with self._lock:
            start = time.perf_counter()
            try:
                discrepancies = await self._reconcile(is_startup=not self._has_run_once.is_set())
            except Exception:
                logger.exception("Reconciliation (%s) failed", reason)
                metrics.RECONCILIATION_FAILURES.inc()
                self.last_ok = False
                return False
            finally:
                metrics.RECONCILIATION_DURATION.observe(time.perf_counter() - start)
            self.runs += 1
            self.last_ok = True
            self.last_run_at = self.clock.now()
            metrics.RECONCILIATION_LAST_SUCCESS.set(time.time())
            self.last_discrepancies = discrepancies
            self._has_run_once.set()
            await self._publish(
                EventType.RECONCILIATION_COMPLETED,
                {"reason": reason, "discrepancies": len(discrepancies)},
            )
            logger.info(
                "Reconciliation (%s) complete: %d discrepancies", reason, len(discrepancies)
            )
            return True

    # ------------------------------------------------------------------
    # core
    # ------------------------------------------------------------------
    async def _reconcile(self, is_startup: bool) -> list[dict]:
        try:
            positions = await self.adapter.positions()
            venue_open = await self.adapter.open_orders()
        except AuthenticationFailed as exc:
            if self.governor is not None:
                self.governor.set_breaker("exchange_auth", f"authentication failed: {exc}")
            raise
        if self.governor is not None:
            self.governor.clear_breaker("exchange_auth")
            self.governor.observe_exchange_positions(len(positions))
        pos_by_symbol = {p.symbol: p for p in positions}
        found: list[dict] = []
        now = self.clock.now()

        # 1. in-flight / unresolved orders
        inflight = await self.store.in_states(
            [
                S.PENDING,
                S.SUBMITTING,
                S.SUBMISSION_UNKNOWN,
                S.RECONCILIATION_REQUIRED,
                S.ACKNOWLEDGED,
                S.PARTIALLY_FILLED,
                S.CANCEL_PENDING,
            ]
        )
        for order in inflight:
            coid = order.client_order_id
            state = S(order.state)
            age = now - utc(order.created_at)
            if state == S.PENDING:
                if age > self.unknown_grace:
                    await self._transition(coid, S.CANCELLED, "abandoned_before_submit")
                continue
            try:
                venue_order = await self.adapter.get_order(coid)
            except ExchangeError as exc:
                logger.warning("Lookup of %s failed during reconcile: %s", coid, exc)
                if state in (S.SUBMITTING, S.SUBMISSION_UNKNOWN, S.RECONCILIATION_REQUIRED):
                    self._block(coid, f"lookup failed: {exc}")
                continue

            if venue_order is not None:
                result = await self.store.apply_venue_update(
                    venue_order,
                    event_id=deterministic_event_id(
                        "reconcile",
                        coid,
                        venue_order.state,
                        venue_order.filled_size,
                        venue_order.updated_at.isoformat(),
                    ),
                )
                if result.applied:
                    found.append(self._discrepancy("missed_order_update", coid, result))
                new_state = S(result.order.state) if result.order is not None else state
                if new_state not in (S.SUBMISSION_UNKNOWN, S.RECONCILIATION_REQUIRED):
                    self._unblock(coid)
                if new_state in (S.FILLED, S.PARTIALLY_FILLED) and self.engine is not None:
                    await self.engine.ensure_protection(coid)
                continue

            # venue has no record of this client id
            if state in (S.SUBMITTING, S.SUBMISSION_UNKNOWN, S.RECONCILIATION_REQUIRED):
                if age > self.unknown_grace and order.symbol not in pos_by_symbol:
                    await self._transition(coid, S.CANCELLED, "not_found_on_exchange")
                    self._unblock(coid)
                    found.append({"kind": "unknown_resolved_not_found", "order": coid})
                else:
                    if state == S.SUBMITTING and is_startup:
                        # crashed between persisting intent and hearing back
                        await self._transition(coid, S.SUBMISSION_UNKNOWN, "crash_recovery")
                    self._block(coid, "not yet visible on venue")
            else:
                found.append({"kind": "open_order_missing_on_venue", "order": coid})
                metrics.RECONCILIATION_DISCREPANCIES.labels("open_order_missing").inc()

        # 2. filled orders must have venue exposure (and protection)
        owned_symbols: set[str] = set()
        unprotected = 0
        for order in await self.store.in_states([S.FILLED, S.PARTIALLY_FILLED]):
            owned_symbols.add(order.symbol)
            pos = pos_by_symbol.get(order.symbol)
            if pos is None:
                if S(order.state) == S.FILLED and self.outcome_monitor is not None:
                    metrics.RECONCILIATION_DISCREPANCIES.labels("missed_close").inc()
                    found.append({"kind": "missed_close", "order": order.client_order_id})
                    await self.outcome_monitor.reconstruct_close(order)
                continue
            if not pos.has_stop_loss and order.protective_status != "flattened":
                unprotected += 1
                metrics.RECONCILIATION_DISCREPANCIES.labels("missing_protective").inc()
                found.append({"kind": "missing_protective", "order": order.client_order_id})
                if self.engine is not None:
                    await self.engine.ensure_protection(order.client_order_id, recheck=True)
            elif (
                order.trail_mode == "stepped"
                and pos.stop_loss_price is not None
                and order.current_stop_price is not None
                and pos.stop_loss_price != order.current_stop_price
            ):
                metrics.RECONCILIATION_DISCREPANCIES.labels("trailing_stop_mismatch").inc()
                found.append(
                    {
                        "kind": "trailing_stop_mismatch",
                        "order": order.client_order_id,
                        "venue_stop": str(pos.stop_loss_price),
                        "recorded_stop": str(order.current_stop_price),
                    }
                )

        metrics.POSITIONS_UNPROTECTED.set(unprotected)
        for order in await self.store.in_states([S.RECONCILIATION_REQUIRED, S.SUBMISSION_UNKNOWN]):
            owned_symbols.add(order.symbol)

        # 3. orphan exposure on the venue
        for symbol, pos in pos_by_symbol.items():
            name = f"orphan_position:{symbol}"
            if symbol in owned_symbols:
                if self.governor is not None:
                    self.governor.clear_breaker(name)
                continue
            metrics.RECONCILIATION_DISCREPANCIES.labels("orphan_position").inc()
            found.append({"kind": "orphan_position", "symbol": symbol, "size": str(pos.size)})
            await self._alert(
                f"CRITICAL: orphan position on venue: {symbol} size={pos.size}. "
                "Adopt or close manually."
            )
            if self.block_on_orphans and self.governor is not None:
                self.governor.set_breaker(name, f"untracked position size={pos.size}")
        if self.governor is not None:
            for name in list(self.governor.breakers):
                if (
                    name.startswith("orphan_position:")
                    and name.split(":", 1)[1] not in pos_by_symbol
                ):
                    self.governor.clear_breaker(name)

        # 4. venue orders carrying our prefix that we have no record of
        for vo in venue_open:
            if not vo.client_order_id.startswith(CLIENT_ORDER_PREFIX):
                continue
            if await self.store.get_by_client_id(vo.client_order_id) is None:
                metrics.RECONCILIATION_DISCREPANCIES.labels("orphan_order").inc()
                found.append({"kind": "orphan_order", "order": vo.client_order_id})
                await self._alert(f"CRITICAL: orphan order on venue: {vo.client_order_id}")

        for d in found:
            await self._publish(EventType.RECONCILIATION_DISCREPANCY, d)
        if self.trailing is not None:
            # Venue stops looser than the derived trailing level are amended, tighter ones
            # adopted, and overdue time stops executed.
            await self.trailing.sync_all("startup" if is_startup else "reconcile")
        return found

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _discrepancy(self, kind: str, coid: str, result) -> dict:
        metrics.RECONCILIATION_DISCREPANCIES.labels(kind).inc()
        return {
            "kind": kind,
            "order": coid,
            "from": str(result.from_state),
            "to": str(result.to_state),
        }

    def _block(self, coid: str, reason: str) -> None:
        if self.governor is not None:
            self.governor.set_breaker(f"unresolved_order:{coid}", reason)

    def _unblock(self, coid: str) -> None:
        if self.governor is not None:
            self.governor.clear_breaker(f"unresolved_order:{coid}")

    async def _transition(self, coid: str, state: OrderState, reason: str) -> None:
        await self.store.transition(
            coid, state, event_id=local_event_id(coid, "reconcile", state, reason), reason=reason
        )

    async def _alert(self, text: str) -> None:
        logger.critical(text)
        if self.telegram is None:
            return
        try:
            await self.telegram.send_text(text)
        except Exception:
            logger.exception("alert delivery failed")

    async def _publish(self, event_type, payload: dict) -> None:
        if self.bus is None:
            return
        try:
            await self.bus.publish(Event.create(event_type, payload, source="reconciler"))
        except Exception:
            logger.exception("publish failed")
