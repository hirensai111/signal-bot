"""Reconciles Delta Exchange state with local DB signals.

Runs on startup and periodically every 5 minutes to detect missed fills,
orphan positions, and orphan orders.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select

from signal_bot.db.models import RiskState, Signal, SignalOutcome

logger = logging.getLogger(__name__)

RECONCILE_INTERVAL_SECONDS = 300  # 5 minutes
ORDER_HISTORY_WINDOW_HOURS = 24


class Reconciler:
    """Cross-checks Delta REST state against local signal records."""

    def __init__(
        self,
        delta_client,
        session_factory,
        telegram=None,
        governor=None,
    ) -> None:
        self.delta_client = delta_client
        self.session_factory = session_factory
        self.telegram = telegram
        self.governor = governor

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._has_run_once = asyncio.Event()

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
        logger.info("Reconciler stopped")

    async def wait_for_first_run(self) -> None:
        """Block until the first reconciliation has completed successfully."""
        await self._has_run_once.wait()

    # ------------------------------------------------------------------
    # main loop
    # ------------------------------------------------------------------
    async def _run_loop(self) -> None:
        # Run immediately on startup
        try:
            await self._reconcile(is_startup=True)
            self._has_run_once.set()
        except Exception:
            logger.exception("Startup reconciliation failed")
            # Retry every 10s until success so engine can eventually start
            while self._running and not self._has_run_once.is_set():
                await asyncio.sleep(10)
                try:
                    await self._reconcile(is_startup=True)
                    self._has_run_once.set()
                except Exception:
                    logger.exception("Startup reconciliation retry failed")

        while self._running:
            await asyncio.sleep(RECONCILE_INTERVAL_SECONDS)
            if not self._running:
                break
            try:
                await self._reconcile(is_startup=False)
            except Exception:
                logger.exception("Periodic reconciliation failed")

    # ------------------------------------------------------------------
    # core reconciliation logic
    # ------------------------------------------------------------------
    async def _reconcile(self, is_startup: bool) -> None:
        logger.info("Running reconciliation (startup=%s)", is_startup)

        # Fetch Delta truth
        positions_resp = await self.delta_client.get_open_positions()
        orders_resp = await self.delta_client.get_open_orders()

        delta_positions = _parse_positions(positions_resp)
        delta_orders = _parse_orders(orders_resp)

        async with self.session_factory() as session:
            # Load DB signals that we expect to be active
            result = await session.execute(
                select(Signal).where(
                    Signal.status.in_(["submitted", "triggered"])
                )
            )
            db_signals = {s.id: s for s in result.scalars().all()}

            # Build lookups by delta identifiers
            db_by_coid: dict[str, Signal] = {}
            db_by_delta_oid: dict[str, Signal] = {}
            db_by_symbol_triggered: dict[str, list[Signal]] = {}

            for sig in db_signals.values():
                if sig.client_order_id:
                    db_by_coid[sig.client_order_id] = sig
                if sig.delta_order_id:
                    db_by_delta_oid[sig.delta_order_id] = sig
                if sig.status == "triggered":
                    db_by_symbol_triggered.setdefault(sig.symbol, []).append(sig)

            # Case 1: signal in DB but no matching Delta position/order
            await self._check_missed_events(
                session, db_signals, delta_positions, delta_orders
            )

            # Case 2: Delta position with no linked signal
            for product_id, pos in delta_positions.items():
                delta_symbol = pos.get("product_symbol", "")
                binance_symbol = self.delta_client.symbol_map.get_binance_symbol(delta_symbol) if hasattr(self.delta_client, 'symbol_map') else None
                if not binance_symbol:
                    logger.warning("Cannot map delta symbol %s to binance", delta_symbol)
                    continue

                matching = [s for s in db_signals.values() if s.symbol == binance_symbol and s.status == "triggered"]
                if not matching:
                    await self._handle_orphan_position(session, pos)

            # Case 3: Delta open order with no linked signal
            for order in delta_orders.values():
                coid = order.get("client_order_id", "")
                if not coid.startswith("sig_"):
                    continue  # not our order
                if coid not in db_by_coid:
                    await self._handle_orphan_order(session, order)

            # Startup-only: rebuild open-positions counter from Delta truth
            if is_startup:
                await self._rebuild_open_positions(session, delta_positions)

            await session.commit()

        logger.info("Reconciliation complete")

    # ------------------------------------------------------------------
    # case 1: missed events
    # ------------------------------------------------------------------
    async def _check_missed_events(
        self,
        session,
        db_signals: dict,
        delta_positions: dict[int, dict],
        delta_orders: dict[str, dict],
    ) -> None:
        for sig in db_signals.values():
            if sig.status == "triggered":
                # Should have a matching open position
                if sig.delta_symbol:
                    product_id = self._resolve_product_id(sig.delta_symbol)
                    if product_id is not None and product_id not in delta_positions:
                        await self._handle_missed_close(session, sig, product_id)

            elif sig.status == "submitted":
                # Should have a matching open order
                if sig.delta_order_id and sig.delta_order_id not in delta_orders:
                    # Order may have filled or been cancelled
                    await self._handle_missed_fill(session, sig)

    async def _handle_missed_fill(self, session, signal: Signal) -> None:
        """Signal is 'submitted' but order is not open on Delta."""
        if not signal.delta_order_id:
            return

        logger.warning(
            "Missed fill: signal %s order %s not open on Delta",
            signal.id,
            signal.delta_order_id,
        )

        try:
            order = await self.delta_client.get_order_status(signal.delta_order_id)
            order_data = order.get("order", order)
            state = order_data.get("state", "")

            if state in ("cancelled", "rejected"):
                signal.status = "cancelled"
                signal.cancelled_reason = f"missed_{state}"
                logger.info("Signal %s was %s (missed event)", signal.id, state)
                return
            elif state == "filled":
                signal.status = "triggered"
                avg = order_data.get("average_fill_price") or order_data.get("price", "0")
                signal.delta_entry_actual = Decimal(str(avg))
                logger.info("Signal %s was filled (missed event)", signal.id)
                return
        except Exception:
            logger.exception("Order status fetch failed for %s", signal.delta_order_id)

        # Unknown — mark as cancelled so it doesn't block forever
        signal.status = "cancelled"
        signal.cancelled_reason = "missed_reconcile_unknown"
        logger.warning("Signal %s marked cancelled (unknown state)", signal.id)

    async def _handle_missed_close(self, session, signal: Signal, product_id: int) -> None:
        """Signal is 'triggered' but no open position on Delta."""
        logger.warning(
            "Missed close: signal %s product_id=%s position closed on Delta",
            signal.id,
            product_id,
        )

        exit_price, exit_reason, _ = await self._reconstruct_exit_from_history(
            signal, product_id
        )

        hold_minutes = self._compute_hold_minutes(signal)
        pnl_pct, pnl_r = self._compute_pnl(signal, exit_price)

        outcome = SignalOutcome(
            signal_id=signal.id,
            exit_price=exit_price,
            exit_ts=datetime.now(timezone.utc),
            pnl_pct=pnl_pct,
            pnl_r=pnl_r,
            hold_minutes=hold_minutes,
            exit_reason=exit_reason,
        )
        session.add(outcome)

        db_status = {"sl_hit": "sl", "tp_hit": "tp", "manual_close": "closed_manual", "liquidation": "liquidated"}.get(
            exit_reason, "closed_manual"
        )
        signal.status = db_status
        logger.info(
            "Reconstructed missed close for signal %s: %s @ %s (pnl_r=%s)",
            signal.id,
            exit_reason,
            exit_price,
            pnl_r,
        )

        if self.governor:
            try:
                await self.governor.record_outcome(signal.id, pnl_r, won=pnl_r > 0)
            except Exception:
                logger.exception("Governor record_outcome failed for missed close %s", signal.id)

    async def _reconstruct_exit_from_history(
        self, signal: Signal, product_id: int
    ) -> tuple[Decimal, str, Decimal]:
        """Best-effort reconstruction of exit from order history."""
        if self.delta_client is None:
            return signal.stop_loss, "manual_close", Decimal("0")

        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=ORDER_HISTORY_WINDOW_HOURS)

        try:
            history = await self.delta_client.get_order_history(
                product_id=product_id,
                start=start.isoformat(),
                end=end.isoformat(),
            )
            orders = history.get("orders", []) if isinstance(history, dict) else []

            for order in orders:
                if order.get("state") != "filled":
                    continue
                oid = str(order.get("id", ""))
                if oid == signal.delta_order_id:
                    continue

                avg_fill = order.get("average_fill_price") or order.get("price", "0")
                price = Decimal(str(avg_fill))

                if order.get("bracket_stop_loss_price") or order.get("stop_loss_price"):
                    return price, "sl_hit", Decimal("0")
                if order.get("bracket_take_profit_price") or order.get("take_profit_price"):
                    return price, "tp_hit", Decimal("0")

                if signal.stop_loss and signal.take_profit:
                    sl_dist = abs(price - signal.stop_loss)
                    tp_dist = abs(price - signal.take_profit)
                    if sl_dist < tp_dist:
                        return price, "sl_hit", Decimal("0")
                    else:
                        return price, "tp_hit", Decimal("0")
        except Exception:
            logger.exception("Order history fetch failed for missed close %s", signal.id)

        return signal.stop_loss, "manual_close", Decimal("0")

    # ------------------------------------------------------------------
    # case 2 & 3: orphans
    # ------------------------------------------------------------------
    async def _handle_orphan_position(self, session, position: dict[str, Any]) -> None:
        symbol = position.get("product_symbol", "unknown")
        size = position.get("size", "?")
        entry = position.get("entry_price", "?")

        msg = (
            f"🔴 CRITICAL: Orphan position on Delta\n"
            f"Symbol: {symbol}\n"
            f"Size: {size}\n"
            f"Entry: {entry}\n"
            f"No matching signal in DB. Manual close or adopt required."
        )
        logger.critical(msg)

        if self.telegram and self.telegram.application:
            try:
                await self.telegram.application.bot.send_message(
                    chat_id=self.telegram.chat_id,
                    text=msg,
                )
            except Exception:
                logger.exception("Failed to send orphan position alert")

    async def _handle_orphan_order(self, session, order: dict[str, Any]) -> None:
        coid = order.get("client_order_id", "unknown")
        symbol = order.get("product_symbol", "unknown")
        state = order.get("state", "?")

        msg = (
            f"🔴 CRITICAL: Orphan order on Delta\n"
            f"ClientOrderID: {coid}\n"
            f"Symbol: {symbol}\n"
            f"State: {state}\n"
            f"No matching signal in DB. Cancel or adopt required."
        )
        logger.critical(msg)

        if self.telegram and self.telegram.application:
            try:
                await self.telegram.application.bot.send_message(
                    chat_id=self.telegram.chat_id,
                    text=msg,
                )
            except Exception:
                logger.exception("Failed to send orphan order alert")

    # ------------------------------------------------------------------
    # startup helpers
    # ------------------------------------------------------------------
    async def _rebuild_open_positions(
        self, session, delta_positions: dict[int, dict]
    ) -> None:
        state = await session.get(RiskState, 1)
        if state is None:
            state = RiskState(id=1)
            session.add(state)

        state.current_open_positions = len(delta_positions)
        logger.info(
            "Rebuilt current_open_positions=%d from Delta truth",
            state.current_open_positions,
        )

    def _resolve_product_id(self, delta_symbol: str) -> Optional[int]:
        """Resolve a Delta symbol to product_id via symbol_map."""
        try:
            return self.delta_client.symbol_map._products.get(delta_symbol, {}).get("id")
        except Exception:
            return None

    @staticmethod
    def _compute_hold_minutes(signal: Signal) -> int:
        created = signal.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return int((datetime.now(timezone.utc) - created).total_seconds() / 60)

    @staticmethod
    def _compute_pnl(signal: Signal, exit_price: Decimal) -> tuple[Decimal, Decimal]:
        entry_f = float(signal.entry)
        stop_f = float(signal.stop_loss)
        exit_f = float(exit_price)

        if entry_f == 0 or entry_f == stop_f:
            return Decimal("0"), Decimal("0")

        if signal.side == "long":
            pnl_pct = Decimal(str((exit_f - entry_f) / entry_f * 100))
            pnl_r = Decimal(str((exit_f - entry_f) / (entry_f - stop_f)))
        else:
            pnl_pct = Decimal(str((entry_f - exit_f) / entry_f * 100))
            pnl_r = Decimal(str((entry_f - exit_f) / (stop_f - entry_f)))

        return pnl_pct, pnl_r


# ---------------------------------------------------------------------------
# parsing helpers
# ---------------------------------------------------------------------------

def _parse_positions(resp: dict) -> dict[int, dict]:
    """Return {product_id: position_dict}."""
    out: dict[int, dict] = {}
    positions = resp.get("positions", []) if isinstance(resp, dict) else resp
    for pos in positions:
        pid = pos.get("product_id")
        if pid is not None:
            out[int(pid)] = pos
    return out


def _parse_orders(resp: dict) -> dict[str, dict]:
    """Return {order_id: order_dict}."""
    out: dict[str, dict] = {}
    orders = resp.get("orders", []) if isinstance(resp, dict) else resp
    for order in orders:
        oid = str(order.get("id", ""))
        if oid:
            out[oid] = order
    return out
