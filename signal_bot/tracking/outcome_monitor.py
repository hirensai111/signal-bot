"""Monitors Delta position/order events and records actual outcomes."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from telegram.helpers import escape_markdown

from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import Signal, SignalOutcome

logger = logging.getLogger(__name__)

ORDER_HISTORY_WINDOW_HOURS = 24

EXIT_STATUS_MAP = {
    "sl_hit": "sl",
    "tp_hit": "tp",
    "manual_close": "closed_manual",
    "liquidation": "liquidated",
    "expired": "expired",
}


class OutcomeMonitor:
    """Consumes Delta WebSocket events to detect position closes and record outcomes."""

    def __init__(
        self,
        delta_events: asyncio.Queue,
        session_factory=None,
        telegram_notifier=None,
        governor=None,
        delta_client=None,
    ) -> None:
        self.delta_events = delta_events
        self.session_factory = session_factory or AsyncSessionLocal
        self.telegram = telegram_notifier
        self.governor = governor
        self.delta_client = delta_client

        self._shutdown = asyncio.Event()
        # product_id -> previous size (for close detection)
        self._position_sizes: dict[int, Decimal] = {}
        # product_id -> signal_id mapping for fast lookup after fill
        self._product_signal_map: dict[int, str] = {}

    def shutdown(self) -> None:
        logger.info("Outcome monitor shutdown requested.")
        self._shutdown.set()

    async def run(self) -> None:
        logger.info("Outcome monitor started (Delta-event driven).")
        summary_task = asyncio.create_task(self._daily_summary_loop())

        while not self._shutdown.is_set():
            try:
                event = await asyncio.wait_for(self.delta_events.get(), timeout=1.0)
            except TimeoutError:
                continue

            try:
                await self._handle_event(event)
            except Exception:
                logger.exception("Unhandled error processing delta event")

        summary_task.cancel()
        try:
            await summary_task
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # event routing
    # ------------------------------------------------------------------
    async def _handle_event(self, event: dict[str, Any]) -> None:
        channel = event.get("channel")
        if channel == "private_positions":
            await self._handle_position_event(event)
        elif channel == "private_orders":
            await self._handle_order_event(event)

    async def _handle_position_event(self, event: dict[str, Any]) -> None:
        data = event.get("data", {})
        product_id = data.get("product_id")
        if product_id is None:
            return

        size = Decimal(str(data.get("size", "0")))
        prev_size = self._position_sizes.get(product_id, Decimal("0"))
        self._position_sizes[product_id] = size

        # Detect close: non-zero -> zero
        if prev_size != 0 and size == 0:
            await self._process_position_close(product_id, data)

    async def _handle_order_event(self, event: dict[str, Any]) -> None:
        """Track order fills so we know which signal owns which product_id."""
        data = event.get("data", {})
        coid = data.get("client_order_id")
        if not coid or not coid.startswith("sig_"):
            return

        state = data.get("state", "")
        if state == "filled":
            product_id = data.get("product_id")
            if product_id is not None:
                self._product_signal_map[int(product_id)] = coid

    # ------------------------------------------------------------------
    # position close processing
    # ------------------------------------------------------------------
    async def _process_position_close(self, product_id: int, position_data: dict[str, Any]) -> None:
        async with self.session_factory() as session:
            signal = await self._resolve_signal(session, product_id, position_data)
            if signal is None:
                logger.critical("Orphan close: no matching signal for product_id=%s", product_id)
                return

            exit_price, exit_reason, exit_fees = await self._determine_exit(
                session, signal, product_id, position_data
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

            db_status = EXIT_STATUS_MAP.get(exit_reason, "closed_manual")
            signal.status = db_status
            await session.commit()

        logger.info(
            "Signal %s closed: reason=%s price=%s pnl_r=%s",
            signal.id,
            exit_reason,
            exit_price,
            pnl_r,
        )

        # Notify outside DB session
        if self.telegram:
            try:
                await self.telegram.notify_outcome(signal, outcome)
            except Exception:
                logger.exception("Telegram notify failed for signal %s", signal.id)

        # Update governor
        if self.governor:
            try:
                await self.governor.record_outcome(signal.id, pnl_r, won=pnl_r > 0)
            except Exception:
                logger.exception("Governor record_outcome failed for signal %s", signal.id)

    async def _resolve_signal(
        self, session, product_id: int, position_data: dict[str, Any]
    ) -> Signal | None:
        """Look up signal by client_order_id, delta_order_id, or symbol+status."""
        # 1. Try client_order_id from our in-memory map
        coid = self._product_signal_map.pop(product_id, None)
        if coid:
            result = await session.execute(select(Signal).where(Signal.client_order_id == coid))
            sig = result.scalar_one_or_none()
            if sig:
                return sig

        # 2. Try delta_order_id from position_data (if present)
        delta_oid = position_data.get("closing_order_id") or position_data.get("order_id")
        if delta_oid:
            result = await session.execute(
                select(Signal).where(Signal.delta_order_id == str(delta_oid))
            )
            sig = result.scalar_one_or_none()
            if sig:
                return sig

        # 3. Fallback: find triggered signal by symbol
        if self.delta_client is not None:
            binance_symbol = self.delta_client.symbol_map.resolve_by_product_id(product_id)
        else:
            binance_symbol = None

        if binance_symbol:
            result = await session.execute(
                select(Signal)
                .where(Signal.symbol == binance_symbol)
                .where(Signal.status == "triggered")
                .order_by(Signal.created_at.desc())
            )
            sig = result.scalar_one_or_none()
            if sig:
                return sig

        return None

    async def _determine_exit(
        self, session, signal: Signal, product_id: int, position_data: dict[str, Any]
    ) -> tuple[Decimal, str, Decimal]:
        """Best-effort determination of exit price, reason, and fees.

        Tries order history first; falls back to SL/TP heuristic.
        """
        exit_fees = Decimal("0")

        if self.delta_client is not None:
            try:
                exit_price, exit_reason, exit_fees = await self._query_order_history(
                    signal, product_id
                )
                if exit_price is not None:
                    return exit_price, exit_reason, exit_fees
            except Exception:
                logger.exception("Order history query failed for signal %s", signal.id)

        # Fallback: liquidation check
        liq_price = position_data.get("liquidation_price")
        if liq_price is not None:
            liq = Decimal(str(liq_price))
            if signal.side == "long" and liq > 0:
                # If we were liquidated, use liq price
                if signal.stop_loss and liq <= signal.stop_loss:
                    return liq, "liquidation", Decimal("0")
            elif signal.side == "short" and liq > 0:
                if signal.stop_loss and liq >= signal.stop_loss:
                    return liq, "liquidation", Decimal("0")

        # Fallback: heuristic — whichever level was closer to current
        # market (we don't have current price, so use SL/TP midpoint logic)
        # Default to manual_close with SL price
        return signal.stop_loss, "manual_close", Decimal("0")

    async def _query_order_history(
        self, signal: Signal, product_id: int
    ) -> tuple[Decimal | None, str | None, Decimal]:
        """Query Delta order history to find SL/TP fill."""
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=ORDER_HISTORY_WINDOW_HOURS)

        history = await self.delta_client.get_order_history(
            product_id=product_id,
            start=start.isoformat(),
            end=end.isoformat(),
        )
        orders = history.get("orders", []) if isinstance(history, dict) else []

        # Look for the entry order to find its ID, then look for child SL/TP
        entry_order_id = signal.delta_order_id
        if not entry_order_id:
            return None, None, Decimal("0")

        # Scan for filled orders that are NOT the entry order
        for order in orders:
            oid = str(order.get("id", ""))
            if oid == entry_order_id:
                continue
            if order.get("state") != "filled":
                continue

            # Check if this looks like a bracket leg
            avg_fill = order.get("average_fill_price") or order.get("price", "0")
            price = Decimal(str(avg_fill))

            if order.get("bracket_stop_loss_price") or order.get("stop_loss_price"):
                return price, "sl_hit", Decimal("0")
            if order.get("bracket_take_profit_price") or order.get("take_profit_price"):
                return price, "tp_hit", Decimal("0")

            # Heuristic: if fill price is closer to SL than TP
            if signal.stop_loss and signal.take_profit:
                sl_dist = abs(price - signal.stop_loss)
                tp_dist = abs(price - signal.take_profit)
                if sl_dist < tp_dist:
                    return price, "sl_hit", Decimal("0")
                else:
                    return price, "tp_hit", Decimal("0")

        return None, None, Decimal("0")

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

    # ------------------------------------------------------------------
    # daily summary
    # ------------------------------------------------------------------
    async def _daily_summary_loop(self) -> None:
        while not self._shutdown.is_set():
            now = datetime.now(timezone.utc)
            next_midnight = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            wait_seconds = (next_midnight - now).total_seconds()

            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=wait_seconds)
                return
            except TimeoutError:
                pass

            await self._post_daily_summary()

    async def _post_daily_summary(self) -> None:
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)

        async with self.session_factory() as session:
            result = await session.execute(
                select(SignalOutcome, Signal)
                .join(Signal, SignalOutcome.signal_id == Signal.id)
                .where(SignalOutcome.exit_ts >= yesterday)
                .where(SignalOutcome.exit_ts < today)
                .order_by(SignalOutcome.exit_ts)
            )
            rows = result.all()

        total = len(rows)
        if total == 0:
            text = "📊 Daily Summary\nNo trades yesterday."
        else:
            outcomes = [r.SignalOutcome for r in rows]
            wins = sum(1 for o in outcomes if o.pnl_r > 0)
            win_rate = wins / total * 100
            avg_r = sum(float(o.pnl_r) for o in outcomes) / total

            gross_profit = sum(float(o.pnl_r) for o in outcomes if o.pnl_r > 0)
            gross_loss = abs(sum(float(o.pnl_r) for o in outcomes if o.pnl_r < 0))
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

            equity = [0.0]
            for o in outcomes:
                equity.append(equity[-1] + float(o.pnl_r))
            peak = equity[0]
            max_dd = 0.0
            for val in equity:
                if val > peak:
                    peak = val
                dd = peak - val
                if dd > max_dd:
                    max_dd = dd

            text = (
                f"📊 Daily Summary\n\n"
                f"Total signals: {total}\n"
                f"Win rate: {wins}/{total} ({win_rate:.1f}%)\n"
                f"Avg R: {avg_r:.2f}\n"
                f"Profit factor: {profit_factor:.2f}\n"
                f"Max drawdown: {max_dd:.2f}R"
            )

        if self.telegram and self.telegram.application:
            try:
                await self.telegram.application.bot.send_message(
                    chat_id=self.telegram.chat_id,
                    text=escape_markdown(text, version=2),
                    parse_mode="MarkdownV2",
                )
            except Exception:
                logger.exception("Failed to send daily summary")
