"""Detects position exits and records each trade's outcome exactly once.

Two records are written per closed trade, in **one** transaction together with
the risk-state update:

* ``signal_outcomes`` — signal quality, kept for the daily summary/Telegram.
  ``pnl_r`` is the *realised* R (net of fees, slippage and funding).
* ``trade_results`` — execution quality: intended vs actual entry, slippage,
  fees, funding, fill ratio, the signal's own R (``signal_r``, measured at its
  intended levels) and the realised R.  The gap between the two is the cost of
  execution.

The ``signal_outcomes`` primary key makes the close idempotent: a duplicated
exit fill, a redelivered position update, or the reconciler reconstructing the
same close cannot double-count PnL or the loss streak.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from telegram.helpers import escape_markdown

from signal_bot.core.clock import SYSTEM_CLOCK, Clock, utc
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import Fill, Order, Signal, SignalOutcome, TradeResult
from signal_bot.events.bus import EventBus, Subscription
from signal_bot.events.envelope import Event, EventType
from signal_bot.execution.adapter import FillPurpose, fill_from_payload
from signal_bot.execution.order_store import OrderStore, local_event_id
from signal_bot.execution.orders import OrderState
from signal_bot.observability import metrics
from signal_bot.observability.logging import bind

logger = logging.getLogger(__name__)

EXIT_STATUS_MAP = {
    "sl_hit": "sl",
    "trail_hit": "trail",
    "time_stop": "time_stop",
    "tp_hit": "tp",
    "manual_close": "closed_manual",
    "liquidation": "liquidated",
    "expired": "expired",
}

_PURPOSE_REASON = {
    FillPurpose.STOP_LOSS: "sl_hit",
    FillPurpose.TAKE_PROFIT: "tp_hit",
    FillPurpose.CLOSE: "manual_close",
    FillPurpose.LIQUIDATION: "liquidation",
}

EXIT_PURPOSES = {str(p) for p in FillPurpose if p != FillPurpose.ENTRY}


class OutcomeMonitor:
    def __init__(
        self,
        bus: EventBus,
        order_store: OrderStore,
        adapter=None,
        session_factory=None,
        telegram_notifier=None,
        governor=None,
        clock: Clock = SYSTEM_CLOCK,
        exit_fill_retries: int = 3,
        exit_fill_retry_delay: float = 1.0,
    ) -> None:
        self.bus = bus
        self.store = order_store
        self.adapter = adapter
        self.session_factory = session_factory or AsyncSessionLocal
        self.telegram = telegram_notifier
        self.governor = governor
        self.clock = clock
        self.exit_fill_retries = exit_fill_retries
        self.exit_fill_retry_delay = exit_fill_retry_delay
        self._shutdown = asyncio.Event()
        self._subscription: Subscription | None = None
        self._position_sizes: dict[str, Decimal] = {}
        self.closed_trades = 0

    def subscribe(self) -> Subscription:
        if self._subscription is None:
            self._subscription = self.bus.subscribe(
                "outcomes", [EventType.EXCHANGE_FILL, EventType.EXCHANGE_POSITION_UPDATE]
            )
        return self._subscription

    def shutdown(self) -> None:
        self._shutdown.set()

    async def run(self) -> None:
        sub = self.subscribe()
        logger.info("Outcome monitor started.")
        summary_task = asyncio.create_task(self._daily_summary_loop())
        try:
            while not self._shutdown.is_set():
                event = await sub.get(timeout=1.0)
                if event is None:
                    continue
                try:
                    with bind(event_id=event.event_id, correlation_id=event.correlation_id):
                        await self.handle_event(event)
                except Exception:
                    metrics.EVENT_PROCESSING_FAILURES.labels("outcomes").inc()
                    logger.exception("Outcome monitor failed on %s", event.event_type)
                finally:
                    await sub.ack(event)
        finally:
            summary_task.cancel()
            try:
                await summary_task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------
    # event handling
    # ------------------------------------------------------------------
    async def handle_event(self, event: Event) -> None:
        if event.event_type == EventType.EXCHANGE_FILL:
            fill = fill_from_payload(event.payload)
            if fill.purpose == FillPurpose.ENTRY or not fill.client_order_id:
                return
            await self.store.record_fill(fill, event.payload.get("venue", "unknown"))
            await self._maybe_close(fill.client_order_id)
        elif event.event_type == EventType.EXCHANGE_POSITION_UPDATE:
            symbol = event.payload.get("symbol")
            size = Decimal(str(event.payload.get("size") or "0"))
            prev = self._position_sizes.get(symbol)
            self._position_sizes[symbol] = size
            if size == 0 and prev not in (None, Decimal("0")):
                await self.on_flat(symbol)

    async def _maybe_close(self, coid: str) -> bool:
        order = await self.store.get_by_client_id(coid)
        if order is None or order.state != OrderState.FILLED:
            return False
        fills = await self.store.fills_for(coid)
        exit_qty = sum((f.qty for f in fills if f.purpose in EXIT_PURPOSES), Decimal("0"))
        if exit_qty < (order.filled_size or 0):
            return False  # partial exit; position still open
        return await self.close_trade(order)

    async def on_flat(self, symbol: str) -> None:
        """The venue reports a flat position: close every filled order on that symbol."""
        orders = [o for o in await self.store.in_states([OrderState.FILLED]) if o.symbol == symbol]
        if not orders:
            # Usually the close was already recorded from its exit fills; true orphan
            # exposure is detected by the reconciler against venue positions.
            logger.debug("Flat position on %s with no open local order", symbol)
            return
        for order in orders:
            await self.reconstruct_close(order)

    async def reconstruct_close(self, order: Order) -> bool:
        """Fetch exit fills from the venue if the stream didn't deliver them, then close."""
        fills = await self.store.fills_for(order.client_order_id)
        if not any(f.purpose in EXIT_PURPOSES for f in fills) and self.adapter is not None:
            for attempt in range(self.exit_fill_retries):
                try:
                    venue_fills = await self.adapter.exit_fills(
                        order.symbol, order.client_order_id, utc(order.submitted_at)
                    )
                except Exception:
                    logger.exception("exit_fills lookup failed for %s", order.client_order_id)
                    venue_fills = []
                for vf in venue_fills:
                    await self.store.record_fill(vf, order.venue)
                if venue_fills:
                    break
                if attempt < self.exit_fill_retries - 1:
                    await asyncio.sleep(self.exit_fill_retry_delay)
        return await self.close_trade(order)

    # ------------------------------------------------------------------
    # the exactly-once close
    # ------------------------------------------------------------------
    async def close_trade(self, order: Order) -> bool:
        coid = order.client_order_id
        with bind(
            order_id=coid, signal_id=str(order.signal_id), correlation_id=order.correlation_id
        ):
            async with self.session_factory() as session:
                signal = await session.get(Signal, order.signal_id)
                existing = await session.get(SignalOutcome, order.signal_id)
                if signal is None:
                    return False
                if existing is not None:
                    await self._ensure_closed(order, existing.exit_reason)
                    return False

                rows = (
                    await session.execute(select(Fill).where(Fill.client_order_id == coid))
                ).scalars()
                fills = list(rows)
                result = compute_trade_result(order, fills, self._funding(order))
                closed_at = result["closed_at"] or self.clock.now()
                outcome = SignalOutcome(
                    signal_id=signal.id,
                    exit_price=result["exit_price"],
                    exit_ts=closed_at,
                    pnl_pct=result["pnl_pct"],
                    pnl_r=result["realized_r"],
                    hold_minutes=_hold_minutes(order, closed_at),
                    exit_reason=result["exit_reason"],
                )
                trade = TradeResult(
                    signal_id=signal.id,
                    venue=order.venue,
                    symbol=order.symbol,
                    side=signal.side,
                    intended_entry=order.intended_entry,
                    actual_entry=result["entry_price"],
                    exit_price=result["exit_price"],
                    exit_reason=result["exit_reason"],
                    size_requested=order.size,
                    size_filled=order.filled_size,
                    gross_pnl=result["gross_pnl"],
                    fees=result["fees"],
                    funding=result["funding"],
                    net_pnl=result["net_pnl"],
                    signal_r=result["signal_r"],
                    realized_r=result["realized_r"],
                    entry_slippage_bps=result["entry_slippage_bps"],
                    closed_at=closed_at,
                )
                session.add_all([outcome, trade])
                signal.status = EXIT_STATUS_MAP.get(result["exit_reason"], "closed_manual")
                tripped = False
                if self.governor is not None and hasattr(self.governor, "apply_outcome"):
                    tripped = await self.governor.apply_outcome(
                        session, signal.id, result["realized_r"], won=result["net_pnl"] > 0
                    )
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    return False

            self.closed_trades += 1
            if self.governor is not None and hasattr(self.governor, "after_outcome"):
                await self.governor.after_outcome(tripped)
            await self._ensure_closed(order, result["exit_reason"])
            logger.info(
                "Trade closed: %s %s reason=%s net=%s realized_r=%s signal_r=%s",
                order.symbol,
                coid,
                result["exit_reason"],
                result["net_pnl"],
                result["realized_r"],
                result["signal_r"],
            )
            await self._publish_close(order, result)
            if self.telegram is not None:
                try:
                    await self.telegram.notify_outcome(signal, outcome)
                except Exception:
                    logger.exception("Telegram notify failed for %s", signal.id)
            return True

    async def _ensure_closed(self, order: Order, reason: str) -> None:
        await self.store.transition(
            order.client_order_id,
            OrderState.CLOSED,
            event_id=local_event_id(order.client_order_id, "closed"),
            reason=reason,
            signal_status=EXIT_STATUS_MAP.get(reason, "closed_manual"),
        )

    def _funding(self, order: Order) -> Decimal:
        fn = getattr(self.adapter, "funding_between", None)
        if fn is None or order.submitted_at is None:
            return Decimal("0")
        try:
            return Decimal(str(fn(order.symbol, utc(order.submitted_at), self.clock.now())))
        except Exception:
            return Decimal("0")

    async def _publish_close(self, order: Order, result: dict[str, Any]) -> None:
        try:
            await self.bus.publish(
                Event.create(
                    EventType.POSITION_CLOSED,
                    {"client_order_id": order.client_order_id, "symbol": order.symbol, **result},
                    source="outcome_monitor",
                    correlation_id=order.correlation_id,
                )
            )
        except Exception:
            logger.exception("POSITION_CLOSED publish failed")

    # ------------------------------------------------------------------
    # daily summary
    # ------------------------------------------------------------------
    async def _daily_summary_loop(self) -> None:
        while not self._shutdown.is_set():
            now = datetime.now(timezone.utc)
            next_midnight = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=(next_midnight - now).total_seconds()
                )
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
        text = format_daily_summary([r.SignalOutcome for r in rows])
        if self.telegram and getattr(self.telegram, "application", None):
            try:
                await self.telegram.application.bot.send_message(
                    chat_id=self.telegram.chat_id,
                    text=escape_markdown(text, version=2),
                    parse_mode="MarkdownV2",
                )
            except Exception:
                logger.exception("Failed to send daily summary")


# ---------------------------------------------------------------------------
# pure helpers (unit-tested directly)
# ---------------------------------------------------------------------------


def compute_trade_result(order: Order, fills: list[Fill], funding: Decimal) -> dict[str, Any]:
    entry_fills = [f for f in fills if f.purpose == str(FillPurpose.ENTRY)]
    exit_fills = [f for f in fills if f.purpose in EXIT_PURPOSES]
    qty = Decimal(str(order.filled_size or 0))
    cv = Decimal(str(order.contract_value or 1))
    entry = Decimal(str(order.avg_fill_price or order.intended_entry))
    direction = Decimal("1") if order.side == "buy" else Decimal("-1")

    exit_qty = sum((Decimal(str(f.qty)) for f in exit_fills), Decimal("0"))
    if exit_qty > 0:
        exit_price = sum(Decimal(str(f.qty)) * Decimal(str(f.price)) for f in exit_fills) / exit_qty
        dominant = max(exit_fills, key=lambda f: f.qty).purpose
        exit_reason = _PURPOSE_REASON.get(FillPurpose(dominant)) or _nearest_level(
            order, exit_price
        )
        exit_reason = _refine_reason(order, exit_reason)
        closed_at = max(utc(f.occurred_at) for f in exit_fills)
    else:
        # No exit fill could be found: assume the worst plausible exit (the stop).
        exit_price = Decimal(str(order.stop_loss_price))
        exit_reason = "manual_close"
        closed_at = None

    closed_qty = min(qty, exit_qty) if exit_qty > 0 else qty
    gross = direction * closed_qty * cv * (exit_price - entry)
    fees = sum((Decimal(str(f.fee)) for f in entry_fills + exit_fills), Decimal("0"))
    if not entry_fills:
        fees += Decimal(str(order.fees or 0))
    net = gross - fees + funding

    intended = Decimal(str(order.intended_entry))
    risk_per_unit = abs(intended - Decimal(str(order.stop_loss_price)))
    intended_risk = Decimal(str(order.size)) * cv * risk_per_unit
    realized_r = net / intended_risk if intended_risk > 0 else Decimal("0")

    if exit_reason == "sl_hit":
        level = Decimal(str(order.stop_loss_price))
    elif exit_reason == "trail_hit":
        level = Decimal(str(order.current_stop_price))
    elif exit_reason == "tp_hit" and order.take_profit_price is not None:
        level = Decimal(str(order.take_profit_price))
    else:
        level = exit_price
    signal_r = direction * (level - intended) / risk_per_unit if risk_per_unit > 0 else Decimal("0")
    slippage_bps = direction * (entry - intended) / intended * Decimal("10000") if intended else 0
    notional = qty * cv * entry
    pnl_pct = net / notional * 100 if notional > 0 else Decimal("0")

    q = Decimal("0.00000001")
    return {
        "entry_price": entry,
        "exit_price": exit_price.quantize(q),
        "exit_reason": exit_reason,
        "gross_pnl": gross.quantize(q),
        "fees": fees.quantize(q),
        "funding": funding.quantize(q),
        "net_pnl": net.quantize(q),
        "realized_r": realized_r.quantize(q),
        "signal_r": signal_r.quantize(q),
        "entry_slippage_bps": Decimal(slippage_bps).quantize(q),
        "fill_ratio": (qty / Decimal(str(order.size))).quantize(q) if order.size else Decimal("0"),
        "pnl_pct": pnl_pct.quantize(q),
        "closed_at": closed_at,
    }


def _nearest_level(order: Order, price: Decimal) -> str:
    stop = order.current_stop_price or order.stop_loss_price
    sl_dist = abs(price - Decimal(str(stop)))
    if order.take_profit_price is None:
        return "sl_hit"
    tp_dist = abs(price - Decimal(str(order.take_profit_price)))
    return "sl_hit" if sl_dist < tp_dist else "tp_hit"


def _refine_reason(order: Order, reason: str) -> str:
    """Name trailed-stop and time-stop exits (research reports them separately)."""
    hint = getattr(order, "exit_reason_hint", None)
    if reason == "manual_close" and hint in ("time_stop", "trail_hit"):
        return hint
    if (
        reason == "sl_hit"
        and getattr(order, "trail_mode", "none") in ("stepped", "native")
        and order.current_stop_price is not None
        and Decimal(str(order.current_stop_price)) != Decimal(str(order.stop_loss_price))
    ):
        return "trail_hit"
    return reason


def _hold_minutes(order: Order, closed_at: datetime) -> int:
    start = utc(order.submitted_at or order.created_at)
    return max(0, int((utc(closed_at) - start).total_seconds() / 60))


def format_daily_summary(outcomes: list[SignalOutcome]) -> str:
    total = len(outcomes)
    if total == 0:
        return "📊 Daily Summary\nNo trades yesterday."
    wins = sum(1 for o in outcomes if o.pnl_r > 0)
    avg_r = sum(float(o.pnl_r) for o in outcomes) / total
    gross_profit = sum(float(o.pnl_r) for o in outcomes if o.pnl_r > 0)
    gross_loss = abs(sum(float(o.pnl_r) for o in outcomes if o.pnl_r < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    equity, peak, max_dd = 0.0, 0.0, 0.0
    for o in outcomes:
        equity += float(o.pnl_r)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return (
        f"📊 Daily Summary\n\n"
        f"Total signals: {total}\n"
        f"Win rate: {wins}/{total} ({wins / total * 100:.1f}%)\n"
        f"Avg R: {avg_r:.2f}\n"
        f"Profit factor: {profit_factor:.2f}\n"
        f"Max drawdown: {max_dd:.2f}R"
    )
