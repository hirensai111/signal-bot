"""Trailing stops and time stops for open positions.

**Stepped trailing (default, ``trail_mode="stepped"``).**  The protective stop is
always a real order on the exchange, so the position stays protected if this
process dies.  After every completed bar the manager recomputes the stop exactly
as the research fill model does: the best price is the highest high (lowest low
for shorts) of the bars completed since the entry bar opened, the trailed level is
that best price minus the trail distance, and the stop is the tighter of the
trailed level and the initial stop.  It never loosens.  If the new level is at
least one tick tighter than the stop on the exchange, the exchange stop is amended.

Levels are computed in the *reference* price space (the Binance candles the
strategy reads) and moved to the venue with the basis fixed at entry
(``Order.price_basis``), the same translation the engine used for the entry and
the initial stop.

**Native trailing (``trail_mode="native"``).**  The venue trails the stop
(Delta ``bracket_trail_amount``).  The manager does not amend it; it only
records the venue's current stop for the control room and runs time stops.

**Time stop.**  At ``Order.time_stop_at`` (the close of the strategy's last
allowed bar) the position is closed at market and the outcome is labelled
``time_stop``.

**Restart.**  :meth:`sync_all` re-derives every open trailing stop from candle
history and the venue's current stop: a venue stop looser than the derived level
is amended (the process may have died between bars), a venue stop tighter than
the record is adopted (never loosen), and an overdue time stop closes the
position.  The reconciler calls it after each pass.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from signal_bot.core.clock import SYSTEM_CLOCK, Clock, utc
from signal_bot.db.models import Order
from signal_bot.events.envelope import Event, EventType, deterministic_event_id
from signal_bot.execution.adapter import (
    AmbiguousOrderOutcome,
    ExchangeAdapter,
    ExchangeError,
    ExchangePosition,
    OrderRejected,
)
from signal_bot.execution.order_store import OrderStore
from signal_bot.execution.orders import OrderState
from signal_bot.features.candle_history import CandleHistory
from signal_bot.ingest.binance_ws import Kline
from signal_bot.observability import metrics
from signal_bot.observability.logging import bind
from signal_bot.signals.trend import trail_stop

logger = logging.getLogger(__name__)

S = OrderState
OPEN_STATES = (S.FILLED, S.PARTIALLY_FILLED)


def _round(value: Decimal, tick: Decimal) -> Decimal:
    if not tick:
        return value
    return (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick


def _tighter(side: str, new: Decimal, old: Decimal | None) -> bool:
    """Is *new* a strictly tighter stop than *old* for a position opened by *side*?"""
    if old is None:
        return True
    return new > old if side == "buy" else new < old


class TrailingStopManager:
    def __init__(
        self,
        adapter: ExchangeAdapter,
        store: OrderStore,
        history: CandleHistory,
        *,
        bus=None,
        alerts=None,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        self.adapter = adapter
        self.store = store
        self.history = history
        self.bus = bus
        self.alerts = alerts
        self.clock = clock
        self._busy: set[str] = set()
        self._closing: set[str] = set()
        self.last_sync: dict[str, Any] = {}

    # ------------------------------------------------------------------
    async def on_bar_close(self, candle: Kline) -> list[dict[str, Any]]:
        """A bar completed: ratchet stops and run time stops for that symbol's positions."""
        if candle.interval != self.history.interval:
            return []
        results = []
        for order in await self._open_orders():
            if self.adapter.binance_symbol(order.symbol) == candle.symbol:
                results.append(await self.sync_order(order))
        return results

    async def sync_all(self, reason: str = "periodic") -> list[dict[str, Any]]:
        """Re-derive every open trailing/time-stopped position (startup, after reconcile)."""
        try:
            positions = {p.symbol: p for p in await self.adapter.positions()}
        except ExchangeError as exc:
            logger.warning("trailing sync (%s) skipped: positions unavailable: %s", reason, exc)
            return []
        out = []
        for order in await self._open_orders():
            out.append(await self.sync_order(order, positions.get(order.symbol), verify=True))
        self.last_sync = {"at": self.clock.now().isoformat(), "reason": reason, "orders": len(out)}
        return out

    async def _open_orders(self) -> list[Order]:
        return [
            o
            for o in await self.store.in_states(OPEN_STATES)
            if o.trail_mode in ("stepped", "native") or o.time_stop_at is not None
        ]

    # ------------------------------------------------------------------
    async def sync_order(
        self, order: Order, venue: ExchangePosition | None = None, *, verify: bool = False
    ) -> dict[str, Any]:
        coid = order.client_order_id
        if coid in self._busy:
            return {"order": coid, "action": "in_progress"}
        self._busy.add(coid)
        try:
            with bind(order_id=coid, correlation_id=order.correlation_id, symbol=order.symbol):
                return await self._sync(order, venue, verify)
        finally:
            self._busy.discard(coid)

    async def _sync(
        self, order: Order, venue: ExchangePosition | None, verify: bool
    ) -> dict[str, Any]:
        coid = order.client_order_id
        if order.protective_status == "flattened":
            return {"order": coid, "action": "flattened"}
        if order.time_stop_at is not None and self.clock.now() >= utc(order.time_stop_at):
            return await self._time_stop(order)
        if order.trail_mode == "native":
            if venue is not None and venue.stop_loss_price is not None:
                await self.store.update_trailing(coid, current_stop_price=venue.stop_loss_price)
            return {"order": coid, "action": "native"}
        if order.trail_mode != "stepped" or not order.trail_distance:
            return {"order": coid, "action": "none"}

        target, best, best_bar = self.target_stop(order)
        current = order.current_stop_price
        if best is not None and (
            order.best_price is None or Decimal(str(best)) != order.best_price
        ):
            await self.store.update_trailing(
                coid, best_price=Decimal(str(best)), best_price_bar=best_bar
            )

        if verify and venue is not None and venue.stop_loss_price is not None:
            venue_stop = venue.stop_loss_price
            if current is not None and _tighter(order.side, venue_stop, current):
                # Someone (or the venue) moved it tighter: adopt it, never loosen.
                await self.store.update_trailing(coid, current_stop_price=venue_stop)
                metrics.TRAILING_STOP_UPDATES.labels("verified_venue_tighter").inc()
                current = venue_stop
            elif current is not None and venue_stop != current:
                # Our record is tighter than the venue: the last amend did not stick.
                current = venue_stop
        if target is None or not _tighter(order.side, target, current):
            return {"order": coid, "action": "unchanged", "stop": current, "target": target}
        return await self._amend(order, target, current, best)

    def target_stop(self, order: Order) -> tuple[Decimal | None, float | None, Any]:
        """The venue stop the research rule gives after the bars completed so far."""
        symbol = self.adapter.binance_symbol(order.symbol)
        if symbol is None or order.trail_from is None or order.reference_stop is None:
            return None, None, None
        bars = self.history.bars_since(symbol, utc(order.trail_from))
        if bars.empty:
            return None, None, None
        long = order.side == "buy"
        best = float(bars["high"].max()) if long else float(bars["low"].min())
        best_bar = (bars["high"].idxmax() if long else bars["low"].idxmin()).to_pydatetime()
        ref = trail_stop(
            "long" if long else "short",
            float(order.reference_stop),
            best,
            float(order.trail_distance),
        )
        basis = Decimal(str(order.price_basis or 0))
        tick = self._tick(order)
        return _round(Decimal(str(ref)) + basis, tick), best, best_bar

    def _tick(self, order: Order) -> Decimal:
        symbol = self.adapter.binance_symbol(order.symbol)
        try:
            return self.adapter.instrument(symbol).tick_size if symbol else Decimal("0")
        except (KeyError, RuntimeError):
            return Decimal("0")

    async def _amend(
        self, order: Order, target: Decimal, current: Decimal | None, best: float | None
    ) -> dict[str, Any]:
        coid = order.client_order_id
        price = await self._market_price(order.symbol)
        if price is not None and (price <= target if order.side == "buy" else price >= target):
            # The researched stop is already through the market (e.g. after a restart):
            # the fill model would have stopped out at the open, so exit now instead of
            # sending a stop the venue would refuse or trigger anyway.
            return await self._close(order, "trail_hit", f"price {price} beyond stop {target}")
        try:
            pos = await self.adapter.amend_stop(order.symbol, coid, target)
        except OrderRejected as exc:
            metrics.TRAILING_STOP_UPDATES.labels("rejected").inc()
            logger.error(
                "Trailing stop amend for %s rejected: %s (stop stays %s)", coid, exc, current
            )
            await self._alert(
                f"Trailing stop for {order.symbol} not moved ({exc}); stop stays {current}"
            )
            return {"order": coid, "action": "rejected", "stop": current, "target": target}
        except (AmbiguousOrderOutcome, ExchangeError) as exc:
            metrics.TRAILING_STOP_UPDATES.labels("unknown").inc()
            logger.warning("Trailing stop amend for %s outcome unknown: %s; re-reading", coid, exc)
            pos = await self._position(order.symbol)
        confirmed = pos.stop_loss_price if pos is not None else None
        if confirmed is not None and _tighter(order.side, confirmed, current):
            await self.store.update_trailing(coid, current_stop_price=confirmed)
            metrics.TRAILING_STOP_UPDATES.labels("moved").inc()
            await self._publish(order, current, confirmed, best)
            logger.info("Trailing stop %s %s: %s -> %s", order.symbol, coid, current, confirmed)
            return {"order": coid, "action": "moved", "from": current, "stop": confirmed}
        return {"order": coid, "action": "not_confirmed", "stop": current, "target": target}

    async def _position(self, symbol: str) -> ExchangePosition | None:
        try:
            return next((p for p in await self.adapter.positions() if p.symbol == symbol), None)
        except ExchangeError:
            return None

    async def _time_stop(self, order: Order) -> dict[str, Any]:
        result = await self._close(order, "time_stop", "time stop reached")
        if result["action"] == "time_stop":
            metrics.TIME_STOPS.inc()
        return result

    async def _close(self, order: Order, hint: str, why: str) -> dict[str, Any]:
        coid = order.client_order_id
        if coid in self._closing:
            return {"order": coid, "action": f"{hint}_in_progress"}
        self._closing.add(coid)
        try:
            await self.store.set_exit_hint(coid, hint)
            await self.adapter.close_position(order.symbol, coid)
        except OrderRejected as exc:
            # Usually "no position": it was already closed by its stop.
            logger.info("%s for %s: nothing to close (%s)", hint, coid, exc)
            return {"order": coid, "action": f"{hint}_nothing_to_close"}
        except ExchangeError as exc:
            logger.error("%s close for %s failed: %s (will retry)", hint, coid, exc)
            await self._alert(f"{hint} close failed for {order.symbol}: {exc}")
            return {"order": coid, "action": f"{hint}_failed"}
        finally:
            self._closing.discard(coid)
        logger.info("%s: closed %s %s (%s)", hint, order.symbol, coid, why)
        return {"order": coid, "action": hint}

    async def _market_price(self, symbol: str) -> Decimal | None:
        try:
            book = await self.adapter.order_book(symbol)
        except ExchangeError:
            return None
        return book.mid

    # ------------------------------------------------------------------
    async def _publish(self, order: Order, old, new, best) -> None:
        if self.bus is None:
            return
        payload = {
            "client_order_id": order.client_order_id,
            "symbol": order.symbol,
            "side": order.side,
            "from": old,
            "to": new,
            "best_reference_price": best,
            "trail_distance": order.trail_distance,
        }
        try:
            await self.bus.publish(
                Event.create(
                    EventType.PROTECTIVE_STOP_AMENDED,
                    payload,
                    "trailing",
                    event_id=deterministic_event_id("trail", order.client_order_id, new),
                    correlation_id=order.correlation_id,
                )
            )
        except Exception:
            logger.exception("publish PROTECTIVE_STOP_AMENDED failed")

    async def _alert(self, text: str) -> None:
        if self.alerts is None:
            return
        try:
            await self.alerts.send_text(text)
        except Exception:
            logger.exception("alert delivery failed")

    async def run_periodic(self, shutdown: asyncio.Event, interval: float = 60.0) -> None:
        """Safety net for time stops that fall between candles (e.g. after an outage)."""
        while not shutdown.is_set():
            try:
                now = self.clock.now()
                for order in await self._open_orders():
                    if order.time_stop_at is not None and now >= utc(order.time_stop_at):
                        await self.sync_order(order)
            except Exception:
                logger.exception("trailing periodic check failed")
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval)
            except TimeoutError:
                pass


def time_stop_at(bar_close, max_hold_bars: int, step: timedelta):
    """Close of the last bar the research fill model allows (entry bar + max_hold_bars)."""
    return bar_close + step * (max_hold_bars + 1)
