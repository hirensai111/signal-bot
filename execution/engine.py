"""Bridge between the signal pipeline and the Delta Exchange India client."""

import asyncio
import enum
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import Signal
from signal_bot.delta.client import DeltaClient
from signal_bot.execution.governor import BaseGovernor, RiskGovernor

logger = logging.getLogger(__name__)

BASIS_ABORT_PCT = Decimal("0.0015")  # 0.15 %
NO_FILL_TIMEOUT = 60.0


class EXEC_MODE(enum.Enum):
    LIVE = "live"
    DRY_RUN = "dry_run"


class ExecutionEngine:
    """Consumes Signals, translates prices to Delta, places bracket orders,
    and tracks fills via the Delta WebSocket events queue.
    """

    def __init__(
        self,
        delta_client: DeltaClient,
        signal_queue: asyncio.Queue,
        delta_events: asyncio.Queue,
        session_factory=None,
        governor: Optional[RiskGovernor] = None,
        exec_mode: EXEC_MODE = EXEC_MODE.DRY_RUN,
        no_fill_timeout: float = NO_FILL_TIMEOUT,
        order_type: str = "limit_ioc",
        reconciler=None,
    ):
        self.delta_client = delta_client
        self.signal_queue = signal_queue
        self.delta_events = delta_events
        self.session_factory = session_factory or AsyncSessionLocal
        self.governor = governor or BaseGovernor()
        self.exec_mode = exec_mode
        self.no_fill_timeout = no_fill_timeout
        self.order_type = order_type
        self.reconciler = reconciler

        # Cache last-set margin mode / leverage per symbol to avoid redundant API calls
        self._margin_mode_cache: dict[str, str] = {}
        self._leverage_cache: dict[str, int] = {}

        # Pending order tracking: client_order_id -> {event_set, event}
        self._pending_orders: dict[str, dict[str, Any]] = {}

        self._running = False
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._running:
            return
        if self.reconciler is not None:
            logger.info("Waiting for first reconciliation before starting engine...")
            await self.reconciler.wait_for_first_run()
            logger.info("First reconciliation complete; starting engine.")
        self._running = True
        self._tasks = [
            asyncio.create_task(self._consume_signals(), name="exec-signals"),
            asyncio.create_task(self._consume_delta_events(), name="exec-delta-events"),
        ]
        logger.info("ExecutionEngine started (mode=%s)", self.exec_mode.value)

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        logger.info("ExecutionEngine stopped")

    # ------------------------------------------------------------------
    # signal consumer
    # ------------------------------------------------------------------
    async def _consume_signals(self) -> None:
        while self._running:
            try:
                signal: Signal = await asyncio.wait_for(self.signal_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._process_signal(signal)
            except Exception:
                logger.exception("Unhandled error processing signal %s", signal.id)

    # ------------------------------------------------------------------
    # main per-signal flow
    # ------------------------------------------------------------------
    async def _process_signal(self, signal: Signal) -> None:
        client_order_id = f"sig_{signal.id}"

        # 1. idempotency / restart-safety
        if signal.status in ("submitted", "triggered", "sl", "tp", "expired", "expired_no_fill", "cancelled"):
            logger.debug("Signal %s already at status=%s — skipping", signal.id, signal.status)
            return
        if signal.client_order_id is not None:
            logger.debug("Signal %s already has client_order_id=%s — skipping", signal.id, signal.client_order_id)
            return

        async with self.session_factory() as session:
            # Re-fetch inside transaction to avoid stale object
            fresh: Optional[Signal] = await session.get(Signal, signal.id)
            if fresh is None:
                logger.warning("Signal %s not found in DB", signal.id)
                return
            if fresh.status in ("submitted", "triggered", "sl", "tp", "expired", "expired_no_fill", "cancelled"):
                logger.debug("Signal %s already at status=%s — skipping", signal.id, fresh.status)
                return
            if fresh.client_order_id is not None:
                logger.debug("Signal %s already has client_order_id=%s — skipping", signal.id, fresh.client_order_id)
                return

            # 2. risk governor pre-flight
            approved, reason = await self.governor.check(fresh)
            if not approved:
                fresh.status = "cancelled"
                fresh.cancelled_reason = reason or "governor_denied"
                fresh.client_order_id = client_order_id
                await session.commit()
                logger.info("Signal %s cancelled by governor: %s", signal.id, reason)
                return

            # 3. symbol resolution
            try:
                delta_symbol = self.delta_client.symbol_map.get_delta_symbol(fresh.symbol)
                product_id = self.delta_client.symbol_map.get_product_id(fresh.symbol)
                product = self.delta_client.symbol_map.get_product(fresh.symbol)
            except (KeyError, RuntimeError) as exc:
                logger.error("Symbol resolution failed for %s: %s", fresh.symbol, exc)
                return

            tick_size = Decimal(str(product.get("tick_size", "1")))
            min_size = Decimal(str(product.get("min_size", product.get("minimum_order_size", "1"))))
            contract_value = Decimal(str(product.get("contract_value", "1")))

            # 4. margin & leverage setup (cached)
            if self.exec_mode == EXEC_MODE.LIVE:
                await self._setup_margin_and_leverage(fresh.symbol, product_id, fresh.leverage)
            else:
                logger.info("[DRY_RUN] Would set margin=isolated, leverage=%dx for %s", fresh.leverage, fresh.symbol)

            # 5. fetch Delta prices & compute basis
            if self.exec_mode == EXEC_MODE.LIVE:
                delta_bid, delta_ask = await self._fetch_delta_prices(delta_symbol)
            else:
                delta_bid = fresh.entry
                delta_ask = fresh.entry

            delta_mid = (delta_bid + delta_ask) / Decimal("2")
            delta_basis = delta_mid - fresh.entry
            basis_pct = abs(delta_basis) / fresh.entry if fresh.entry != 0 else Decimal("0")

            if basis_pct > BASIS_ABORT_PCT:
                fresh.status = "cancelled"
                fresh.cancelled_reason = "cross_venue_basis_too_wide"
                fresh.delta_basis = delta_basis
                fresh.client_order_id = client_order_id
                await session.commit()
                logger.warning(
                    "Signal %s aborted: basis=%.4f%% (threshold=0.15%%)",
                    signal.id,
                    float(basis_pct) * 100,
                )
                return

            # 6. translate & round prices
            delta_entry = self._round_to_tick(fresh.entry + delta_basis, tick_size)
            delta_sl = self._round_to_tick(fresh.stop_loss + delta_basis, tick_size)
            delta_tp = self._round_to_tick(fresh.take_profit + delta_basis, tick_size)

            # 7. compute size
            size = self._compute_size(fresh, contract_value)
            size = self._round_to_tick(size, min_size)  # min_size acts as lot size
            if size < min_size:
                fresh.status = "cancelled"
                fresh.cancelled_reason = "size_below_min"
                fresh.delta_basis = delta_basis
                fresh.client_order_id = client_order_id
                await session.commit()
                logger.warning("Signal %s aborted: size %s below min %s", signal.id, size, min_size)
                return

            # 8. determine order price (limit IOC offset by 1 tick)
            if self.order_type == "limit_ioc":
                if fresh.side == "long":
                    order_price = delta_ask + tick_size
                else:
                    order_price = delta_bid - tick_size
                order_price = self._round_to_tick(order_price, tick_size)
            else:
                order_price = delta_entry

            # 9. persist pre-submission state
            fresh.client_order_id = client_order_id
            fresh.delta_symbol = delta_symbol
            fresh.delta_entry_intended = delta_entry
            fresh.delta_basis = delta_basis
            fresh.status = "submitted"
            await session.commit()
            await session.refresh(fresh)

        # 10. place order (outside the DB session to avoid long transaction)
        if self.exec_mode == EXEC_MODE.LIVE:
            try:
                resp = await self.delta_client.place_bracket_order(
                    symbol=delta_symbol,
                    side="buy" if fresh.side == "long" else "sell",
                    size=int(size),
                    entry_price=order_price,
                    stop_loss_price=delta_sl,
                    take_profit_price=delta_tp,
                    client_order_id=client_order_id,
                    order_type=self.order_type,
                )
                delta_order_id = str(resp.get("id", resp.get("order_id", "")))
            except Exception as exc:
                logger.exception("Order placement failed for %s", client_order_id)
                async with self.session_factory() as session:
                    fresh = await session.get(Signal, signal.id)
                    if fresh:
                        fresh.status = "cancelled"
                        fresh.cancelled_reason = f"placement_error: {exc}"
                        await session.commit()
                return
        else:
            delta_order_id = f"dry_{client_order_id}"
            logger.info(
                "[DRY_RUN] Would place %s %s size=%s entry=%s sl=%s tp=%s coid=%s",
                delta_symbol,
                fresh.side,
                size,
                order_price,
                delta_sl,
                delta_tp,
                client_order_id,
            )

        # Update with delta_order_id
        async with self.session_factory() as session:
            fresh = await session.get(Signal, signal.id)
            if fresh:
                fresh.delta_order_id = delta_order_id
                await session.commit()

        # 11. DRY_RUN short-circuit: immediately mark triggered
        if self.exec_mode == EXEC_MODE.DRY_RUN:
            async with self.session_factory() as session:
                fresh = await session.get(Signal, signal.id)
                if fresh:
                    fresh.status = "triggered"
                    fresh.delta_entry_actual = fresh.delta_entry_intended
                    fresh.slippage_bps = Decimal("0")
                    fresh.entry_fees_usdt = Decimal("0")
                    await session.commit()
            logger.info("[DRY_RUN] Signal %s marked triggered immediately", signal.id)
            return

        # 12. wait for fill
        filled = await self._track_order_fill(client_order_id, delta_order_id, fresh)

        async with self.session_factory() as session:
            fresh = await session.get(Signal, signal.id)
            if fresh is None:
                return
            if filled:
                fresh.status = "triggered"
                # slippage & fees populated by _track_order_fill
                await session.commit()
                logger.info("Signal %s filled (order %s)", signal.id, delta_order_id)
            else:
                fresh.status = "expired_no_fill"
                await session.commit()
                logger.info("Signal %s expired without fill (order %s)", signal.id, delta_order_id)

    # ------------------------------------------------------------------
    # margin / leverage (cached)
    # ------------------------------------------------------------------
    async def _setup_margin_and_leverage(self, symbol: str, product_id: int, leverage: int) -> None:
        if self._margin_mode_cache.get(symbol) != "isolated":
            await self.delta_client.set_margin_mode(product_id, "isolated")
            self._margin_mode_cache[symbol] = "isolated"
        if self._leverage_cache.get(symbol) != leverage:
            await self.delta_client.set_leverage(product_id, leverage)
            self._leverage_cache[symbol] = leverage

    # ------------------------------------------------------------------
    # price helpers
    # ------------------------------------------------------------------
    async def _fetch_delta_prices(self, delta_symbol: str) -> tuple[Decimal, Decimal]:
        book = await self.delta_client.get_l2_orderbook(delta_symbol)
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            raise RuntimeError(f"Empty orderbook for {delta_symbol}")
        best_bid = Decimal(str(bids[0][0]))
        best_ask = Decimal(str(asks[0][0]))
        return best_bid, best_ask

    @staticmethod
    def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
        if tick == 0:
            return value
        return (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick

    # ------------------------------------------------------------------
    # sizing
    # ------------------------------------------------------------------
    @staticmethod
    def _compute_size(signal: Signal, contract_value: Decimal) -> Decimal:
        risk_amount = signal.account_size_assumed * signal.risk_pct / Decimal("100")
        stop_distance_pct = abs(signal.entry - signal.stop_loss) / signal.entry
        if stop_distance_pct == 0:
            return Decimal("0")
        position_notional = risk_amount / stop_distance_pct
        if contract_value == 0:
            return Decimal("0")
        return position_notional / contract_value

    # ------------------------------------------------------------------
    # fill tracking
    # ------------------------------------------------------------------
    async def _track_order_fill(
        self, client_order_id: str, delta_order_id: str, signal: Signal
    ) -> bool:
        event_set = asyncio.Event()
        self._pending_orders[client_order_id] = {
            "event_set": event_set,
            "event": None,
        }
        deadline = asyncio.get_event_loop().time() + self.no_fill_timeout
        filled = False

        try:
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    logger.warning("No-fill timeout for %s", client_order_id)
                    try:
                        await self.delta_client.cancel_order(delta_order_id)
                    except Exception:
                        logger.exception("Cancel failed for %s", delta_order_id)
                    break

                try:
                    await asyncio.wait_for(event_set.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    continue

                event_set.clear()
                event = self._pending_orders[client_order_id]["event"]
                if event is None:
                    continue

                state = event.get("state", "")
                if state == "filled":
                    avg_price = Decimal(str(event.get("average_fill_price", event.get("price", "0"))))
                    filled_size = Decimal(str(event.get("filled_size", "0")))
                    fee = Decimal(str(event.get("commission", event.get("fee", "0"))))
                    async with self.session_factory() as session:
                        fresh = await session.get(Signal, signal.id)
                        if fresh:
                            fresh.delta_entry_actual = avg_price
                            fresh.entry_fees_usdt = fee
                            if fresh.delta_entry_intended and fresh.delta_entry_intended != 0:
                                slippage = (avg_price - fresh.delta_entry_intended) / fresh.delta_entry_intended
                                fresh.slippage_bps = slippage * Decimal("10000")
                            await session.commit()
                    filled = True
                    break
                elif state == "partially_filled":
                    filled_size = Decimal(str(event.get("filled_size", "0")))
                    logger.warning("Partial fill on %s: %s contracts", client_order_id, filled_size)
                    # continue waiting
                elif state in ("cancelled", "rejected"):
                    logger.warning("Order %s state=%s", client_order_id, state)
                    break
                # else: open, etc. — keep waiting
        finally:
            self._pending_orders.pop(client_order_id, None)

        return filled

    # ------------------------------------------------------------------
    # delta events consumer
    # ------------------------------------------------------------------
    async def _consume_delta_events(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self.delta_events.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if event.get("channel") != "private_orders":
                continue

            data = event.get("data", {})
            coid = data.get("client_order_id")
            if coid and coid in self._pending_orders:
                self._pending_orders[coid]["event"] = data
                self._pending_orders[coid]["event_set"].set()
                logger.debug("Delta event for %s: state=%s", coid, data.get("state"))
            else:
                logger.debug("Ignored delta event for coid=%s", coid)
