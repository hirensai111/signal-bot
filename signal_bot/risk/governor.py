"""Production risk governor: persisted kill switch, daily limits, circuit breakers.

The governor gates *every* order submission and fails closed:

* the persisted kill switch blocks until an operator explicitly clears it;
* transient **circuit breakers** (exchange stream down, unresolved orders,
  authentication failure, orphan exposure, ...) block while their condition
  holds and are cleared by the component that raised them once recovered;
* any error while reading risk state, or an unavailable balance, is a denial;
* :attr:`healthy` turns false if the state store is unreachable or the
  monitoring loop stalls, and the execution engine refuses to submit while the
  governor is unhealthy.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select

from signal_bot.config import settings
from signal_bot.core.clock import SYSTEM_CLOCK, Clock
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import OperatorAudit, RiskState, Signal
from signal_bot.events.envelope import Event, EventType
from signal_bot.execution.orders import SIGNAL_EXPOSURE_STATUSES
from signal_bot.observability import metrics

logger = logging.getLogger(__name__)

ALERT_COOLDOWN_SECONDS = 3600  # one alert per reason per hour
BALANCE_CACHE_TTL = 30.0
CHECK_INTERVAL_SECONDS = 30.0


class RiskGovernor:
    """Consulted before every order placement.  See module docstring."""

    def __init__(
        self,
        session_factory=None,
        exchange=None,
        telegram=None,
        daily_loss_limit_r: float | None = None,
        max_trades_per_day: int | None = None,
        max_concurrent_positions: int | None = None,
        max_consecutive_losses: int | None = None,
        min_balance_usdt: float | None = None,
        *,
        bus=None,
        clock: Clock = SYSTEM_CLOCK,
        check_interval: float = CHECK_INTERVAL_SECONDS,
        delta_client=None,  # deprecated alias for ``exchange``
    ):
        self.session_factory = session_factory or AsyncSessionLocal
        self.exchange = exchange if exchange is not None else delta_client
        self.telegram = telegram
        self.bus = bus
        self.clock = clock
        self.check_interval = check_interval

        def pick(value, default):
            return value if value is not None else default

        self.daily_loss_limit_r = Decimal(
            str(pick(daily_loss_limit_r, settings.daily_loss_limit_r))
        )
        self.max_trades_per_day = pick(max_trades_per_day, settings.max_trades_per_day)
        self.max_concurrent_positions = pick(
            max_concurrent_positions, settings.max_concurrent_positions
        )
        self.max_consecutive_losses = pick(max_consecutive_losses, settings.max_consecutive_losses)
        self.min_balance_usdt = Decimal(str(pick(min_balance_usdt, settings.min_balance_usdt)))

        self._breakers: dict[str, str] = {}
        self._alert_cooldown: dict[str, datetime] = {}
        self._balance_cache: Decimal | None = None
        self._balance_cache_time: float = 0.0
        self._kill_switch_cached = False
        self._kill_reason_cached: str | None = None
        self._state_store_ok = True
        self._last_successful_check: float | None = None
        self._exchange_open_positions: int | None = None

        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._background: set[asyncio.Task] = set()
        # Most recent pre-trade verdict (periodic loop or a real signal), for the control room.
        self.last_verdict: tuple[bool, str | None] | None = None
        # Portfolio rules (risk/portfolio.py), wired by main for the trend strategy:
        # ``portfolio_check(signal)`` re-admits a sized signal against the current book just
        # before submission; ``portfolio_monitor()`` returns the exposure summary used for
        # the aggregate-drawdown breaker.
        self.portfolio_check = None
        self.portfolio_monitor = None
        self.portfolio_strategies: frozenset[str] = frozenset({"trend_breakout"})

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._periodic_check_loop(), name="risk-periodic"),
            asyncio.create_task(self._daily_reset_loop(), name="risk-daily-reset"),
        ]
        logger.info("RiskGovernor started")

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        logger.info("RiskGovernor stopped")

    # ------------------------------------------------------------------
    # health & circuit breakers
    # ------------------------------------------------------------------
    @property
    def healthy(self) -> bool:
        if not self._state_store_ok:
            return False
        if self._running and self._last_successful_check is not None:
            stale_after = self.check_interval * 3
            if self.clock.monotonic() - self._last_successful_check > stale_after:
                return False
        return True

    @property
    def breakers(self) -> dict[str, str]:
        return dict(self._breakers)

    def set_breaker(self, name: str, reason: str) -> None:
        if self._breakers.get(name) == reason:
            return
        first = name not in self._breakers
        self._breakers[name] = reason
        metrics.CIRCUIT_BREAKERS_ACTIVE.set(len(self._breakers))
        logger.warning("Circuit breaker OPEN: %s (%s)", name, reason)
        if first:
            self._spawn(self._publish(EventType.RISK_HALT, {"breaker": name, "reason": reason}))

    def clear_breaker(self, name: str) -> None:
        if self._breakers.pop(name, None) is None:
            return
        metrics.CIRCUIT_BREAKERS_ACTIVE.set(len(self._breakers))
        logger.info("Circuit breaker cleared: %s", name)
        self._spawn(self._publish(EventType.RISK_RESUMED, {"breaker": name}))

    def clear_breakers_with_prefix(self, prefix: str) -> None:
        for name in [n for n in self._breakers if n.startswith(prefix)]:
            self.clear_breaker(name)

    def gate_open(self) -> tuple[bool, str | None]:
        """Synchronous last-moment gate used immediately before a network submit."""
        if self._kill_switch_cached:
            return False, f"kill_switch_active: {self._kill_reason_cached or 'manual'}"
        if self._breakers:
            name, reason = next(iter(self._breakers.items()))
            return False, f"circuit_breaker:{name}: {reason}"
        if not self.healthy:
            return False, "risk_governor_unhealthy"
        return True, None

    def observe_exchange_positions(self, count: int) -> None:
        """Reconciler feeds the venue's open-position count (source of truth)."""
        self._exchange_open_positions = count

    # ------------------------------------------------------------------
    # pre-trade check
    # ------------------------------------------------------------------
    async def check(self, signal: Signal | None = None) -> tuple[bool, str | None]:
        if self._breakers:
            name, reason = next(iter(self._breakers.items()))
            return self._deny("circuit_breaker", f"circuit_breaker:{name}: {reason}")

        try:
            verdict = await self._check_state()
        except Exception:
            logger.exception("Risk state unavailable — failing closed")
            self._state_store_ok = False
            metrics.GOVERNOR_HEALTHY.set(0)
            return self._deny("risk_state_unavailable", "risk_state_unavailable")

        self._state_store_ok = True
        self._last_successful_check = self.clock.monotonic()
        metrics.GOVERNOR_HEALTHY.set(1 if self.healthy else 0)
        if verdict is not None:
            return self._deny(verdict.split(":")[0], verdict)

        try:
            balance = await self._get_balance()
        except Exception:
            logger.exception("Balance unavailable — failing closed")
            return self._deny("balance_unavailable", "balance_unavailable")
        if balance < self.min_balance_usdt:
            return self._deny(
                "min_balance_usdt",
                f"min_balance_usdt breached: {balance} < {self.min_balance_usdt}",
            )
        if (
            signal is not None
            and self.portfolio_check is not None
            and getattr(signal, "strategy", None) in self.portfolio_strategies
        ):
            try:
                ok, reason = await self.portfolio_check(signal)
            except Exception:
                logger.exception("Portfolio check failed — failing closed")
                return self._deny("portfolio_unavailable", "portfolio_unavailable")
            if not ok:
                return self._deny("portfolio", reason or "portfolio")
        self.last_verdict = (True, None)
        return True, None

    async def check_portfolio_drawdown(self) -> dict | None:
        """Open/clear the ``portfolio_drawdown`` breaker from marked-to-market equity."""
        if self.portfolio_monitor is None:
            return None
        summary = await self.portfolio_monitor()
        if summary is None:
            return None
        if not summary.get("entries_allowed", True):
            self.set_breaker(
                "portfolio_drawdown",
                f"drawdown {summary.get('drawdown_pct')}% >= halt "
                f"{summary['limits']['drawdown_halt'] * 100:.1f}% of peak equity",
            )
        else:
            self.clear_breaker("portfolio_drawdown")
        return summary

    async def _check_state(self) -> str | None:
        async with self.session_factory() as session:
            state = await self._get_state(session)
            self._roll_day_if_needed(state)

            self._kill_switch_cached = bool(state.kill_switch_active)
            self._kill_reason_cached = state.kill_switch_reason
            metrics.KILL_SWITCH_ACTIVE.set(1 if state.kill_switch_active else 0)
            if state.kill_switch_active:
                await session.commit()
                return f"kill_switch_active: {state.kill_switch_reason or 'manual'}"

            open_count = await session.scalar(
                select(func.count())
                .select_from(Signal)
                .where(Signal.status.in_(SIGNAL_EXPOSURE_STATUSES))
            )
            # Local exposure only: venue positions we don't own are blocked separately by
            # the reconciler's orphan_position breaker, and a cached venue count would go
            # stale between reconciliation passes.
            open_count = open_count or 0
            state.current_open_positions = open_count
            metrics.OPEN_POSITIONS.set(open_count)
            await session.commit()

            if state.daily_pnl_r <= self.daily_loss_limit_r:
                return f"daily_loss_limit_r breached: {state.daily_pnl_r}R <= {self.daily_loss_limit_r}R"
            if state.daily_trade_count >= self.max_trades_per_day:
                return (
                    f"max_trades_per_day breached: "
                    f"{state.daily_trade_count} >= {self.max_trades_per_day}"
                )
            if open_count >= self.max_concurrent_positions:
                return (
                    f"max_concurrent_positions breached: "
                    f"{open_count} >= {self.max_concurrent_positions}"
                )
            # Worst case if every open stop and this new trade's stop are hit (1R each):
            # stops a breach caused by trades that were already open when the limit hit.
            projected = state.daily_pnl_r - Decimal(open_count + 1)
            if projected < self.daily_loss_limit_r:
                return (
                    f"daily_loss_budget exhausted: {state.daily_pnl_r}R realised with "
                    f"{open_count} open trade(s) could reach {projected}R < {self.daily_loss_limit_r}R"
                )
            if state.consecutive_losses >= self.max_consecutive_losses:
                return (
                    f"max_consecutive_losses breached: "
                    f"{state.consecutive_losses} >= {self.max_consecutive_losses}"
                )
        return None

    def _deny(self, rule: str, reason: str) -> tuple[bool, str]:
        metrics.RISK_DENIALS.labels(rule).inc()
        self.last_verdict = (False, reason)
        return False, reason

    # ------------------------------------------------------------------
    # counters
    # ------------------------------------------------------------------
    async def record_submission(self, signal_id: uuid.UUID) -> None:
        """Count a trade when it is *sent*, so max_trades_per_day bounds exposure taken."""
        async with self.session_factory() as session:
            state = await self._get_state(session)
            self._roll_day_if_needed(state)
            state.daily_trade_count += 1
            await session.commit()

    async def record_outcome(self, signal_id: uuid.UUID, pnl_r: Decimal, won: bool) -> None:
        async with self.session_factory() as session:
            tripped = await self.apply_outcome(session, signal_id, pnl_r, won)
            await session.commit()
        await self.after_outcome(tripped)

    async def apply_outcome(self, session, signal_id, pnl_r: Decimal, won: bool) -> bool:
        """Mutate risk state inside the caller's transaction (exactly-once with the outcome row).

        Returns ``True`` if the kill switch was auto-tripped; call :meth:`after_outcome`
        after the transaction commits.
        """
        state = await self._get_state(session)
        self._roll_day_if_needed(state)
        state.daily_pnl_r += Decimal(str(pnl_r))
        state.consecutive_losses = 0 if won else state.consecutive_losses + 1
        tripped = False
        if state.consecutive_losses >= self.max_consecutive_losses and not state.kill_switch_active:
            state.kill_switch_active = True
            state.kill_switch_reason = "auto: max_consecutive_losses"
            state.kill_switch_set_at = self.clock.now()
            tripped = True
        logger.info(
            "Outcome applied: signal=%s pnl_r=%s won=%s daily_r=%s streak=%s",
            signal_id,
            pnl_r,
            won,
            state.daily_pnl_r,
            state.consecutive_losses,
        )
        return tripped

    async def after_outcome(self, tripped: bool) -> None:
        if not tripped:
            return
        self._kill_switch_cached = True
        self._kill_reason_cached = "auto: max_consecutive_losses"
        logger.critical("Kill switch AUTO-TRIPPED by consecutive losses")
        metrics.KILL_SWITCH_ACTIVE.set(1)
        await self._send_alert("Kill switch AUTO-TRIPPED\nReason: max_consecutive_losses")
        await self._publish(EventType.RISK_HALT, {"kill_switch": "auto: max_consecutive_losses"})

    # ------------------------------------------------------------------
    # management API
    # ------------------------------------------------------------------
    async def set_kill_switch(self, reason: str, actor: str = "system", source_ip=None) -> None:
        # Engage in memory first: even if the DB write fails, this process stops trading.
        self._kill_switch_cached = True
        self._kill_reason_cached = reason
        metrics.KILL_SWITCH_ACTIVE.set(1)
        try:
            async with self.session_factory() as session:
                state = await self._get_state(session)
                state.kill_switch_active = True
                state.kill_switch_reason = reason
                state.kill_switch_set_at = self.clock.now()
                session.add(
                    OperatorAudit(actor=actor, action="halt", reason=reason, source_ip=source_ip)
                )
                await session.commit()
        finally:
            logger.critical("Kill switch ACTIVATED by %s: %s", actor, reason)
            await self._send_alert(f"Kill switch ACTIVATED\nBy: {actor}\nReason: {reason}")
            await self._publish(EventType.RISK_HALT, {"kill_switch": reason, "actor": actor})

    async def clear_kill_switch(
        self, actor: str = "operator", reason: str | None = None, source_ip=None
    ) -> None:
        async with self.session_factory() as session:
            state = await self._get_state(session)
            state.kill_switch_active = False
            state.kill_switch_reason = None
            state.kill_switch_set_at = None
            session.add(
                OperatorAudit(actor=actor, action="resume", reason=reason, source_ip=source_ip)
            )
            await session.commit()
        self._kill_switch_cached = False
        self._kill_reason_cached = None
        metrics.KILL_SWITCH_ACTIVE.set(0)
        logger.warning("Kill switch CLEARED by %s", actor)
        await self._send_alert(f"Kill switch CLEARED by {actor}")
        await self._publish(EventType.RISK_RESUMED, {"kill_switch": "cleared", "actor": actor})

    async def get_status(self) -> dict:
        async with self.session_factory() as session:
            state = await self._get_state(session)
            return {
                "kill_switch_active": state.kill_switch_active,
                "kill_switch_reason": state.kill_switch_reason,
                "kill_switch_set_at": state.kill_switch_set_at,
                "daily_pnl_r": state.daily_pnl_r,
                "daily_trade_count": state.daily_trade_count,
                "consecutive_losses": state.consecutive_losses,
                "last_reset_date": state.last_reset_date,
                "current_open_positions": state.current_open_positions,
                "circuit_breakers": dict(self._breakers),
                "healthy": self.healthy,
                "blocking_reason": self.last_verdict[1] if self.last_verdict else None,
                "limits": {
                    "daily_loss_limit_r": self.daily_loss_limit_r,
                    "max_trades_per_day": self.max_trades_per_day,
                    "max_concurrent_positions": self.max_concurrent_positions,
                    "max_consecutive_losses": self.max_consecutive_losses,
                    "min_balance_usdt": self.min_balance_usdt,
                },
            }

    def cached_status(self) -> dict:
        """Best-effort status that never touches the database (used when it is down)."""
        return {
            "kill_switch_active": self._kill_switch_cached,
            "kill_switch_reason": self._kill_reason_cached,
            "circuit_breakers": dict(self._breakers),
            "healthy": self.healthy,
            "state_store_ok": self._state_store_ok,
            "blocking_reason": self.last_verdict[1] if self.last_verdict else None,
        }

    # ------------------------------------------------------------------
    # background tasks
    # ------------------------------------------------------------------
    async def _periodic_check_loop(self) -> None:
        while self._running:
            try:
                await self.check_portfolio_drawdown()
            except Exception:
                logger.exception("Portfolio drawdown check failed")
            try:
                allowed, reason = await self.check(None)
                if not allowed and reason and self._should_alert(reason):
                    logger.warning("Periodic risk check failed: %s", reason)
                    await self._send_alert(f"Risk check failed\n{reason}")
            except Exception:
                logger.exception("Periodic risk check error")
            await asyncio.sleep(self.check_interval)

    async def _daily_reset_loop(self) -> None:
        while self._running:
            now = self.clock.now()
            next_midnight = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            await asyncio.sleep((next_midnight - now).total_seconds())
            try:
                await self._do_daily_reset()
                logger.info("Daily risk counters reset (00:00 UTC)")
                await self._send_alert("Daily risk counters reset")
            except Exception:
                logger.exception("Daily reset error")

    def _today(self) -> date:
        return self.clock.now().date()

    def _roll_day_if_needed(self, state: RiskState) -> None:
        """Reset counters if the process was down over midnight UTC."""
        today = self._today()
        if state.last_reset_date is None:
            state.last_reset_date = today
        elif state.last_reset_date < today:
            state.daily_pnl_r = Decimal("0")
            state.daily_trade_count = 0
            state.consecutive_losses = 0
            state.last_reset_date = today

    async def _do_daily_reset(self) -> None:
        """Zero daily counters; the kill switch is intentionally preserved."""
        async with self.session_factory() as session:
            state = await self._get_state(session)
            state.daily_pnl_r = Decimal("0")
            state.daily_trade_count = 0
            state.consecutive_losses = 0
            state.last_reset_date = self._today()
            await session.commit()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    async def _get_state(self, session) -> RiskState:
        state = await session.get(RiskState, 1)
        if state is None:
            state = RiskState(
                id=1,
                kill_switch_active=False,
                daily_pnl_r=Decimal("0"),
                daily_trade_count=0,
                consecutive_losses=0,
                current_open_positions=0,
            )
            session.add(state)
            await session.flush()
        return state

    async def _get_balance(self) -> Decimal:
        if self.exchange is None:
            return Decimal("999999")
        now = self.clock.monotonic()
        if self._balance_cache is not None and (now - self._balance_cache_time) < BALANCE_CACHE_TTL:
            return self._balance_cache
        balance = Decimal(str(await self.exchange.available_balance()))
        self._balance_cache = balance
        self._balance_cache_time = now
        return balance

    def _should_alert(self, reason: str) -> bool:
        now = self.clock.now()
        key = reason.split(":")[0]
        last = self._alert_cooldown.get(key)
        if last is None or (now - last).total_seconds() > ALERT_COOLDOWN_SECONDS:
            self._alert_cooldown[key] = now
            return True
        return False

    async def _send_alert(self, message: str) -> None:
        if self.telegram is None:
            return
        try:
            if hasattr(self.telegram, "send_text"):
                await self.telegram.send_text(message)
            elif getattr(self.telegram, "application", None):
                await self.telegram.application.bot.send_message(
                    chat_id=self.telegram.chat_id, text=message
                )
        except Exception:
            logger.exception("Telegram alert failed")

    async def _publish(self, event_type: EventType, payload: dict) -> None:
        if self.bus is None:
            return
        try:
            await self.bus.publish(Event.create(event_type, payload, source="risk_governor"))
        except Exception:
            logger.exception("Failed to publish %s", event_type)

    def _spawn(self, coro) -> None:
        try:
            task = asyncio.get_running_loop().create_task(coro)
        except RuntimeError:
            coro.close()
            return
        self._background.add(task)
        task.add_done_callback(self._background.discard)


# ---------------------------------------------------------------------------
# CLI (operator controls; every action is written to operator_audit)
# ---------------------------------------------------------------------------


async def _cli_kill(reason: str, actor: str) -> None:
    await RiskGovernor().set_kill_switch(reason, actor=actor)
    print(f"Kill switch ACTIVATED: {reason}")


async def _cli_clear(actor: str, reason: str | None) -> None:
    await RiskGovernor().clear_kill_switch(actor=actor, reason=reason)
    print("Kill switch CLEARED")


async def _cli_status() -> None:
    status = await RiskGovernor().get_status()
    for k, v in status.items():
        print(f"{k}: {v}")


def main() -> None:
    import getpass

    parser = argparse.ArgumentParser(description="Risk governor CLI")
    parser.add_argument("--kill", type=str, help="Activate kill switch with reason")
    parser.add_argument("--clear", action="store_true", help="Clear kill switch")
    parser.add_argument("--reason", type=str, default=None, help="Why the kill switch is cleared")
    parser.add_argument("--status", action="store_true", help="Print risk state")
    args = parser.parse_args()
    actor = f"cli:{getpass.getuser()}"

    if args.kill:
        asyncio.run(_cli_kill(args.kill, actor))
    elif args.clear:
        if not args.reason:
            parser.error("--clear requires --reason (recorded in the operator audit trail)")
        asyncio.run(_cli_clear(actor, args.reason))
    elif args.status:
        asyncio.run(_cli_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
