"""Production risk governor with kill switch, daily limits, and Telegram alerts."""

import argparse
import asyncio
import logging
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import RiskState, Signal

logger = logging.getLogger(__name__)

ALERT_COOLDOWN_SECONDS = 3600  # one alert per reason per hour
BALANCE_CACHE_TTL = 30.0


class RiskGovernor:
    """Singleton risk governor consulted before every order placement.

    Maintains persistent state in ``risk_state`` (single-row table) and runs
    two background tasks:

    * **Periodic check** (every 30 s) — refreshes open-position count and runs
      the full rule set, emitting a Telegram alert if breached.
    * **Daily reset** — at 00:00 UTC zeros daily counters.  The kill switch is
      intentionally **not** auto-reset.
    """

    def __init__(
        self,
        session_factory=None,
        delta_client=None,
        telegram=None,
        daily_loss_limit_r: float = None,
        max_trades_per_day: int = None,
        max_concurrent_positions: int = None,
        max_consecutive_losses: int = None,
        min_balance_usdt: float = None,
    ):
        self.session_factory = session_factory or AsyncSessionLocal
        self.delta_client = delta_client
        self.telegram = telegram

        self.daily_loss_limit_r = Decimal(
            str(
                daily_loss_limit_r
                if daily_loss_limit_r is not None
                else settings.daily_loss_limit_r
            )
        )
        self.max_trades_per_day = (
            max_trades_per_day if max_trades_per_day is not None else settings.max_trades_per_day
        )
        self.max_concurrent_positions = (
            max_concurrent_positions
            if max_concurrent_positions is not None
            else settings.max_concurrent_positions
        )
        self.max_consecutive_losses = (
            max_consecutive_losses
            if max_consecutive_losses is not None
            else settings.max_consecutive_losses
        )
        self.min_balance_usdt = Decimal(
            str(min_balance_usdt if min_balance_usdt is not None else settings.min_balance_usdt)
        )

        # Rate-limiting for Telegram alerts
        self._alert_cooldown: dict[str, datetime] = {}

        # Balance cache
        self._balance_cache: Decimal | None = None
        self._balance_cache_time: float = 0.0

        self._running = False
        self._tasks: list[asyncio.Task] = []

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
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        logger.info("RiskGovernor stopped")

    # ------------------------------------------------------------------
    # pre-trade check
    # ------------------------------------------------------------------
    async def check(self, signal: Signal | None = None) -> tuple[bool, str | None]:
        async with self.session_factory() as session:
            state = await self._get_state(session)

            if state.kill_switch_active:
                return False, f"kill_switch_active: {state.kill_switch_reason or 'manual'}"

            # Refresh open-position count from DB for accuracy
            open_count = await session.scalar(
                select(func.count()).select_from(Signal).where(Signal.status == "triggered")
            )
            state.current_open_positions = open_count or 0

            if state.daily_pnl_r <= self.daily_loss_limit_r:
                return False, (
                    f"daily_loss_limit_r breached: "
                    f"{state.daily_pnl_r}R <= {self.daily_loss_limit_r}R"
                )

            if state.daily_trade_count >= self.max_trades_per_day:
                return False, (
                    f"max_trades_per_day breached: "
                    f"{state.daily_trade_count} >= {self.max_trades_per_day}"
                )

            if state.current_open_positions >= self.max_concurrent_positions:
                return False, (
                    f"max_concurrent_positions breached: "
                    f"{state.current_open_positions} >= {self.max_concurrent_positions}"
                )

            if state.consecutive_losses >= self.max_consecutive_losses:
                return False, (
                    f"max_consecutive_losses breached: "
                    f"{state.consecutive_losses} >= {self.max_consecutive_losses}"
                )

            balance = await self._get_balance()
            if balance < self.min_balance_usdt:
                return False, (f"min_balance_usdt breached: {balance} < {self.min_balance_usdt}")

            await session.commit()
            return True, None

    # ------------------------------------------------------------------
    # outcome update API (called by outcome tracker)
    # ------------------------------------------------------------------
    async def record_outcome(
        self,
        signal_id: uuid.UUID,
        pnl_r: Decimal,
        won: bool,
    ) -> None:
        async with self.session_factory() as session:
            state = await self._get_state(session)
            state.daily_pnl_r += Decimal(str(pnl_r))
            state.daily_trade_count += 1
            if won:
                state.consecutive_losses = 0
            else:
                state.consecutive_losses += 1

            # Auto-trip kill switch on hard limits
            if state.consecutive_losses >= self.max_consecutive_losses:
                state.kill_switch_active = True
                state.kill_switch_reason = "auto: max_consecutive_losses"
                state.kill_switch_set_at = datetime.now(timezone.utc)
                logger.critical(
                    "Kill switch AUTO-TRIPPED by consecutive_losses=%d",
                    state.consecutive_losses,
                )
                await self._send_alert(
                    f"🔒 Kill switch AUTO-TRIPPED\n"
                    f"Reason: consecutive_losses={state.consecutive_losses}"
                )

            await session.commit()
            logger.info(
                "Outcome recorded: signal=%s pnl_r=%s won=%s daily_r=%s streak=%s",
                signal_id,
                pnl_r,
                won,
                state.daily_pnl_r,
                state.consecutive_losses,
            )

    # ------------------------------------------------------------------
    # management API
    # ------------------------------------------------------------------
    async def set_kill_switch(self, reason: str) -> None:
        async with self.session_factory() as session:
            state = await self._get_state(session)
            state.kill_switch_active = True
            state.kill_switch_reason = reason
            state.kill_switch_set_at = datetime.now(timezone.utc)
            await session.commit()
        logger.critical("Kill switch ACTIVATED: %s", reason)
        await self._send_alert(f"🔒 Kill switch ACTIVATED\nReason: {reason}")

    async def clear_kill_switch(self) -> None:
        async with self.session_factory() as session:
            state = await self._get_state(session)
            state.kill_switch_active = False
            state.kill_switch_reason = None
            state.kill_switch_set_at = None
            await session.commit()
        logger.info("Kill switch CLEARED")
        await self._send_alert("✅ Kill switch CLEARED")

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
            }

    # ------------------------------------------------------------------
    # background tasks
    # ------------------------------------------------------------------
    async def _periodic_check_loop(self) -> None:
        while self._running:
            try:
                allowed, reason = await self.check(None)
                if not allowed and self._should_alert(reason):
                    logger.warning("Periodic risk check failed: %s", reason)
                    await self._send_alert(f"⚠️ Risk check failed\n{reason}")
            except Exception:
                logger.exception("Periodic risk check error")
            await asyncio.sleep(30)

    async def _daily_reset_loop(self) -> None:
        while self._running:
            now = datetime.now(timezone.utc)
            # Sleep until next midnight UTC
            next_midnight = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            wait = (next_midnight - now).total_seconds()
            logger.debug("Daily reset in %.0fs", wait)
            await asyncio.sleep(wait)

            try:
                await self._do_daily_reset()
                logger.info("Daily risk counters reset (00:00 UTC)")
                await self._send_alert("📅 Daily risk counters reset")
            except Exception:
                logger.exception("Daily reset error")

    async def _do_daily_reset(self) -> None:
        """Public helper for tests — zeros daily counters, preserves kill switch."""
        async with self.session_factory() as session:
            state = await self._get_state(session)
            state.daily_pnl_r = Decimal("0")
            state.daily_trade_count = 0
            state.consecutive_losses = 0
            state.last_reset_date = date.today()
            await session.commit()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    async def _get_state(self, session) -> RiskState:
        state = await session.get(RiskState, 1)
        if state is None:
            state = RiskState(id=1)
            session.add(state)
            await session.commit()
            await session.refresh(state)
        return state

    async def _get_balance(self) -> Decimal:
        if self.delta_client is None:
            return Decimal("999999")

        now = time.time()
        if self._balance_cache is not None and (now - self._balance_cache_time) < BALANCE_CACHE_TTL:
            return self._balance_cache

        try:
            resp = await self.delta_client.get_balance()
            balances = resp.get("balances", []) if isinstance(resp, dict) else []
            for b in balances:
                if b.get("asset", "").upper() == "USDT":
                    bal = Decimal(str(b.get("available_balance", b.get("balance", "0"))))
                    self._balance_cache = bal
                    self._balance_cache_time = now
                    return bal
        except Exception:
            logger.exception("Balance fetch failed")

        return Decimal("0")

    def _should_alert(self, reason: str) -> bool:
        now = datetime.now(timezone.utc)
        key = reason.split(":")[0]  # use first token as stable key
        last = self._alert_cooldown.get(key)
        if last is None or (now - last).total_seconds() > ALERT_COOLDOWN_SECONDS:
            self._alert_cooldown[key] = now
            return True
        return False

    async def _send_alert(self, message: str) -> None:
        if self.telegram is None:
            return
        try:
            if hasattr(self.telegram, "application") and self.telegram.application:
                await self.telegram.application.bot.send_message(
                    chat_id=self.telegram.chat_id,
                    text=message,
                )
        except Exception:
            logger.exception("Telegram alert failed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


async def _cli_kill(reason: str) -> None:
    gov = RiskGovernor()
    await gov.set_kill_switch(reason)
    print(f"Kill switch ACTIVATED: {reason}")


async def _cli_clear() -> None:
    gov = RiskGovernor()
    await gov.clear_kill_switch()
    print("Kill switch CLEARED")


async def _cli_status() -> None:
    gov = RiskGovernor()
    status = await gov.get_status()
    for k, v in status.items():
        print(f"{k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Risk governor CLI")
    parser.add_argument("--kill", type=str, help="Activate kill switch with reason")
    parser.add_argument("--clear", action="store_true", help="Clear kill switch")
    parser.add_argument("--status", action="store_true", help="Print risk state")
    args = parser.parse_args()

    if args.kill:
        asyncio.run(_cli_kill(args.kill))
    elif args.clear:
        asyncio.run(_cli_clear())
    elif args.status:
        asyncio.run(_cli_status())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
