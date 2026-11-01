"""Telegram delivery layer for signal notifications and bot commands."""

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.helpers import escape_markdown

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import Signal, SignalOutcome

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Sends signal/outcome messages and handles /status command."""

    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
    ) -> None:
        self.bot_token = bot_token or settings.telegram_bot_token
        self.chat_id = chat_id or settings.telegram_chat_id
        if not self.bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN is not set.")
        self.application = (
            Application.builder().token(self.bot_token).build() if self.bot_token else None
        )

    async def start(self) -> None:
        if not self.application:
            logger.error("Cannot start Telegram bot: no token.")
            return
        self.application.add_handler(CommandHandler("status", self._cmd_status))
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling(drop_pending_updates=True)
        logger.info("Telegram bot polling started.")

    async def stop(self) -> None:
        if not self.application:
            return
        await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()

    async def notify_signal(self, signal: Signal) -> None:
        if not self.chat_id or not self.application:
            logger.debug("Telegram not configured; skipping signal notification.")
            return
        text = self._format_signal(signal)
        escaped = escape_markdown(text, version=2)
        try:
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=escaped,
                parse_mode="MarkdownV2",
            )
        except Exception:
            logger.exception("Failed to send signal notification for %s", signal.id)

    async def notify_outcome(self, signal: Signal, outcome: SignalOutcome) -> None:
        if not self.chat_id or not self.application:
            return
        text = self._format_outcome(signal, outcome)
        escaped = escape_markdown(text, version=2)
        try:
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=escaped,
                parse_mode="MarkdownV2",
            )
        except Exception:
            logger.exception("Failed to send outcome notification for %s", signal.id)

    @staticmethod
    def _format_signal(signal: Signal) -> str:
        side_emoji = "🟢" if signal.side == "long" else "🔴"
        entry_f = float(signal.entry)
        stop_f = float(signal.stop_loss)
        tp_f = float(signal.take_profit)

        stop_pct = (stop_f - entry_f) / entry_f * 100
        if signal.side == "long":
            stop_pct = -abs(stop_pct)
        else:
            stop_pct = abs(stop_pct)

        tp_pct = (tp_f - entry_f) / entry_f * 100
        if signal.side == "long":
            tp_pct = abs(tp_pct)
        else:
            tp_pct = -abs(tp_pct)

        rr = abs(tp_pct / stop_pct) if stop_pct != 0 else 0

        lines = [
            f"{side_emoji} {signal.side.upper()}  {signal.symbol}  ({signal.est_hold_minutes}m)",
            "",
            f"Entry:    {signal.entry}",
            f"Stop:     {signal.stop_loss}   ({stop_pct:+.2f}%)",
            f"Target:   {signal.take_profit}   ({tp_pct:+.2f}%)",
            f"Leverage: {signal.leverage}x",
            f"Risk:     {signal.risk_pct}% of account",
            f"R:R:      {rr:.2f}",
            f"Hold:     ~{signal.est_hold_minutes} min",
        ]

        if signal.pattern_features:
            lines.append("")
            lines.append("Pattern:")
            for key, value in signal.pattern_features.items():
                lines.append(f"  {key}: {value}")

        return "\n".join(lines)

    @staticmethod
    def _format_outcome(signal: Signal, outcome: SignalOutcome) -> str:
        emoji = "✅" if outcome.exit_reason == "tp" else "❌"
        pnl_pct = float(outcome.pnl_pct)
        pnl_r = float(outcome.pnl_r)
        return (
            f"{emoji} {signal.symbol} {outcome.exit_reason.upper()} hit, "
            f"{pnl_pct:+.2f}% ({pnl_r:+.1f}R), {outcome.hold_minutes} min"
        )

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        async with AsyncSessionLocal() as session:
            open_result = await session.execute(select(Signal).where(Signal.status == "triggered"))
            open_signals = open_result.scalars().all()

            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            outcome_result = await session.execute(
                select(SignalOutcome, Signal)
                .join(Signal, SignalOutcome.signal_id == Signal.id)
                .where(SignalOutcome.exit_ts >= today)
            )
            today_outcomes = outcome_result.all()

        lines: list[str] = []
        lines.append("Open Signals")
        if open_signals:
            for s in open_signals:
                lines.append(f"• {s.side.upper()} {s.symbol} @ {s.entry} (lev {s.leverage}x)")
        else:
            lines.append("  None")

        lines.append("")
        lines.append("Today's PnL")
        if today_outcomes:
            total_r = sum(float(o.SignalOutcome.pnl_r) for o in today_outcomes)
            wins = sum(1 for o in today_outcomes if o.SignalOutcome.pnl_r > 0)
            total = len(today_outcomes)
            lines.append(f"Trades: {total}")
            lines.append(f"Win rate: {wins}/{total}")
            lines.append(f"Total R: {total_r:.2f}")
        else:
            lines.append("  No closed trades today")

        text = "\n".join(lines)
        escaped = escape_markdown(text, version=2)
        await update.message.reply_text(escaped, parse_mode="MarkdownV2")
