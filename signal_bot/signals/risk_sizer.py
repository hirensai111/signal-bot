"""Risk sizing: converts SignalCandidate into a fully-sized Signal and persists it."""

import logging
import math
from datetime import datetime
from decimal import Decimal

from signal_bot.config import settings
from signal_bot.db.connection import AsyncSessionLocal
from signal_bot.db.models import Signal
from signal_bot.signals.sweep_detector import SignalCandidate

logger = logging.getLogger(__name__)


class RiskSizer:
    """Sizes positions from a SignalCandidate and persists to the signals table."""

    DEFAULT_HOLD_MINUTES = {"1m": 30, "5m": 120}

    def __init__(
        self,
        session_factory=None,
        account_size: float | None = None,
        risk_pct: float | None = None,
        max_leverage: int | None = None,
        hold_time_map: dict[str, int] | None = None,
    ) -> None:
        self.session_factory = session_factory or AsyncSessionLocal
        self.account_size = (
            Decimal(str(account_size))
            if account_size is not None
            else Decimal(str(settings.account_size_usdt))
        )
        self.risk_pct = (
            Decimal(str(risk_pct))
            if risk_pct is not None
            else Decimal(str(settings.risk_pct_per_trade))
        )
        self.max_leverage = max_leverage if max_leverage is not None else settings.max_leverage
        self.hold_time_map = hold_time_map or dict(self.DEFAULT_HOLD_MINUTES)

    async def size_and_persist(
        self,
        candidate: SignalCandidate,
        interval: str,
        created_at: datetime | None = None,
    ) -> Signal | None:
        """Compute size, cap leverage, and persist. Returns None if leverage exceeds max."""
        risk_amount = self.account_size * self.risk_pct / Decimal("100")
        stop_distance_pct = abs(candidate.entry - candidate.stop) / candidate.entry

        if stop_distance_pct == 0:
            logger.warning(
                "signal_skipped_zero_stop: %s %s entry=%s stop=%s",
                candidate.symbol,
                candidate.side,
                candidate.entry,
                candidate.stop,
            )
            return None

        position_notional = risk_amount / stop_distance_pct
        leverage = math.ceil(position_notional / self.account_size)

        if leverage > self.max_leverage:
            logger.info(
                "signal_skipped_leverage_too_high: %s %s required=%dx max=%dx",
                candidate.symbol,
                candidate.side,
                leverage,
                self.max_leverage,
            )
            return None

        est_hold = self.hold_time_map.get(interval, 30)

        signal = Signal(
            symbol=candidate.symbol,
            side=candidate.side,
            entry=candidate.entry,
            stop_loss=candidate.stop,
            take_profit=candidate.proposed_tp,
            leverage=leverage,
            risk_pct=self.risk_pct,
            account_size_assumed=self.account_size,
            est_hold_minutes=est_hold,
            pattern_features=candidate.pattern_features,
            created_at=created_at,
            status="pending",
        )

        async with self.session_factory() as session:
            session.add(signal)
            await session.commit()
            await session.refresh(signal)

        logger.info(
            "Signal triggered: %s %s entry=%s stop=%s tp=%s leverage=%dx risk=%s%%",
            signal.symbol,
            signal.side,
            signal.entry,
            signal.stop_loss,
            signal.take_profit,
            signal.leverage,
            signal.risk_pct,
        )

        return signal
