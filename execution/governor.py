"""Risk governor protocol — placeholder for prompt-12 integration."""

from typing import Optional, Protocol

from signal_bot.db.models import Signal


class RiskGovernor(Protocol):
    """Protocol for a pre-trade risk check."""

    async def check(self, signal: Optional[Signal] = None) -> tuple[bool, Optional[str]]:
        """Return (approved: bool, reason_if_denied: Optional[str])."""
        ...


class BaseGovernor:
    """Always-approves governor.  Use as default or for dry-run."""

    async def check(self, signal: Optional[Signal] = None) -> tuple[bool, Optional[str]]:
        return True, None
