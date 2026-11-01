"""Injectable clock so time-dependent logic (timeouts, staleness, grace windows) is testable."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...

    def monotonic(self) -> float: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        return time.monotonic()


class ManualClock:
    """Deterministic clock for simulations and tests. Time only moves when advanced."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2024, 1, 1, tzinfo=timezone.utc)
        self._mono = 0.0

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._mono

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)
        self._mono += seconds

    def set(self, when: datetime) -> None:
        delta = (when - self._now).total_seconds()
        if delta < 0:
            raise ValueError("ManualClock cannot move backwards")
        self.advance(delta)


def utc(dt: datetime | None) -> datetime | None:
    """Normalise naive datetimes (SQLite drops tzinfo) to UTC-aware."""
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


SYSTEM_CLOCK = SystemClock()
