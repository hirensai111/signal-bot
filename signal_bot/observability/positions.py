"""In-memory view of venue positions fed by the event bus.

It needs no database, so open-position visibility survives a database outage
(the ops API serves it at /api/status even while the governor fails closed).
"""

from __future__ import annotations

from typing import Any

from signal_bot.events.bus import EventBus
from signal_bot.events.envelope import Event, EventType


class PositionView:
    def __init__(self, bus: EventBus, name: str = "position_view") -> None:
        self.sub = bus.subscribe(name, [EventType.EXCHANGE_POSITION_UPDATE])
        self.positions: dict[str, dict[str, Any]] = {}
        self._running = False

    def apply(self, event: Event) -> None:
        payload = event.payload
        symbol = payload.get("symbol")
        if not symbol:
            return
        if float(payload.get("size") or 0) == 0:
            self.positions.pop(symbol, None)
        else:
            self.positions[symbol] = {**payload, "as_of": event.occurred_at.isoformat()}

    async def drain(self) -> None:
        while not self.sub.queue.empty():
            self.apply(self.sub.queue.get_nowait())

    async def run(self) -> None:
        self._running = True
        while self._running:
            event = await self.sub.get(timeout=1.0)
            if event is not None:
                self.apply(event)

    def stop(self) -> None:
        self._running = False

    def snapshot(self) -> list[dict[str, Any]]:
        return sorted(self.positions.values(), key=lambda p: p["symbol"])
