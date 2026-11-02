"""The event contract shared by every component.

Every significant fact in the system (a candle closing, a signal being generated,
an exchange order update, a risk halt) is an :class:`Event` with a stable
identity.  Exchange-originated events get a *deterministic* ``event_id`` derived
from the exchange's own identifiers so that the same fill delivered twice (WS
redelivery, REST reconciliation, replay after restart) collapses to one journal
row and one financial effect.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any

SCHEMA_VERSION = 1


class EventType(StrEnum):
    # market data
    CANDLE_CLOSED = "CANDLE_CLOSED"
    # strategy
    SIGNAL_GENERATED = "SIGNAL_GENERATED"
    # local order lifecycle (emitted by the order store after a persisted transition)
    ORDER_STATE_CHANGED = "ORDER_STATE_CHANGED"
    # normalised exchange facts (emitted by venue adapters / streams)
    EXCHANGE_ORDER_UPDATE = "EXCHANGE_ORDER_UPDATE"
    EXCHANGE_FILL = "EXCHANGE_FILL"
    EXCHANGE_POSITION_UPDATE = "EXCHANGE_POSITION_UPDATE"
    STREAM_CONNECTED = "STREAM_CONNECTED"
    STREAM_DISCONNECTED = "STREAM_DISCONNECTED"
    # position lifecycle
    POSITION_CLOSED = "POSITION_CLOSED"
    PROTECTIVE_ORDER_FAILED = "PROTECTIVE_ORDER_FAILED"
    PROTECTIVE_STOP_AMENDED = "PROTECTIVE_STOP_AMENDED"
    # risk & operations
    RISK_DENIED = "RISK_DENIED"
    RISK_HALT = "RISK_HALT"
    RISK_RESUMED = "RISK_RESUMED"
    RECONCILIATION_COMPLETED = "RECONCILIATION_COMPLETED"
    RECONCILIATION_DISCREPANCY = "RECONCILIATION_DISCREPANCY"
    OPERATOR_ACTION = "OPERATOR_ACTION"


# Events whose loss would corrupt financial state or the audit trail.  They are
# written to the journal before being delivered to any subscriber.
DURABLE_EVENT_TYPES: frozenset[str] = frozenset(
    {
        EventType.SIGNAL_GENERATED,
        EventType.ORDER_STATE_CHANGED,
        EventType.EXCHANGE_ORDER_UPDATE,
        EventType.EXCHANGE_FILL,
        EventType.EXCHANGE_POSITION_UPDATE,
        EventType.STREAM_CONNECTED,
        EventType.STREAM_DISCONNECTED,
        EventType.POSITION_CLOSED,
        EventType.PROTECTIVE_ORDER_FAILED,
        EventType.PROTECTIVE_STOP_AMENDED,
        EventType.RISK_DENIED,
        EventType.RISK_HALT,
        EventType.RISK_RESUMED,
        EventType.RECONCILIATION_COMPLETED,
        EventType.RECONCILIATION_DISCREPANCY,
        EventType.OPERATOR_ACTION,
    }
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_event_id() -> str:
    return uuid.uuid4().hex


def deterministic_event_id(*parts: Any) -> str:
    """Stable id from source identifiers, e.g. ``("delta", "fill", trade_id)``."""
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, set | frozenset):
        return sorted(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serialisable")


def to_jsonable(payload: dict[str, Any]) -> dict[str, Any]:
    """Round-trip through JSON so payloads stored in the journal equal live ones."""
    return json.loads(json.dumps(payload, default=_json_default, sort_keys=True))


@dataclass(frozen=True, slots=True)
class Event:
    event_type: str
    payload: dict[str, Any]
    source: str
    event_id: str = field(default_factory=new_event_id)
    occurred_at: datetime = field(default_factory=_utcnow)
    recorded_at: datetime = field(default_factory=_utcnow)
    correlation_id: str | None = None
    causation_id: str | None = None
    schema_version: int = SCHEMA_VERSION
    sequence: int | None = None  # assigned by the journal on append

    @classmethod
    def create(
        cls,
        event_type: str,
        payload: dict[str, Any],
        source: str,
        *,
        event_id: str | None = None,
        occurred_at: datetime | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> Event:
        return cls(
            event_type=str(event_type),
            payload=to_jsonable(payload),
            source=source,
            event_id=event_id or new_event_id(),
            occurred_at=occurred_at or _utcnow(),
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    @property
    def is_durable(self) -> bool:
        return self.event_type in DURABLE_EVENT_TYPES

    def with_sequence(self, sequence: int | None) -> Event:
        return replace(self, sequence=sequence)

    def caused(self, event_type: str, payload: dict[str, Any], source: str, **kw: Any) -> Event:
        """Create a follow-on event that inherits this event's correlation id."""
        return Event.create(
            event_type,
            payload,
            source,
            correlation_id=kw.pop("correlation_id", None) or self.correlation_id,
            causation_id=self.event_id,
            **kw,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "occurred_at": self.occurred_at.isoformat(),
            "recorded_at": self.recorded_at.isoformat(),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "payload": self.payload,
        }
