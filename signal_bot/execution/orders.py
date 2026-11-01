"""Persistent order state machine.

    pending ──► submitting ──► acknowledged ──► partially_filled ──► filled ──► closed
                    │               │                  │               │
                    ├─► rejected    ├─► cancel_pending ┴─► (remainder   └─► reconcile_required
                    │               │       │               cancelled =>
                    └─► submission_unknown  └─► cancelled    filled w/ partial size)
                            │
                            └─► (lookup by client_order_id) ─► any venue state
                                └─► reconcile_required (not resolvable yet; trading blocked)

``filled`` means "entry finished, position open with ``filled_size``"; a
partially filled order whose remainder is cancelled/expired becomes ``filled``
with ``filled_size < size``.  ``closed`` means the position has been exited.
"""

from __future__ import annotations

from enum import StrEnum

from signal_bot.execution.adapter import VenueOrderState


class OrderState(StrEnum):
    PENDING = "pending"
    SUBMITTING = "submitting"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CLOSED = "closed"
    CANCEL_PENDING = "cancel_pending"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUBMISSION_UNKNOWN = "submission_unknown"
    RECONCILIATION_REQUIRED = "reconcile_required"


S = OrderState

TERMINAL_STATES: frozenset[OrderState] = frozenset({S.CLOSED, S.CANCELLED, S.REJECTED, S.EXPIRED})

# States in which the venue may hold exposure (or an order that can create it).
EXPOSURE_STATES: frozenset[OrderState] = frozenset(
    {
        S.SUBMITTING,
        S.ACKNOWLEDGED,
        S.PARTIALLY_FILLED,
        S.FILLED,
        S.CANCEL_PENDING,
        S.SUBMISSION_UNKNOWN,
        S.RECONCILIATION_REQUIRED,
    }
)

# Unresolved states block new submissions until reconciled.
UNRESOLVED_STATES: frozenset[OrderState] = frozenset(
    {S.SUBMISSION_UNKNOWN, S.RECONCILIATION_REQUIRED}
)

_VENUE_OUTCOMES = {S.ACKNOWLEDGED, S.PARTIALLY_FILLED, S.FILLED, S.CANCELLED, S.REJECTED, S.EXPIRED}

ALLOWED_TRANSITIONS: dict[OrderState, frozenset[OrderState]] = {
    S.PENDING: frozenset({S.SUBMITTING, S.CANCELLED, S.REJECTED}),
    S.SUBMITTING: frozenset(_VENUE_OUTCOMES | {S.SUBMISSION_UNKNOWN, S.RECONCILIATION_REQUIRED}),
    S.SUBMISSION_UNKNOWN: frozenset(_VENUE_OUTCOMES | {S.RECONCILIATION_REQUIRED}),
    S.RECONCILIATION_REQUIRED: frozenset(_VENUE_OUTCOMES | {S.CLOSED}),
    S.ACKNOWLEDGED: frozenset(
        {
            S.PARTIALLY_FILLED,
            S.FILLED,
            S.CANCEL_PENDING,
            S.CANCELLED,
            S.EXPIRED,
            S.REJECTED,
            S.RECONCILIATION_REQUIRED,
        }
    ),
    S.PARTIALLY_FILLED: frozenset(
        {S.PARTIALLY_FILLED, S.FILLED, S.CANCEL_PENDING, S.RECONCILIATION_REQUIRED}
    ),
    S.CANCEL_PENDING: frozenset(
        {S.CANCELLED, S.EXPIRED, S.PARTIALLY_FILLED, S.FILLED, S.RECONCILIATION_REQUIRED}
    ),
    S.FILLED: frozenset({S.CLOSED, S.RECONCILIATION_REQUIRED}),
    S.CLOSED: frozenset(),
    S.CANCELLED: frozenset(),
    S.REJECTED: frozenset(),
    S.EXPIRED: frozenset(),
}

# Mirror onto the legacy ``signals.status`` column (kept for Telegram/status/daily summary).
SIGNAL_STATUS: dict[OrderState, str] = {
    S.PENDING: "pending",
    S.SUBMITTING: "submitted",
    S.ACKNOWLEDGED: "submitted",
    S.CANCEL_PENDING: "submitted",
    S.PARTIALLY_FILLED: "triggered",
    S.FILLED: "triggered",
    S.CLOSED: "closed",  # refined to sl/tp/... by the outcome monitor
    S.CANCELLED: "cancelled",
    S.REJECTED: "cancelled",
    S.EXPIRED: "expired_no_fill",
    S.SUBMISSION_UNKNOWN: "submission_unknown",
    S.RECONCILIATION_REQUIRED: "reconcile_required",
}

# signals.status values that represent possible venue exposure.
SIGNAL_EXPOSURE_STATUSES: tuple[str, ...] = (
    "submitted",
    "triggered",
    "submission_unknown",
    "reconcile_required",
)


class InvalidTransition(Exception):
    def __init__(self, current: OrderState, target: OrderState) -> None:
        super().__init__(f"illegal order transition {current} -> {target}")
        self.current = current
        self.target = target


def can_transition(current: OrderState, target: OrderState) -> bool:
    return target in ALLOWED_TRANSITIONS[current]


def state_from_venue(venue_state: VenueOrderState, filled_size, size) -> OrderState:
    """Map a venue order state onto the local machine (partial-then-cancel => filled)."""
    if venue_state == VenueOrderState.OPEN:
        return S.PARTIALLY_FILLED if filled_size > 0 else S.ACKNOWLEDGED
    if venue_state == VenueOrderState.PARTIALLY_FILLED:
        return S.FILLED if filled_size >= size else S.PARTIALLY_FILLED
    if venue_state == VenueOrderState.FILLED:
        return S.FILLED
    if venue_state in (VenueOrderState.CANCELLED, VenueOrderState.EXPIRED):
        if filled_size > 0:
            return S.FILLED
        return S.CANCELLED if venue_state == VenueOrderState.CANCELLED else S.EXPIRED
    return S.REJECTED
