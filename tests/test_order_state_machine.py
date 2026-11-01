"""Order state machine: transition table, venue mapping, and store sequence rules."""

from datetime import timedelta
from decimal import Decimal

import pytest

from signal_bot.execution.adapter import ExchangeOrder, VenueOrderState
from signal_bot.execution.orders import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATES,
    OrderState,
    can_transition,
    state_from_venue,
)
from tests.harness import Harness

S = OrderState


def test_terminal_states_have_no_exits():
    for state in TERMINAL_STATES:
        assert ALLOWED_TRANSITIONS[state] == frozenset()


def test_every_state_has_a_transition_entry():
    assert set(ALLOWED_TRANSITIONS) == set(OrderState)


@pytest.mark.parametrize(
    "current,target,ok",
    [
        (S.PENDING, S.SUBMITTING, True),
        (S.PENDING, S.FILLED, False),  # must persist intent first
        (S.SUBMITTING, S.SUBMISSION_UNKNOWN, True),
        (S.SUBMISSION_UNKNOWN, S.FILLED, True),  # resolved by lookup
        (S.SUBMISSION_UNKNOWN, S.SUBMITTING, False),  # never resubmit
        (S.ACKNOWLEDGED, S.CANCEL_PENDING, True),
        (S.PARTIALLY_FILLED, S.CANCELLED, False),  # exposure exists: becomes FILLED
        (S.FILLED, S.CLOSED, True),
        (S.CLOSED, S.FILLED, False),
        (S.CANCELLED, S.ACKNOWLEDGED, False),
    ],
)
def test_transition_table(current, target, ok):
    assert can_transition(current, target) is ok


@pytest.mark.parametrize(
    "venue,filled,size,expected",
    [
        (VenueOrderState.OPEN, 0, 10, S.ACKNOWLEDGED),
        (VenueOrderState.OPEN, 3, 10, S.PARTIALLY_FILLED),
        (VenueOrderState.PARTIALLY_FILLED, 10, 10, S.FILLED),
        (VenueOrderState.FILLED, 10, 10, S.FILLED),
        (VenueOrderState.CANCELLED, 0, 10, S.CANCELLED),
        (VenueOrderState.CANCELLED, 4, 10, S.FILLED),  # partial then remainder cancelled
        (VenueOrderState.EXPIRED, 0, 10, S.EXPIRED),
        (VenueOrderState.REJECTED, 0, 10, S.REJECTED),
    ],
)
def test_state_from_venue(venue, filled, size, expected):
    assert state_from_venue(venue, Decimal(filled), Decimal(size)) == expected


async def _order_in_store(tmp_path):
    h = await Harness.create(tmp_path)
    sig = await h.make_signal()
    order = await h.store.create(
        sig,
        venue="sim",
        client_order_id="sig_test",
        symbol="BTCUSD",
        side="buy",
        order_type="limit",
        size=Decimal("10"),
        contract_value=Decimal("0.001"),
        limit_price=Decimal("65000"),
        intended_entry=Decimal("65000"),
        stop_loss_price=Decimal("64000"),
        take_profit_price=Decimal("67000"),
    )
    await h.store.transition("sig_test", S.SUBMITTING, event_id="e-submit")
    await h.store.transition("sig_test", S.ACKNOWLEDGED, event_id="e-ack")
    return h, sig, order


def _update(h, state, filled, ts_offset=0):
    return ExchangeOrder(
        client_order_id="sig_test",
        exchange_order_id="x1",
        symbol="BTCUSD",
        side="buy",
        size=Decimal("10"),
        filled_size=Decimal(filled),
        avg_fill_price=Decimal("65000") if filled else None,
        state=state,
        updated_at=h.clock.now() + timedelta(seconds=ts_offset),
    )


async def test_same_event_id_is_applied_once(tmp_path):
    h, sig, _ = await _order_in_store(tmp_path)
    first = await h.store.apply_venue_update(_update(h, VenueOrderState.OPEN, 4, 1), event_id="ev1")
    again = await h.store.apply_venue_update(_update(h, VenueOrderState.OPEN, 4, 1), event_id="ev1")
    assert first.outcome == "applied" and again.outcome == "duplicate"
    assert len(await h.store.transitions_for("sig_test")) == 3  # submit, ack, partial
    await h.close()


async def test_reordered_updates_never_move_backwards(tmp_path):
    """'filled' arrives before the earlier 'partially_filled' — the late partial is stale."""
    h, sig, _ = await _order_in_store(tmp_path)
    filled = await h.store.apply_venue_update(
        _update(h, VenueOrderState.FILLED, 10, 2), event_id="ev-filled"
    )
    late_partial = await h.store.apply_venue_update(
        _update(h, VenueOrderState.OPEN, 4, 1), event_id="ev-partial"
    )
    late_ack = await h.store.apply_venue_update(
        _update(h, VenueOrderState.OPEN, 0, 3), event_id="ev-ack-late"
    )
    assert filled.applied
    assert late_partial.outcome == "stale"
    assert late_ack.outcome == "stale"  # filled_size can't decrease
    order = await h.store.get_by_client_id("sig_test")
    assert order.state == S.FILLED and order.filled_size == Decimal("10")
    assert (await h.signal(sig)).status == "triggered"
    await h.close()


async def test_partial_fill_progress_is_a_self_transition(tmp_path):
    h, _, _ = await _order_in_store(tmp_path)
    a = await h.store.apply_venue_update(_update(h, VenueOrderState.OPEN, 3, 1), event_id="p1")
    b = await h.store.apply_venue_update(_update(h, VenueOrderState.OPEN, 6, 2), event_id="p2")
    c = await h.store.apply_venue_update(_update(h, VenueOrderState.OPEN, 6, 3), event_id="p3")
    assert (a.outcome, b.outcome, c.outcome) == ("applied", "applied", "noop")
    assert (await h.store.get_by_client_id("sig_test")).filled_size == Decimal("6")
    await h.close()


async def test_transitions_are_announced_on_the_bus(tmp_path):
    h, _, _ = await _order_in_store(tmp_path)
    changes = await h.journal.read(0, ["ORDER_STATE_CHANGED"])
    assert [e.payload["to_state"] for e in changes] == ["submitting", "acknowledged"]
    await h.close()
