"""The failure scenarios the platform must survive, as automated end-to-end tests.

Each test drives the production execution stack (engine, order store, risk
governor, outcome monitor, reconciler, event bus + SQL journal) against the
fault-injecting simulated venue.  See docs/failure-scenarios.md for the matrix.
"""

from datetime import timedelta
from decimal import Decimal

import pytest

from signal_bot.execution.engine import client_order_id_for
from signal_bot.execution.orders import OrderState
from tests.harness import Harness

pytestmark = pytest.mark.scenario
S = OrderState


@pytest.fixture
async def h(tmp_path):
    harness = await Harness.create(tmp_path)
    yield harness
    await harness.close()


async def _hit_take_profit(h, symbol="BTCUSD", price="67100"):
    await h.venue.move_market(symbol, Decimal(price), Decimal(price) + Decimal("0.5"))
    await h.drain()


# ---------------------------------------------------------------------------
# 1. Exchange accepts the order but the HTTP response times out
# ---------------------------------------------------------------------------


async def test_accepted_but_response_lost_is_resolved_by_lookup_not_resubmitted(h):
    h.venue.faults.add("place_order", "timeout_after_accept")
    sig = await h.make_signal()
    await h.submit(sig)

    order = await h.order_for(sig)
    assert order.state == S.FILLED  # discovered via GET by client_order_id
    assert h.venue.calls["place_order"] == 1  # never resubmitted
    assert h.venue.calls["get_order"] >= 1
    states = [t.to_state for t in await h.store.transitions_for(order.client_order_id)]
    assert states[:2] == ["submitting", "submission_unknown"]
    assert "filled" in states
    assert not any(b.startswith("unresolved_order") for b in h.governor.breakers)
    assert order.protective_status == "confirmed"


async def test_request_lost_before_accept_blocks_trading_until_reconciled(tmp_path):
    h = await Harness.create(tmp_path, unknown_grace_seconds=120)
    h.venue.faults.add("place_order", "timeout_before_accept")
    sig = await h.make_signal()
    await h.submit(sig)

    order = await h.order_for(sig)
    assert order.state == S.RECONCILIATION_REQUIRED  # not found yet: still unknown, NOT cancelled
    assert h.venue.calls["place_order"] == 1
    assert any(b.startswith("unresolved_order") for b in h.governor.breakers)

    # Any further signal is blocked while the outcome is unresolved.
    other = await h.make_signal(symbol="ETHUSDT", entry="3500", stop="3450", tp="3600")
    await h.submit(other)
    assert (await h.signal(other)).cancelled_reason.startswith("circuit_breaker:unresolved_order")
    assert h.venue.calls["place_order"] == 1

    # Within the grace window the reconciler keeps it unresolved...
    await h.reconciler.reconcile_now()
    assert (await h.order_for(sig)).state == S.RECONCILIATION_REQUIRED
    # ...after it, with no venue order and no position, it is safely cancelled.
    h.clock.advance(121)
    await h.reconciler.reconcile_now()
    assert (await h.order_for(sig)).state == S.CANCELLED
    assert (await h.order_for(sig)).state_reason == "not_found_on_exchange"
    assert h.governor.breakers == {}


async def test_server_error_after_accept_on_resting_order(h):
    h.config.order_type = "limit"
    h.venue.faults.add("place_order", "server_error_after_accept")
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.order_for(sig)).state == S.ACKNOWLEDGED
    assert h.venue.calls["place_order"] == 1


# ---------------------------------------------------------------------------
# 2. Same fill arrives twice
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fault", ["duplicate_events", "duplicate_fills_new_id"])
async def test_duplicate_fill_has_financial_effect_once(h, fault):
    sig = await h.make_signal()
    await h.submit(sig)
    h.venue.faults.add("stream", fault, times=-1)
    await _hit_take_profit(h)
    # and once more via the reconciler pulling fills over REST
    await h.monitor.reconstruct_close(await h.order_for(sig))

    outcomes = await h.outcomes()
    results = await h.trade_results()
    assert len(outcomes) == 1 and len(results) == 1
    fills = await h.store.fills_for(client_order_id_for(sig.id))
    assert len(fills) == len({f.fill_id for f in fills}) == 2  # one entry, one exit
    risk = await h.risk_state()
    assert risk.daily_pnl_r == results[0].realized_r  # counted exactly once
    assert risk.consecutive_losses == 0


# ---------------------------------------------------------------------------
# 3. Exchange WebSocket disconnects
# ---------------------------------------------------------------------------


async def test_stream_disconnect_blocks_orders_then_reconnect_reconciles(h):
    sig = await h.make_signal()
    await h.submit(sig)

    await h.venue.disconnect_stream()
    await h.drain()
    assert "exchange_stream" in h.governor.breakers

    # While disconnected, the position is stopped out — the client never hears about it.
    await h.venue.move_market("BTCUSD", Decimal("63900"), Decimal("63900.5"))
    await h.drain()
    assert (await h.order_for(sig)).state == S.FILLED  # local view is stale

    blocked = await h.make_signal(symbol="ETHUSDT", entry="3500", stop="3450", tp="3600")
    await h.submit(blocked)
    assert (await h.signal(blocked)).cancelled_reason.startswith("circuit_breaker:exchange_stream")
    assert h.venue.calls["place_order"] == 1

    await h.venue.reconnect_stream()
    await h.drain()  # reconnect -> reconcile -> missed close reconstructed -> breaker cleared
    assert "exchange_stream" not in h.governor.breakers
    order = await h.order_for(sig)
    assert order.state == S.CLOSED
    [result] = await h.trade_results()
    assert result.exit_reason == "sl_hit" and result.net_pnl < 0


# ---------------------------------------------------------------------------
# 4. Process crashes between submission and database update
# ---------------------------------------------------------------------------


async def test_crash_after_submit_recovers_from_intent_and_venue_state(tmp_path):
    h = await Harness.create(tmp_path)
    sig = await h.make_signal()

    # Crash simulation: the venue accepted the order but the process died before
    # recording the response.  Only the persisted intent survives.
    h.venue.faults.add("place_order", "timeout_after_accept")
    h.engine.resolve_unknown = _crash  # the process dies right after the call
    with pytest.raises(SystemExit):
        await h.engine.process_signal(sig)
    order = await h.order_for(sig)
    assert order.state == S.SUBMISSION_UNKNOWN

    # "Restart": fresh engine/reconciler objects over the same database and venue.
    restarted = await Harness.create(tmp_path, book=False, reset_db=False)
    restarted.venue = h.venue
    restarted.engine.adapter = restarted.reconciler.adapter = h.venue
    restarted.monitor.adapter = h.venue
    restarted.governor.exchange = h.venue
    await restarted.reconciler.reconcile_now(reason="startup")
    await restarted.drain()

    order = await restarted.order_for(sig)
    assert order.state == S.FILLED
    assert order.protective_status == "confirmed"
    assert h.venue.calls["place_order"] == 1  # recovery never resubmits
    assert restarted.governor.breakers == {}
    await restarted.close()
    await h.close()


async def _crash(*_a, **_k):
    raise SystemExit("simulated crash")


async def test_restart_expires_signals_that_were_only_in_memory(h):
    stale = await h.make_signal(created_at=h.clock.now() - timedelta(hours=1))
    await h.engine._expire_stale_pending_signals()
    assert (await h.signal(stale)).status == "expired"


# ---------------------------------------------------------------------------
# 5. Database becomes unavailable
# ---------------------------------------------------------------------------


async def test_database_outage_stops_submissions_keeps_position_visibility(h):
    from signal_bot.observability.positions import PositionView

    view = PositionView(h.bus)
    sig = await h.make_signal()
    await h.submit(sig)
    await view.drain()
    assert view.positions["BTCUSD"]["size"] == "100"

    blocked = await h.make_signal(symbol="ETHUSDT", entry="3500", stop="3450", tp="3600")
    h.factory.down = True
    assert await h.governor.check(None) == (False, "risk_state_unavailable")
    assert h.governor.healthy is False
    order = await h.engine.process_signal(blocked)
    assert order is None
    assert h.venue.calls["place_order"] == 1  # nothing new sent

    # The venue keeps publishing; events are delivered and buffered for the journal.
    await h.venue.move_market("BTCUSD", Decimal("63900"), Decimal("63900.5"))
    await view.drain()
    assert "BTCUSD" not in view.positions or view.positions["BTCUSD"]["size"] == "0"
    assert h.bus.journal_healthy is False and h.bus.pending_journal_writes > 0

    # Recovery: database back, journal flushed, close recorded, governor healthy again.
    h.factory.down = False
    await h.bus.flush_pending()
    assert h.bus.pending_journal_writes == 0
    await h.reconciler.reconcile_now()
    await h.drain()
    assert (await h.order_for(sig)).state == S.CLOSED
    assert (await h.governor.check(None))[0] is True
    assert h.governor.healthy is True


# ---------------------------------------------------------------------------
# 6. Risk limit or kill switch activates
# ---------------------------------------------------------------------------


async def test_kill_switch_rejects_every_submission_until_explicitly_cleared(h):
    await h.governor.set_kill_switch("operator halt", actor="test")
    for symbol, entry, stop, tp in [
        ("BTCUSDT", "65000", "64000", "67000"),
        ("ETHUSDT", "3500", "3450", "3600"),
        ("BTCUSDT", "65000", "66000", "63000"),
    ]:
        side = "short" if Decimal(stop) > Decimal(entry) else "long"
        sig = await h.make_signal(symbol=symbol, side=side, entry=entry, stop=stop, tp=tp)
        await h.submit(sig)
        assert (await h.signal(sig)).cancelled_reason.startswith("kill_switch_active")
    assert "place_order" not in h.venue.calls

    h.governor.clear_breakers_with_prefix("")  # clearing breakers does NOT clear the kill switch
    sig = await h.make_signal()
    await h.submit(sig)
    assert "place_order" not in h.venue.calls

    await h.governor.clear_kill_switch(actor="test", reason="reviewed")
    sig = await h.make_signal()
    await h.submit(sig)
    assert h.venue.calls["place_order"] == 1


async def test_consecutive_losses_trip_kill_switch(tmp_path):
    h = await Harness.create(tmp_path, governor_kwargs={"max_consecutive_losses": 2})
    for _ in range(2):
        sig = await h.make_signal()
        await h.submit(sig)
        await h.venue.move_market("BTCUSD", Decimal("63900"), Decimal("63900.5"))
        await h.drain()
        await h.venue.move_market("BTCUSD", Decimal("64999.5"), Decimal("65000"))
    assert (await h.risk_state()).kill_switch_active is True
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason.startswith("kill_switch_active")
    await h.close()


async def test_daily_loss_limit_blocks_new_trades(tmp_path):
    h = await Harness.create(tmp_path, governor_kwargs={"daily_loss_limit_r": -1.0})
    sig = await h.make_signal()
    await h.submit(sig)
    await h.venue.move_market("BTCUSD", Decimal("63900"), Decimal("63900.5"))
    await h.drain()
    assert (await h.risk_state()).daily_pnl_r < Decimal("-1")
    await h.venue.move_market("BTCUSD", Decimal("64999.5"), Decimal("65000"))
    nxt = await h.make_signal()
    await h.submit(nxt)
    assert (await h.signal(nxt)).cancelled_reason.startswith("daily_loss_limit_r")
    await h.close()


# ---------------------------------------------------------------------------
# 7. A candle or fill event arrives late
# ---------------------------------------------------------------------------


async def test_reordered_venue_events_converge_to_correct_state(tmp_path):
    """Stream delivers final-before-partial; the REST response is lost so only the
    (reordered) stream can tell us what happened."""
    h = await Harness.create(tmp_path)
    h.venue.set_book("BTCUSD", Decimal("64999.5"), Decimal("65000"), size=Decimal("40"), levels=3)
    h.venue.faults.add("stream", "reorder_events")
    h.venue.faults.add("place_order", "timeout_after_accept")
    h.venue.faults.add("get_order", "not_found", times=3)
    sig = await h.make_signal()
    await h.engine.process_signal(sig)
    assert (await h.order_for(sig)).state == S.RECONCILIATION_REQUIRED

    await h.drain()  # reversed: final(80) -> partial(80) -> partial(40) -> open(0)
    order = await h.order_for(sig)
    assert order.state == S.FILLED
    assert order.filled_size == Decimal("80")
    states = [t.to_state for t in await h.store.transitions_for(order.client_order_id)]
    assert states.count("filled") == 1
    assert "partially_filled" not in states  # late partials were recognised as stale
    assert not any(b.startswith("unresolved_order") for b in h.governor.breakers)
    await h.close()


async def test_late_signal_from_old_candle_is_not_traded(h):
    sig = await h.make_signal(created_at=h.clock.now() - timedelta(minutes=10))
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason == "signal_stale"


# ---------------------------------------------------------------------------
# 8. Protective order fails after entry fill
# ---------------------------------------------------------------------------


async def test_missing_protective_stop_halts_and_attaches_standalone_stop(h):
    h.venue.faults.add("place_order", "no_protective_legs")
    sig = await h.make_signal()
    await h.submit(sig)

    order = await h.order_for(sig)
    assert order.protective_status == "recovered"
    assert h.venue.position("BTCUSD").has_stop_loss
    assert (await h.risk_state()).kill_switch_active is True  # critical state until reviewed
    assert any("CRITICAL" in m for m in h.alerts.messages)
    journal = await h.journal.read(0, ["PROTECTIVE_ORDER_FAILED"])
    assert len(journal) == 1


async def test_protective_recovery_flattens_when_stop_cannot_be_attached(h):
    h.venue.faults.add("place_order", "no_protective_legs")
    h.venue.faults.add("protective", "reject", times=-1)
    sig = await h.make_signal()
    await h.submit(sig)

    order = await h.order_for(sig)
    assert order.protective_status == "flattened"
    assert h.venue.position("BTCUSD") is None
    assert order.state == S.CLOSED
    [result] = await h.trade_results()
    assert result.exit_reason == "manual_close"
    assert (await h.risk_state()).kill_switch_active is True


async def test_unprotectable_and_unflattenable_position_requires_reconciliation(h):
    h.venue.faults.add("place_order", "no_protective_legs")
    h.venue.faults.add("protective", "reject", times=-1)
    h.venue.faults.add("close_position", "unavailable", times=-1)
    sig = await h.make_signal()
    await h.submit(sig)
    order = await h.order_for(sig)
    assert order.protective_status == "failed"
    assert order.state == S.RECONCILIATION_REQUIRED
    assert (await h.risk_state()).kill_switch_active is True
