"""Reconciler: venue truth vs local intent."""

import asyncio
from decimal import Decimal

import pytest

from signal_bot.execution.adapter import OrderRequest
from signal_bot.execution.orders import OrderState
from tests.harness import Harness

S = OrderState


@pytest.fixture
async def h(tmp_path):
    harness = await Harness.create(tmp_path, unknown_grace_seconds=60)
    yield harness
    await harness.close()


async def test_synced_state_has_no_discrepancies(h):
    sig = await h.make_signal()
    await h.submit(sig)
    assert await h.reconciler.reconcile_now() is True
    assert h.reconciler.last_discrepancies == []
    assert h.governor.breakers == {}


async def test_missed_fill_is_adopted_from_venue(h):
    h.config.order_type = "limit"
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.order_for(sig)).state == S.ACKNOWLEDGED
    # The resting order fills while every stream event is lost.
    h.venue.faults.add("stream", "drop_events", times=-1)
    order = h.venue._orders[(await h.order_for(sig)).client_order_id]
    h.venue._fill(order, order.request.size, Decimal("65000"), "maker", order.purpose)
    order.state = order.state.FILLED
    h.venue._attach_legs(order, attach=True)

    await h.reconciler.reconcile_now()
    await h.drain()
    local = await h.order_for(sig)
    assert local.state == S.FILLED
    assert local.protective_status == "confirmed"
    assert {d["kind"] for d in h.reconciler.last_discrepancies} == {"missed_order_update"}
    fills = await h.store.fills_for(local.client_order_id)
    assert sum((f.qty for f in fills if f.purpose == "entry"), Decimal("0")) == local.filled_size
    await h.reconciler.reconcile_now()
    assert len(await h.store.fills_for(local.client_order_id)) == len(fills)


async def test_missed_close_is_reconstructed(h):
    sig = await h.make_signal()
    await h.submit(sig)
    h.venue.faults.add("stream", "drop_events", times=-1)
    await h.venue.move_market("BTCUSD", Decimal("67100"), Decimal("67100.5"))
    await h.drain()
    assert (await h.order_for(sig)).state == S.FILLED
    await h.reconciler.reconcile_now()
    assert (await h.order_for(sig)).state == S.CLOSED
    [result] = await h.trade_results()
    assert result.exit_reason == "tp_hit"


async def test_orphan_position_blocks_trading_until_gone(h):
    # Someone trades manually on the account.
    await h.venue.place_order(OrderRequest("manual-1", "ETHUSD", "buy", Decimal("5"), "market"))
    await h.reconciler.reconcile_now()
    assert "orphan_position:ETHUSD" in h.governor.breakers
    assert any("orphan position" in m for m in h.alerts.messages)

    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason.startswith("circuit_breaker:orphan_position")

    await h.venue.close_position("ETHUSD", "manual-close")
    await h.reconciler.reconcile_now()
    assert "orphan_position:ETHUSD" not in h.governor.breakers


async def test_orphan_order_with_our_prefix_alerts(h):
    h.config.order_type = "limit"
    await h.venue.place_order(
        OrderRequest("sig_deadbeef", "BTCUSD", "buy", Decimal("1"), "limit", Decimal("60000"))
    )
    await h.reconciler.reconcile_now()
    assert {"kind": "orphan_order", "order": "sig_deadbeef"} in h.reconciler.last_discrepancies


async def test_pending_order_never_sent_is_cancelled_after_grace(h):
    sig = await h.make_signal()
    await h.store.create(
        sig,
        venue="sim",
        client_order_id="sig_pending",
        symbol="BTCUSD",
        side="buy",
        order_type="limit_ioc",
        size=Decimal("1"),
        contract_value=Decimal("0.001"),
        limit_price=Decimal("65000"),
        intended_entry=Decimal("65000"),
        stop_loss_price=Decimal("64000"),
        take_profit_price=Decimal("67000"),
    )
    await h.reconciler.reconcile_now()
    assert (await h.store.get_by_client_id("sig_pending")).state == S.PENDING
    h.clock.advance(61)
    await h.reconciler.reconcile_now()
    assert (await h.store.get_by_client_id("sig_pending")).state == S.CANCELLED


async def test_missing_protective_stop_found_by_reconciler(h):
    sig = await h.make_signal()
    await h.submit(sig)
    h.venue._positions["BTCUSD"].stop_price = None  # stop silently cancelled on venue
    await h.reconciler.reconcile_now()
    await h.drain()
    assert h.venue.position("BTCUSD").has_stop_loss
    assert "missing_protective" in {d["kind"] for d in h.reconciler.last_discrepancies}
    assert (await h.risk_state()).kill_switch_active is True


async def test_auth_failure_during_reconcile_opens_breaker(h):
    h.venue.faults.add("positions", "auth_failure")
    assert await h.reconciler.reconcile_now() is False
    assert "exchange_auth" in h.governor.breakers
    assert await h.reconciler.reconcile_now() is True
    assert "exchange_auth" not in h.governor.breakers


async def test_engine_waits_for_first_successful_reconciliation(h):
    h.venue.faults.add("positions", "unavailable", times=2)
    h.reconciler.retry_delay = 0.01
    h.reconciler.start()
    start = asyncio.create_task(h.engine.start())
    await asyncio.sleep(0)
    assert not start.done()
    await asyncio.wait_for(start, timeout=2)
    assert h.reconciler.runs == 1
    assert h.venue.calls["positions"] == 3
