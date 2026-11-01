"""Simulated venue matching, ledger and fault plan."""

from decimal import Decimal

import pytest

from signal_bot.events import EventBus, InMemoryJournal
from signal_bot.execution.adapter import (
    AmbiguousOrderOutcome,
    OrderRejected,
    OrderRequest,
    VenueOrderState,
)
from signal_bot.venues.simulated import CostModel, FaultPlan, SimulatedVenue
from tests.harness import BTC


def _venue(**cost):
    bus = EventBus(InMemoryJournal())
    v = SimulatedVenue({"BTCUSDT": BTC}, bus, costs=CostModel(funding_rate=Decimal("0"), **cost))
    v.set_book(
        "BTCUSD", Decimal("99.5"), Decimal("100"), size=Decimal("10"), levels=3, tick=Decimal("0.5")
    )
    return v, bus


async def test_market_order_walks_the_book_vwap():
    v, _ = _venue()
    o = await v.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("25"), "market"))
    assert o.state == VenueOrderState.FILLED
    assert o.avg_fill_price == Decimal("100.4")  # 10@100 + 10@100.5 + 5@101
    assert v.position("BTCUSD").size == Decimal("25")


async def test_ioc_respects_limit_and_cancels_remainder():
    v, _ = _venue()
    o = await v.place_order(
        OrderRequest("a", "BTCUSD", "buy", Decimal("25"), "limit_ioc", Decimal("100"))
    )
    assert o.state == VenueOrderState.CANCELLED and o.filled_size == Decimal("10")


async def test_ledger_fees_and_realized_pnl():
    v, _ = _venue(taker_fee=Decimal("0.001"))
    await v.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("10"), "market"))
    v.set_book("BTCUSD", Decimal("110"), Decimal("110.5"), size=Decimal("10"))
    await v.place_order(
        OrderRequest("b", "BTCUSD", "sell", Decimal("10"), "market", reduce_only=True)
    )
    kinds = [e.kind for e in v.ledger]
    assert kinds == ["deposit", "fee", "fee", "realized_pnl"]
    # pnl = 10 contracts * 0.001 * (110 - 100) = 0.1 ; fees = 0.001 + 0.0011
    assert v.cash == Decimal("10000") + Decimal("0.1") - Decimal("0.0021")
    assert v.equity() == v.cash


async def test_mark_to_market_equity():
    v, _ = _venue(taker_fee=Decimal("0"))
    await v.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("10"), "market"))
    v.set_book("BTCUSD", Decimal("119.5"), Decimal("120.5"))
    assert v.unrealized_pnl() == Decimal("0.2")  # 10 * 0.001 * (120 - 100)


async def test_duplicate_client_order_id_rejected():
    v, _ = _venue()
    await v.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("1"), "market"))
    with pytest.raises(OrderRejected):
        await v.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("1"), "market"))


async def test_timeout_after_accept_still_creates_the_order():
    v, _ = _venue()
    v.faults.add("place_order", "timeout_after_accept")
    with pytest.raises(AmbiguousOrderOutcome):
        await v.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("1"), "market"))
    assert (await v.get_order("a")).state == VenueOrderState.FILLED


async def test_stop_gaps_through_and_fills_at_the_book():
    v, _ = _venue()
    await v.place_order(
        OrderRequest(
            "a",
            "BTCUSD",
            "buy",
            Decimal("5"),
            "market",
            stop_loss_price=Decimal("95"),
            take_profit_price=Decimal("110"),
        )
    )
    v._attach_legs(v._orders["a"], attach=True)
    await v.move_market("BTCUSD", Decimal("90"), Decimal("90.5"), size=Decimal("10"))
    exit_fill = v.fills[-1]
    assert exit_fill.purpose == "stop_loss" and exit_fill.price == Decimal("90")
    assert v.position("BTCUSD") is None


def test_fault_plan_counts_and_validates():
    plan = FaultPlan()
    plan.add("place_order", "reject", times=2)
    assert plan.take("place_order").action == "reject"
    assert plan.take("place_order").action == "reject"
    assert plan.take("place_order") is None
    with pytest.raises(ValueError):
        plan.add("place_order", "explode")


async def test_disconnected_stream_publishes_nothing():
    v, bus = _venue()
    sub = bus.subscribe("x")
    await v.disconnect_stream()
    await v.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("1"), "market"))
    events = [sub.queue.get_nowait().event_type for _ in range(sub.lag)]
    assert events == ["STREAM_DISCONNECTED"]
