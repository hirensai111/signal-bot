"""Execution engine against the simulated venue (real store, governor and bus)."""

from datetime import timedelta
from decimal import Decimal

import pytest

from signal_bot.execution.engine import ExecutionConfig, ExecutionEngine, client_order_id_for
from signal_bot.execution.orders import OrderState
from tests.harness import Harness

S = OrderState


@pytest.fixture
async def h(tmp_path):
    harness = await Harness.create(tmp_path)
    yield harness
    await harness.close()


def test_client_order_id_is_deterministic_and_venue_safe():
    import uuid

    sid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert client_order_id_for(sid) == client_order_id_for(sid)
    assert client_order_id_for(sid).startswith("sig_")
    assert len(client_order_id_for(sid)) == 32


def test_size_accounts_for_price_and_contract_value():
    """Regression: contracts = notional / (contract_value * price), not notional / contract_value."""
    from signal_bot.db.models import Signal

    sig = Signal(
        entry=Decimal("65000"),
        stop_loss=Decimal("64000"),
        risk_pct=Decimal("1"),
        account_size_assumed=Decimal("10000"),
    )
    contracts = ExecutionEngine.compute_size(sig, Decimal("0.001"))
    notional = contracts * Decimal("0.001") * Decimal("65000")
    assert notional == pytest.approx(
        Decimal("6500"), rel=Decimal("1e-9")
    )  # $100 risk / 1.538% stop


async def test_long_bracket_translated_with_basis_and_rounded(h):
    # Delta mid is 19.55 above the Binance entry: every level shifts by it, rounded to tick 0.5
    h.venue.set_book("BTCUSD", Decimal("65019.5"), Decimal("65020"))
    sig = await h.make_signal(entry="65000.2", stop="64000.1", tp="67000.3")
    order = await h.submit(sig)
    order = await h.order_for(sig)
    assert order.side == "buy"
    assert order.intended_entry == Decimal("65020")
    assert order.stop_loss_price == Decimal("64019.5")  # 64000.1 + 19.55 basis -> tick
    assert order.take_profit_price == Decimal("67020")
    # IOC limit: best ask + 10 bps, rounded up to the tick (65020 * 1.001 = 65085.02)
    assert order.limit_price == Decimal("65085.5")
    assert order.state == S.FILLED
    assert order.protective_status == "confirmed"
    pos = h.venue.position("BTCUSD")
    assert pos.has_stop_loss and pos.stop_loss_price == Decimal("64019.5")


async def test_fixed_target_without_trail_is_a_plain_bracket(h):
    """The +1.5R variant: bracket with stop and target, no trailing state on the order."""
    sig = await h.make_signal(entry="65000", stop="64000", tp="66500")  # no trail_distance
    await h.submit(sig)
    order = await h.order_for(sig)
    assert order.state == S.FILLED and order.protective_status == "confirmed"
    assert order.trail_mode in (None, "none") and not order.trail_distance
    assert order.take_profit_price is not None and order.take_profit_price > order.intended_entry
    pos = h.venue.position("BTCUSD")
    assert pos.has_stop_loss and pos.take_profit_price == order.take_profit_price


async def test_short_bracket(h):
    sig = await h.make_signal(side="short", entry="65000", stop="66000", tp="63000")
    await h.submit(sig)
    order = await h.order_for(sig)
    assert order.side == "sell"
    assert order.limit_price < order.intended_entry  # best bid - 10 bps, rounded down
    assert order.state == S.FILLED
    assert h.venue.position("BTCUSD").size == -order.filled_size


def test_entry_limit_allows_slippage_but_at_least_one_tick():
    limit = ExecutionEngine.entry_limit_price
    tick = Decimal("0.5")
    bid, ask = Decimal("64999.5"), Decimal("65000")
    assert limit("buy", bid, ask, tick, Decimal("10")) == Decimal("65065")  # 65065.0 exactly
    assert limit("sell", bid, ask, tick, Decimal("10")) == Decimal("64934.5")  # 64934.4995 down
    # 0 bps (or a tick wider than the allowance) still crosses the touch by one tick
    assert limit("buy", bid, ask, tick, Decimal("0")) == Decimal("65000.5")
    assert limit("sell", bid, ask, tick, Decimal("0")) == Decimal("64999")
    assert limit(
        "buy", Decimal("0.0701"), Decimal("0.0702"), Decimal("0.0001"), Decimal("1")
    ) == Decimal("0.0703")


async def test_fill_mirrors_execution_details_onto_signal(h):
    sig = await h.make_signal()
    await h.submit(sig)
    s = await h.signal(sig)
    assert s.status == "triggered"
    assert s.delta_entry_actual == Decimal("65000")
    assert s.entry_fees_usdt > 0
    assert s.client_order_id == client_order_id_for(sig.id)


async def test_signal_processed_once(h):
    sig = await h.make_signal()
    await h.submit(sig)
    await h.submit(sig)
    assert h.venue.calls.get("place_order") == 1


async def test_basis_too_wide_aborts_before_any_order(h):
    h.venue.set_book("BTCUSD", Decimal("65199.5"), Decimal("65200"))  # 0.31 %
    sig = await h.make_signal()
    await h.submit(sig)
    s = await h.signal(sig)
    assert s.status == "cancelled" and s.cancelled_reason == "cross_venue_basis_too_wide"
    assert "place_order" not in h.venue.calls


async def test_size_below_min_aborts(h):
    from signal_bot.db.models import Signal

    sig = await h.make_signal()
    async with h.raw_factory() as session:  # $0.50 of risk -> 0.5 contracts < lot size 1
        (await session.get(Signal, sig.id)).risk_pct = Decimal("0.005")
        await session.commit()
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason == "size_below_min"


async def test_stale_order_book_aborts(h):
    h.venue.faults.add("order_book", "stale_book", age_seconds=60)
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason == "stale_order_book"
    assert "place_order" not in h.venue.calls


async def test_empty_order_book_aborts(h):
    h.venue.faults.add("order_book", "empty_book")
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason == "empty_order_book"


async def test_stale_signal_is_not_executed(h):
    sig = await h.make_signal(created_at=h.clock.now() - timedelta(minutes=30))
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason == "signal_stale"
    assert "place_order" not in h.venue.calls


async def test_governor_denial_cancels_without_venue_call(h):
    await h.governor.set_kill_switch("test")
    sig = await h.make_signal()
    await h.submit(sig)
    s = await h.signal(sig)
    assert s.status == "cancelled" and s.cancelled_reason.startswith("kill_switch_active")
    assert "place_order" not in h.venue.calls


async def test_missing_governor_fails_closed(h):
    h.engine.governor = None
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason == "no_risk_governor"


async def test_unhealthy_governor_fails_closed(h):
    h.governor._state_store_ok = False
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason == "risk_governor_unhealthy"


async def test_governor_exception_fails_closed(h):
    async def boom(_):
        raise RuntimeError("bug")

    h.governor.check = boom
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.signal(sig)).cancelled_reason == "risk_governor_error"


async def test_thin_book_partial_fill_becomes_filled_with_partial_size(h):
    h.venue.set_book("BTCUSD", Decimal("64999.5"), Decimal("65000"), size=Decimal("30"), levels=2)
    sig = await h.make_signal()
    await h.submit(sig)
    order = await h.order_for(sig)
    # 30 at best ask + 30 at ask+1tick (the IOC limit) ; the rest of 100 is cancelled
    assert order.state == S.FILLED
    assert order.filled_size == Decimal("60")
    assert order.avg_fill_price == Decimal("65000.25")
    assert h.venue.position("BTCUSD").size == Decimal("60")


async def test_no_liquidity_within_limit_cancels(h):
    h.venue.set_book("BTCUSD", Decimal("64999.5"), Decimal("65000"), size=Decimal("0"))
    sig = await h.make_signal()
    await h.submit(sig)
    order = await h.order_for(sig)
    assert order.state == S.CANCELLED
    assert (await h.signal(sig)).status == "cancelled"
    assert h.venue.position("BTCUSD") is None


async def test_definitive_reject(h):
    h.venue.faults.add("place_order", "reject", reason="insufficient margin")
    sig = await h.make_signal()
    await h.submit(sig)
    order = await h.order_for(sig)
    assert order.state == S.REJECTED and "insufficient margin" in order.state_reason


async def test_auth_failure_rejects_and_opens_breaker(h):
    h.venue.faults.add("place_order", "auth_failure")
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.order_for(sig)).state == S.REJECTED
    assert "exchange_auth" in h.governor.breakers
    nxt = await h.make_signal(symbol="ETHUSDT", entry="3500", stop="3450", tp="3600")
    await h.submit(nxt)
    assert (await h.signal(nxt)).cancelled_reason.startswith("circuit_breaker:exchange_auth")


async def test_resting_order_times_out_and_is_cancelled(tmp_path):
    h = await Harness.create(
        tmp_path, config=ExecutionConfig(order_type="limit", no_fill_timeout=10)
    )
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.order_for(sig)).state == S.ACKNOWLEDGED
    h.clock.advance(11)
    await h.engine.watch_once()  # -> cancel_pending + cancel request
    await h.drain()
    await h.engine.watch_once()
    order = await h.order_for(sig)
    assert order.state == S.CANCELLED
    assert [t.to_state for t in await h.store.transitions_for(order.client_order_id)] == [
        "submitting",
        "acknowledged",
        "cancel_pending",
        "cancelled",
    ]
    await h.close()


async def test_submission_counts_toward_daily_trade_limit(h):
    sig = await h.make_signal()
    await h.submit(sig)
    assert (await h.risk_state()).daily_trade_count == 1


async def test_second_signal_on_exposed_symbol_is_refused(h):
    """Venues net per product: a new order must never silently reduce an open trade."""
    first = await h.make_signal()
    await h.submit(first)
    second = await h.make_signal(side="short", entry="65000", stop="66000", tp="63000")
    await h.submit(second)
    assert (await h.signal(second)).cancelled_reason == "symbol_already_exposed"
    assert h.venue.calls["place_order"] == 1
    assert h.venue.position("BTCUSD").size > 0


async def test_fast_stop_out_before_protection_check_is_not_a_failure(h):
    """The stop fills before the engine verifies it: no false kill switch."""
    h.venue.set_book("BTCUSD", Decimal("64999.5"), Decimal("65000"), size=Decimal("1000"))
    sig = await h.make_signal()
    original = h.venue.positions

    async def stop_out_then_report():
        await h.venue.move_market("BTCUSD", Decimal("63900"), Decimal("63900.5"))
        h.venue.positions = original
        return await original()

    h.venue.positions = stop_out_then_report
    await h.submit(sig)
    order = await h.order_for(sig)
    assert order.protective_status == "not_required"
    assert order.state == S.CLOSED
    assert (await h.risk_state()).kill_switch_active is False
