"""Trailing stops through the execution path (signal -> order -> venue -> restart -> outcome).

Runs the production engine, order store, outcome monitor, reconciler and trailing
manager against the fault-injecting simulated venue.  The parity test checks that
the live stepped trail places exactly the stop the research fill model uses on
every bar.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from decimal import Decimal

import pandas as pd
import pytest

from signal_bot.db.models import Signal
from signal_bot.execution.engine import ExecutionConfig
from signal_bot.execution.orders import OrderState
from signal_bot.execution.trailing import TrailingStopManager
from signal_bot.features.candle_history import CandleHistory
from signal_bot.ingest.binance_ws import Kline
from signal_bot.research.fill_model import (
    NO_TARGET,
    CostModel,
    Intent,
    _Bars,
    open_trade,
    step_trade,
)
from tests.harness import Harness

STEP = timedelta(hours=4)
D = Decimal


def bar(t, o, h, l, c) -> Kline:
    return Kline(
        "BTCUSDT",
        "4h",
        t,
        D(str(o)),
        D(str(h)),
        D(str(l)),
        D(str(c)),
        D("100"),
        D("1e7"),
        10,
        D("50"),
        True,
    )


# A long that climbs for six bars, then falls through its trailed stop on bar 8.
PATH = [
    (65000, 65400, 64800, 65300),  # bar j (entry bar)
    (65300, 66500, 65200, 66400),
    (66400, 67800, 66300, 67600),
    (67600, 67700, 66900, 67000),  # pull-back: the stop must not loosen
    (67000, 69200, 66950, 69000),
    (69000, 70100, 68800, 69900),
    (69900, 70000, 69100, 69300),
    (69300, 69400, 67900, 68000),  # trailed stop 70100 - 1500 = 68600 is hit here
]
TRAIL = 1500.0
ENTRY, STOP = 65000.0, 64000.0


async def make_env(tmp_path, *, trail_mode="stepped", **config):
    cfg = ExecutionConfig(
        unknown_lookup_attempts=2,
        unknown_lookup_backoff=0.001,
        protective_confirm_timeout=0.01,
        protective_poll_interval=0.005,
        protective_retry_attempts=2,
        no_fill_timeout=30,
        trail_mode=trail_mode,
        **config,
    )
    h = await Harness.create(tmp_path, config=cfg)
    h.venue.set_book("BTCUSD", D("64999.5"), D("65000.5"))  # mid 65000: zero basis
    h.history = CandleHistory("4h")
    h.trailing = TrailingStopManager(
        h.venue, h.store, h.history, bus=h.bus, alerts=h.alerts, clock=h.clock
    )
    h.reconciler.trailing = h.trailing
    h.t0 = h.clock.now().replace(minute=0, second=0, microsecond=0)
    return h


async def trend_signal(h: Harness, contracts: str = "5", max_hold_bars: int = 180) -> Signal:
    sig = Signal(
        id=uuid.uuid4(),
        symbol="BTCUSDT",
        side="long",
        entry=D(str(ENTRY)),
        stop_loss=D(str(STOP)),
        take_profit=None,
        leverage=5,
        risk_pct=D("0.5"),
        account_size_assumed=D("10000"),
        est_hold_minutes=max_hold_bars * 240,
        status="pending",
        created_at=h.clock.now(),
        strategy="trend_breakout",
        trail_distance=D(str(TRAIL)),
        trail_from=h.t0,
        time_stop_at=h.t0 + STEP * (max_hold_bars + 1),
        planned_contracts=D(contracts),
    )
    async with h.raw_factory() as session:
        session.add(sig)
        await session.commit()
    return sig


async def feed(h: Harness, k: int) -> list:
    o, hi, lo, c = PATH[k]
    candle = bar(h.t0 + STEP * k, o, hi, lo, c)
    assert h.history.append(candle) == "appended"
    h.clock.set(max(h.clock.now(), h.t0 + STEP * (k + 1)))
    # The market is at the bar's close when the bar completes.
    await h.venue.move_market("BTCUSD", D(str(c)) - D("0.5"), D(str(c)) + D("0.5"))
    out = await h.trailing.on_bar_close(candle)
    await h.drain()
    return out


def research_stops() -> list[float]:
    """Stop the fill model uses on each bar k >= j+1 (decision bar i = 0, entry bar j = 1)."""
    idx = pd.date_range("2025-01-01", periods=len(PATH) + 1, freq="4h", tz="UTC")
    rows = [(ENTRY, ENTRY, ENTRY, ENTRY)] + PATH
    frame = pd.DataFrame(rows, columns=["open", "high", "low", "close"], index=idx, dtype=float)
    frame["quote_volume"] = 1e12
    bars = _Bars.of(frame)
    zero = CostModel(0, 0, 0, 0, 1.0, 1e9, 0)
    it = Intent(0, "long", ENTRY, STOP, NO_TARGET, 180, "t", TRAIL)
    _, ot = open_trade(bars, 0, it, 1.0, zero)
    stops = []
    for k in range(ot.j, len(bars)):
        closed = step_trade(ot, bars, k, zero)
        stops.append(ot.stop)
        if closed:
            break
    return stops[1:]  # bar j uses the initial stop; from j+1 the trail applies


async def test_stepped_trail_places_the_research_stop_on_every_bar(tmp_path):
    h = await make_env(tmp_path)
    try:
        sig = await trend_signal(h)
        order = await h.submit(sig)
        order = await h.store.get_by_client_id(order.client_order_id)
        assert order.state == OrderState.FILLED and order.trail_mode == "stepped"
        assert order.current_stop_price == D("64000") and order.take_profit_price is None
        expected = research_stops()
        live = []
        for k in range(len(PATH) - 1):  # every bar before the stop-out bar
            await feed(h, k)
            order = await h.store.get_by_client_id(order.client_order_id)
            live.append(float(order.current_stop_price - (order.price_basis or 0)))
            assert h.venue.position("BTCUSD").stop_loss_price == order.current_stop_price
        assert live == expected[: len(live)]
        assert live[2] == live[3]  # the pull-back bar does not loosen the stop
    finally:
        await h.close()


async def test_stop_out_at_the_trailed_level_is_recorded_as_trail_hit(tmp_path):
    h = await make_env(tmp_path)
    try:
        order = await h.submit(await trend_signal(h))
        for k in range(len(PATH) - 1):
            await feed(h, k)
        # bar 8 trades down through 68600: the venue stop fires
        await h.venue.move_market("BTCUSD", D("68500"), D("68500.5"))
        await h.drain()
        order = await h.store.get_by_client_id(order.client_order_id)
        assert order.state == OrderState.CLOSED
        [result] = await h.trade_results()
        assert result.exit_reason == "trail_hit"
        # signal R at the trailed level: (68600 - 65000) / 1000
        assert result.signal_r == pytest.approx(D("3.6"), abs=D("0.001"))
    finally:
        await h.close()


async def test_restart_reapplies_ratchets_missed_while_down(tmp_path):
    h = await make_env(tmp_path)
    try:
        order = await h.submit(await trend_signal(h))
        await feed(h, 0)
        await feed(h, 1)
        # "Process down" for bars 2..5: history (REST warm-up on restart) has them, no amends ran.
        for k in range(2, 6):
            o, hi, lo, c = PATH[k]
            h.history.append(bar(h.t0 + STEP * k, o, hi, lo, c))
        h.clock.set(h.t0 + STEP * 6)
        await h.venue.move_market("BTCUSD", D("69899.5"), D("69900.5"))  # bar 5 close
        stale = await h.store.get_by_client_id(order.client_order_id)
        assert h.venue.position("BTCUSD").stop_loss_price == stale.current_stop_price
        assert await h.reconciler.reconcile_now(reason="startup")
        fresh = await h.store.get_by_client_id(order.client_order_id)
        assert fresh.current_stop_price == D("68600")  # 70100 - 1500
        assert h.venue.position("BTCUSD").stop_loss_price == D("68600")
    finally:
        await h.close()


async def test_a_tighter_venue_stop_is_adopted_and_never_loosened(tmp_path):
    h = await make_env(tmp_path)
    try:
        order = await h.submit(await trend_signal(h))
        await feed(h, 0)  # research level stays 64000 (65400 - 1500 is lower)
        # an operator tightens the stop on the exchange
        await h.venue.amend_stop("BTCUSD", order.client_order_id, D("65100"))
        await h.trailing.sync_all("test")
        fresh = await h.store.get_by_client_id(order.client_order_id)
        assert fresh.current_stop_price == D("65100")
        await feed(h, 1)  # research level after bar 1: 66500 - 1500 = 65000 < 65100
        fresh = await h.store.get_by_client_id(order.client_order_id)
        assert fresh.current_stop_price == D("65100")
        assert h.venue.position("BTCUSD").stop_loss_price == D("65100")
        await feed(h, 2)  # 67800 - 1500 = 66300: tighter again, so it moves
        assert h.venue.position("BTCUSD").stop_loss_price == D("66300")
    finally:
        await h.close()


async def test_rejected_amend_keeps_the_existing_stop_and_retries_next_bar(tmp_path):
    h = await make_env(tmp_path)
    try:
        order = await h.submit(await trend_signal(h))
        await feed(h, 0)
        h.venue.faults.add("amend_stop", "reject")
        [res] = await feed(h, 1)
        assert res["action"] == "rejected"
        fresh = await h.store.get_by_client_id(order.client_order_id)
        assert fresh.current_stop_price == D("64000")  # still protected at the old level
        assert h.venue.position("BTCUSD").stop_loss_price == D("64000")
        assert any("not moved" in m for m in h.alerts.messages)
        [res] = await feed(h, 2)
        assert res["action"] == "moved"
        assert h.venue.position("BTCUSD").stop_loss_price == D("66300")  # 67800 - 1500
    finally:
        await h.close()


async def test_amend_with_lost_response_is_resolved_from_the_venue(tmp_path):
    h = await make_env(tmp_path)
    try:
        order = await h.submit(await trend_signal(h))
        await feed(h, 0)
        h.venue.faults.add("amend_stop", "timeout_after_accept")
        [res] = await feed(h, 1)
        assert res["action"] == "moved"  # read back from venue positions
        fresh = await h.store.get_by_client_id(order.client_order_id)
        assert fresh.current_stop_price == D("65000")
    finally:
        await h.close()


async def test_recovery_stop_is_placed_at_the_trailed_level_not_the_initial(tmp_path):
    h = await make_env(tmp_path)
    try:
        order = await h.submit(await trend_signal(h))
        for k in range(3):
            await feed(h, k)
        h.venue._positions["BTCUSD"].stop_price = None  # the venue lost the stop
        await h.reconciler.reconcile_now(reason="test")
        await h.drain()
        assert h.venue.position("BTCUSD").stop_loss_price == D("66300")
        fresh = await h.store.get_by_client_id(order.client_order_id)
        assert fresh.protective_status == "recovered"
    finally:
        await h.close()


async def test_time_stop_closes_the_position_and_labels_the_outcome(tmp_path):
    h = await make_env(tmp_path)
    try:
        order = await h.submit(await trend_signal(h, max_hold_bars=2))
        await feed(h, 0)
        await feed(h, 1)
        await feed(h, 2)  # close of entry bar + 2 bars == time_stop_at
        await h.drain()
        fresh = await h.store.get_by_client_id(order.client_order_id)
        assert fresh.state == OrderState.CLOSED
        [result] = await h.trade_results()
        assert result.exit_reason == "time_stop"
    finally:
        await h.close()


async def test_native_mode_sends_a_venue_trailing_stop(tmp_path):
    h = await make_env(tmp_path, trail_mode="native")
    try:
        order = await h.submit(await trend_signal(h))
        order = await h.store.get_by_client_id(order.client_order_id)
        assert order.trail_mode == "native"
        pos = h.venue.position("BTCUSD")
        assert pos.trail_amount == D("1500")
        await h.venue.move_market("BTCUSD", D("69999.5"), D("70000.5"))
        assert h.venue.position("BTCUSD").stop_loss_price == D("68500")  # continuous: mid - trail
        await h.venue.move_market("BTCUSD", D("68400"), D("68400.5"))
        await h.drain()
        [result] = await h.trade_results()
        assert result.exit_reason in ("trail_hit", "sl_hit")
    finally:
        await h.close()


async def test_paper_broker_restart_keeps_trailing_state(tmp_path):
    from signal_bot.venues.paper import PaperBroker
    from tests.harness import BTC

    async def book(symbol):
        return broker._books[symbol]

    broker = PaperBroker({"BTCUSDT": BTC}, None, book, state_path=str(tmp_path / "p.json"))
    broker.set_book("BTCUSD", D("64999.5"), D("65000.5"))
    from signal_bot.execution.adapter import OrderRequest

    await broker.place_order(
        OrderRequest("c1", "BTCUSD", "buy", D("5"), "market", trail_amount=D("1500"))
    )
    await broker.checkpoint()
    again = PaperBroker({"BTCUSDT": BTC}, None, book, state_path=str(tmp_path / "p.json"))
    assert again.restore()
    pos = again._positions["BTCUSD"]
    assert (
        pos.trail_amount == D("1500") and pos.stop_price is not None and pos.trail_best is not None
    )


async def test_restart_with_the_market_already_through_the_trailed_stop_exits(tmp_path):
    h = await make_env(tmp_path)
    try:
        order = await h.submit(await trend_signal(h))
        await feed(h, 0)
        for k in range(1, 6):  # missed while down: best high 70100 -> stop 68600
            o, hi, lo, c = PATH[k]
            h.history.append(bar(h.t0 + STEP * k, o, hi, lo, c))
        h.clock.set(h.t0 + STEP * 7)
        await h.venue.move_market("BTCUSD", D("67999.5"), D("68000.5"))  # now below 68600
        await h.reconciler.reconcile_now(reason="startup")
        await h.drain()
        fresh = await h.store.get_by_client_id(order.client_order_id)
        assert fresh.state == OrderState.CLOSED
        [result] = await h.trade_results()
        assert result.exit_reason == "trail_hit"
    finally:
        await h.close()
