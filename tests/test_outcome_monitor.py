"""Outcome monitor: trade-result math, signal vs execution quality, idempotent closes."""

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from signal_bot.core.clock import utc
from signal_bot.execution.orders import OrderState
from signal_bot.tracking.outcome_monitor import compute_trade_result, format_daily_summary
from signal_bot.venues.simulated import CostModel
from tests.harness import Harness


def _order(side="buy", entry="65000", stop="64000", tp="67000", avg="65010", size="100"):
    return SimpleNamespace(
        side=side,
        filled_size=Decimal(size),
        size=Decimal(size),
        contract_value=Decimal("0.001"),
        avg_fill_price=Decimal(avg),
        intended_entry=Decimal(entry),
        stop_loss_price=Decimal(stop),
        take_profit_price=Decimal(tp),
        fees=Decimal("0"),
    )


def _fill(purpose, qty, price, fee="0"):
    from datetime import datetime, timezone

    return SimpleNamespace(
        purpose=purpose,
        qty=Decimal(qty),
        price=Decimal(price),
        fee=Decimal(fee),
        occurred_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


def test_signal_quality_vs_execution_quality_long_tp():
    order = _order()
    fills = [_fill("entry", "100", "65010", "3.25"), _fill("take_profit", "100", "67000", "1.34")]
    r = compute_trade_result(order, fills, Decimal("-0.5"))
    assert r["exit_reason"] == "tp_hit"
    assert r["signal_r"] == Decimal("2")  # the idea: +2R at its intended levels
    assert r["gross_pnl"] == Decimal("199")  # 100 * 0.001 * (67000 - 65010)
    assert r["net_pnl"] == Decimal("199") - Decimal("4.59") - Decimal("0.5")
    assert r["realized_r"] < r["signal_r"]  # slippage + fees + funding = execution drag
    assert r["entry_slippage_bps"] == pytest.approx(Decimal("1.5384615"), rel=Decimal("1e-6"))


def test_short_stop_with_gap_loses_more_than_one_r():
    order = _order(side="sell", entry="65000", stop="66000", tp="63000", avg="65000")
    fills = [_fill("entry", "100", "65000"), _fill("stop_loss", "100", "66400")]
    r = compute_trade_result(order, fills, Decimal("0"))
    assert r["exit_reason"] == "sl_hit"
    assert r["signal_r"] == Decimal("-1")
    assert r["realized_r"] == Decimal("-1.4")


def test_missing_exit_fills_assume_the_stop():
    r = compute_trade_result(_order(avg="65000"), [], Decimal("0"))
    assert r["exit_reason"] == "manual_close"
    assert r["exit_price"] == Decimal("64000")
    assert r["realized_r"] == Decimal("-1")


def test_daily_summary_format():
    outs = [SimpleNamespace(pnl_r=Decimal("2")), SimpleNamespace(pnl_r=Decimal("-1"))]
    text = format_daily_summary(outs)
    assert "Win rate: 1/2 (50.0%)" in text and "Profit factor: 2.00" in text
    assert "No trades" in format_daily_summary([])


async def test_partial_exit_keeps_trade_open(tmp_path):
    h = await Harness.create(tmp_path)
    sig = await h.make_signal()
    await h.submit(sig)
    # Thin bid book: the stop can only exit 30 of 100 contracts on this tick.
    h.venue.set_book("BTCUSD", Decimal("63900"), Decimal("63900.5"), size=Decimal("30"), levels=1)
    await h.venue.on_book(h.venue._books["BTCUSD"])
    await h.drain()
    assert (await h.order_for(sig)).state == OrderState.FILLED
    assert await h.outcomes() == []
    await h.venue.move_market("BTCUSD", Decimal("63800"), Decimal("63800.5"))
    await h.drain()
    assert (await h.order_for(sig)).state == OrderState.CLOSED
    [result] = await h.trade_results()
    assert result.exit_price == Decimal("63830")  # VWAP of 30 @ 63900 and 70 @ 63800
    await h.close()


async def test_funding_is_attributed_to_the_trade(tmp_path):
    h = await Harness.create(tmp_path, costs=CostModel(funding_rate=Decimal("0.001")))
    sig = await h.make_signal()
    await h.submit(sig)
    await h.venue.on_book(h.venue._books["BTCUSD"])  # establishes the funding baseline
    h.clock.advance(8 * 3600)
    await h.venue.move_market("BTCUSD", Decimal("64999.5"), Decimal("65000"))
    await h.venue.move_market("BTCUSD", Decimal("67100"), Decimal("67100.5"))
    await h.drain()
    [result] = await h.trade_results()
    assert result.funding < 0  # long paid funding
    assert result.net_pnl == pytest.approx(
        result.gross_pnl - result.fees + result.funding, abs=1e-6
    )
    await h.close()


async def test_outcome_recorded_once_and_governor_updated_in_same_transaction(tmp_path):
    h = await Harness.create(tmp_path)
    sig = await h.make_signal()
    await h.submit(sig)
    await h.venue.move_market("BTCUSD", Decimal("67100"), Decimal("67100.5"))
    await h.drain()
    order = await h.order_for(sig)
    for _ in range(3):
        assert await h.monitor.close_trade(order) is False
    risk = await h.risk_state()
    [result] = await h.trade_results()
    assert risk.daily_pnl_r == result.realized_r
    assert (await h.signal(sig)).status == "tp"
    closed = await h.journal.read(0, ["POSITION_CLOSED"])
    assert len(closed) == 1
    assert result.closed_at is not None and (utc(result.closed_at) - h.clock.now()) < timedelta(1)
    await h.close()
