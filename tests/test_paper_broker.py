"""Paper broker: live-book matching, latency, ledger persistence, restart safety."""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from signal_bot.core.clock import utc
from signal_bot.db.models import Order, PaperLedgerEntry
from signal_bot.events import EventBus, InMemoryJournal
from signal_bot.execution.adapter import BookLevel, BookSnapshot, FillPurpose, OrderRequest
from signal_bot.execution.orders import OrderState
from signal_bot.ops.fork_tp import HistoricalExit, seed
from signal_bot.venues.paper import PaperBroker
from signal_bot.venues.simulated import CostModel
from tests.harness import BTC, Harness


class ScriptedBooks:
    """Stands in for the live Delta L2 endpoint."""

    def __init__(self) -> None:
        self.calls = 0
        self.bid, self.ask, self.size = Decimal("64990"), Decimal("65010"), Decimal("40")

    async def __call__(self, symbol: str) -> BookSnapshot:
        self.calls += 1
        levels = 3
        return BookSnapshot(
            symbol,
            tuple(BookLevel(self.bid - 10 * i, self.size) for i in range(levels)),
            tuple(BookLevel(self.ask + 10 * i, self.size) for i in range(levels)),
            datetime.now(timezone.utc),
        )


def _broker(tmp_path, books, session_factory=None, **cost):
    return PaperBroker(
        {"BTCUSDT": BTC},
        EventBus(InMemoryJournal()),
        books,
        session_factory=session_factory,
        costs=CostModel(funding_rate=Decimal("0"), **cost),
        state_path=str(tmp_path / "paper.json"),
    )


async def test_entry_pays_spread_and_walks_live_depth(tmp_path):
    books = ScriptedBooks()
    broker = _broker(tmp_path, books)
    o = await broker.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("100"), "market"))
    assert books.calls == 1  # fresh book fetched at submission time
    # 40 @ 65010, 40 @ 65020, 20 @ 65030 -> VWAP 65018 (vs 65000 mid: 18 of slippage)
    assert o.avg_fill_price == Decimal("65018")
    assert o.fees == Decimal("100") * Decimal("0.001") * Decimal("65018") * Decimal("0.0005")


async def test_latency_is_applied_before_matching(tmp_path):
    books = ScriptedBooks()
    broker = _broker(tmp_path, books, latency_seconds=0.01)
    o = await broker.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("1"), "market"))
    assert o.avg_fill_price == Decimal("65010")


async def test_ledger_is_persisted_exactly_once(tmp_path):
    h = await Harness.create(tmp_path)
    books = ScriptedBooks()
    broker = _broker(tmp_path, books, session_factory=h.raw_factory)
    await broker.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("10"), "market"))
    await broker.checkpoint()
    await broker.checkpoint()
    async with h.raw_factory() as session:
        count = await session.scalar(select(func.count()).select_from(PaperLedgerEntry))
    assert count == 2  # initial deposit + one fee
    await h.close()


async def test_state_survives_restart(tmp_path):
    books = ScriptedBooks()
    broker = _broker(tmp_path, books)
    await broker.place_order(
        OrderRequest(
            "a",
            "BTCUSD",
            "buy",
            Decimal("10"),
            "market",
            stop_loss_price=Decimal("64000"),
            take_profit_price=Decimal("67000"),
        )
    )
    restarted = _broker(tmp_path, books)
    assert restarted.restore() is True
    pos = restarted.position("BTCUSD")
    assert pos.size == Decimal("10") and pos.has_stop_loss
    assert restarted.cash == broker.cash
    assert (await restarted.get_order("a")).filled_size == Decimal("10")


async def test_account_summary_and_equity_curve(tmp_path):
    books = ScriptedBooks()
    broker = _broker(tmp_path, books)
    await broker.place_order(OrderRequest("a", "BTCUSD", "buy", Decimal("10"), "market"))
    books.bid, books.ask = Decimal("65990"), Decimal("66010")
    await broker.refresh_book("BTCUSD")
    point = broker.snapshot_equity()
    summary = broker.account_summary()
    assert point["unrealized"] > 0
    assert summary["fees_paid"] > 0 and summary["equity"] == point["equity"]


@pytest.mark.parametrize("replay_before_start", [False, True])
async def test_paper_fork_keeps_original_fill_and_uses_only_the_new_exit(
    tmp_path, replay_before_start
):
    h = await Harness.create(tmp_path)
    books = ScriptedBooks()
    source_path = tmp_path / "main"
    tp_path = tmp_path / "tp"
    source = _broker(source_path, books, session_factory=h.raw_factory)
    signal = await h.make_signal(side="short", stop="66000")
    size = Decimal("10")
    initial_stop = Decimal("66010")
    trailed_stop = Decimal("65500")
    cutoff = datetime.now(timezone.utc) + timedelta(minutes=5)
    touched = cutoff - timedelta(minutes=2)
    deadline = cutoff + timedelta(days=10)
    try:
        fill = await source.place_order(
            OrderRequest("a", "BTCUSD", "sell", size, "market", stop_loss_price=initial_stop)
        )
        source._post_amount("funding:BTCUSD:test", "funding", "BTCUSD", Decimal("3"), None)
        await source.amend_stop("BTCUSD", "a", trailed_stop)
        order = await h.store.create(
            signal,
            venue="paper",
            client_order_id="a",
            symbol="BTCUSD",
            side="sell",
            order_type="market",
            size=size,
            contract_value=BTC.contract_value,
            limit_price=None,
            intended_entry=Decimal("65010"),
            stop_loss_price=initial_stop,
            take_profit_price=None,
            trail_mode="stepped",
            trail_distance=Decimal("1500"),
            price_basis=Decimal("10"),
            time_stop_at=deadline,
        )
        async with h.raw_factory.begin() as session:
            copied = await session.get(Order, order.id)
            copied.state = OrderState.FILLED
            copied.filled_size = size
            copied.avg_fill_price = fill.avg_fill_price
            copied.fees = fill.fees
            copied.current_stop_price = trailed_stop
            copied.protective_status = "confirmed"
            funding = await session.scalar(
                select(PaperLedgerEntry).where(PaperLedgerEntry.kind == "funding")
            )
            funding.occurred_at = cutoff - timedelta(seconds=20)
        await h.store.record_fill(source.fills[0], "paper")

        async def lookup_exit(symbol, long, stop, target, filled_at, at):
            assert symbol == "BTCUSD" and not long and filled_at < touched < at
            assert stop == initial_stop and target == Decimal("63510")
            if replay_before_start:
                return HistoricalExit(
                    FillPurpose.TAKE_PROFIT, target, touched, touched + timedelta(minutes=1)
                )
            return None

        tp_path.mkdir()
        original_checkpoint = (source_path / "paper.json").read_text(encoding="utf-8")
        summary = await seed(
            json.loads(original_checkpoint),
            tp_path / "paper.json",
            h.raw_factory,
            {
                "BTCUSD": {
                    "binance_symbol": "BTCUSDT",
                    "product_id": BTC.product_id,
                    "tick_size": str(BTC.tick_size),
                    "contract_value": str(BTC.contract_value),
                }
            },
            cutoff,
            lookup_exit=lookup_exit,
        )
        target = Decimal("63510")
        assert Decimal(summary[0]["target"]) == target
        assert (source_path / "paper.json").read_text(encoding="utf-8") == original_checkpoint

        tp = _broker(tp_path, books, session_factory=h.raw_factory)
        assert tp.restore()
        await tp.restore_ledger()
        copied = await h.store.get_by_client_id("a")
        assert copied.trail_mode == "none"
        assert utc(copied.time_stop_at) == deadline
        assert source.position("BTCUSD").stop_loss_price == trailed_stop
        if replay_before_start:
            assert tp.position("BTCUSD") is None
            assert copied.state == OrderState.CLOSED and copied.state_reason == "tp_hit"
            (trade,) = await h.trade_results()
            assert trade.exit_price == target and trade.exit_reason == "tp_hit"
            assert utc(trade.closed_at) == touched + timedelta(minutes=1)
            assert len(await h.store.fills_for("a")) == 2
            assert trade.funding == 0 and tp.account_summary()["funding"] == 0
            assert tp.cash == pytest.approx(
                source.cash - Decimal("3") + trade.net_pnl + fill.fees, abs=Decimal("0.00000001")
            )
            return

        assert tp.cash == source.cash
        assert tp.account_summary()["fees_paid"] == source.account_summary()["fees_paid"]
        position = tp.position("BTCUSD")
        assert position.entry_price == fill.avg_fill_price and position.size == -size
        assert position.stop_loss_price == initial_stop and position.take_profit_price == target

        books.bid, books.ask = Decimal("63490"), Decimal("63500")
        await tp.refresh_book("BTCUSD")
        assert tp.position("BTCUSD") is None
        assert len(tp.fills) == 1
        assert tp.fills[0].purpose == FillPurpose.TAKE_PROFIT and tp.fills[0].price == target
    finally:
        await h.close()
