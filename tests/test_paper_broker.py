"""Paper broker: live-book matching, latency, ledger persistence, restart safety."""

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import func, select

from signal_bot.db.models import PaperLedgerEntry
from signal_bot.events import EventBus, InMemoryJournal
from signal_bot.execution.adapter import BookLevel, BookSnapshot, OrderRequest
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
