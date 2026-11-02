"""Delta adapter: error classification and payload normalisation."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from signal_bot.execution.adapter import (
    AmbiguousOrderOutcome,
    AuthenticationFailed,
    OrderRejected,
    OrderRequest,
    VenueOrderState,
)
from signal_bot.execution.delta_adapter import (
    DeltaExchangeAdapter,
    normalize_ws_message,
    parse_book,
    parse_order,
)

REQ = OrderRequest(
    "sig_abc",
    "BTCUSD",
    "buy",
    Decimal("10"),
    "limit_ioc",
    Decimal("65000"),
    Decimal("64000"),
    Decimal("67000"),
)


def _adapter(**client_methods):
    client = MagicMock()
    for name, value in client_methods.items():
        setattr(client, name, value)
    return DeltaExchangeAdapter(client)


def _http_error(status):
    return aiohttp.ClientResponseError(MagicMock(), (), status=status, message=f"HTTP {status}")


@pytest.mark.parametrize(
    "exc,expected",
    [
        (TimeoutError(), AmbiguousOrderOutcome),
        (aiohttp.ServerDisconnectedError(), AmbiguousOrderOutcome),
        (_http_error(502), AmbiguousOrderOutcome),
        (_http_error(503), AmbiguousOrderOutcome),
        (_http_error(400), OrderRejected),
        (_http_error(429), OrderRejected),
        (_http_error(401), AuthenticationFailed),
        (_http_error(403), AuthenticationFailed),
        (aiohttp.ClientConnectorError(MagicMock(), OSError("refused")), OrderRejected),
    ],
)
async def test_placement_errors_are_classified(exc, expected):
    adapter = _adapter(place_bracket_order=AsyncMock(side_effect=exc))
    with pytest.raises(expected):
        await adapter.place_order(REQ)


async def test_success_without_order_id_is_ambiguous():
    adapter = _adapter(place_bracket_order=AsyncMock(return_value={"success": True, "result": {}}))
    with pytest.raises(AmbiguousOrderOutcome):
        await adapter.place_order(REQ)


async def test_success_false_is_rejected():
    adapter = _adapter(
        place_bracket_order=AsyncMock(return_value={"success": False, "error": {"code": "x"}})
    )
    with pytest.raises(OrderRejected):
        await adapter.place_order(REQ)


async def test_lookup_404_means_not_found():
    adapter = _adapter(get_order_by_client_id=AsyncMock(side_effect=_http_error(404)))
    assert await adapter.get_order("sig_abc") is None


async def test_balance_reads_asset_symbol():
    adapter = _adapter(
        get_balance=AsyncMock(
            return_value={"result": [{"asset_symbol": "USD", "available_balance": "123.4"}]}
        )
    )
    assert await adapter.available_balance() == Decimal("123.4")


def test_parse_order_states():
    base = {"id": 1, "client_order_id": "sig_a", "size": 10, "side": "buy"}
    assert parse_order({**base, "state": "open", "unfilled_size": 10}).state == VenueOrderState.OPEN
    partial = parse_order({**base, "state": "open", "unfilled_size": 4})
    assert partial.state == VenueOrderState.PARTIALLY_FILLED and partial.filled_size == 6
    assert parse_order({**base, "state": "closed", "unfilled_size": 0}).state == "filled"
    ioc_rest = parse_order({**base, "state": "closed", "unfilled_size": 3})
    assert ioc_rest.state == VenueOrderState.CANCELLED and ioc_rest.filled_size == 7


def test_parse_book_accepts_both_shapes():
    v2 = parse_book(
        "BTCUSD",
        {"result": {"buy": [{"price": "1", "size": 2}], "sell": [{"price": "1.5", "size": 3}]}},
    )
    legacy = parse_book("BTCUSD", {"bids": [["1", "2"]], "asks": [["1.5", "3"]]})
    assert v2.best_bid == legacy.best_bid == Decimal("1")
    assert v2.best_ask == legacy.best_ask == Decimal("1.5")


@pytest.mark.parametrize(
    "msg",
    [
        {
            "type": "orders",
            "id": 9,
            "client_order_id": "sig_a",
            "state": "closed",
            "size": 5,
            "unfilled_size": 0,
            "average_fill_price": "65000",
            "side": "buy",
            "product_symbol": "BTCUSD",
            "timestamp": 1704067200000000,
        },
        {
            "channel": "private_orders",
            "data": {
                "id": 9,
                "client_order_id": "sig_a",
                "state": "closed",
                "size": 5,
                "unfilled_size": 0,
                "side": "buy",
                "product_symbol": "BTCUSD",
                "timestamp": 1704067200000000,
            },
        },
    ],
)
def test_ws_order_messages_normalise_with_stable_ids(msg):
    [first] = normalize_ws_message(msg)
    [again] = normalize_ws_message(msg)
    assert first.event_type == "EXCHANGE_ORDER_UPDATE"
    assert first.event_id == again.event_id  # redelivery dedupes in the journal
    assert first.payload["state"] == "filled"


def test_ws_position_and_unknown_messages():
    [pos] = normalize_ws_message(
        {"type": "positions", "product_symbol": "BTCUSD", "size": 0, "timestamp": 1704067200000000}
    )
    assert pos.event_type == "EXCHANGE_POSITION_UPDATE" and pos.payload["size"] == "0"
    assert normalize_ws_message({"type": "heartbeat"}) == []


async def test_cancel_sends_product_with_order_id():
    from signal_bot.execution.delta_adapter import DeltaExchangeAdapter

    client = MagicMock()
    client.get_order_by_client_id = AsyncMock(
        return_value={
            "result": {
                "id": 9,
                "client_order_id": "sig_a",
                "product_symbol": "BTCUSD",
                "state": "open",
                "size": 1,
                "unfilled_size": 1,
                "side": "buy",
            }
        }
    )
    client.symbol_map._products = {"BTCUSD": {"id": 27}}
    client.cancel_order = AsyncMock(return_value={})
    await DeltaExchangeAdapter(client).cancel_order("sig_a", None)
    client.cancel_order.assert_awaited_once_with("9", product_id=27, product_symbol="BTCUSD")


async def test_ws_reports_down_once_while_never_connected():
    import aiohttp

    from signal_bot.delta.client import DeltaClient, DeltaWebSocket

    calls = []

    async def on_down(reason):
        calls.append(reason)

    ws = DeltaWebSocket(
        DeltaClient(api_key="k", api_secret="s", mode="testnet"), AsyncMock(), on_disconnect=on_down
    )
    for _ in range(5):
        await ws._mark_disconnected(f"error: {aiohttp.ClientError.__name__}")
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# Documented private-channel payloads (docs.delta.exchange, "Private Channels")
# ---------------------------------------------------------------------------

ORDER_UPDATE = {
    "type": "orders",
    "action": "update",
    "reason": "fill",
    "symbol": "BTCUSD",
    "product_id": 27,
    "order_id": 1234,
    "client_order_id": "sig_a",
    "size": 100,
    "unfilled_size": 0,
    "average_fill_price": "8999.00",
    "limit_price": "9000.00",
    "side": "buy",
    "stop_order_type": None,
    "bracket_order": True,
    "state": "closed",
    "seq_no": 2,
    "timestamp": 1594105083998848,
}


def test_documented_order_update_normalises():
    [event] = normalize_ws_message(ORDER_UPDATE)
    p = event.payload
    assert (p["client_order_id"], p["exchange_order_id"], p["symbol"]) == (
        "sig_a",
        "1234",
        "BTCUSD",
    )
    assert p["state"] == "filled" and p["filled_size"] == "100"
    assert p["updated_at"].startswith("2020-07-07")  # microsecond timestamp


def test_order_and_position_snapshots_emit_one_event_per_row():
    orders = {
        "type": "orders",
        "action": "snapshot",
        "symbol": "BTCUSD",
        "meta": {"seq_no": 7, "timestamp": 1594149235554045},
        "result": [
            {
                "id": 1,
                "client_order_id": "sig_a",
                "side": "buy",
                "size": 1,
                "unfilled_size": 1,
                "state": "open",
            },
            {
                "id": 2,
                "client_order_id": "sig_b",
                "side": "sell",
                "size": 2,
                "unfilled_size": 2,
                "state": "open",
            },
        ],
        "success": True,
    }
    events = normalize_ws_message(orders)
    assert [e.payload["client_order_id"] for e in events] == ["sig_a", "sig_b"]
    assert all(e.payload["symbol"] == "BTCUSD" for e in events)

    positions = {
        "type": "positions",
        "action": "snapshot",
        "result": [
            {"product_symbol": "ZECUSD", "product_id": 357, "size": -1686, "entry_price": "238.02"}
        ],
        "success": True,
    }
    [pos] = normalize_ws_message(positions)
    assert pos.payload["symbol"] == "ZECUSD" and pos.payload["size"] == "-1686"


def test_v2_user_trades_short_keys_are_not_misread():
    """In v2/user_trades "c" is the client order id; there is no commission field."""
    msg = {
        "type": "v2/user_trades",
        "sy": "BTCUSD",
        "f": "1234-abcd-qwer-3456",
        "R": "normal",
        "u": 1998,
        "o": 3283999,
        "S": "buy",
        "s": 190,
        "p": "17289.2",
        "po": 5,
        "r": "maker",
        "c": "sig_a",
        "t": 1685794274866438,
        "se": 4,
    }
    [event] = normalize_ws_message(msg)
    p = event.payload
    assert p["fill_id"] == "delta:1234-abcd-qwer-3456"
    assert p["client_order_id"] == "sig_a"
    assert p["fee"] == "0" and p["liquidity"] == "maker"
    assert p["occurred_at"].startswith("2023-06-03") and p["purpose"] == "unknown"


def test_user_trades_with_commission_and_liquidation_reason():
    msg = {
        "type": "user_trades",
        "symbol": "BTCUSD",
        "fill_id": "f-1",
        "reason": "liquidation",
        "order_id": 9,
        "side": "sell",
        "size": 10,
        "price": "60000",
        "role": "taker",
        "client_order_id": "sig_a",
        "commission": "0.3",
        "timestamp": 1544091555086559,
    }
    [event] = normalize_ws_message(msg)
    assert event.payload["fee"] == "0.3" and event.payload["purpose"] == "liquidation"


def test_control_messages_are_ignored():
    for msg in (
        {"type": "heartbeat"},
        {"type": "subscriptions"},
        {"type": "key-auth", "success": True},
    ):
        assert normalize_ws_message(msg) == []


async def test_unknown_fills_are_classified_by_order_side_and_close_the_trade(tmp_path):
    """A Delta-style exit fill (no purpose, opposite side) closes the trade exactly once."""
    from datetime import datetime, timezone

    from signal_bot.execution.adapter import ExchangeFill, FillPurpose
    from signal_bot.execution.orders import OrderState
    from tests.harness import Harness

    h = await Harness.create(tmp_path)
    sig = await h.make_signal()
    await h.submit(sig)  # long BTCUSD, 100 contracts filled at 65000
    order = await h.order_for(sig)
    now = datetime.now(timezone.utc)

    def fill(fid, side, price):
        return ExchangeFill(
            fill_id=f"delta:{fid}",
            client_order_id=order.client_order_id,
            exchange_order_id="x",
            symbol="BTCUSD",
            side=side,
            qty=Decimal("100"),
            price=Decimal(price),
            fee=Decimal("1"),
            liquidity="taker",
            purpose=FillPurpose.UNKNOWN,
            occurred_at=now,
        )

    assert await h.store.record_fill(fill("entry-dup", "buy", "65000"), "delta")
    from sqlalchemy import select

    from signal_bot.db.models import Fill

    async with h.raw_factory() as s:
        stored = await s.scalar(select(Fill).where(Fill.fill_id == "delta:entry-dup"))
    assert stored.purpose == "entry"

    from signal_bot.events import Event, EventType

    exit_fill = fill("exit-1", "sell", "67000")
    for _ in range(2):  # delivered twice
        await h.monitor.handle_event(
            Event.create(EventType.EXCHANGE_FILL, exit_fill.to_payload("delta"), "delta")
        )
    assert (await h.order_for(sig)).state == OrderState.CLOSED
    [result] = await h.trade_results()
    assert result.exit_reason == "tp_hit" and result.gross_pnl == Decimal("200")
    await h.close()


# ---------------------------------------------------------------------------
# trailing stops, stop amendment, equity (Signal Bot 2.1)
# ---------------------------------------------------------------------------


def _mapped_adapter(**methods):
    adapter = _adapter(**methods)
    sm = adapter.client.symbol_map
    sm._products = {
        "BTCUSD": {
            "id": 27,
            "contract_value": "0.001",
            "tick_size": "0.5",
            "position_size_limit": "125000",
        }
    }
    sm._delta_to_binance = {"BTCUSD": "BTCUSDT"}
    sm.get_product = lambda s: sm._products["BTCUSD"]
    sm.get_delta_symbol = lambda s: "BTCUSD"
    return adapter


STOP_CHILD = {
    "id": 777,
    "product_symbol": "BTCUSD",
    "side": "sell",
    "size": 5,
    "unfilled_size": 5,
    "state": "open",
    "stop_order_type": "stop_loss_order",
    "stop_price": "64500.5",
    "trail_amount": None,
    "reduce_only": True,
}
POSITION = {"product_symbol": "BTCUSD", "size": 5, "entry_price": "65000", "unrealized_pnl": "12.5"}


async def test_place_order_sends_the_trail_amount():
    adapter = _mapped_adapter(
        place_bracket_order=AsyncMock(
            return_value={"success": True, "result": {"id": 1, "state": "open", "size": 10}}
        )
    )
    from dataclasses import replace

    await adapter.place_order(
        replace(REQ, stop_loss_price=None, take_profit_price=None, trail_amount=Decimal("450"))
    )
    kwargs = adapter.client.place_bracket_order.call_args.kwargs
    assert kwargs["trail_amount"] == Decimal("450")
    assert kwargs["stop_loss_price"] is None and kwargs["take_profit_price"] is None


async def test_amend_stop_edits_the_resting_stop_order():
    adapter = _mapped_adapter(
        get_open_orders=AsyncMock(return_value={"result": [STOP_CHILD]}),
        get_open_positions=AsyncMock(return_value={"result": [POSITION]}),
        edit_order=AsyncMock(return_value={"success": True, "result": {}}),
        attach_position_bracket=AsyncMock(),
    )
    pos = await adapter.amend_stop("BTCUSD", "sig_x", Decimal("65000"))
    adapter.client.edit_order.assert_awaited_once_with("777", 27, stop_price=Decimal("65000"))
    adapter.client.attach_position_bracket.assert_not_awaited()
    assert pos is not None and pos.has_stop_loss


async def test_amend_stop_attaches_a_stop_when_none_is_resting():
    adapter = _mapped_adapter(
        get_open_orders=AsyncMock(return_value={"result": []}),
        get_open_positions=AsyncMock(return_value={"result": [POSITION]}),
        edit_order=AsyncMock(),
        attach_position_bracket=AsyncMock(return_value={"success": True, "result": {}}),
    )
    await adapter.amend_stop("BTCUSD", "sig_x", Decimal("65000"))
    adapter.client.attach_position_bracket.assert_awaited_once_with(27, Decimal("65000"))


@pytest.mark.parametrize("status,expected", [(400, OrderRejected), (502, AmbiguousOrderOutcome)])
async def test_amend_stop_classifies_errors(status, expected):
    adapter = _mapped_adapter(
        get_open_orders=AsyncMock(return_value={"result": [STOP_CHILD]}),
        edit_order=AsyncMock(side_effect=_http_error(status)),
    )
    with pytest.raises(expected):
        await adapter.amend_stop("BTCUSD", "sig_x", Decimal("65000"))


async def test_positions_take_the_stop_price_and_trail_from_the_resting_stop_order():
    child = {**STOP_CHILD, "stop_price": "64100", "trail_amount": "450"}
    adapter = _mapped_adapter(
        get_open_orders=AsyncMock(return_value={"result": [child]}),
        get_open_positions=AsyncMock(return_value={"result": [POSITION]}),
    )
    [pos] = await adapter.positions()
    assert pos.has_stop_loss and pos.stop_loss_price == Decimal("64100")
    assert pos.trail_amount == Decimal("450")


async def test_account_equity_is_wallet_balance_plus_unrealised_pnl():
    adapter = _mapped_adapter(
        get_balance=AsyncMock(
            return_value={
                "result": [{"asset_symbol": "USD", "balance": "1000", "available_balance": "700"}]
            }
        ),
        get_open_orders=AsyncMock(return_value={"result": []}),
        get_open_positions=AsyncMock(return_value={"result": [POSITION]}),
    )
    assert await adapter.account_equity() == Decimal("1012.5")


def test_instrument_carries_the_venue_position_limit():
    adapter = _mapped_adapter()
    spec = adapter.instrument("BTCUSDT")
    assert spec.contract_value == Decimal("0.001") and spec.max_contracts == Decimal("125000")


def test_parse_order_reads_stop_and_trail_fields():
    order = parse_order({**STOP_CHILD, "trail_amount": "450", "client_order_id": "x"})
    assert order.stop_price == Decimal("64500.5") and order.trail_amount == Decimal("450")
