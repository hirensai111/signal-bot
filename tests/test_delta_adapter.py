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
