"""Tests for Delta Exchange India REST + WS client."""

import asyncio
import hashlib
import hmac
import json
from decimal import Decimal
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio

from delta.client import (
    ENDPOINTS,
    DeltaClient,
    DeltaSymbolMap,
    DeltaWebSocket,
    WS_AUTH_PATH,
)


# Override session-scoped DB setup from conftest.py — delta tests are pure unit tests.
@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db() -> AsyncGenerator[None, None]:
    yield


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_products_response():
    return {
        "products": [
            {"id": 27, "symbol": "BTCUSD", "tick_size": "0.5", "contract_value": "1"},
            {"id": 28, "symbol": "ETHUSD", "tick_size": "0.05", "contract_value": "1"},
            {"id": 29, "symbol": "SOLUSD", "tick_size": "0.01", "contract_value": "1"},
            {"id": 30, "symbol": "LINKUSD", "tick_size": "0.01", "contract_value": "1"},
            {"id": 31, "symbol": "AVAXUSD", "tick_size": "0.01", "contract_value": "1"},
            {"id": 32, "symbol": "ADAUSD", "tick_size": "0.0001", "contract_value": "1"},
        ]
    }


@pytest.fixture
def client():
    return DeltaClient(api_key="test_key", api_secret="test_secret", mode="testnet")


# ---------------------------------------------------------------------------
# mode / endpoint selection
# ---------------------------------------------------------------------------

def test_default_mode_is_testnet():
    with patch("delta.client.settings.delta_mode", "testnet"):
        c = DeltaClient(api_key="k", api_secret="s")
        assert c.mode == "testnet"
        assert c.rest_url == ENDPOINTS["testnet"]["rest"]
        assert c.ws_url == ENDPOINTS["testnet"]["ws"]


def test_production_mode():
    c = DeltaClient(api_key="k", api_secret="s", mode="production")
    assert c.mode == "production"
    assert c.rest_url == ENDPOINTS["production"]["rest"]
    assert c.ws_url == ENDPOINTS["production"]["ws"]


def test_invalid_mode_raises():
    with pytest.raises(ValueError, match="Invalid DELTA_MODE"):
        DeltaClient(api_key="k", api_secret="s", mode="staging")


# ---------------------------------------------------------------------------
# HMAC signature
# ---------------------------------------------------------------------------

def test_hmac_signature_known_vector(client: DeltaClient):
    """Verify the exact signing message format for a frozen timestamp."""
    with patch("delta.client.time.time", return_value=1609459200.0):
        headers = client._sign_request("GET", "/v2/products", "", "")

    expected_message = "GET1609459200/v2/products"
    expected_sig = hmac.new(
        b"test_secret",
        expected_message.encode(),
        hashlib.sha256,
    ).hexdigest()

    assert headers["api-key"] == "test_key"
    assert headers["timestamp"] == "1609459200"
    assert headers["signature"] == expected_sig


def test_hmac_signature_with_query_string(client: DeltaClient):
    with patch("delta.client.time.time", return_value=1609459200.0):
        headers = client._sign_request("GET", "/v2/orders", "product_id=27&state=open", "")

    expected_message = "GET1609459200/v2/orders?product_id=27&state=open"
    expected_sig = hmac.new(
        b"test_secret",
        expected_message.encode(),
        hashlib.sha256,
    ).hexdigest()
    assert headers["signature"] == expected_sig


def test_hmac_signature_with_body(client: DeltaClient):
    with patch("delta.client.time.time", return_value=1609459200.0):
        body = '{"product_id":27,"size":10}'
        headers = client._sign_request("POST", "/v2/orders", "", body)

    expected_message = f"POST1609459200/v2/orders{body}"
    expected_sig = hmac.new(
        b"test_secret",
        expected_message.encode(),
        hashlib.sha256,
    ).hexdigest()
    assert headers["signature"] == expected_sig


# ---------------------------------------------------------------------------
# REST methods (mocked _request)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_products(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock, return_value={"products": [1, 2]}):
        resp = await client.get_products()
    assert resp == {"products": [1, 2]}


@pytest.mark.asyncio
async def test_get_balance(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock, return_value={"balances": []}):
        resp = await client.get_balance()
    assert resp == {"balances": []}


@pytest.mark.asyncio
async def test_get_open_positions(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(
        client, "_request", new_callable=AsyncMock, return_value={"positions": []}
    ):
        resp = await client.get_open_positions()
    assert resp == {"positions": []}


@pytest.mark.asyncio
async def test_set_leverage(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
        await client.set_leverage(27, 10)
    mock_req.assert_awaited_once_with(
        "POST", "/v2/products/27/orders/leverage", data={"leverage": 10}
    )


@pytest.mark.asyncio
async def test_set_margin_mode_is_no_op(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
        resp = await client.set_margin_mode(27, "isolated")
    mock_req.assert_not_awaited()
    assert resp["success"] is True


@pytest.mark.asyncio
async def test_cancel_order(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
        await client.cancel_order("12345", product_symbol="BTCUSD")
    mock_req.assert_awaited_once_with("DELETE", "/v2/orders", data={"id": 12345, "product_symbol": "BTCUSD"})


@pytest.mark.asyncio
async def test_cancel_all_orders_no_product(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
        await client.cancel_all_orders()
    mock_req.assert_awaited_once_with("DELETE", "/v2/orders", data=None)


@pytest.mark.asyncio
async def test_cancel_all_orders_with_product(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
        await client.cancel_all_orders(product_id=27)
    mock_req.assert_awaited_once_with("DELETE", "/v2/orders", data={"product_id": 27})


@pytest.mark.asyncio
async def test_get_order_status(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
        await client.get_order_status("ord-1")
    mock_req.assert_awaited_once_with("GET", "/v2/orders/ord-1")


@pytest.mark.asyncio
async def test_get_order_history(client: DeltaClient):
    client._session = MagicMock()
    with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
        await client.get_order_history(27, "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z")
    mock_req.assert_awaited_once_with(
        "GET",
        "/v2/orders/history",
        params={
            "product_id": 27,
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-02T00:00:00Z",
        },
    )


# ---------------------------------------------------------------------------
# Bracket order payload
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_place_bracket_order_payload(client: DeltaClient):
    client._session = MagicMock()
    # seed products cache so symbol resolution works
    client._products_cache = _make_products_response()["products"]

    with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
        await client.place_bracket_order(
            symbol="BTCUSD",
            side="buy",
            size=10,
            entry_price=Decimal("65000"),
            stop_loss_price=Decimal("64000"),
            take_profit_price=Decimal("67000"),
            client_order_id="test-coid-1",
            order_type="limit_ioc",
        )

    assert mock_req.await_count == 1
    _, call_kwargs = mock_req.call_args
    body = call_kwargs["data"]
    assert body["product_id"] == 27
    assert body["size"] == 10
    assert body["side"] == "buy"
    assert body["order_type"] == "limit_ioc"
    assert body["client_order_id"] == "test-coid-1"
    assert body["limit_price"] == "65000"
    assert body["bracket_stop_loss_price"] == "64000"
    assert body["bracket_stop_loss_limit_price"] == "64000"
    assert body["bracket_take_profit_price"] == "67000"
    assert body["bracket_take_profit_limit_price"] == "67000"


# ---------------------------------------------------------------------------
# Symbol map
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_symbol_map_loads_and_translates():
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client._session = MagicMock()

    with patch.object(
        client, "_request", new_callable=AsyncMock, return_value=_make_products_response()
    ):
        await client.symbol_map.load()

    assert client.symbol_map.get_delta_symbol("BTCUSDT") == "BTCUSD"
    assert client.symbol_map.get_delta_symbol("ETHUSDT") == "ETHUSD"
    assert client.symbol_map.get_product_id("BTCUSDT") == 27
    assert client.symbol_map.get_product_id("ETHUSDT") == 28
    assert client.symbol_map.resolve_by_product_id(27) == "BTCUSDT"


@pytest.mark.asyncio
async def test_symbol_map_hard_fails_on_missing_symbol():
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client._session = MagicMock()

    partial = {
        "products": [
            {"id": 27, "symbol": "BTCUSD"},
            {"id": 28, "symbol": "ETHUSD"},
            # missing SOLUSD, LINKUSD, AVAXUSD, ADAUSD
        ]
    }

    with patch.object(client, "_request", new_callable=AsyncMock, return_value=partial):
        with pytest.raises(RuntimeError, match="Symbols not available on Delta India"):
            await client.symbol_map.load()


# ---------------------------------------------------------------------------
# WebSocket auth + reconnect
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_authenticate_message():
    """Verify the WS auth message uses the correct signing path."""
    client = DeltaClient(api_key="ws_key", api_secret="ws_secret", mode="testnet")
    client._session = MagicMock()

    ws_mock = AsyncMock()
    ws_mock.send_json = AsyncMock()

    ws_client = DeltaWebSocket(client, asyncio.Queue())

    with patch("delta.client.time.time", return_value=1609459200.0):
        await ws_client._authenticate(ws_mock)

    ws_mock.send_json.assert_awaited_once()
    msg = ws_mock.send_json.await_args[0][0]
    assert msg["type"] == "auth"
    payload = msg["payload"]
    assert payload["api-key"] == "ws_key"
    assert payload["timestamp"] == "1609459200"

    expected_path = WS_AUTH_PATH["testnet"]
    expected_message = f"GET1609459200{expected_path}"
    expected_sig = hmac.new(
        b"ws_secret",
        expected_message.encode(),
        hashlib.sha256,
    ).hexdigest()
    assert payload["signature"] == expected_sig


class _FakeWSConnection:
    """Async-context-manager stand-in for aiohttp.ClientWebSocketResponse."""

    def __init__(self, messages):
        self._messages = messages
        self.send_json = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


@pytest.mark.asyncio
async def test_ws_reconnect_on_error():
    """Simulate a connection failure and ensure the loop retries."""
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")

    connect_attempts = 0

    def failing_connect(*args, **kwargs):
        nonlocal connect_attempts
        connect_attempts += 1
        raise aiohttp.ClientError("connection refused")

    mock_session = MagicMock()
    mock_session.ws_connect = MagicMock(side_effect=failing_connect)
    client._session = mock_session

    queue = asyncio.Queue()
    ws_client = DeltaWebSocket(client, queue)
    ws_client._running = True

    # Patch sleep so the retry loop spins quickly, but still yields control.
    real_sleep = asyncio.sleep

    async def fast_sleep(_duration: float) -> None:
        await real_sleep(0)

    with patch("delta.client.asyncio.sleep", fast_sleep):
        task = asyncio.create_task(ws_client._run())
        # Yield to let the task body execute at least one iteration.
        await real_sleep(0.01)
        ws_client._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert connect_attempts >= 1


@pytest.mark.asyncio
async def test_ws_puts_events_on_queue():
    """Simulate a text message and verify it lands on the queue."""
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")

    msg1 = MagicMock()
    msg1.type = aiohttp.WSMsgType.TEXT
    msg1.data = json.dumps({"type": "position", "channel": "private_positions", "data": {"x": 1}})

    msg2 = MagicMock()
    msg2.type = aiohttp.WSMsgType.TEXT
    msg2.data = json.dumps({"type": "order", "channel": "private_orders", "data": {"y": 2}})

    msg3 = MagicMock()
    msg3.type = aiohttp.WSMsgType.CLOSED

    fake_ws = _FakeWSConnection([msg1, msg2, msg3])

    mock_session = MagicMock()
    mock_session.ws_connect = MagicMock(return_value=fake_ws)
    client._session = mock_session

    queue = asyncio.Queue()
    ws_client = DeltaWebSocket(client, queue)
    ws_client._running = True

    with patch.object(ws_client, "_authenticate", new_callable=AsyncMock):
        task = asyncio.create_task(ws_client._connect_and_listen())
        await asyncio.wait_for(task, timeout=1.0)

    assert queue.qsize() == 2
    e1 = await queue.get()
    assert e1["type"] == "position"
    e2 = await queue.get()
    assert e2["type"] == "order"
