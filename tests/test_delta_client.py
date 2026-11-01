"""Tests for Delta Exchange India REST + WS client."""

import asyncio
import hashlib
import hmac
import json
from collections.abc import AsyncGenerator
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio

from signal_bot.delta.client import (
    ENDPOINTS,
    WS_AUTH_PATH,
    DeltaClient,
    DeltaWebSocket,
    DeltaWebSocketAuthError,
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
    with patch("signal_bot.delta.client.settings.delta_mode", "testnet"):
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
    with patch("signal_bot.delta.client.time.time", return_value=1609459200.0):
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
    with patch("signal_bot.delta.client.time.time", return_value=1609459200.0):
        headers = client._sign_request("GET", "/v2/orders", "product_id=27&state=open", "")

    expected_message = "GET1609459200/v2/orders?product_id=27&state=open"
    expected_sig = hmac.new(
        b"test_secret",
        expected_message.encode(),
        hashlib.sha256,
    ).hexdigest()
    assert headers["signature"] == expected_sig


def test_hmac_signature_with_body(client: DeltaClient):
    with patch("signal_bot.delta.client.time.time", return_value=1609459200.0):
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
    with patch.object(
        client, "_request", new_callable=AsyncMock, return_value={"products": [1, 2]}
    ):
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
    with patch.object(client, "_request", new_callable=AsyncMock, return_value={"positions": []}):
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
    mock_req.assert_awaited_once_with(
        "DELETE", "/v2/orders", data={"id": 12345, "product_symbol": "BTCUSD"}
    )


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
async def test_symbol_map_keeps_mapped_symbols_and_reports_missing():
    """Testnet lists no LINKUSD/AVAXUSD: trade what maps, report the rest, don't crash."""
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client._session = MagicMock()
    partial = {"products": [{"id": 27, "symbol": "BTCUSD"}, {"id": 28, "symbol": "ETHUSD"}]}

    with patch.object(client, "_request", new_callable=AsyncMock, return_value=partial):
        await client.symbol_map.load()

    assert client.symbol_map.delta_symbols == ["BTCUSD", "ETHUSD"]
    assert any(m.startswith("SOLUSDT") for m in client.symbol_map.missing)
    with pytest.raises(KeyError):
        client.symbol_map.get_delta_symbol("SOLUSDT")


@pytest.mark.asyncio
async def test_symbol_map_strict_or_nothing_mapped_fails():
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client._session = MagicMock()
    partial = {"products": [{"id": 27, "symbol": "BTCUSD"}]}
    with patch.object(client, "_request", new_callable=AsyncMock, return_value=partial):
        with pytest.raises(RuntimeError, match="Symbols not available on Delta India"):
            await client.symbol_map.load(strict=True)

    empty = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    empty._session = MagicMock()
    with patch.object(empty, "_request", new_callable=AsyncMock, return_value={"products": []}):
        with pytest.raises(RuntimeError):
            await empty.symbol_map.load()


@pytest.mark.asyncio
async def test_symbol_map_only_maps_live_perpetuals():
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client._session = MagicMock()
    products = {
        "result": [
            {"id": 1, "symbol": "BTCUSD", "contract_type": "perpetual_futures", "state": "expired"},
            {"id": 2, "symbol": "ETHUSD", "contract_type": "call_options", "state": "live"},
            {"id": 3, "symbol": "SOLUSD", "contract_type": "perpetual_futures", "state": "live"},
        ]
    }
    with patch.object(client, "_request", new_callable=AsyncMock, return_value=products):
        await client.symbol_map.load()
    assert client.symbol_map.delta_symbols == ["SOLUSD"]


@pytest.mark.asyncio
async def test_get_products_follows_pagination_cursor():
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client._session = MagicMock()
    pages = [
        {"result": [{"id": 1, "symbol": "A"}], "meta": {"after": "cur1"}},
        {"result": [{"id": 2, "symbol": "B"}], "meta": {"after": None}},
    ]
    with patch.object(client, "_request", new_callable=AsyncMock, side_effect=pages) as req:
        resp = await client.get_products()
    assert [p["symbol"] for p in resp["products"]] == ["A", "B"]
    assert req.await_args_list[1].kwargs["params"] == {"after": "cur1"}


# ---------------------------------------------------------------------------
# WebSocket auth + reconnect
# ---------------------------------------------------------------------------


def _text(payload) -> MagicMock:
    msg = MagicMock()
    msg.type = aiohttp.WSMsgType.TEXT
    msg.data = json.dumps(payload)
    return msg


def _closed() -> MagicMock:
    msg = MagicMock()
    msg.type = aiohttp.WSMsgType.CLOSED
    return msg


AUTH_OK = {"type": "key-auth", "success": True, "status_code": 200, "status": "authenticated"}


def test_testnet_socket_is_the_documented_private_endpoint():
    assert DeltaClient(api_key="k", api_secret="s", mode="testnet").ws_url == (
        "wss://socket-ind.testnet.deltaex.org"
    )
    assert DeltaClient(api_key="k", api_secret="s", mode="production").ws_url == (
        "wss://socket.india.delta.exchange"
    )


@pytest.mark.asyncio
async def test_ws_key_auth_message_and_success_response():
    """key-auth signs GET + timestamp + /live and waits for the auth response."""
    client = DeltaClient(api_key="ws_key", api_secret="ws_secret", mode="testnet")
    client._session = MagicMock()
    ws = _FakeWSConnection([_text({"type": "heartbeat"}), _text(AUTH_OK)])
    ws_client = DeltaWebSocket(client, asyncio.Queue())

    with patch("signal_bot.delta.client.time.time", return_value=1609459200.0):
        reply = await ws_client._authenticate(ws)

    assert reply["success"] is True
    msg = ws.sent[0]
    assert msg["type"] == "key-auth"
    payload = msg["payload"]
    assert payload["api-key"] == "ws_key"
    assert payload["timestamp"] == 1609459200  # a number, per the docs
    expected = hmac.new(b"ws_secret", f"GET1609459200{WS_AUTH_PATH}".encode(), hashlib.sha256)
    assert WS_AUTH_PATH == "/live"
    assert payload["signature"] == expected.hexdigest()


@pytest.mark.asyncio
async def test_ws_key_auth_failure_raises_with_status():
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client._session = MagicMock()
    rejected = {
        "type": "key-auth",
        "success": False,
        "status_code": 401,
        "status": "invalid_signature",
        "message": "Invalid Signature",
    }
    ws = _FakeWSConnection([_text(rejected)])
    with pytest.raises(DeltaWebSocketAuthError, match="invalid_signature"):
        await DeltaWebSocket(client, asyncio.Queue())._authenticate(ws)


class _FakeWSConnection:
    """Async-context-manager stand-in for aiohttp.ClientWebSocketResponse."""

    def __init__(self, messages, hang_when_empty: bool = False):
        self._messages = list(messages)
        self.hang_when_empty = hang_when_empty
        self.sent: list[dict] = []

    async def send_json(self, payload):
        self.sent.append(payload)

    async def receive(self, timeout=None):
        if self._messages:
            return self._messages.pop(0)
        if self.hang_when_empty:
            await asyncio.sleep(timeout or 0)
            raise TimeoutError
        return _closed()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


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

    with patch("signal_bot.delta.client.asyncio.sleep", fast_sleep):
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
async def test_ws_subscribes_documented_channels_after_auth_and_emits_events():
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client.symbol_map._delta_to_binance = {"BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT"}
    fake_ws = _FakeWSConnection(
        [
            _text(AUTH_OK),
            _text({"type": "subscriptions", "channels": []}),
            _text({"type": "heartbeat"}),
            _text({"type": "positions", "symbol": "BTCUSD", "size": 1}),
            _text({"type": "orders", "symbol": "BTCUSD", "state": "open"}),
        ]
    )
    mock_session = MagicMock()
    mock_session.ws_connect = MagicMock(return_value=fake_ws)
    client._session = mock_session

    queue = asyncio.Queue()
    ws_client = DeltaWebSocket(client, queue)
    ws_client._running = True
    await asyncio.wait_for(ws_client._connect_and_listen(), timeout=1.0)

    types = [m["type"] for m in fake_ws.sent]
    assert types == ["key-auth", "enable_heartbeat", "subscribe"]  # subscribe only after auth
    channels = fake_ws.sent[2]["payload"]["channels"]
    assert [c["name"] for c in channels] == ["orders", "positions", "user_trades"]
    assert all(c["symbols"] == ["BTCUSD", "ETHUSD"] for c in channels)
    # control messages (subscriptions, heartbeat) are not forwarded
    assert [(await queue.get())["type"] for _ in range(queue.qsize())] == ["positions", "orders"]


@pytest.mark.asyncio
async def test_ws_reconnects_when_heartbeats_stop():
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")
    client.symbol_map._delta_to_binance = {"BTCUSD": "BTCUSDT"}
    fake_ws = _FakeWSConnection([_text(AUTH_OK)], hang_when_empty=True)
    mock_session = MagicMock()
    mock_session.ws_connect = MagicMock(return_value=fake_ws)
    client._session = mock_session
    ws_client = DeltaWebSocket(client, asyncio.Queue(), heartbeat_timeout=0.05)
    ws_client._running = True
    # returns (to reconnect) instead of hanging on a silent socket
    await asyncio.wait_for(ws_client._connect_and_listen(), timeout=1.0)


@pytest.mark.asyncio
async def test_ws_connect_sends_origin_header():
    """Regression: Delta India edge requires Origin header on WS handshake."""
    client = DeltaClient(api_key="k", api_secret="s", mode="testnet")

    fake_ws = _FakeWSConnection([_text(AUTH_OK), _closed()])

    mock_session = MagicMock()
    mock_session.ws_connect = MagicMock(return_value=fake_ws)
    client._session = mock_session

    queue = asyncio.Queue()
    ws_client = DeltaWebSocket(client, queue)
    ws_client._running = True

    await asyncio.wait_for(ws_client._connect_and_listen(), timeout=1.0)

    mock_session.ws_connect.assert_called_once()
    _, kwargs = mock_session.ws_connect.call_args
    assert "headers" in kwargs
    headers = kwargs["headers"]
    assert headers["Origin"] == "https://demo.delta.exchange"
    assert "Mozilla/5.0" in headers["User-Agent"]
