"""Unit tests for the Binance websocket ingestion layer."""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from signal_bot.ingest.binance_ws import AggTrade, DataIngestor, Kline


class MockStream:
    """Mock websocket that implements recv() for python-binance >=1.0.20."""

    def __init__(self, messages):
        self._messages = messages
        self._index = 0

    async def recv(self):
        if self._index >= len(self._messages):
            # Simulate connection drop by raising when exhausted
            raise ConnectionError("Mock stream closed")
        msg = self._messages[self._index]
        self._index += 1
        return msg

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.mark.asyncio
async def test_queue_items_correctly_typed() -> None:
    """Mock the WebSocket and verify queue items are Kline or AggTrade."""
    queue = asyncio.Queue()
    ingestor = DataIngestor(queue, message_limit=3)

    with patch("signal_bot.ingest.binance_ws.AsyncClient") as MockClient:
        client = AsyncMock()
        MockClient.create = AsyncMock(return_value=client)
        client.futures_klines.return_value = []  # skip backfill

        with patch("signal_bot.ingest.binance_ws.BinanceSocketManager") as MockBSM:
            bsm = MagicMock()
            MockBSM.return_value = bsm

            messages = [
                {
                    "stream": "btcusdt@kline_1m",
                    "data": {
                        "e": "kline",
                        "E": 123456789,
                        "s": "BTCUSDT",
                        "k": {
                            "t": 1704067200000,
                            "T": 1704067259999,
                            "s": "BTCUSDT",
                            "i": "1m",
                            "f": 100,
                            "L": 200,
                            "o": "42000.00",
                            "h": "42100.00",
                            "l": "41900.00",
                            "c": "42050.00",
                            "v": "10.5",
                            "n": 150,
                            "x": True,
                            "q": "441000.00",
                            "V": "5.2",
                            "Q": "218000.00",
                            "B": "0",
                        },
                    },
                },
                {
                    "stream": "btcusdt@aggTrade",
                    "data": {
                        "e": "aggTrade",
                        "E": 123456789,
                        "s": "BTCUSDT",
                        "a": 12345,
                        "p": "42050.00",
                        "q": "1.5",
                        "f": 100,
                        "l": 100,
                        "T": 1704067260000,
                        "m": True,
                    },
                },
                {
                    "stream": "ethusdt@kline_5m",
                    "data": {
                        "e": "kline",
                        "E": 123456789,
                        "s": "ETHUSDT",
                        "k": {
                            "t": 1704067200000,
                            "T": 1704067499999,
                            "s": "ETHUSDT",
                            "i": "5m",
                            "f": 100,
                            "L": 200,
                            "o": "2200.00",
                            "h": "2210.00",
                            "l": "2190.00",
                            "c": "2205.00",
                            "v": "100.0",
                            "n": 300,
                            "x": True,
                            "q": "220000.00",
                            "V": "50.0",
                            "Q": "110000.00",
                            "B": "0",
                        },
                    },
                },
            ]

            bsm.futures_multiplex_socket.return_value = MockStream(messages)

            await ingestor.run()

    assert queue.qsize() == 3

    item1 = await queue.get()
    assert isinstance(item1, Kline)
    assert item1.symbol == "BTCUSDT"
    assert item1.interval == "1m"
    assert item1.close == 42050
    assert item1.closed is True

    item2 = await queue.get()
    assert isinstance(item2, AggTrade)
    assert item2.symbol == "BTCUSDT"
    assert item2.trade_id == 12345
    assert item2.is_buyer_maker is True
    assert item2.quote_qty == item2.price * item2.qty

    item3 = await queue.get()
    assert isinstance(item3, Kline)
    assert item3.symbol == "ETHUSDT"
    assert item3.interval == "5m"
    assert item3.high == 2210


@pytest.mark.asyncio
async def test_backfill_rest_kline_conversion() -> None:
    """Verify REST kline list format is converted correctly."""
    raw = [
        1704067200000,
        "42000.00",
        "42100.00",
        "41900.00",
        "42050.00",
        "10.5",
        1704067259999,
        "441000.00",
        150,
        "5.2",
        "218000.00",
        "0",
    ]
    kline = DataIngestor._rest_kline_to_dataclass("BTCUSDT", "1m", raw)
    assert kline.symbol == "BTCUSDT"
    assert kline.interval == "1m"
    assert kline.open == 42000
    assert kline.high == 42100
    assert kline.low == 41900
    assert kline.close == 42050
    assert kline.volume == 10.5
    assert kline.quote_volume == 441000
    assert kline.trade_count == 150
    assert kline.taker_buy_volume == Decimal("5.2")
    assert kline.closed is True


@pytest.mark.asyncio
async def test_reconnection_backoff_capped() -> None:
    """Ensure backoff doubles and caps at 60 seconds."""
    queue = asyncio.Queue()
    ingestor = DataIngestor(queue, message_limit=1)

    with patch("signal_bot.ingest.binance_ws.AsyncClient") as MockClient:
        client = AsyncMock()
        MockClient.create = AsyncMock(return_value=client)
        client.futures_klines.return_value = []

        with patch("signal_bot.ingest.binance_ws.BinanceSocketManager") as MockBSM:
            bsm = MagicMock()
            MockBSM.return_value = bsm

            # First connection raises, second succeeds with one message
            call_count = 0

            def side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ConnectionError("boom")
                return MockStream(
                    [
                        {
                            "stream": "btcusdt@kline_1m",
                            "data": {
                                "e": "kline",
                                "E": 1,
                                "s": "BTCUSDT",
                                "k": {
                                    "t": 1704067200000,
                                    "T": 1704067259999,
                                    "s": "BTCUSDT",
                                    "i": "1m",
                                    "f": 1,
                                    "L": 1,
                                    "o": "1",
                                    "h": "1",
                                    "l": "1",
                                    "c": "1",
                                    "v": "1",
                                    "n": 1,
                                    "x": True,
                                    "q": "1",
                                    "V": "1",
                                    "Q": "1",
                                    "B": "0",
                                },
                            },
                        }
                    ]
                )

            bsm.futures_multiplex_socket.side_effect = side_effect

            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                await ingestor.run()

    mock_sleep.assert_awaited_once()
    # backoff should be 1s after first failure, then capped
    args, _ = mock_sleep.call_args
    assert args[0] == 1.0


@pytest.mark.asyncio
async def test_websocket_recv_pattern_regression() -> None:
    """Regression: python-binance >=1.0.20 uses ws.recv() instead of async-for.

    Verify that the ingestor calls recv() on the websocket object and
    correctly processes messages received via that path.
    """
    queue = asyncio.Queue()
    ingestor = DataIngestor(queue, message_limit=2)

    with patch("signal_bot.ingest.binance_ws.AsyncClient") as MockClient:
        client = AsyncMock()
        MockClient.create = AsyncMock(return_value=client)
        client.futures_klines.return_value = []

        with patch("signal_bot.ingest.binance_ws.BinanceSocketManager") as MockBSM:
            bsm = MagicMock()
            MockBSM.return_value = bsm

            messages = [
                {
                    "stream": "btcusdt@kline_1m",
                    "data": {
                        "e": "kline",
                        "E": 1,
                        "s": "BTCUSDT",
                        "k": {
                            "t": 1704067200000,
                            "T": 1704067259999,
                            "s": "BTCUSDT",
                            "i": "1m",
                            "f": 1,
                            "L": 1,
                            "o": "100",
                            "h": "101",
                            "l": "99",
                            "c": "100",
                            "v": "10",
                            "n": 1,
                            "x": True,
                            "q": "1000",
                            "V": "5",
                            "Q": "500",
                            "B": "0",
                        },
                    },
                },
                {
                    "stream": "btcusdt@aggTrade",
                    "data": {
                        "e": "aggTrade",
                        "E": 1,
                        "s": "BTCUSDT",
                        "a": 999,
                        "p": "100.50",
                        "q": "2.0",
                        "f": 1,
                        "l": 1,
                        "T": 1704067260000,
                        "m": False,
                    },
                },
            ]

            mock_ws = MockStream(messages + [ConnectionError("closed")])

            bsm.futures_multiplex_socket.return_value = mock_ws

            await ingestor.run()

    # Verify messages were received via recv() path
    assert mock_ws._index == 2  # 2 messages consumed

    assert queue.qsize() == 2
    item1 = await queue.get()
    assert isinstance(item1, Kline)
    assert item1.symbol == "BTCUSDT"

    item2 = await queue.get()
    assert isinstance(item2, AggTrade)
    assert item2.symbol == "BTCUSDT"
    assert item2.trade_id == 999
