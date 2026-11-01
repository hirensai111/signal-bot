"""Tests for ExecutionEngine."""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from signal_bot.db.models import Signal
from signal_bot.execution.engine import EXEC_MODE, ExecutionEngine
from signal_bot.execution.governor import BaseGovernor


# Override session-scoped DB setup from conftest.py — these are pure unit tests.
@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db() -> AsyncGenerator[None, None]:
    yield


# ---------------------------------------------------------------------------
# mocks
# ---------------------------------------------------------------------------


class MockSession:
    def __init__(self, signals: dict):
        self._signals = signals

    async def get(self, model, id):
        return self._signals.get(id)

    async def commit(self):
        pass

    async def refresh(self, obj):
        pass


class MockSessionContext:
    def __init__(self, signals: dict):
        self._signals = signals

    async def __aenter__(self):
        return MockSession(self._signals)

    async def __aexit__(self, *args):
        pass


class MockSessionFactory:
    def __init__(self, signals: dict):
        self.signals = signals

    def __call__(self):
        return MockSessionContext(self.signals)


class DenyGovernor(BaseGovernor):
    async def check(self, signal: Signal) -> tuple[bool, str | None]:
        return False, "test_denied"


def _make_signal(**kwargs) -> Signal:
    defaults = {
        "symbol": "BTCUSDT",
        "side": "long",
        "entry": Decimal("65000"),
        "stop_loss": Decimal("64000"),
        "take_profit": Decimal("67000"),
        "leverage": 10,
        "risk_pct": Decimal("1.0"),
        "account_size_assumed": Decimal("10000"),
        "est_hold_minutes": 30,
        "status": "pending",
    }
    defaults.update(kwargs)
    signal = Signal(**defaults)
    signal.id = uuid.uuid4()
    return signal


def _make_delta_client(basis_shift: Decimal = Decimal("0")):
    """Return a MagicMock DeltaClient with product metadata wired in."""
    client = MagicMock()
    client.symbol_map = MagicMock()
    client.symbol_map.get_delta_symbol = MagicMock(return_value="BTCUSD")
    client.symbol_map.get_product_id = MagicMock(return_value=27)
    client.symbol_map.get_product = MagicMock(
        return_value={
            "id": 27,
            "symbol": "BTCUSD",
            "tick_size": "0.5",
            "min_size": "1",
            "contract_value": "1",
        }
    )

    mid = Decimal("65000") + basis_shift
    client.get_l2_orderbook = AsyncMock(
        return_value={
            "bids": [[str(mid - Decimal("0.5")), "10"]],
            "asks": [[str(mid + Decimal("0.5")), "8"]],
        }
    )
    client.set_margin_mode = AsyncMock()
    client.set_leverage = AsyncMock()
    client.place_bracket_order = AsyncMock(return_value={"id": "ord-123"})
    client.cancel_order = AsyncMock()
    return client


# ---------------------------------------------------------------------------
# 1. bracket params & price translation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bracket_params_computed_correctly():
    """Verify size, translated prices, and limit IOC offset are correct."""
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client(basis_shift=Decimal("50.5"))

    captured: dict = {}

    async def capture_place(*args, **kwargs):
        captured["kwargs"] = kwargs
        return {"id": "ord-123"}

    delta_client.place_bracket_order = capture_place

    signal = _make_signal()
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        exec_mode=EXEC_MODE.LIVE,
    )
    # Patch fill tracker so we don't wait 60 s
    engine._track_order_fill = AsyncMock(return_value=True)
    await engine._process_signal(signal)

    assert signal.status == "triggered"
    assert signal.client_order_id == f"sig_{signal.id}"
    assert signal.delta_symbol == "BTCUSD"
    assert signal.delta_basis == Decimal("50.5")
    assert signal.delta_entry_intended == Decimal("65050.5")

    kwargs = captured["kwargs"]
    assert kwargs["symbol"] == "BTCUSD"
    assert kwargs["side"] == "buy"

    # size = (10000 * 0.01) / ((65000-64000)/65000) / 1 = 100 / 0.0153846... = 6500
    assert kwargs["size"] == 6500

    # limit IOC long = delta_ask + tick = 65051.0 + 0.5 = 65051.5
    assert kwargs["entry_price"] == Decimal("65051.5")

    # SL = 64000 + 50.5 = 64050.5, rounded to tick 0.5 -> 64050.5
    assert kwargs["stop_loss_price"] == Decimal("64050.5")

    # TP = 67000 + 50.5 = 67050.5
    assert kwargs["take_profit_price"] == Decimal("67050.5")

    assert kwargs["client_order_id"] == f"sig_{signal.id}"
    assert kwargs["order_type"] == "limit_ioc"

    # margin & leverage should be set once
    delta_client.set_margin_mode.assert_awaited_once_with(27, "isolated")
    delta_client.set_leverage.assert_awaited_once_with(27, 10)


@pytest.mark.asyncio
async def test_bracket_params_for_short():
    """Verify short side uses correct sign and limit IOC price below bid."""
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client(basis_shift=Decimal("0"))

    captured: dict = {}

    async def capture_place(*args, **kwargs):
        captured["kwargs"] = kwargs
        return {"id": "ord-123"}

    delta_client.place_bracket_order = capture_place

    signal = _make_signal(side="short")
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        exec_mode=EXEC_MODE.LIVE,
    )
    engine._track_order_fill = AsyncMock(return_value=False)
    await engine._process_signal(signal)

    kwargs = captured["kwargs"]
    assert kwargs["side"] == "sell"
    # short limit IOC = delta_bid - tick = 64999.5 - 0.5 = 64999.0
    assert kwargs["entry_price"] == Decimal("64999.0")


# ---------------------------------------------------------------------------
# 2. idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idempotency_skips_submitted():
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client()
    delta_client.place_bracket_order = AsyncMock()

    signal = _make_signal(status="submitted")
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        exec_mode=EXEC_MODE.LIVE,
    )
    await engine._process_signal(signal)

    delta_client.place_bracket_order.assert_not_called()


@pytest.mark.asyncio
async def test_idempotency_skips_triggered():
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client()
    delta_client.place_bracket_order = AsyncMock()

    signal = _make_signal(status="triggered")
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        exec_mode=EXEC_MODE.LIVE,
    )
    await engine._process_signal(signal)

    delta_client.place_bracket_order.assert_not_called()


# ---------------------------------------------------------------------------
# 3. cross-venue basis abort
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_basis_too_wide_aborts():
    """Delta mid is >0.15% away from Binance entry → cancel."""
    signals = {}
    session_factory = MockSessionFactory(signals)
    # 2% shift — well above 0.15% threshold
    delta_client = _make_delta_client(basis_shift=Decimal("1300"))
    delta_client.place_bracket_order = AsyncMock()

    signal = _make_signal()
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        exec_mode=EXEC_MODE.LIVE,
    )
    await engine._process_signal(signal)

    assert signal.status == "cancelled"
    assert signal.cancelled_reason == "cross_venue_basis_too_wide"
    delta_client.place_bracket_order.assert_not_called()


# ---------------------------------------------------------------------------
# 4. size below minimum
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_size_below_min_aborts():
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client()
    delta_client.symbol_map.get_product = MagicMock(
        return_value={
            "id": 27,
            "symbol": "BTCUSD",
            "tick_size": "0.5",
            "min_size": "20000",  # well above the computed size of ~6500
            "contract_value": "1",
        }
    )
    delta_client.place_bracket_order = AsyncMock()

    signal = _make_signal()
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        exec_mode=EXEC_MODE.LIVE,
    )
    await engine._process_signal(signal)

    assert signal.status == "cancelled"
    assert signal.cancelled_reason == "size_below_min"
    delta_client.place_bracket_order.assert_not_called()


# ---------------------------------------------------------------------------
# 5. governor denial
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_governor_denial_cancels():
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client()
    delta_client.place_bracket_order = AsyncMock()

    signal = _make_signal()
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        governor=DenyGovernor(),
        exec_mode=EXEC_MODE.LIVE,
    )
    await engine._process_signal(signal)

    assert signal.status == "cancelled"
    assert signal.cancelled_reason == "test_denied"
    delta_client.place_bracket_order.assert_not_called()


# ---------------------------------------------------------------------------
# 6. no-fill timeout
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_fill_timeout_expires():
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client()
    delta_client.place_bracket_order = AsyncMock(return_value={"id": "ord-123"})
    delta_client.cancel_order = AsyncMock()

    signal = _make_signal()
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        exec_mode=EXEC_MODE.LIVE,
        no_fill_timeout=0.15,
    )
    await engine._process_signal(signal)

    assert signal.status == "expired_no_fill"
    delta_client.cancel_order.assert_awaited_once_with("ord-123")


# ---------------------------------------------------------------------------
# 7. fill event resolves to triggered
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fill_event_marks_triggered():
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client()
    delta_client.place_bracket_order = AsyncMock(return_value={"id": "ord-123"})
    delta_client.cancel_order = AsyncMock()

    signal = _make_signal()
    signals[signal.id] = signal

    delta_events = asyncio.Queue()

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=delta_events,
        session_factory=session_factory,
        exec_mode=EXEC_MODE.LIVE,
        no_fill_timeout=2.0,
    )

    # Start the events consumer so it can route the fill event
    await engine.start()

    # Kick off order processing in background
    task = asyncio.create_task(engine._process_signal(signal))

    # Give the engine time to reach the wait-for-fill state
    await asyncio.sleep(0.2)

    # Inject a fill event
    await delta_events.put(
        {
            "channel": "private_orders",
            "data": {
                "client_order_id": f"sig_{signal.id}",
                "state": "filled",
                "average_fill_price": "65001.0",
                "filled_size": "6500",
                "commission": "5.2",
            },
        }
    )

    await asyncio.wait_for(task, timeout=3.0)
    await engine.stop()

    assert signal.status == "triggered"
    assert signal.delta_entry_actual == Decimal("65001.0")
    assert signal.entry_fees_usdt == Decimal("5.2")
    # slippage = (65001 - 65000) / 65000 * 10000 = ~0.1538 bps
    assert signal.slippage_bps is not None
    delta_client.cancel_order.assert_not_called()


# ---------------------------------------------------------------------------
# 8. DRY_RUN short-circuit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dry_run_short_circuits_to_triggered():
    signals = {}
    session_factory = MockSessionFactory(signals)
    delta_client = _make_delta_client()
    delta_client.place_bracket_order = AsyncMock()

    signal = _make_signal()
    signals[signal.id] = signal

    engine = ExecutionEngine(
        delta_client=delta_client,
        signal_queue=asyncio.Queue(),
        delta_events=asyncio.Queue(),
        session_factory=session_factory,
        exec_mode=EXEC_MODE.DRY_RUN,
    )
    await engine._process_signal(signal)

    assert signal.status == "triggered"
    assert signal.delta_entry_actual == signal.delta_entry_intended
    delta_client.place_bracket_order.assert_not_called()
    delta_client.set_margin_mode.assert_not_called()
    delta_client.set_leverage.assert_not_called()
