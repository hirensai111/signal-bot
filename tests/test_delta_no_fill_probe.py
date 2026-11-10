"""Safety and cleanup checks for the separately invoked testnet order probe."""

import asyncio
import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scripts.test_delta_no_fill import (
    FIXTURE_FIELDS,
    TESTNET_REST,
    Probe,
    ProbeError,
    guard_request,
    main,
    make_plan,
    sanitize,
    verify_order,
)
from signal_bot.execution.adapter import VenueOrderState
from signal_bot.execution.delta_adapter import parse_order


@pytest.fixture
def plan():
    return make_plan(
        {
            "id": 84,
            "symbol": "BTCUSD",
            "state": "live",
            "trading_status": "operational",
            "contract_type": "perpetual_futures",
            "tick_size": "0.1",
        },
        {"mark_price": "100000", "price_band": {"lower_limit": "1", "upper_limit": "200000"}},
        {"buy": [{"price": "99999"}], "sell": [{"price": "100001"}]},
        "nf" + "a" * 24,
    )


def exchange_order(plan, state="open", unfilled=1):
    return {
        **deepcopy(plan),
        "id": 123456,
        "product_symbol": "BTCUSD",
        "state": state,
        "unfilled_size": unfilled,
    }


def test_no_confirmation_exits_before_network(monkeypatch):
    monkeypatch.setattr("sys.argv", ["test_delta_no_fill"])
    run = AsyncMock()
    monkeypatch.setattr(asyncio, "run", run)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    run.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        {"mode": "production"},
        {"rest_url": "https://api.india.delta.exchange"},
        {"data": {"product_id": 84, "size": 2, "order_type": "limit_order", "post_only": True}},
        {"data": {"product_id": 84, "size": 1, "order_type": "market_order", "reduce_only": False}},
        {"data": {"product_id": 84, "size": 1, "order_type": "limit_order", "post_only": False}},
        {"data": {"product_id": 85, "size": 1, "order_type": "limit_order", "post_only": True}},
        {"method": "DELETE", "data": {"product_id": 84}},
        {"path": "/v2/orders/bracket"},
    ],
)
def test_mutation_guards_reject_unsafe_requests(plan, change):
    args = {
        "mode": "testnet",
        "rest_url": TESTNET_REST,
        "product_id": 84,
        "method": "POST",
        "path": "/v2/orders",
        "data": plan,
    }
    args.update(change)
    with pytest.raises(ProbeError):
        guard_request(**args)


def test_plan_is_small_distant_post_only_and_on_tick(plan):
    assert plan["size"] == 1
    assert plan["post_only"] is True
    assert Decimal(plan["limit_price"]) <= Decimal("99999") * Decimal("0.95")
    assert Decimal(plan["limit_price"]) % Decimal("0.1") == 0
    assert Decimal(plan["bracket_stop_loss_price"]) < Decimal(plan["limit_price"])
    assert Decimal(plan["bracket_take_profit_price"]) > Decimal("100001")


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 999),
        ("client_order_id", "another_order"),
        ("size", 2),
        ("unfilled_size", 0),
        ("state", "closed"),
        ("bracket_stop_loss_price", "1"),
    ],
)
def test_exact_exchange_validation_rejects_mismatches(plan, field, value):
    row = exchange_order(plan)
    row[field] = value
    with pytest.raises(ProbeError):
        verify_order(row, plan, 123456, {"open"})


def test_fixture_sanitization_keeps_structure_without_account_data(plan):
    raw = {
        "success": True,
        "result": {
            **exchange_order(plan),
            "user_id": 987654,
            "account_id": 444,
            "api_key": "secret",
            "balance": "1234567",
            "arbitrary_new_field": "private",
        },
    }
    clean = sanitize(raw)
    assert clean["result"]["id"] == 700000001
    assert clean["result"]["client_order_id"] == "fixture_nofill_0001"
    assert (
        not {"user_id", "account_id", "api_key", "balance", "arbitrary_new_field"}
        & clean["result"].keys()
    )
    assert clean["result"]["bracket_stop_loss_price"] == plan["bracket_stop_loss_price"]


class FakeExchange:
    def __init__(self, plan, fill_on_cancel=False):
        self.order = exchange_order(plan)
        self.size = 0
        self.fill_on_cancel = fill_on_cancel
        self.cancellations = []
        self.recoveries = []

    async def get_order_by_client_id(self, client_id):
        assert client_id == self.order["client_order_id"]
        return {"success": True, "result": deepcopy(self.order)}

    async def cancel_order(self, order_id, product_id):
        self.cancellations.append((order_id, product_id))
        self.order["state"] = "cancelled"
        if self.fill_on_cancel:
            self.order.update(state="closed", unfilled_size=0)
            self.size = 1
        return {"success": True, "result": deepcopy(self.order)}

    async def get_open_positions(self, product_id):
        return {"success": True, "result": {"size": self.size}}

    async def get_open_orders(self):
        return {
            "success": True,
            "result": [] if self.order["state"] != "open" else [deepcopy(self.order)],
        }

    async def _request(self, method, path):
        assert (method, path) == ("GET", "/v2/positions/margined")
        return {"success": True, "result": []}

    async def place_order(self, request):
        self.recoveries.append(request)
        assert request["reduce_only"] is True
        assert request["side"] == "sell"
        assert request["size"] == 1
        self.size = 0
        return {"success": True, "result": {"id": 456}}


def setup_probe(tmp_path, plan, **kwargs):
    client = FakeExchange(plan, **kwargs)
    probe = Probe(client, tmp_path / "fixture.json")
    probe.plan = plan
    probe.client_id = plan["client_order_id"]
    probe.attempted = True
    return probe, client


@pytest.mark.asyncio
async def test_ambiguous_placement_resolves_id_and_cancels_without_resubmission(tmp_path, plan):
    probe, client = setup_probe(tmp_path, plan)
    # Simulates POST accepted but its response lost: no local exchange order ID.
    assert probe.order_id is None
    await probe.cleanup()
    assert client.cancellations == [("123456", 84)]
    assert client.recoveries == []
    assert probe.results["Cancellation"] == "PASS"
    assert probe.results["Reconciliation"].startswith("PASS")


@pytest.mark.asyncio
async def test_fill_during_cancel_is_recovered_once_and_fails_no_fill_test(tmp_path, plan):
    probe, client = setup_probe(tmp_path, plan, fill_on_cancel=True)
    await probe.cleanup()
    assert len(client.recoveries) == 1
    assert probe.fill_seen
    assert client.size == 0
    assert probe.results["Cancellation"].startswith("FAIL")
    assert probe.results["Reconciliation"].startswith("PASS")


@pytest.mark.asyncio
async def test_cancellation_payload_mismatch_does_not_skip_reconciliation(tmp_path, plan):
    probe, client = setup_probe(tmp_path, plan)
    client.order["bracket_stop_loss_price"] = "1"
    await probe.cleanup()
    assert client.cancellations
    assert probe.results["Cancellation"].startswith("FAIL")
    assert probe.results["Reconciliation"].startswith("PASS")


@pytest.mark.asyncio
async def test_oversized_position_never_gets_a_recovery_order(tmp_path, plan, monkeypatch):
    probe, client = setup_probe(tmp_path, plan)
    client.size = 2
    monkeypatch.setattr("scripts.test_delta_no_fill.asyncio.sleep", AsyncMock())
    with pytest.raises(
        ProbeError, match="cleanup_incomplete_unexpected_position_exceeds_probe_size"
    ):
        await probe.cleanup()
    assert not client.recoveries


@pytest.mark.asyncio
async def test_lookup_not_found_falls_back_to_own_resting_order(tmp_path, plan):
    probe, client = setup_probe(tmp_path, plan)

    class NotFound(Exception):
        status = 404

    client.get_order_by_client_id = AsyncMock(side_effect=NotFound())
    found = await probe.resolve()
    assert found["id"] == 123456
    assert probe.order_id == 123456
    assert not client.recoveries


def test_recovery_does_not_cancel_other_reduce_only_orders(tmp_path, plan):
    probe, _ = setup_probe(tmp_path, plan)
    leg = {
        "product_id": 84,
        "size": 1,
        "side": "sell",
        "reduce_only": True,
        "stop_order_type": "stop_loss_order",
        "stop_price": plan["bracket_stop_loss_price"],
    }
    assert probe.expected_bracket(leg)
    assert not probe.expected_bracket({**leg, "stop_price": "1"})
    assert not probe.expected_bracket({**leg, "product_id": 85})
    assert not probe.expected_bracket({**leg, "size": 2})


@pytest.fixture
def captured_exchange():
    path = Path(__file__).parent / "fixtures" / "delta_testnet_no_fill.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_real_testnet_fixtures_preserve_order_brackets_and_parser_states(captured_exchange):
    exchanges = captured_exchange["exchanges"]
    plan = next(e["payload"] for e in exchanges if e["name"] == "request")
    placement = next(e["payload"]["result"] for e in exchanges if e["name"] == "placement")
    lookup = next(e["payload"]["result"] for e in exchanges if e["name"] == "lookup_open")
    cancel = next(e["payload"]["result"] for e in exchanges if e["name"] == "cancel_response")
    updates = [e["payload"] for e in exchanges if e["name"] == "ws_order"]
    assert captured_exchange["unexpected_fill"] is False
    assert {e["state"] for e in updates} == {"open", "cancelled"}
    assert all(v.startswith("PASS") for v in captured_exchange["results"].values())
    for order in [placement, lookup, cancel, *updates]:
        verify_order(order, plan, 700000001, {"open", "cancelled"})
        parsed = parse_order(order)
        expected = VenueOrderState.OPEN if order["state"] == "open" else VenueOrderState.CANCELLED
        assert parsed.state == expected
        assert parsed.filled_size == 0
        assert parsed.size == 1
        assert parsed.client_order_id == "fixture_nofill_0001"
        assert parsed.exchange_order_id == "700000001"
    assert any(
        e["name"] == "reconciled_realtime_position" and e["payload"]["result"]["size"] == 0
        for e in exchanges
    )
    assert any(
        e["name"] == "reconciled_open_orders" and e["payload"]["result"] == [] for e in exchanges
    )


def test_real_testnet_fixtures_contain_only_sanitized_exchange_fields(captured_exchange):
    def inspect(value):
        if isinstance(value, dict):
            assert set(value) <= FIXTURE_FIELDS
            for key, item in value.items():
                if key in {"id", "order_id"} and item is not None:
                    assert item == 700000001
                if key == "client_order_id" and item is not None:
                    assert item == "fixture_nofill_0001"
                inspect(item)
        elif isinstance(value, list):
            for item in value:
                inspect(item)

    for entry in captured_exchange["exchanges"]:
        inspect(entry["payload"])
