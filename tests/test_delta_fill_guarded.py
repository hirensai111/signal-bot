"""Offline safeguards; these tests never place an exchange order."""

import asyncio
import json
import time
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scripts.test_delta_fill_guarded import (
    EVIDENCE_FIELDS,
    Evidence,
    FillProbe,
    ProbeError,
    compare_fills,
    fill_plan,
    is_protection,
    main,
    validate_environment,
)
from signal_bot.execution.adapter import VenueOrderState
from signal_bot.execution.delta_adapter import parse_fill, parse_order


@pytest.fixture
def market():
    product = {
        "id": 84,
        "symbol": "BTCUSD",
        "state": "live",
        "trading_status": "operational",
        "contract_type": "perpetual_futures",
        "notional_type": "vanilla",
        "contract_unit_currency": "BTC",
        "contract_value": "0.001",
        "tick_size": "0.1",
    }
    ticker = {
        "timestamp": int(time.time() * 1_000_000),
        "mark_price": "78000",
        "price_band": {"lower_limit": "1", "upper_limit": "156000"},
    }
    book = {"buy": [{"price": "77999"}], "sell": [{"price": "78001"}]}
    return product, ticker, book


@pytest.fixture
def plan(market):
    return fill_plan(*market, "gf" + "a" * 24)


def test_confirmation_required_before_network(monkeypatch):
    monkeypatch.setattr("sys.argv", ["test_delta_fill_guarded"])
    run = AsyncMock()
    monkeypatch.setattr(asyncio, "run", run)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    run.assert_not_called()


@pytest.mark.parametrize(
    "mode,execution,url",
    [
        ("production", "paper", ""),
        ("production", "live", ""),
        ("testnet", "live", ""),
        ("testnet", "paper", "wss://socket.india.delta.exchange"),
    ],
)
def test_production_and_custom_socket_are_rejected(mode, execution, url):
    with pytest.raises(ProbeError):
        validate_environment(mode, execution, url)


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract_value", "0.1"),
        ("min_size", 2),
        ("state", "settled"),
        ("notional_type", "inverse"),
        ("tick_size", "0"),
        ("contract_type", "call_options"),
    ],
)
def test_bad_contract_metadata_prevents_entry(market, field, value):
    product, ticker, book = deepcopy(market)
    product[field] = value
    with pytest.raises(ProbeError):
        fill_plan(product, ticker, book, "gf" + "b" * 24)


def test_minimum_order_must_fit_hard_cap(market):
    with pytest.raises(ProbeError, match="minimum_contract_exceeds_notional_cap"):
        fill_plan(*market, "gf" + "b" * 24, Decimal("50"))
    with pytest.raises(ProbeError, match="notional_cap_out_of_bounds"):
        fill_plan(*market, "gf" + "b" * 24, Decimal("101"))


def test_stale_market_prevents_entry(market):
    product, ticker, book = deepcopy(market)
    ticker["timestamp"] -= 60_000_000
    with pytest.raises(ProbeError, match="stale_ticker"):
        fill_plan(product, ticker, book, "gf" + "b" * 24)


def stop_order(plan):
    return {
        "id": 456,
        "product_id": 84,
        "product_symbol": "BTCUSD",
        "side": "sell",
        "size": 1,
        "unfilled_size": 1,
        "state": "pending",
        "order_type": "market_order",
        "stop_order_type": "stop_loss_order",
        "reduce_only": True,
        "stop_trigger_method": "last_traded_price",
        "stop_price": plan["bracket_stop_loss_price"],
    }


@pytest.mark.parametrize(
    "change",
    [
        {"state": "cancelled"},
        {"reduce_only": False},
        {"size": 0},
        {"size": 2},
        {"stop_price": "1"},
        {"side": "buy"},
        {"order_type": "limit_order"},
        {"stop_order_type": "take_profit_order"},
        {"product_id": 85},
    ],
)
def test_false_protection_is_not_accepted(plan, change):
    assert is_protection(stop_order(plan), plan)
    assert not is_protection({**stop_order(plan), **change}, plan)


def test_bracket_fields_on_entry_do_not_prove_a_live_stop(plan):
    assert not is_protection({**plan, "id": 123, "state": "closed", "unfilled_size": 0}, plan)


def sample_fills(plan):
    order = {
        **plan,
        "id": 123,
        "state": "closed",
        "unfilled_size": 0,
        "average_fill_price": "78001",
        "paid_commission": "0.04",
    }
    rest = [
        {
            "id": "fill-uuid",
            "order_id": 123,
            "size": "1",
            "price": "78001",
            "side": "buy",
            "role": "taker",
            "commission": "0.04",
        }
    ]
    trades = [{**rest[0], "fill_id": "fill-uuid", "client_order_id": plan["client_order_id"]}]
    return rest, trades, order, [deepcopy(order)]


def test_real_fill_identifiers_prices_quantity_client_id_and_fees_must_agree(plan):
    rest, trades, order, updates = sample_fills(plan)
    compare_fills(rest, trades, order, updates, plan, plan["client_order_id"], "buy")


@pytest.mark.parametrize(
    "field,value",
    [
        ("fill_id", "another-fill"),
        ("order_id", 999),
        ("size", "2"),
        ("price", "78002"),
        ("commission", "0.05"),
        ("client_order_id", "another-client"),
    ],
)
def test_mismatched_trade_fails_verification(plan, field, value):
    rest, trades, order, updates = sample_fills(plan)
    trades[0][field] = value
    with pytest.raises(ProbeError):
        compare_fills(rest, trades, order, updates, plan, plan["client_order_id"], "buy")


def test_missing_fee_is_not_silently_treated_as_zero(plan):
    rest, trades, order, updates = sample_fills(plan)
    del trades[0]["commission"]
    with pytest.raises(ProbeError, match="ws_fee_missing"):
        compare_fills(rest, trades, order, updates, plan, plan["client_order_id"], "buy")


def test_sanitization_preserves_relations_but_removes_account_information(plan):
    evidence = Evidence()
    rest, trades, _, _ = sample_fills(plan)
    raw = {
        "result": [
            {
                **trades[0],
                "user_id": 777,
                "api_key": "secret",
                "balance": "999",
                "meta_data": {"ip": "1.2.3.4"},
            }
        ]
    }
    clean = evidence.clean(raw)["result"][0]
    other = evidence.clean(rest[0])
    assert clean["fill_id"] == other["id"]
    assert clean["order_id"] == other["order_id"]
    assert clean["client_order_id"] != plan["client_order_id"]
    assert not {"user_id", "api_key", "balance", "meta_data"} & clean.keys()


class FakeExchange:
    def __init__(self, plan, *, ambiguous=False, protection=True, lookup_error=False):
        self.plan = plan
        self.entry = None
        self.close = None
        self.position = 0
        self.calls = []
        self.ambiguous = ambiguous
        self.protection = protection
        self.lookup_error = lookup_error

    async def place_order(self, request):
        if request.get("reduce_only"):
            self.calls.append("close")
            assert request["size"] == 1
            assert request["side"] == "sell"
            self.position = 0
            self.close = {**request, "id": 789, "state": "closed", "unfilled_size": 0}
            return {"result": self.close}
        self.calls.append("entry")
        self.entry = {**request, "id": 123, "state": "closed", "unfilled_size": 0}
        self.position = 1
        if self.ambiguous:
            raise TimeoutError()
        return {"result": self.entry}

    async def get_order_by_client_id(self, client_id):
        self.calls.append("lookup")
        if self.lookup_error and client_id == self.plan["client_order_id"]:
            raise TimeoutError()
        return {"result": self.entry if client_id == self.plan["client_order_id"] else self.close}

    async def get_open_positions(self, product_id):
        self.calls.append("position")
        return {"result": {"size": self.position}}

    async def get_open_orders(self):
        self.calls.append("orders")
        return {"result": [stop_order(self.plan)] if self.position and self.protection else []}

    async def get_order_status(self, order_id):
        return {"result": stop_order(self.plan)}

    async def _request(self, method, path):
        assert (method, path) == ("GET", "/v2/positions/margined")
        return {"result": []}


def make_probe(tmp_path, plan, **kwargs):
    client = FakeExchange(plan, **kwargs)
    probe = FillProbe(client, tmp_path / "fixture.json")
    probe.plan = plan
    probe.client_id = plan["client_order_id"]
    probe.close_id = probe.client_id + "c"
    return probe, client


@pytest.mark.asyncio
async def test_ambiguous_entry_is_looked_up_and_never_resubmitted(tmp_path, plan):
    probe, client = make_probe(tmp_path, plan, ambiguous=True)
    order = await probe.submit_entry()
    assert order["id"] == 123
    assert client.calls == ["entry", "lookup"]
    with pytest.raises(ProbeError, match="entry_already_attempted_or_halted"):
        await probe.submit_entry()
    await probe.cleanup()
    assert client.calls.count("entry") == 1
    assert client.calls.count("close") == 1
    assert client.position == 0


@pytest.mark.asyncio
async def test_missing_stop_skips_evidence_wait_and_flattens_immediately(tmp_path, plan):
    probe, client = make_probe(tmp_path, plan, protection=False)
    probe.fill_evidence = AsyncMock()
    with pytest.raises(ProbeError, match="stop_unconfirmed_flatten_immediately"):
        try:
            await probe.stage(None)
        finally:
            await probe.cleanup()
    probe.fill_evidence.assert_not_called()
    assert probe.halted
    assert client.calls.count("close") == 1
    assert client.position == 0
    assert probe.results["Account flat"] == "PASS"


@pytest.mark.asyncio
async def test_lookup_failure_never_prevents_exposed_position_flattening(
    tmp_path, plan, monkeypatch
):
    probe, client = make_probe(tmp_path, plan, lookup_error=True)
    await probe.submit_entry()
    monkeypatch.setattr("scripts.test_delta_fill_guarded.asyncio.sleep", AsyncMock())
    await probe.cleanup()
    assert client.calls.count("close") == 1
    assert client.position == 0
    assert probe.results["Account flat"] == "PASS"


@pytest.fixture
def captured_fill():
    return json.loads(
        (Path(__file__).parent / "fixtures" / "delta_testnet_fill.json").read_text(encoding="utf-8")
    )


def test_observed_entry_and_close_rest_ws_fills_match_and_parse(captured_fill):
    events = captured_fill["exchanges"]
    plan = next(e["payload"] for e in events if e["name"] == "entry_request")
    trades = [e["payload"] for e in events if e["name"] == "ws_user_trades"]
    orders = [e["payload"] for e in events if e["name"] == "ws_orders"]
    assert captured_fill["result"] == "PASS"
    assert captured_fill["stage_error"] is None
    assert captured_fill["cleanup_error"] is None
    assert all(v == "PASS" for v in captured_fill["results"].values())
    for prefix, side in (("entry", "buy"), ("close", "sell")):
        request = next(e["payload"] for e in events if e["name"] == prefix + "_request")
        order = [e["payload"]["result"] for e in events if e["name"] == prefix + "_lookup"][-1]
        fills = [e["payload"]["result"] for e in events if e["name"] == prefix + "_rest_fills"][-1]
        _, matched = compare_fills(
            fills, trades, order, orders, plan, request["client_order_id"], side
        )
        parsed_order = parse_order(order)
        assert parsed_order.state == VenueOrderState.FILLED
        assert parsed_order.filled_size == 1
        for raw in matched:
            parsed = parse_fill(raw)
            assert parsed.client_order_id == request["client_order_id"]
            assert parsed.qty == Decimal(str(raw["size"]))
            assert parsed.fee == Decimal(str(raw["commission"]))
            assert parsed.exchange_order_id == str(order["id"])
        if prefix == "close":
            assert order["reduce_only"] is True
            assert request["reduce_only"] is True


def test_observed_protection_is_a_separate_pending_reduce_only_market_stop(captured_fill):
    events = captured_fill["exchanges"]
    plan = next(e["payload"] for e in events if e["name"] == "entry_request")
    entry = next(e["payload"]["result"] for e in events if e["name"] == "entry_ack")
    stops = [e["payload"]["result"] for e in events if e["name"] == "protective_stop_lookup"]
    assert stops
    for stop in stops:
        assert is_protection(stop, plan)
        assert stop["id"] != entry["id"]
        assert stop["state"] == "pending"
        assert stop["order_type"] == "market_order"
    position = next(
        e["payload"]["result"] for e in events if e["name"] == "entry_realtime_position"
    )
    assert position["size"] == 1
    final_position = next(
        e["payload"]["result"] for e in events if e["name"] == "final_realtime_position"
    )
    assert final_position["size"] == 0
    assert next(e["payload"]["result"] for e in events if e["name"] == "final_open_orders") == []


def test_observed_fill_evidence_contains_no_account_fields_or_raw_identifiers(captured_fill):
    def inspect(value):
        if isinstance(value, dict):
            assert set(value) <= EVIDENCE_FIELDS
            for key, item in value.items():
                if key in {"id", "order_id", "fill_id"} and item is not None:
                    assert isinstance(item, int) and 710000001 <= item < 710001000
                if key == "client_order_id" and item is not None:
                    assert item.startswith("fixture_fill_")
                inspect(item)
        elif isinstance(value, list):
            for item in value:
                inspect(item)

    for entry in captured_fill["exchanges"]:
        inspect(entry["payload"])
