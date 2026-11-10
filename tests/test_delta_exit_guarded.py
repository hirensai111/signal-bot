"""Offline exit/restart safety tests; no live orders are submitted."""

import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripts.test_delta_exit_guarded import ExitProbe, compare_exit, main, near_trigger
from scripts.test_delta_fill_guarded import EVIDENCE_FIELDS
from scripts.test_delta_no_fill import ProbeError
from signal_bot.execution.adapter import ExchangeFill, ExchangeUnavailable, FillPurpose
from signal_bot.execution.delta_adapter import DeltaExchangeAdapter, parse_order
from tests.harness import Harness


def test_scenario_confirmation_is_required_before_network(monkeypatch):
    monkeypatch.setattr("sys.argv", ["probe", "--scenario", "stop-loss"])
    run = AsyncMock()
    monkeypatch.setattr(asyncio, "run", run)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    run.assert_not_called()


@pytest.mark.parametrize(
    "scenario,field",
    [("stop-loss", "bracket_stop_loss_price"), ("take-profit", "bracket_take_profit_price")],
)
def test_near_trigger_changes_only_the_intended_leg(scenario, field):
    plan = {"size": 1, "bracket_stop_loss_price": "98000", "bracket_take_profit_price": "102000"}
    ticker = {"mark_price": "100000"}
    book = {"buy": [{"price": "99999"}], "sell": [{"price": "100001"}]}
    result = near_trigger(plan, ticker, book, Decimal("0.1"), scenario)
    assert result[field] != plan[field]
    assert {k: v for k, v in result.items() if k not in (field, "bracket_stop_trigger_method")} == {
        k: v for k, v in plan.items() if k != field
    }
    assert result["bracket_stop_trigger_method"] == "mark_price"
    assert Decimal(result[field]) % Decimal("0.1") == 0
    assert near_trigger(plan, ticker, book, Decimal("0.1"), "restart") == plan


def test_trigger_comparison_requires_actual_transitions_and_fill_ids():
    plan = {"product_id": 84}
    order = {
        "id": 2,
        "product_id": 84,
        "stop_order_type": "stop_loss_order",
        "reduce_only": True,
        "state": "closed",
        "size": 1,
        "unfilled_size": 0,
    }
    fill = {
        "id": "uuid-fill",
        "order_id": 2,
        "size": "1",
        "price": "78000",
        "commission": "0.04",
        "side": "sell",
    }
    trade = {**fill, "fill_id": "uuid-fill"}
    events = [{**order, "state": "pending", "unfilled_size": 1}, order]
    assert compare_exit([fill], [trade], order, events, plan, "stop_loss_order") == [fill]
    with pytest.raises(ProbeError, match="pending_exit_transition_missing"):
        compare_exit([fill], [trade], order, [order], plan, "stop_loss_order")
    with pytest.raises(ProbeError, match="exit_rest_ws_fill_ids_mismatch"):
        compare_exit(
            [fill], [{**trade, "fill_id": "another"}], order, events, plan, "stop_loss_order"
        )


async def test_public_cache_retry_keeps_freshness_gate(tmp_path, monkeypatch):
    client = MagicMock()
    client.symbol_map._products = {"BTCUSD": {"id": 84}}
    client._request = AsyncMock(return_value={"result": {}})
    probe = ExitProbe(client, tmp_path / "fixture.json", "stop-loss")
    planner = MagicMock(side_effect=[ProbeError("stale_ticker"), {"safe_plan": True}])
    monkeypatch.setattr("scripts.test_delta_exit_guarded.fill_plan", planner)
    monkeypatch.setattr("scripts.test_delta_exit_guarded.asyncio.sleep", AsyncMock())
    assert (await probe.fresh_market())[2] == {"safe_plan": True}
    assert client._request.await_count == 4
    assert all(
        "_verification_nonce" in call.kwargs["params"] for call in client._request.await_args_list
    )
    planner.side_effect = ProbeError("stale_ticker")
    with pytest.raises(ProbeError, match="stale_ticker"):
        await probe.fresh_market()


async def test_flat_before_rest_fill_publication_defers_outcome_and_retries(tmp_path):
    h = await Harness.create(tmp_path)
    try:
        signal = await h.make_signal()
        await h.submit(signal)
        order = await h.order_for(signal)
        original = h.venue.exit_fills
        h.venue.exit_fills = AsyncMock(return_value=[])
        h.monitor.exit_fill_retries = 1
        assert await h.monitor.reconstruct_close(order) is False
        assert await h.outcomes() == []
        assert (await h.order_for(signal)).state == "filled"
        h.venue.exit_fills = original
        h.venue.faults.add("stream", "drop_events", times=-1)
        await h.venue.move_market("BTCUSD", Decimal("63800"), Decimal("63800.5"))
        assert await h.monitor.reconstruct_close(await h.order_for(signal)) is True
        assert await h.monitor.reconstruct_close(await h.order_for(signal)) is False
        [outcome] = await h.outcomes()
        assert outcome.exit_reason == "sl_hit"
    finally:
        await h.close()


async def test_startup_gate_waits_for_complete_missed_entry_history(tmp_path):
    h = await Harness.create(tmp_path)
    try:
        h.config.order_type = "limit"
        signal = await h.make_signal()
        await h.submit(signal)
        h.venue.faults.add("stream", "drop_events", times=-1)
        local = await h.order_for(signal)
        remote = h.venue._orders[local.client_order_id]
        h.venue._fill(remote, remote.request.size, Decimal("65000"), "maker", remote.purpose)
        remote.state = remote.state.FILLED
        h.venue._attach_legs(remote, attach=True)
        original = h.venue.fills_for
        h.venue.fills_for = AsyncMock(return_value=[])
        assert await h.reconciler.reconcile_now() is False
        assert not h.reconciler._has_run_once.is_set()
        assert (await h.order_for(signal)).protective_status == "confirmed"
        h.venue.fills_for = original
        assert await h.reconciler.reconcile_now() is True
        assert h.reconciler._has_run_once.is_set()
        fills = await h.store.fills_for(local.client_order_id)
        assert sum(f.qty for f in fills if f.purpose == "entry") == local.size
    finally:
        await h.close()


@pytest.mark.parametrize(
    "kind,purpose",
    [("stop_loss_order", FillPurpose.STOP_LOSS), ("take_profit_order", FillPurpose.TAKE_PROFIT)],
)
async def test_delta_backfill_uses_actual_exit_order_type(kind, purpose):
    client = MagicMock()
    client.symbol_map._products = {"BTCUSD": {"id": 84}}
    client.get_fills = AsyncMock(
        return_value={
            "result": [
                {
                    "id": "exit-one",
                    "order_id": 2,
                    "product_symbol": "BTCUSD",
                    "side": "sell",
                    "size": "1",
                    "price": "78000",
                    "commission": "0.04",
                },
                {
                    "id": "exit-two",
                    "order_id": 2,
                    "product_symbol": "BTCUSD",
                    "side": "sell",
                    "size": "1",
                    "price": "78000",
                    "commission": "0.04",
                },
            ]
        }
    )
    client.get_order_status = AsyncMock(
        return_value={"result": {"id": 2, "stop_order_type": kind, "reduce_only": True}}
    )
    adapter = DeltaExchangeAdapter(client)
    adapter.get_order = AsyncMock(
        return_value=parse_order(
            {
                "id": 1,
                "client_order_id": "parent",
                "product_symbol": "BTCUSD",
                "side": "buy",
                "size": 2,
                "unfilled_size": 0,
                "state": "closed",
            }
        )
    )
    fills = await adapter.exit_fills("BTCUSD", "parent")
    assert len(fills) == 2
    assert all(f.purpose == purpose and f.client_order_id == "parent" for f in fills)
    client.get_order_status.assert_awaited_once_with("2")
    client.get_order_status.return_value = {"result": {"id": 999, "stop_order_type": kind}}
    with pytest.raises(ExchangeUnavailable, match="identity mismatch"):
        await adapter.exit_fills("BTCUSD", "parent")


@pytest.mark.parametrize("orphan_client_id", [None, "tagged-cleanup-close-without-local-order"])
async def test_orphan_stream_fill_is_enriched_without_duplicate_execution_or_outcome(
    tmp_path, orphan_client_id
):
    h = await Harness.create(tmp_path)
    try:
        signal = await h.make_signal()
        await h.submit(signal)
        order = await h.order_for(signal)
        orphan = ExchangeFill(
            fill_id="delta:orphan-child",
            client_order_id=orphan_client_id,
            exchange_order_id="child-2",
            symbol="BTCUSD",
            side="sell",
            qty=order.size,
            price=Decimal("67000"),
            fee=Decimal("1"),
            liquidity="taker",
            purpose=FillPurpose.UNKNOWN,
            occurred_at=h.clock.now(),
        )
        assert await h.store.record_fill(orphan, "sim")
        known = replace(
            orphan, client_order_id=order.client_order_id, purpose=FillPurpose.STOP_LOSS
        )
        assert await h.store.record_fill(known, "sim") is False
        assert await h.store.record_fill(known, "sim") is False
        fills = await h.store.fills_for(order.client_order_id)
        exits = [f for f in fills if f.fill_id == orphan.fill_id]
        assert len(exits) == 1 and exits[0].purpose == "stop_loss"
        assert await h.monitor._maybe_close(order.client_order_id)
        assert await h.monitor._maybe_close(order.client_order_id) is False
        [outcome] = await h.outcomes()
        assert outcome.exit_reason == "sl_hit"  # explicit order purpose wins over price proximity
        assert len(await h.trade_results()) == 1
        assert len(await h.journal.read(0, ["POSITION_CLOSED"])) == 1
    finally:
        await h.close()


@pytest.mark.parametrize(
    "scenario,kind,purpose,reason",
    [
        ("stop_loss", "stop_loss_order", "stop_loss", "sl_hit"),
        ("take_profit", "take_profit_order", "take_profit", "tp_hit"),
    ],
)
def test_observed_real_exit_transitions_fills_and_once_only_outcomes(
    scenario, kind, purpose, reason
):
    observed = json.loads(
        (Path(__file__).parent / "fixtures" / ("delta_" + scenario + ".json")).read_text(
            encoding="utf-8"
        )
    )
    assert observed["result"] == "PASS"
    assert all(v == "PASS" for v in observed["results"].values())
    events = observed["exchanges"]
    plan = next(e["payload"] for e in events if e["name"] == "entry_request")
    orders = [e["payload"] for e in events if e["name"] == "ws_orders"]
    filled = next(
        o for o in orders if o.get("stop_order_type") == kind and o.get("state") == "closed"
    )
    assert any(o.get("id") == filled["id"] and o.get("reason") == "stop_trigger" for o in orders)
    assert any(o.get("id") == filled["id"] and o.get("state") == "pending" for o in orders)
    assert any(
        o.get("stop_order_type") != kind
        and o.get("stop_order_type") in {"stop_loss_order", "take_profit_order"}
        and o.get("state") == "cancelled"
        for o in orders
    )
    detail = next(
        e["payload"]["result"]
        for e in events
        if e["name"] in {"target_order_observed", "target_order_final_readonly"}
        and e["payload"]["result"].get("state") == "closed"
    )
    fills = [e["payload"]["result"] for e in events if e["name"] == "trigger_rest_fills"][-1]
    trades = [e["payload"] for e in events if e["name"] == "ws_user_trades"]
    compare_exit(fills, trades, detail, orders, plan, kind)
    status = observed["bot_status"]
    assert status["state"] == "closed"
    assert status["outcomes"] == status["trade_results"] == status["close_events"] == 1
    assert status["fill_count"] == 2
    assert Decimal(status["entry_qty"]) == Decimal(status["exit_qty"]) == 1
    assert status["exit_reason"] == reason
    assert status["exit_purposes"] == [purpose]
    assert status["mutation_attempts"] == 0
    assert status["duplicate_replay_complete"] is True


def test_observed_real_process_restart_recovers_before_signals_and_preserves_protection():
    observed = json.loads(
        (Path(__file__).parent / "fixtures" / "delta_restart.json").read_text(encoding="utf-8")
    )
    assert observed["result"] == "PASS"
    assert all(v == "PASS" for v in observed["results"].values())
    status = observed["bot_status"]
    assert status["startup_state"] == "filled"
    assert Decimal(status["startup_entry_qty"]) == 1
    assert status["startup_protection"] == "confirmed"
    assert status["reconciled_before_signals"] is True
    assert status["duplicate_signal_rejected"] is True
    assert status["duplicate_replay_complete"] is True
    assert status["mutation_attempts"] == 0
    assert status["outcomes"] == status["trade_results"] == status["close_events"] == 1
    assert Decimal(status["entry_qty"]) == Decimal(status["exit_qty"]) == 1
    snapshots = [
        e["payload"]["result"] for e in observed["exchanges"] if e["name"] == "scenario_live_orders"
    ]
    assert len(snapshots) >= 2
    assert {o["id"] for o in snapshots[0]} == {o["id"] for o in snapshots[-1]}
    assert len(snapshots[0]) == 2


@pytest.mark.parametrize("scenario", ["stop_loss", "take_profit", "restart"])
def test_observed_scenario_evidence_excludes_account_fields_and_ends_flat(scenario):
    observed = json.loads(
        (Path(__file__).parent / "fixtures" / ("delta_" + scenario + ".json")).read_text(
            encoding="utf-8"
        )
    )

    def inspect(value):
        if isinstance(value, dict):
            assert set(value) <= EVIDENCE_FIELDS
            for key, item in value.items():
                if key in {"id", "order_id", "fill_id"} and item is not None:
                    assert isinstance(item, int) and 710000001 <= item < 710010000
                if key == "client_order_id" and item is not None:
                    assert item.startswith("fixture_fill_")
                inspect(item)
        elif isinstance(value, list):
            for item in value:
                inspect(item)

    for event in observed["exchanges"]:
        inspect(event["payload"])
    assert (
        next(
            e["payload"]["result"]
            for e in observed["exchanges"]
            if e["name"] == "final_realtime_position"
        )["size"]
        == 0
    )
    assert (
        next(
            e["payload"]["result"]
            for e in observed["exchanges"]
            if e["name"] == "final_open_orders"
        )
        == []
    )
