"""The testnet trail check: refuses production, cleans up, reports the findings."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from signal_bot.delta.trail_check import run_check


def _client(mode="testnet"):
    c = MagicMock()
    c.mode = mode
    c.get_products = AsyncMock(
        return_value={"products": [{"symbol": "BTCUSD", "id": 84, "tick_size": "0.5"}]}
    )
    c.get_l2_orderbook = AsyncMock(
        return_value={"result": {"buy": [{"price": "65000"}], "sell": [{"price": "65001"}]}}
    )
    c.place_order = AsyncMock(return_value={"success": True, "result": {"id": 1}})
    c.get_order_by_client_id = AsyncMock(
        return_value={"result": {"state": "closed", "average_fill_price": "65001"}}
    )
    child = {
        "id": 9,
        "product_id": 84,
        "stop_order_type": "stop_loss_order",
        "stop_price": "64675.5",
        "trail_amount": "325",
    }
    c.get_open_orders = AsyncMock(return_value={"result": [child]})
    c.get_open_positions = AsyncMock(
        side_effect=[
            {"result": [{"size": 1, "entry_price": "65001"}]},  # A: fields
            {"result": [{"size": 1}]},  # A flatten
            {"result": [{"size": 1}]},  # B flatten
            {"result": [{"size": 0}]},  # final
        ]
    )
    c.edit_order = AsyncMock(return_value={"success": True})
    c.cancel_order = AsyncMock(return_value={})
    return c


async def test_refuses_to_run_outside_testnet():
    with pytest.raises(SystemExit):
        await run_check(_client("production"))


async def test_runs_both_legs_and_always_flattens():
    c = _client()
    report = await run_check(c, sample_seconds=0.0, sample_every=0.0)
    bodies = [call.args[0] for call in c.place_order.await_args_list]
    entry_a, close_a, entry_b, close_b = bodies[0], bodies[1], bodies[2], bodies[3]
    assert entry_a["bracket_trail_amount"] == "325.0" and "bracket_stop_loss_price" not in entry_a
    assert close_a["reduce_only"] and close_b["reduce_only"]
    assert "bracket_stop_loss_price" in entry_b and "bracket_trail_amount" not in entry_b
    c.edit_order.assert_awaited()
    assert report["a_children"][0]["trail_amount"] == "325"
    assert report["final_cleanup"] == "ok"
    assert set(report["conclusion"]) == {
        "native_trail_accepted_with_sign",
        "native_trail_moves",
        "stepped_edit_works",
    }
