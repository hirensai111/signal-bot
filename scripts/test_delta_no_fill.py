"""Guarded Delta TESTNET placement -> lookup -> cancel probe.

Run: DELTA_MODE=testnet EXEC_MODE=paper python -m scripts.test_delta_no_fill
     --confirm-testnet-order

Requires an otherwise idle demo account. Never retries entry placement. A timeout
is treated as an ambiguous placement and resolved by client ID before cleanup.
Only an unexpected fill permits a reduce-only recovery market order. Cleanup is
bounded and best effort; process termination or a network outage can interrupt it.
Sanitized exchange fixtures are written even when verification fails.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import uuid
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path

TESTNET_REST = "https://cdn-ind.testnet.deltaex.org"
TESTNET_WS = "wss://socket-ind.testnet.deltaex.org"
TERMINAL = {"cancelled", "canceled", "closed", "filled", "rejected"}
BRACKETS = (
    "bracket_stop_loss_price",
    "bracket_stop_loss_limit_price",
    "bracket_take_profit_price",
    "bracket_take_profit_limit_price",
)
FIXTURE_FIELDS = set(BRACKETS) | {
    "success",
    "result",
    "meta",
    "type",
    "action",
    "channels",
    "name",
    "symbols",
    "id",
    "order_id",
    "client_order_id",
    "product_id",
    "product_symbol",
    "symbol",
    "size",
    "unfilled_size",
    "side",
    "state",
    "order_type",
    "limit_price",
    "post_only",
    "reduce_only",
    "time_in_force",
    "bracket_order",
    "bracket_orders",
    "bracket_stop_trigger_method",
    "stop_order_type",
    "stop_price",
    "stop_trigger_method",
    "bracket_trail_amount",
    "trail_amount",
    "average_fill_price",
    "cancellation_reason",
    "reason",
    "seq_no",
    "seq_id",
    "timestamp",
    "created_at",
    "updated_at",
    "status",
    "status_code",
    "size_limit",
    "tick_size",
    "contract_value",
}


class ProbeError(Exception):
    """A fixed, account-data-free failure reason."""


def require(condition, reason):
    if not condition:
        raise ProbeError(reason)


def dec(value):
    result = Decimal(str(value))
    require(result.is_finite(), "non_finite_number")
    return result


def body(response):
    require(isinstance(response, dict), "invalid_response_envelope")
    require(response.get("success") is not False, "exchange_rejected_request")
    require("result" in response, "missing_result")
    return response["result"]


def rows(response):
    result = body(response)
    require(isinstance(result, list), "expected_result_list")
    require(all(isinstance(row, dict) for row in result), "invalid_result_row")
    require(not (response.get("meta") or {}).get("after"), "unexpected_pagination")
    return result


def position_size(response):
    result = body(response)
    require(isinstance(result, dict) and "size" in result, "invalid_position_response")
    return dec(result["size"])


def sanitize(value, key=""):
    """Allowlist exchange fields; replace test order identifiers consistently."""
    if isinstance(value, dict):
        return {k: sanitize(v, k) for k, v in value.items() if k in FIXTURE_FIELDS}
    if isinstance(value, list):
        return [sanitize(v, key) for v in value]
    if key in {"id", "order_id"}:
        return None if value is None else 700000001
    if key == "client_order_id":
        return None if value is None else "fixture_nofill_0001"
    return value


def safe_error(exc):
    if isinstance(exc, ProbeError):
        return str(exc)
    status = getattr(exc, "status", None)
    return type(exc).__name__ + (f"_HTTP_{status}" if status else "")


def guard_request(mode, rest_url, product_id, method, path, data):
    require(mode == "testnet" and rest_url == TESTNET_REST, "testnet_endpoint_required")
    if method == "GET":
        return
    require(path == "/v2/orders" and method in {"POST", "DELETE"}, "mutation_not_allowed")
    require(data and int(data.get("product_id", -1)) == product_id, "wrong_mutation_product")
    if method == "POST":
        require(data.get("size") == 1, "one_contract_only")
        require(
            (data.get("order_type") == "limit_order" and data.get("post_only") is True)
            or (data.get("order_type") == "market_order" and data.get("reduce_only") is True),
            "unsafe_order_type",
        )
    else:
        require(data.get("id") is not None, "cancel_requires_exact_id")


def make_plan(product, ticker, book, client_id):
    require(product.get("symbol") == "BTCUSD", "wrong_product")
    require(product.get("state") == "live", "product_not_live")
    require(product.get("trading_status") == "operational", "product_not_operational")
    require(product.get("contract_type") == "perpetual_futures", "wrong_contract_type")
    # Delta specifies size as a positive integer number of contracts. Refuse to
    # increase size if product metadata advertises a larger minimum or lot.
    for field in ("min_size", "minimum_order_size", "lot_size"):
        require(dec(product.get(field) or 1) == 1, "one_contract_not_valid")
    tick = dec(product["tick_size"])
    require(tick > 0, "invalid_tick")
    require(re.fullmatch(r"nf[0-9a-f]{24}", client_id) is not None, "invalid_client_id")
    bids = book.get("buy") or book.get("bids") or []
    asks = book.get("sell") or book.get("asks") or []
    require(bids and asks, "empty_order_book")

    def price(level):
        return dec(level["price"] if isinstance(level, dict) else level[0])

    bid, ask = max(map(price, bids)), min(map(price, asks))
    mark = dec(ticker["mark_price"])
    require(0 < bid < ask and mark > 0, "invalid_market")
    require(abs(bid / mark - 1) < Decimal("0.02"), "book_mark_disagreement")

    def floor(value):
        return (value / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    def ceil(value):
        return (value / tick).to_integral_value(rounding=ROUND_UP) * tick

    entry = floor(min(bid, mark) * Decimal("0.95"))
    stop = floor(entry * Decimal("0.98"))
    take = ceil(max(ask, mark) * Decimal("1.05"))
    band = ticker.get("price_band") or {}
    lower, upper = dec(band.get("lower_limit", 0)), dec(band.get("upper_limit", 0))
    require(0 < lower <= stop < entry < bid < ask < take <= upper, "unsafe_price_or_band")
    require(entry <= bid * Decimal("0.95"), "insufficient_market_distance")
    return {
        "product_id": int(product["id"]),
        "size": 1,
        "side": "buy",
        "order_type": "limit_order",
        "time_in_force": "gtc",
        "post_only": True,
        "reduce_only": False,
        "limit_price": str(entry),
        "client_order_id": client_id,
        "bracket_stop_trigger_method": "last_traded_price",
        "bracket_stop_loss_price": str(stop),
        "bracket_stop_loss_limit_price": str(stop),
        "bracket_take_profit_price": str(take),
        "bracket_take_profit_limit_price": str(take),
    }


def verify_order(row, plan, order_id, states):
    require(isinstance(row, dict), "invalid_order_response")
    require(str(row.get("id", row.get("order_id"))) == str(order_id), "order_id_mismatch")
    require(row.get("client_order_id") == plan["client_order_id"], "client_id_mismatch")
    require(row.get("state") in states, "order_state_mismatch")
    for field in ("size", "unfilled_size"):
        require(dec(row.get(field)) == 1, field + "_mismatch_or_fill")
    require(int(row.get("product_id", -1)) == plan["product_id"], "product_id_mismatch")
    require(row.get("product_symbol", row.get("symbol")) == "BTCUSD", "symbol_mismatch")
    for field in ("side", "order_type", "time_in_force"):
        require(row.get(field) == plan[field], field + "_mismatch")
    for field in ("limit_price", *BRACKETS):
        require(dec(row.get(field)) == dec(plan[field]), field + "_mismatch")
    # Some private stream versions omit these flags. Never invent omitted values.
    if "post_only" in row:
        require(row["post_only"] is True, "post_only_not_accepted")
    if "reduce_only" in row:
        require(row["reduce_only"] is False, "unexpected_reduce_only")


class Probe:
    def __init__(self, client, fixture_path):
        self.client = client
        self.fixture_path = fixture_path
        self.client_id = "nf" + uuid.uuid4().hex[:24]
        self.plan = None
        self.order_id = None
        self.attempted = False
        self.events = []
        self.transcript = []
        self.fill_seen = False
        self.results = dict.fromkeys(
            ("Placement", "Order lookup", "WebSocket", "Cancellation", "Reconciliation"),
            "FAIL (not reached)",
        )

    def record(self, name, payload):
        self.transcript.append({"name": name, "payload": sanitize(payload)})

    def own(self, row):
        return row.get("client_order_id") == self.client_id or (
            self.order_id is not None
            and str(row.get("id", row.get("order_id"))) == str(self.order_id)
        )

    def expected_bracket(self, row):
        expected = {
            "stop_loss_order": self.plan["bracket_stop_loss_price"],
            "take_profit_order": self.plan["bracket_take_profit_price"],
        }.get(row.get("stop_order_type"))
        return (
            int(row.get("product_id", -1)) == self.plan["product_id"]
            and row.get("reduce_only") is True
            and row.get("side") == "sell"
            and dec(row.get("size", 0)) == 1
            and expected is not None
            and dec(row.get("stop_price", 0)) == dec(expected)
        )

    async def account_empty(self):
        orders = rows(await self.client.get_open_orders())
        require(not orders, "account_has_resting_orders")
        positions = rows(await self.client._request("GET", "/v2/positions/margined"))
        require(
            all("size" in p and dec(p["size"]) == 0 for p in positions), "account_has_positions"
        )
        require(
            position_size(await self.client.get_open_positions(product_id=self.plan["product_id"]))
            == 0,
            "realtime_position_not_flat",
        )

    async def receive(self, ws):
        import aiohttp

        message = await ws.receive(timeout=10)
        require(message.type == aiohttp.WSMsgType.TEXT, "websocket_closed")
        event = json.loads(message.data)
        require(isinstance(event, dict), "invalid_ws_message")
        require(event.get("type") != "error", "websocket_exchange_error")
        require(event.get("success") is not False, "websocket_exchange_rejection")
        return event

    async def bootstrap(self, ws, socket):
        self.record("ws_auth", await socket._authenticate(ws))
        await ws.send_json({"type": "enable_heartbeat"})
        await ws.send_json(socket.subscription_message())
        ready = set()
        while ready != {"subscriptions", "orders", "positions"}:
            event = await self.receive(ws)
            kind = event.get("type")
            if kind == "subscriptions":
                channels = {c["name"]: c.get("symbols") for c in event.get("channels", [])}
                require(
                    all(
                        channels.get(k) == ["BTCUSD"]
                        for k in ("orders", "positions", "user_trades")
                    ),
                    "subscription_not_acknowledged",
                )
                self.record("ws_subscriptions", event)
                ready.add(kind)
            elif kind in {"orders", "positions"} and event.get("action") == "snapshot":
                require(event.get("result") == [], "nonempty_private_snapshot")
                self.record("ws_" + kind + "_baseline", event)
                ready.add(kind)

    async def listen(self, ws):
        while True:
            event = await self.receive(ws)
            kind = event.get("type")
            candidates = event.get("result") if isinstance(event.get("result"), list) else [event]
            for row in candidates:
                if kind == "orders" and self.own(row):
                    self.record("ws_order", event)
                    self.events.append(row)
                    if dec(row.get("unfilled_size", 1)) < 1 or row.get("reason") == "fill":
                        self.fill_seen = True
                elif kind == "positions" and dec(row.get("size", 0)) != 0:
                    self.fill_seen = True
                elif kind == "user_trades" and (
                    str(row.get("order_id")) == str(self.order_id)
                    or row.get("client_order_id") == self.client_id
                ):
                    self.fill_seen = True

    async def wait_order_event(self, states, listener):
        for _ in range(50):
            require(not self.fill_seen, "unexpected_fill")
            if listener.done():
                await listener
                raise ProbeError("websocket_listener_stopped")
            for event in self.events:
                if event.get("state") in states:
                    verify_order(event, self.plan, self.order_id, states)
                    return event
            await asyncio.sleep(0.1)
        raise ProbeError("websocket_order_update_timeout")

    async def stage(self, listener):
        self.attempted = True  # before network I/O: an exception may hide acceptance
        placed = await self.client.place_order(self.plan)
        self.record("placement", placed)
        raw = body(placed)
        require(isinstance(raw, dict) and raw.get("id"), "missing_placement_order_id")
        require(raw.get("client_order_id") == self.client_id, "placement_client_id_mismatch")
        require(
            int(raw.get("product_id", -1)) == self.plan["product_id"], "placement_product_mismatch"
        )
        self.order_id = raw["id"]
        verify_order(raw, self.plan, self.order_id, {"open"})
        self.results["Placement"] = "PASS"
        lookup = await self.client.get_order_by_client_id(self.client_id)
        self.record("lookup_open", lookup)
        verify_order(body(lookup), self.plan, self.order_id, {"open"})
        self.results["Order lookup"] = "PASS"
        await self.wait_order_event({"open"}, listener)
        self.results["WebSocket"] = "PASS (open update)"

    async def resolve(self):
        """Look up even when placement timed out; no entry retry is permitted."""
        try:
            response = await self.client.get_order_by_client_id(self.client_id)
            raw = body(response)
            require(isinstance(raw, dict), "invalid_cleanup_lookup")
            require(raw.get("client_order_id") == self.client_id, "cleanup_client_id_mismatch")
            require(
                int(raw.get("product_id", -1)) == self.plan["product_id"],
                "cleanup_product_mismatch",
            )
            require(raw.get("id"), "cleanup_order_id_missing")
            if self.order_id is not None:
                require(str(raw["id"]) == str(self.order_id), "cleanup_order_id_mismatch")
            self.order_id = raw["id"]
            self.record("cleanup_lookup", response)
            if dec(raw.get("unfilled_size", 1)) < 1:
                self.fill_seen = True
            return raw
        except Exception as exc:
            if getattr(exc, "status", None) != 404:
                raise
            # Independently locate an accepted entry if client-ID lookup lags.
            matches = [
                row
                for row in rows(await self.client.get_open_orders())
                if row.get("client_order_id") == self.client_id
            ]
            require(len(matches) <= 1, "duplicate_client_id")
            if matches:
                raw = matches[0]
                require(
                    int(raw.get("product_id", -1)) == self.plan["product_id"],
                    "cleanup_product_mismatch",
                )
                require(raw.get("id"), "cleanup_order_id_missing")
                self.order_id = raw["id"]
                self.record("cleanup_open_order_fallback", {"result": raw})
                return raw
            return None

    async def cleanup(self):
        """Always resolve/cancel, then reconcile both realtime and margined views."""
        if not self.attempted:
            return
        cancelled = False
        recovered = False
        absent_reads = 0
        last_error = None
        for _ in range(12):
            try:
                order = await self.resolve()
                if order is None:
                    # An accepted request may be briefly invisible. Require several
                    # consecutive absent reads plus empty account state before exit.
                    absent_reads += 1
                else:
                    absent_reads = 0
                    if order.get("state") not in TERMINAL:
                        response = await self.client.cancel_order(
                            str(self.order_id), product_id=self.plan["product_id"]
                        )
                        self.record("cancel_response", response)
                        body(response)
                        order = await self.resolve()
                    if order and order.get("state") in {"cancelled", "canceled"}:
                        cancelled = True
                        # Verification failure must never skip fill recovery or
                        # account reconciliation after the exchange cancelled.
                        try:
                            verify_order(order, self.plan, self.order_id, {"cancelled", "canceled"})
                            self.results["Cancellation"] = "PASS"
                        except Exception as exc:
                            self.results["Cancellation"] = "FAIL (" + safe_error(exc) + ")"

                # /positions/margined can lag by 10s. Always use the realtime
                # product endpoint before deciding whether to submit recovery.
                size = position_size(
                    await self.client.get_open_positions(product_id=self.plan["product_id"])
                )
                if size:
                    self.fill_seen = True
                    require(abs(size) <= 1, "unexpected_position_exceeds_probe_size")
                    require(
                        order is not None and order.get("state") in TERMINAL,
                        "entry_not_terminal_before_recovery",
                    )
                    require(not recovered, "recovery_pending_or_failed")
                    # Mark before sending: never blindly retry an ambiguous close.
                    recovered = True
                    recovery = {
                        "product_id": self.plan["product_id"],
                        "size": int(abs(size)),
                        "side": "sell" if size > 0 else "buy",
                        "order_type": "market_order",
                        "reduce_only": True,
                        "client_order_id": self.client_id + "rc",
                    }
                    response = await self.client.place_order(recovery)
                    self.record("unexpected_fill_reduce_only_recovery", response)
                    body(response)
                    continue

                resting_response = await self.client.get_open_orders()
                resting = rows(resting_response)
                # The account was empty before entry. Match every expected leg
                # field instead of broadly cancelling all orders for the product.
                if self.fill_seen:
                    for row in resting:
                        if self.expected_bracket(row):
                            response = await self.client.cancel_order(
                                str(row["id"]), product_id=self.plan["product_id"]
                            )
                            body(response)
                    resting_response = await self.client.get_open_orders()
                    resting = rows(resting_response)
                require(not resting, "resting_orders_after_cleanup")
                positions_response = await self.client._request("GET", "/v2/positions/margined")
                positions = rows(positions_response)
                require(
                    all("size" in p and dec(p["size"]) == 0 for p in positions),
                    "positions_after_cleanup",
                )
                realtime_response = await self.client.get_open_positions(
                    product_id=self.plan["product_id"]
                )
                require(
                    position_size(realtime_response) == 0,
                    "position_changed_during_reconciliation",
                )
                if (
                    cancelled
                    or (order is not None and order.get("state") in TERMINAL)
                    or absent_reads >= 10
                ):
                    self.record("reconciled_open_orders", resting_response)
                    self.record("reconciled_margined_positions", positions_response)
                    self.record("reconciled_realtime_position", realtime_response)
                    self.results["Reconciliation"] = "PASS (flat; no resting orders)"
                    if self.fill_seen:
                        self.results["Cancellation"] = "FAIL (unexpected fill; account recovered)"
                    return
            except Exception as exc:
                last_error = safe_error(exc)
                # Losing a lookup response must not prevent cancellation when a
                # prior authenticated response already supplied the exact ID.
                if self.order_id is not None and not isinstance(exc, ProbeError):
                    try:
                        response = await self.client.cancel_order(
                            str(self.order_id), product_id=self.plan["product_id"]
                        )
                        self.record("cleanup_cancel_retry", response)
                    except Exception:
                        pass
            await asyncio.sleep(1)
        raise ProbeError("cleanup_incomplete_" + (last_error or "order_unresolved"))

    async def run(self):
        from signal_bot.delta.client import DeltaWebSocket

        ws = None
        listener = None
        try:
            await self.client.initialize()
            product = self.client.symbol_map._products["BTCUSD"]
            # Plan from fresh public responses immediately before socket setup.
            ticker = body(await self.client._request("GET", "/v2/tickers/BTCUSD"))
            book = body(await self.client.get_l2_orderbook("BTCUSD"))
            self.plan = make_plan(product, ticker, book, self.client_id)
            await self.account_empty()
            socket = DeltaWebSocket(
                self.client,
                asyncio.Queue(),
                symbols=["BTCUSD"],
                ws_url=TESTNET_WS,
                auth_path="/live",
            )
            ws = await self.client._session.ws_connect(
                TESTNET_WS,
                headers={
                    "Origin": "https://demo.delta.exchange",
                    "User-Agent": "Mozilla/5.0 (signalbot-testnet-probe)",
                },
                heartbeat=20,
            )
            await asyncio.wait_for(self.bootstrap(ws, socket), timeout=15)
            # Refresh after authentication; refuse a stale/crossed plan.
            ticker = body(await self.client._request("GET", "/v2/tickers/BTCUSD"))
            book = body(await self.client.get_l2_orderbook("BTCUSD"))
            self.plan = make_plan(product, ticker, book, self.client_id)
            await self.account_empty()
            self.record("request", self.plan)
            print(
                "Preflight: PASS (testnet; idle account; 1 BTCUSD contract; post-only; 5% below market)",
                flush=True,
            )
            listener = asyncio.create_task(self.listen(ws))
            try:
                await asyncio.wait_for(self.stage(listener), timeout=12)
            finally:
                # Cancellation of the stage must never skip cleanup.
                await self.cleanup()
            if self.results["Cancellation"] == "PASS":
                await self.wait_order_event({"cancelled", "canceled"}, listener)
                self.results["WebSocket"] = "PASS (open and cancellation updates)"
            require(not self.fill_seen, "unexpected_fill_test_failed")
        except Exception as exc:
            self.record_error = safe_error(exc)
            print("Probe error: " + self.record_error, flush=True)
            if self.results["WebSocket"] == "PASS (open update)":
                self.results["WebSocket"] = "FAIL (cancellation update unverified)"
        finally:
            if listener:
                listener.cancel()
                try:
                    await listener
                except (asyncio.CancelledError, Exception):
                    pass
            if ws:
                await ws.close()
            await self.client.close()
            if self.fill_seen:
                self.results["Placement"] = "FAIL (unexpected fill)"
            self.fixture_path.parent.mkdir(parents=True, exist_ok=True)
            fixture = {
                "environment": "testnet",
                "results": self.results,
                "unexpected_fill": self.fill_seen,
                "exchanges": self.transcript,
            }
            self.fixture_path.write_text(json.dumps(fixture, indent=2) + "\n", encoding="utf-8")
            for name, result in self.results.items():
                print(f"{name}: {result}", flush=True)
            print("Sanitized fixtures saved.", flush=True)
        return (
            0
            if all(value.startswith("PASS") for value in self.results.values())
            and not self.fill_seen
            else 1
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm-testnet-order",
        action="store_true",
        help="Explicitly authorize one testnet limit order and reduce-only unexpected-fill recovery",
    )
    args = parser.parse_args()
    if not args.confirm_testnet_order:
        parser.error("--confirm-testnet-order is required; no network requests made")
    # Never inherit production mode or custom websocket destinations from .env.
    os.environ["DELTA_MODE"] = "testnet"
    os.environ["EXEC_MODE"] = "paper"
    os.environ["SYMBOLS"] = '["BTCUSDT"]'
    logging.disable(logging.CRITICAL)
    from signal_bot.config import settings
    from signal_bot.delta.client import DeltaClient

    class TestnetClient(DeltaClient):
        async def _request(self, method, path, params=None, data=None):
            product = self.symbol_map._products.get("BTCUSD", {})
            guard_request(self.mode, self.rest_url, product.get("id"), method, path, data)
            return await super()._request(method, path, params=params, data=data)

    require(
        settings.delta_api_key.strip() and settings.delta_api_secret.strip(), "credentials_missing"
    )
    fixture_path = (
        Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "delta_testnet_no_fill.json"
    )
    return asyncio.run(Probe(TestnetClient(mode="testnet", request_timeout=5), fixture_path).run())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("Probe setup error: " + safe_error(exc), flush=True)
        raise SystemExit(1) from None
