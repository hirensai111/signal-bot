"""One explicitly confirmed Delta TESTNET fill and controlled reduce-only close.

PowerShell:
  $env:DELTA_MODE = "testnet"; $env:EXEC_MODE = "paper"
  python -m scripts.test_delta_fill_guarded --confirm-testnet-fill

Requires an idle demo account. Rejects production configuration. The standalone
probe places a REAL demo order despite EXEC_MODE=paper; that setting prevents
accidental use of the normal live engine. It never retries an entry submission.
Missing stop protection causes immediate flattening, not another entry or a halt
alone. Stop triggering and restart recovery are deliberately separate tests.

Cleanup needs a running process and exchange connectivity; neither a client-side
finally block nor a stop order guarantees recovery through an outage or SIGKILL.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
import uuid
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path

from scripts.test_delta_no_fill import (
    FIXTURE_FIELDS,
    TERMINAL,
    TESTNET_REST,
    TESTNET_WS,
    ProbeError,
    body,
    dec,
    position_size,
    require,
    rows,
    safe_error,
)
from scripts.test_delta_no_fill import (
    Probe as NoFillProbe,
)

HARD_NOTIONAL_CAP = Decimal("100")
REPORT_FIELDS = (
    "Preflight checks",
    "Entry order acknowledged",
    "Entry fill confirmed",
    "REST / WebSocket match",
    "User trade parsed",
    "Fees reconciled",
    "Protective stop confirmed",
    "Reduce-only close",
    "Account flat",
    "Remaining orders: 0",
)
EVIDENCE_FIELDS = FIXTURE_FIELDS | {
    "fill_id",
    "price",
    "role",
    "commission",
    "paid_commission",
    "notional",
    "entry_price",
    "contract_type",
    "contract_unit_currency",
    "notional_type",
    "state",
    "trading_status",
    "min_size",
    "minimum_order_size",
    "lot_size",
}


def validate_environment(mode, exec_mode, ws_url=""):
    require(mode == "testnet", "production_or_unknown_mode_rejected")
    require(exec_mode == "paper", "paper_exec_mode_required")
    require(not ws_url or ws_url == TESTNET_WS, "custom_socket_rejected")


def fill_plan(product, ticker, book, client_id, cap=HARD_NOTIONAL_CAP):
    require(0 < cap <= HARD_NOTIONAL_CAP, "notional_cap_out_of_bounds")
    require(product.get("symbol") == "BTCUSD", "btc_product_required")
    require(
        product.get("state") == "live" and product.get("trading_status") == "operational",
        "product_not_operational",
    )
    require(product.get("contract_type") == "perpetual_futures", "perpetual_required")
    require(product.get("notional_type") == "vanilla", "vanilla_contract_required")
    require(product.get("contract_unit_currency") == "BTC", "btc_contract_unit_required")
    for field in ("min_size", "minimum_order_size", "lot_size"):
        require(dec(product.get(field) or 1) == 1, "minimum_size_not_one")
    tick, contract = dec(product["tick_size"]), dec(product["contract_value"])
    require(tick > 0 and 0 < contract <= Decimal("0.001"), "invalid_contract_constraints")
    require(len(client_id) <= 32 and client_id.startswith(("gf", "sig_")), "invalid_client_id")
    bids, asks = book.get("buy") or [], book.get("sell") or []
    require(bids and asks, "empty_book")
    bid = max(dec(x["price"]) for x in bids)
    ask = min(dec(x["price"]) for x in asks)
    mark = dec(ticker["mark_price"])
    require(0 < bid < ask and mark > 0, "invalid_market")
    require(ask / bid < Decimal("1.002"), "spread_too_wide")
    require(abs(ask / mark - 1) < Decimal("0.005"), "book_mark_disagreement")
    timestamp = dec(ticker["timestamp"]) / Decimal("1000000")
    require(-2 <= Decimal(str(time.time())) - timestamp <= 10, "stale_ticker")

    def rounded(value, rounding):
        return (value / tick).to_integral_value(rounding=rounding) * tick

    limit = rounded(ask * Decimal("1.001"), ROUND_UP)
    stop = rounded(min(mark, bid) * Decimal("0.98"), ROUND_DOWN)
    take = rounded(max(mark, ask) * Decimal("1.02"), ROUND_UP)
    band = ticker.get("price_band") or {}
    require(
        0
        < dec(band.get("lower_limit", 0))
        <= stop
        < bid
        < ask
        <= limit
        < take
        <= dec(band.get("upper_limit", 0)),
        "unsafe_bracket_prices",
    )
    require(limit * contract <= cap, "minimum_contract_exceeds_notional_cap")
    return {
        "product_id": int(product["id"]),
        "size": 1,
        "side": "buy",
        "order_type": "limit_order",
        "time_in_force": "ioc",
        "limit_price": str(limit),
        "post_only": False,
        "reduce_only": False,
        "client_order_id": client_id,
        "bracket_stop_trigger_method": "last_traded_price",
        # Omit bracket limit prices so the protective stop is a market stop.
        "bracket_stop_loss_price": str(stop),
        "bracket_take_profit_price": str(take),
    }


def is_protection(order, plan, kind="stop_loss_order"):
    price_field = (
        "bracket_stop_loss_price" if kind == "stop_loss_order" else "bracket_take_profit_price"
    )
    return (
        isinstance(order, dict)
        and order.get("id") is not None
        and int(order.get("product_id", -1)) == plan["product_id"]
        and order.get("state") in {"open", "pending"}
        and order.get("stop_order_type") == kind
        and order.get("reduce_only") is True
        and order.get("side") == "sell"
        and dec(order.get("size", 0)) == 1
        and dec(order.get("unfilled_size", 0)) == 1
        and dec(order.get("stop_price", 0)) == dec(plan[price_field])
        and (kind != "stop_loss_order" or order.get("order_type") == "market_order")
        and order.get("stop_trigger_method")
        == plan.get("bracket_stop_trigger_method", "last_traded_price")
    )


def verify_identity(order, plan, client_id, order_id=None):
    require(isinstance(order, dict), "order_not_object")
    require(order.get("client_order_id") == client_id, "client_id_mismatch")
    require(order.get("id") is not None, "order_id_missing")
    if order_id is not None:
        require(str(order["id"]) == str(order_id), "order_id_mismatch")
    require(int(order.get("product_id", -1)) == plan["product_id"], "order_product_mismatch")
    require(dec(order.get("size", 0)) == 1, "order_size_mismatch")


def trade_map(items):
    result = {}
    for item in items:
        fill_id = str(item.get("fill_id") or item.get("id") or "")
        require(fill_id, "fill_id_missing")
        if fill_id in result:
            require(result[fill_id] == item, "conflicting_duplicate_fill")
        result[fill_id] = item
    return result


def compare_fills(rest, trades, order, ws_orders, plan, client_id, side):
    """Match actual exchange fill IDs; absent fees never silently become zero."""
    verify_identity(order, plan, client_id)
    oid = str(order["id"])
    rest = trade_map([r for r in rest if str(r.get("order_id")) == oid])
    trades = trade_map([r for r in trades if str(r.get("order_id")) == oid])
    require(rest and rest.keys() == trades.keys(), "rest_ws_fill_ids_mismatch")
    require(
        order.get("state") in {"closed", "filled"} and dec(order.get("unfilled_size", 1)) == 0,
        "order_not_filled",
    )
    qty = Decimal(0)
    fees = Decimal(0)
    weighted = Decimal(0)
    for fid, fill in rest.items():
        trade = trades[fid]
        require(trade.get("client_order_id") == client_id, "trade_client_id_mismatch")
        if fill.get("client_order_id") is not None:
            require(fill["client_order_id"] == client_id, "rest_fill_client_id_mismatch")
        for field in ("price", "size"):
            require(
                field in fill and field in trade and dec(fill[field]) == dec(trade[field]),
                "fill_" + field + "_mismatch",
            )
        require(fill.get("side") == trade.get("side") == side, "fill_side_mismatch")
        require(fill.get("role") == trade.get("role"), "fill_role_mismatch")
        require("commission" in fill and fill["commission"] is not None, "rest_fee_missing")
        require("commission" in trade and trade["commission"] is not None, "ws_fee_missing")
        require(dec(fill["commission"]) == dec(trade["commission"]), "fill_fee_mismatch")
        qty += dec(fill["size"])
        weighted += dec(fill["size"]) * dec(fill["price"])
        fees += dec(fill["commission"])
    require(qty == 1, "filled_quantity_not_one")
    require(
        "paid_commission" in order and order["paid_commission"] is not None, "order_fee_missing"
    )
    require(
        abs(dec(order["paid_commission"]) - fees) <= Decimal("0.00000001"), "order_fee_mismatch"
    )
    require(
        abs(dec(order.get("average_fill_price")) - weighted / qty) <= Decimal("0.00000001"),
        "average_fill_price_mismatch",
    )
    filled_updates = [
        o
        for o in ws_orders
        if str(o.get("id")) == oid
        and o.get("state") in {"closed", "filled"}
        and dec(o.get("unfilled_size", 1)) == 0
    ]
    require(filled_updates, "filled_order_ws_update_missing")
    for update in filled_updates:
        verify_identity(update, plan, client_id, order["id"])
        require(
            dec(update["average_fill_price"]) == dec(order["average_fill_price"]),
            "ws_order_price_mismatch",
        )
    return list(rest.values()), list(trades.values())


class Evidence:
    """Keep execution metadata; remove account fields and normalize identifiers."""

    def __init__(self):
        self.ids = {}
        self.clients = {}
        self.events = []

    def clean(self, value, key=""):
        if isinstance(value, dict):
            return {k: self.clean(v, k) for k, v in value.items() if k in EVIDENCE_FIELDS}
        if isinstance(value, list):
            return [self.clean(v, key) for v in value]
        if key in {"id", "order_id", "fill_id"} and value is not None:
            return self.ids.setdefault(str(value), 710000001 + len(self.ids))
        if key == "client_order_id" and value is not None:
            return self.clients.setdefault(str(value), "fixture_fill_" + str(len(self.clients) + 1))
        return value

    def record(self, name, payload):
        self.events.append({"name": name, "payload": self.clean(payload)})


class FillProbe(NoFillProbe):
    def __init__(self, client, output, cap=HARD_NOTIONAL_CAP):
        super().__init__(client, output)
        self.client_id = "gf" + uuid.uuid4().hex[:24]
        self.close_id = self.client_id + "c"
        self.results = dict.fromkeys(REPORT_FIELDS, "FAIL (not reached)")
        self.evidence = Evidence()
        self.order_events = []
        self.trade_events = []
        self.position_events = []
        self.halted = False
        self.close_attempted = False
        self.protected_ids = set()
        self.cap = cap
        self.stage_error = None
        self.cleanup_error = None

    def record(self, name, payload):
        self.evidence.record(name, payload)

    async def listen(self, ws):
        while True:
            event = await self.receive(ws)
            kind = event.get("type")
            candidates = event.get("result") if isinstance(event.get("result"), list) else [event]
            for row in candidates:
                if kind in {"orders", "user_trades", "positions"}:
                    if int(row.get("product_id", -1)) != self.plan["product_id"]:
                        continue
                    self.record("ws_" + kind, row)
                    target = (
                        self.order_events
                        if kind == "orders"
                        else self.trade_events
                        if kind == "user_trades"
                        else self.position_events
                    )
                    target.append(row)

    async def lookup(self, client_id, name):
        try:
            response = await self.client.get_order_by_client_id(client_id)
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return None
            raise
        order = body(response)
        verify_identity(order, self.plan, client_id)
        self.record(name, response)
        return order

    async def submit_entry(self):
        require(not self.attempted and not self.halted, "entry_already_attempted_or_halted")
        self.attempted = True
        self.record("entry_request", self.plan)
        try:
            response = await self.client.place_order(self.plan)
            self.record("entry_ack", response)
            order = body(response)
            verify_identity(order, self.plan, self.client_id)
        except Exception as exc:
            self.record("entry_submission_error", {"status": safe_error(exc)})
            # Never submit again, including after HTTP rejection or timeout.
            order = await self.lookup(self.client_id, "entry_lookup_after_submission_error")
            if order is None:
                raise ProbeError("entry_submission_unresolved_no_resubmit") from None
        self.order_id = order["id"]
        self.results["Entry order acknowledged"] = "PASS"
        return order

    async def check_protection(self):
        response = await self.client.get_open_orders()
        open_orders = rows(response)
        # The preflight account is empty, so these are test-product events only.
        own_product = [
            o for o in open_orders if int(o.get("product_id", -1)) == self.plan["product_id"]
        ]
        self.record(
            "orders_after_entry", {"success": response.get("success"), "result": own_product}
        )
        candidates = [o for o in own_product if is_protection(o, self.plan)]
        if not candidates:
            self.halted = True
            raise ProbeError("stop_unconfirmed_flatten_immediately")
        for stop in candidates:
            detail = await self.client.get_order_status(str(stop["id"]))
            self.record("protective_stop_lookup", detail)
            require(is_protection(body(detail), self.plan), "stop_detail_not_protective")
            self.protected_ids.add(str(stop["id"]))
        for order in own_product:
            if is_protection(order, self.plan, "take_profit_order"):
                self.protected_ids.add(str(order["id"]))
        self.results["Protective stop confirmed"] = "PASS"

    async def fill_evidence(self, client_id, side, listener, prefix, timeout=6):
        deadline = asyncio.get_running_loop().time() + timeout
        last_error = None
        while asyncio.get_running_loop().time() < deadline:
            if listener.done():
                await listener
                raise ProbeError("private_stream_stopped")
            if prefix == "entry":
                await self.check_protection()
            order = await self.lookup(client_id, prefix + "_lookup")
            if order is not None:
                response = await self.client.get_fills(
                    product_id=self.plan["product_id"], page_size=100
                )
                # Do not capture historical account fills: retain this exact order only.
                fills = [r for r in rows(response) if str(r.get("order_id")) == str(order["id"])]
                self.record(
                    prefix + "_rest_fills", {"success": response.get("success"), "result": fills}
                )
                try:
                    result = compare_fills(
                        fills,
                        self.trade_events,
                        order,
                        self.order_events,
                        self.plan,
                        client_id,
                        side,
                    )
                    return order, result
                except ProbeError as exc:
                    last_error = exc
            await asyncio.sleep(0.2)
        raise last_error or ProbeError("fill_evidence_timeout")

    async def stage(self, listener):
        await self.submit_entry()
        # Check realtime position first. Stop protection is verified BEFORE
        # waiting for fill history or asynchronous reporting to catch up.
        for _ in range(5):
            position = await self.client.get_open_positions(product_id=self.plan["product_id"])
            size = position_size(position)
            if size:
                self.record("entry_realtime_position", position)
                require(size == 1, "entry_position_size_mismatch")
                self.results["Entry fill confirmed"] = "PASS"
                await self.check_protection()
                break
            await asyncio.sleep(0.1)
        else:
            raise ProbeError("minimum_entry_did_not_fill")
        order, (_, trades) = await self.fill_evidence(self.client_id, "buy", listener, "entry")
        require(order.get("side") == "buy", "entry_side_mismatch")
        require(
            dec(order["average_fill_price"]) <= dec(self.plan["limit_price"]),
            "entry_exceeded_limit",
        )
        require(
            any(dec(p.get("size", 0)) == 1 for p in self.position_events),
            "entry_position_ws_missing",
        )
        self.results["REST / WebSocket match"] = "PASS"
        self.results["Fees reconciled"] = "PASS"
        from signal_bot.execution.delta_adapter import parse_fill

        for trade in trades:
            parsed = parse_fill(trade)
            require(
                parsed.client_order_id == self.client_id
                and parsed.qty > 0
                and parsed.fee == dec(trade["commission"]),
                "user_trade_parser_mismatch",
            )
        self.results["User trade parsed"] = "PASS"

    async def cleanup(self):
        if not self.attempted:
            return
        self.halted = True
        last_error = None
        terminal_entry = False
        clean_reads = 0
        for _ in range(20):
            try:
                # Even if order lookup is delayed, an observed position requires
                # reduction NOW. The IOC entry cannot rest beyond matching.
                realtime = await self.client.get_open_positions(product_id=self.plan["product_id"])
                size = position_size(realtime)
                if size:
                    require(
                        abs(size) <= 1 and size == size.to_integral_value(),
                        "unexpected_exposure_size",
                    )
                    if not self.close_attempted:
                        self.close_attempted = True  # set before network I/O
                        close = {
                            "product_id": self.plan["product_id"],
                            "size": int(abs(size)),
                            "side": "sell" if size > 0 else "buy",
                            "order_type": "market_order",
                            "reduce_only": True,
                            "client_order_id": self.close_id,
                        }
                        self.record("close_request", close)
                        try:
                            response = await self.client.place_order(close)
                            self.record("close_ack", response)
                            verify_identity(body(response), self.plan, self.close_id)
                        except Exception as exc:
                            self.record("close_submission_error", {"status": safe_error(exc)})
                    # Query its client ID before deciding anything about an
                    # ambiguous close. Never blindly submit another close.
                    close_order = await self.lookup(self.close_id, "cleanup_close_lookup")
                    if close_order:
                        require(close_order.get("reduce_only") is True, "close_not_reduce_only")
                    await asyncio.sleep(0.2)
                    continue

                # Exposure reduction takes priority over entry-order reporting.
                try:
                    entry = await self.lookup(self.client_id, "cleanup_entry_lookup")
                except Exception as exc:
                    self.record("cleanup_lookup_error", {"status": safe_error(exc)})
                    entry = None
                if entry:
                    self.order_id = entry["id"]
                    if entry.get("state") not in TERMINAL:
                        try:
                            response = await self.client.cancel_order(
                                str(entry["id"]), product_id=self.plan["product_id"]
                            )
                            self.record("cancel_entry", response)
                            body(response)
                            entry = await self.lookup(self.client_id, "cleanup_entry_final")
                        except Exception as exc:
                            self.record("cleanup_cancel_error", {"status": safe_error(exc)})
                    terminal_entry = entry is not None and entry.get("state") in TERMINAL
                require(
                    position_size(
                        await self.client.get_open_positions(product_id=self.plan["product_id"])
                    )
                    == 0,
                    "position_changed_before_order_cleanup",
                )
                resting_response = await self.client.get_open_orders()
                resting = rows(resting_response)
                for order in resting:
                    ours = (
                        str(order.get("id")) in self.protected_ids
                        or is_protection(order, self.plan)
                        or is_protection(order, self.plan, "take_profit_order")
                    )
                    if ours:
                        response = await self.client.cancel_order(
                            str(order["id"]), product_id=self.plan["product_id"]
                        )
                        self.record("cancel_remaining_protection", response)
                        body(response)
                resting_response = await self.client.get_open_orders()
                require(not rows(resting_response), "remaining_unexpected_orders")
                margined = await self.client._request("GET", "/v2/positions/margined")
                require(
                    all("size" in p and dec(p["size"]) == 0 for p in rows(margined)),
                    "margined_positions_not_flat_yet",
                )
                realtime = await self.client.get_open_positions(product_id=self.plan["product_id"])
                require(position_size(realtime) == 0, "realtime_position_not_flat")
                clean_reads += 1
                # Multiple flat checks defend against an ambiguous IOC entry
                # being accepted after a timed-out response and absent lookup.
                if terminal_entry or clean_reads >= 10:
                    self.record("final_open_orders", resting_response)
                    self.record("final_margined_positions", margined)
                    self.record("final_realtime_position", realtime)
                    self.results["Account flat"] = "PASS"
                    self.results["Remaining orders: 0"] = "PASS"
                    if self.close_attempted:
                        closed = await self.lookup(self.close_id, "close_final_lookup")
                        require(
                            closed is not None and closed.get("reduce_only") is True,
                            "close_reconciliation_missing",
                        )
                        require(
                            closed.get("state") in {"closed", "filled"}
                            and dec(closed.get("unfilled_size", 1)) == 0,
                            "close_not_filled",
                        )
                        self.results["Reduce-only close"] = "PASS"
                    return
            except Exception as exc:
                last_error = safe_error(exc)
            await asyncio.sleep(0.5)
        raise ProbeError("cleanup_unresolved_" + (last_error or "entry_or_close_pending"))

    async def run(self):
        from signal_bot.delta.client import DeltaWebSocket

        ws = None
        listener = None
        try:
            await self.client.initialize()
            product = self.client.symbol_map._products["BTCUSD"]
            self.record("public_contract", {k: v for k, v in product.items() if k != "id"})
            ticker = body(await self.client._request("GET", "/v2/tickers/BTCUSD"))
            book = body(await self.client.get_l2_orderbook("BTCUSD"))
            self.plan = fill_plan(product, ticker, book, self.client_id, self.cap)
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
                    "User-Agent": "Mozilla/5.0 (signalbot-guarded-fill)",
                },
                heartbeat=20,
            )
            await asyncio.wait_for(self.bootstrap(ws, socket), timeout=15)
            ticker = body(await self.client._request("GET", "/v2/tickers/BTCUSD"))
            book = body(await self.client.get_l2_orderbook("BTCUSD"))
            self.plan = fill_plan(product, ticker, book, self.client_id, self.cap)
            await self.account_empty()
            self.results["Preflight checks"] = "PASS"
            print(
                "Preflight: PASS (authenticated testnet; empty account; one contract; <= $100 demo notional)",
                flush=True,
            )
            listener = asyncio.create_task(self.listen(ws))
            try:
                await asyncio.wait_for(self.stage(listener), timeout=15)
            except Exception as exc:
                self.stage_error = safe_error(exc)
                self.halted = True
                print("Guarded recovery: " + self.stage_error, flush=True)
            finally:
                try:
                    await self.cleanup()
                except Exception as exc:
                    self.cleanup_error = safe_error(exc)
                    print("CLEANUP INCOMPLETE: " + self.cleanup_error, flush=True)
            # Only inspect close fill evidence AFTER confirming the account flat.
            if (
                self.results["Reduce-only close"] == "PASS"
                and self.stage_error is None
                and self.cleanup_error is None
            ):
                try:
                    await self.fill_evidence(self.close_id, "sell", listener, "close")
                    require(
                        any(dec(p.get("size", 1)) == 0 for p in self.position_events),
                        "close_position_ws_missing",
                    )
                except Exception as exc:
                    self.stage_error = safe_error(exc)
                    self.results["REST / WebSocket match"] = "FAIL (close evidence incomplete)"
                    self.results["Fees reconciled"] = "FAIL (close evidence incomplete)"
                    print("Evidence error: " + self.stage_error, flush=True)
        except Exception as exc:
            self.stage_error = safe_error(exc)
            print("Probe error: " + self.stage_error, flush=True)
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
            passed = (
                all(v == "PASS" for v in self.results.values())
                and self.stage_error is None
                and self.cleanup_error is None
            )
            report = (
                "DELTA TESTNET — GUARDED FILL TEST\n\n"
                + "\n".join(f"{k:30} {v}" for k, v in self.results.items())
                + "\n\nRESULT: "
                + ("PASS" if passed else "FAIL")
                + "\n"
            )
            fixture = {
                "environment": "testnet",
                "test": "fill_then_controlled_close",
                "results": self.results,
                "result": "PASS" if passed else "FAIL",
                "entry_halted": self.halted,
                "stage_error": self.stage_error,
                "cleanup_error": self.cleanup_error,
                "exchanges": self.evidence.events,
            }
            self.fixture_path.parent.mkdir(parents=True, exist_ok=True)
            # Preserve every attempt, including failures, rather than overwrite
            # useful evidence with the next run's results.
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "_" + uuid.uuid4().hex[:6]
            archive = self.fixture_path.with_name(self.fixture_path.stem + "_" + stamp + ".json")
            archive.write_text(json.dumps(fixture, indent=2) + "\n", encoding="utf-8")
            self.fixture_path.write_text(json.dumps(fixture, indent=2) + "\n", encoding="utf-8")
            self.fixture_path.with_suffix(".txt").write_text(report, encoding="utf-8")
            print(report, flush=True)
        return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm-testnet-fill",
        action="store_true",
        help="Authorize one real demo fill with embedded SL/TP and guarded reduce-only closure",
    )
    parser.add_argument(
        "--max-notional-usd", default="100", help="May lower, never raise, the hard $100 demo cap"
    )
    args = parser.parse_args()
    if not args.confirm_testnet_fill:
        parser.error("--confirm-testnet-fill is required; no network requests made")
    cap = dec(args.max_notional_usd)
    require(0 < cap <= HARD_NOTIONAL_CAP, "notional_cap_out_of_bounds")
    logging.disable(logging.CRITICAL)
    from signal_bot.config import settings

    validate_environment(settings.delta_mode, settings.exec_mode, settings.delta_ws_url)
    require(
        settings.delta_api_key.strip() and settings.delta_api_secret.strip(), "credentials_missing"
    )

    output = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "delta_testnet_fill.json"
    return asyncio.run(FillProbe(make_guarded_client(cap), output, cap).run())


def make_guarded_client(cap=HARD_NOTIONAL_CAP):
    from signal_bot.delta.client import DeltaClient

    class GuardedClient(DeltaClient):
        entry_sent = False
        close_sent = False

        async def _request(self, method, path, params=None, data=None):
            require(
                self.mode == "testnet" and self.rest_url == TESTNET_REST,
                "testnet_endpoint_required",
            )
            if method != "GET":
                require(
                    path == "/v2/orders" and method in {"POST", "DELETE"}, "mutation_not_allowed"
                )
                require(
                    data
                    and int(data.get("product_id", -1))
                    == self.symbol_map._products["BTCUSD"]["id"],
                    "wrong_mutation_product",
                )
                if method == "POST":
                    require(data.get("size") == 1, "one_contract_only")
                    if data.get("reduce_only") is True:
                        require(
                            not self.close_sent and data.get("order_type") == "market_order",
                            "close_already_attempted_or_unsafe",
                        )
                        self.close_sent = True
                    else:
                        require(not self.entry_sent, "entry_resubmission_forbidden")
                        require(
                            data.get("order_type") == "limit_order"
                            and data.get("time_in_force") == "ioc",
                            "capped_ioc_entry_required",
                        )
                        require(
                            dec(data["limit_price"])
                            * dec(self.symbol_map._products["BTCUSD"]["contract_value"])
                            <= cap,
                            "entry_exceeds_cap",
                        )
                        require(
                            dec(data.get("bracket_stop_loss_price", 0)) > 0
                            and dec(data.get("bracket_take_profit_price", 0)) > 0,
                            "embedded_protection_required",
                        )
                        self.entry_sent = True
                else:
                    require(data.get("id") is not None, "exact_cancel_id_required")
            return await super()._request(method, path, params=params, data=data)

    return GuardedClient(mode="testnet", request_timeout=3)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("Probe setup error: " + safe_error(exc), flush=True)
        raise SystemExit(1) from None
