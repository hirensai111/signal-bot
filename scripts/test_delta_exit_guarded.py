"""Isolated TESTNET stop-loss, take-profit or real bot-process restart check.

Run ONE scenario at a time, with --confirm-testnet-scenario. Requires an idle demo
account. One contract and the hard USD 100 cap from the fill probe are retained.
Natural price movement is observed for a bounded interval; no trigger means
INCONCLUSIVE, never PASS. The parent guardian retains stream/cleanup control when
the read-only bot worker process stops or restarts. Production mode is rejected.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
import uuid
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path

from scripts.delta_validation_worker import seed_database
from scripts.test_delta_fill_guarded import (
    FillProbe,
    fill_plan,
    make_guarded_client,
    validate_environment,
)
from scripts.test_delta_no_fill import (
    TESTNET_WS,
    ProbeError,
    body,
    dec,
    position_size,
    require,
    rows,
    safe_error,
)

LABELS = {
    "stop-loss": (
        "Stop-loss triggered",
        "Stop-loss fill parsed",
        "stop_loss_order",
        "sl_hit",
        "stop_loss",
    ),
    "take-profit": (
        "Take-profit triggered",
        "Take-profit fill parsed",
        "take_profit_order",
        "tp_hit",
        "take_profit",
    ),
}


def near_trigger(plan, ticker, book, tick, scenario, bps=Decimal("1")):
    result = dict(plan)
    if scenario == "restart":
        return result
    require(Decimal("0.1") <= bps <= Decimal("10"), "trigger_distance_out_of_bounds")
    result["bracket_stop_trigger_method"] = "mark_price"
    bid = max(dec(x["price"]) for x in book["buy"])
    ask = min(dec(x["price"]) for x in book["sell"])
    mark = dec(ticker["mark_price"])
    if scenario == "stop-loss":
        price = min(bid, mark) * (1 - bps / 10000)
        price = (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick
        require(0 < price < bid, "invalid_near_stop")
        result["bracket_stop_loss_price"] = str(price)
    else:
        require(scenario == "take-profit", "invalid_scenario")
        price = max(ask, mark) * (1 + bps / 10000)
        price = (price / tick).to_integral_value(rounding=ROUND_UP) * tick
        require(price > ask, "invalid_near_take_profit")
        result["bracket_take_profit_price"] = str(price)
    return result


def compare_exit(fills, trades, order, events, plan, kind):
    oid = str(order["id"])
    rest = {str(f["id"]): f for f in fills if str(f.get("order_id")) == oid}
    stream = {str(f["fill_id"]): f for f in trades if str(f.get("order_id")) == oid}
    require(rest and rest.keys() == stream.keys(), "exit_rest_ws_fill_ids_mismatch")
    require(
        order.get("stop_order_type") == kind and order.get("reduce_only") is True, "wrong_exit_type"
    )
    require(
        order.get("state") == "closed" and dec(order.get("unfilled_size", 1)) == 0,
        "trigger_not_fully_filled",
    )
    require(
        int(order.get("product_id", -1)) == plan["product_id"] and dec(order.get("size", 0)) == 1,
        "exit_product_or_size_mismatch",
    )
    require(
        any(str(e.get("id")) == oid and e.get("state") == "pending" for e in events),
        "pending_exit_transition_missing",
    )
    require(
        any(
            str(e.get("id")) == oid
            and e.get("state") == "closed"
            and dec(e.get("unfilled_size", 1)) == 0
            for e in events
        ),
        "closed_exit_transition_missing",
    )
    require(sum((dec(f["size"]) for f in rest.values()), Decimal(0)) == 1, "exit_quantity_mismatch")
    for fid, fill in rest.items():
        trade = stream[fid]
        require(fill.get("side") == trade.get("side") == "sell", "exit_side_mismatch")
        for field in ("size", "price", "commission"):
            require(
                field in fill and field in trade and dec(fill[field]) == dec(trade[field]),
                "exit_" + field + "_mismatch",
            )
    return list(rest.values())


class ExitProbe(FillProbe):
    def __init__(self, client, output, scenario, observe=45, bps=Decimal("1")):
        super().__init__(client, output)
        self.signal_id = uuid.uuid4()
        self.client_id = "sig_" + self.signal_id.hex[:28]
        self.close_id = self.client_id[:-2] + "rc"
        self.scenario, self.observe, self.bps = scenario, observe, bps
        self.results = {
            "Preflight checks": "FAIL (not reached)",
            "Sibling order cleanup": "FAIL (not reached)",
            "Outcomes recorded once": "FAIL (not reached)",
            "No duplicate submissions": "FAIL (not reached)",
            "Final account flat": "FAIL (not reached)",
            "Residual orders: 0": "FAIL (not reached)",
        }
        if scenario in LABELS:
            self.results.update(
                {
                    LABELS[scenario][0]: "INCONCLUSIVE (not observed)",
                    LABELS[scenario][1]: "INCONCLUSIVE (not observed)",
                }
            )
        else:
            self.results.update(
                {
                    "Restart reconciliation": "FAIL (not reached)",
                    "Protection preserved": "FAIL (not reached)",
                }
            )
        self.work_dir = (
            Path(__file__).resolve().parents[1]
            / "data"
            / "delta_exit_validation"
            / self.signal_id.hex
        )
        self.db_path, self.status_path = self.work_dir / "worker.db", self.work_dir / "status.json"
        self.process = None
        self.bot_status = {}
        self.siblings = set()
        self.automatic_exit = False
        self.inconclusive = False

    async def worker_start(self):
        if self.status_path.exists():
            self.status_path.unlink()
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "scripts.delta_validation_worker",
            "--database",
            str(self.db_path),
            "--status",
            str(self.status_path),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        return await self.worker_wait(lambda s: s.get("ready") is True, timeout=30)

    async def fresh_market(self):
        product = self.client.symbol_map._products["BTCUSD"]
        for attempt in range(3):
            # The testnet CDN occasionally returns an old cached ticker. Keep
            # the freshness threshold; use a unique public query cache key.
            nonce = {"_verification_nonce": str(time.time_ns())}
            ticker = body(await self.client._request("GET", "/v2/tickers/BTCUSD", params=nonce))
            book = body(await self.client._request("GET", "/v2/l2orderbook/BTCUSD", params=nonce))
            try:
                plan = fill_plan(product, ticker, book, self.client_id)
                return ticker, book, plan
            except ProbeError as exc:
                if str(exc) != "stale_ticker" or attempt == 2:
                    raise
                await asyncio.sleep(0.2)

    async def worker_wait(self, predicate, timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = json.loads(self.status_path.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError, PermissionError):
                value = {}
            require("worker_error" not in value, "bot_worker_error")
            if predicate(value):
                self.bot_status = value
                return value
            require(
                self.process is not None and self.process.returncode is None, "bot_worker_stopped"
            )
            await asyncio.sleep(0.2)
        raise ProbeError("bot_worker_verification_timeout")

    async def worker_command(self, command):
        require(
            self.process is not None and self.process.returncode is None, "bot_worker_not_running"
        )
        self.process.stdin.write((command + "\n").encode())
        await self.process.stdin.drain()

    async def worker_stop(self):
        if self.process is not None and self.process.returncode is None:
            try:
                await self.worker_command("stop")
            except (BrokenPipeError, ConnectionResetError):
                pass
            try:
                await asyncio.wait_for(self.process.wait(), timeout=8)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
        self.process = None

    def leg_metadata(self, row, kind):
        field = (
            "bracket_stop_loss_price" if kind == "stop_loss_order" else "bracket_take_profit_price"
        )
        return (
            row.get("stop_order_type") == kind
            and int(row.get("product_id", -1)) == self.plan["product_id"]
            and row.get("reduce_only") is True
            and row.get("side") == "sell"
            and dec(row.get("stop_price", 0)) == dec(self.plan[field])
            and dec(row.get("size", 0)) == 1
        )

    async def actual_legs(self):
        response = await self.client.get_open_orders()
        current = rows(response)
        self.record("scenario_live_orders", response)
        found = {}
        for kind in ("stop_loss_order", "take_profit_order"):
            for order in [*self.order_events, *current]:
                if self.leg_metadata(order, kind):
                    found[kind] = order["id"]
                    self.protected_ids.add(str(order["id"]))
        return found

    async def stage(self, listener):
        await seed_database(self.db_path, self.plan, self.signal_id, self.contract_value)
        await self.worker_start()
        if self.scenario == "restart":
            # Actual worker process exits before entry. Its persisted submitting
            # intent must recover a fill that occurs while that process is down.
            await self.worker_stop()
        # Worker startup may take seconds. Refresh executable prices and the
        # persisted intent immediately before the only entry submission.
        ticker, book, fresh_plan = await self.fresh_market()
        product = self.client.symbol_map._products["BTCUSD"]
        self.plan = near_trigger(
            fresh_plan,
            ticker,
            book,
            dec(product["tick_size"]),
            self.scenario,
            self.bps,
        )
        from sqlalchemy import select
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from signal_bot.db.models import Order, Signal

        db = create_async_engine("sqlite+aiosqlite:///" + self.db_path.as_posix())
        try:
            async with async_sessionmaker(db, expire_on_commit=False)() as session:
                local = await session.scalar(
                    select(Order).where(Order.client_order_id == self.client_id)
                )
                signal = await session.get(Signal, self.signal_id)
                local.stop_loss_price = signal.stop_loss = dec(self.plan["bracket_stop_loss_price"])
                local.take_profit_price = signal.take_profit = dec(
                    self.plan["bracket_take_profit_price"]
                )
                local.limit_price = local.intended_entry = signal.entry = dec(
                    self.plan["limit_price"]
                )
                await session.commit()
        finally:
            await db.dispose()
        await self.submit_entry()
        entry = await self.lookup(self.client_id, "scenario_entry_lookup")
        require(entry is not None and dec(entry.get("unfilled_size", 1)) == 0, "entry_not_filled")
        self.results["Entry fill confirmed"] = "PASS"
        legs = await self.actual_legs()
        position = position_size(
            await self.client.get_open_positions(product_id=self.plan["product_id"])
        )
        if position:
            require(position == 1, "unexpected_position_size")
            try:
                await self.check_protection()
            except ProbeError:
                # A genuine automatic exit may complete between the position
                # read and stop lookup. Flat is safe; verify the actual leg next.
                require(
                    position_size(
                        await self.client.get_open_positions(product_id=self.plan["product_id"])
                    )
                    == 0,
                    "stop_unconfirmed_flatten_immediately",
                )
        if self.scenario == "restart":
            require(
                position == 1 and len(legs) == 2, "restart_requires_existing_protected_position"
            )
            before = set(map(str, legs.values()))
            status = await self.worker_start()
            require(
                status.get("reconciled_before_signals") is True
                and status.get("startup_state") == "filled",
                "startup_did_not_recover_position_first",
            )
            require(dec(status.get("startup_entry_qty", 0)) == 1, "restart_missed_entry_fill")
            require(status.get("startup_protection") == "confirmed", "restart_stop_unconfirmed")
            after = await self.actual_legs()
            require(set(map(str, after.values())) == before, "restart_changed_exchange_protection")
            require(
                position_size(
                    await self.client.get_open_positions(product_id=self.plan["product_id"])
                )
                == 1,
                "restart_position_changed",
            )
            await self.worker_command("duplicate_signal")
            await self.worker_wait(lambda s: s.get("duplicate_signal_rejected") is True)
            self.results["Restart reconciliation"] = "PASS"
            self.results["Protection preserved"] = "PASS"
            self.results["No duplicate submissions"] = "PASS"
            return

        trigger_label, parsed_label, kind, _, _ = LABELS[self.scenario]
        require(kind in legs, "intended_exit_order_not_found")
        target_id = legs[kind]
        self.siblings = {str(v) for k, v in legs.items() if k != kind}
        deadline = time.monotonic() + self.observe
        trigger_seen_at = None
        while time.monotonic() < deadline:
            if listener.done():
                await listener
                raise ProbeError("guardian_stream_stopped")
            detail = await self.client.get_order_status(str(target_id))
            self.record("target_order_observed", detail)
            order = body(detail)
            size = position_size(
                await self.client.get_open_positions(product_id=self.plan["product_id"])
            )
            triggered = order.get("state") == "closed" or any(
                str(e.get("id")) == str(target_id) and e.get("reason") == "stop_trigger"
                for e in self.order_events
            )
            if triggered:
                trigger_seen_at = trigger_seen_at or time.monotonic()
                if size and time.monotonic() - trigger_seen_at >= 2:
                    raise ProbeError("trigger_failed_to_flatten")
            if (
                size == 0
                and order.get("state") == "closed"
                and dec(order.get("unfilled_size", 1)) == 0
            ):
                self.results[trigger_label] = "PASS"
                self.automatic_exit = True
                # Bounded history propagation after the exchange is flat.
                for _ in range(15):
                    response = await self.client.get_fills(
                        product_id=self.plan["product_id"], page_size=100
                    )
                    fills = [f for f in rows(response) if str(f.get("order_id")) == str(target_id)]
                    self.record(
                        "trigger_rest_fills", {"success": response.get("success"), "result": fills}
                    )
                    try:
                        compare_exit(
                            fills, self.trade_events, order, self.order_events, self.plan, kind
                        )
                        self.results[parsed_label] = "PASS"
                        return
                    except ProbeError:
                        await asyncio.sleep(0.2)
                raise ProbeError("trigger_fill_evidence_mismatch")
            if order.get("state") in {"cancelled", "canceled"}:
                self.inconclusive = True
                self.results[trigger_label] = "INCONCLUSIVE (intended leg did not execute)"
                return
            if size == 0:
                # Position and order reads are not an atomic snapshot. The leg
                # can fill between those reads; await its final state instead
                # of declaring the genuine execution inconclusive.
                await asyncio.sleep(0.1)
                continue
            require(size == 1, "unexpected_observed_exposure")
            try:
                await self.check_protection()
            except ProbeError:
                require(
                    position_size(
                        await self.client.get_open_positions(product_id=self.plan["product_id"])
                    )
                    == 0,
                    "stop_unconfirmed_flatten_immediately",
                )
            await asyncio.sleep(0.4)
        self.inconclusive = True
        self.results[trigger_label] = "INCONCLUSIVE (observation deadline)"

    async def verify_bot(self):
        await self.worker_command("reconcile")
        status = await self.worker_wait(
            lambda s: s.get("state") == "closed" and s.get("outcomes") == 1, timeout=25
        )
        if self.automatic_exit:
            _, _, _, reason, purpose = LABELS[self.scenario]
            require(
                status.get("exit_reason") == reason and status.get("exit_purposes") == [purpose],
                "bot_exit_classification_mismatch",
            )
        await self.worker_command("duplicate")
        status = await self.worker_wait(lambda s: s.get("duplicate_replay_complete") is True)
        require(
            status.get("outcomes")
            == status.get("trade_results")
            == status.get("close_events")
            == 1,
            "duplicate_outcome_or_close_event",
        )
        require(
            dec(status.get("entry_qty", 0)) == dec(status.get("exit_qty", 0)) == 1,
            "bot_missed_fill",
        )
        require(status.get("mutation_attempts") == 0, "bot_attempted_duplicate_submission")
        self.results["Outcomes recorded once"] = "PASS"
        self.results["No duplicate submissions"] = "PASS"

    async def run(self):
        from signal_bot.delta.client import DeltaWebSocket

        ws, listener = None, None
        error = None
        try:
            await self.client.initialize()
            product = self.client.symbol_map._products["BTCUSD"]
            self.contract_value = product["contract_value"]
            ticker, book, self.plan = await self.fresh_market()
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
                    "User-Agent": "Mozilla/5.0 (signalbot-exit-validation)",
                },
                heartbeat=20,
            )
            await asyncio.wait_for(self.bootstrap(ws, socket), timeout=15)
            ticker, book, fresh_plan = await self.fresh_market()
            self.plan = near_trigger(
                fresh_plan,
                ticker,
                book,
                dec(product["tick_size"]),
                self.scenario,
                self.bps,
            )
            await self.account_empty()
            self.results["Preflight checks"] = "PASS"
            print(
                f"Scenario {self.scenario}: preflight PASS; one contract; testnet only", flush=True
            )
            listener = asyncio.create_task(self.listen(ws))
            try:
                await asyncio.wait_for(self.stage(listener), timeout=self.observe + 60)
            finally:
                await self.cleanup()
            self.results["Final account flat"] = self.results.get("Account flat", "FAIL")
            self.results["Residual orders: 0"] = self.results.get("Remaining orders: 0", "FAIL")
            for sibling in self.siblings:
                response = await self.client.get_order_status(sibling)
                self.record("sibling_final_state", response)
                require(
                    body(response).get("state") in {"cancelled", "canceled"},
                    "sibling_not_cancelled",
                )
            self.results["Sibling order cleanup"] = "PASS"
            await self.verify_bot()
        except Exception as exc:
            error = safe_error(exc)
            print("Scenario error: " + error, flush=True)
        finally:
            if not self.attempted and self.plan is not None:
                try:
                    await self.account_empty()
                    self.results["Final account flat"] = "PASS"
                    self.results["Residual orders: 0"] = "PASS"
                except Exception:
                    pass
            await self.worker_stop()
            if listener:
                listener.cancel()
                try:
                    await listener
                except (asyncio.CancelledError, Exception):
                    pass
            if ws:
                await ws.close()
            await self.client.close()
            # Base cleanup/placement adds useful auxiliary rows; retain them as
            # evidence without making a controlled close mandatory for a trigger.
            for name in (
                "Reduce-only close",
                "Account flat",
                "Remaining orders: 0",
                "Entry fill confirmed",
                "Entry order acknowledged",
                "Protective stop confirmed",
            ):
                if self.results.get(name, "").startswith("FAIL (not reached)"):
                    self.results.pop(name, None)
            passed = error is None and all(v == "PASS" for v in self.results.values())
            result = (
                "FAIL"
                if error
                else "INCONCLUSIVE"
                if self.inconclusive
                else "PASS"
                if passed
                else "FAIL"
            )
            evidence = {
                "environment": "testnet",
                "scenario": self.scenario,
                "result": result,
                "error": error,
                "results": self.results,
                "bot_status": self.bot_status,
                "exchanges": self.evidence.events,
            }
            self.fixture_path.parent.mkdir(parents=True, exist_ok=True)
            self.fixture_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
            archive = self.fixture_path.with_name(
                self.fixture_path.stem
                + "_"
                + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                + "_"
                + uuid.uuid4().hex[:6]
                + ".json"
            )
            archive.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
            report = (
                "DELTA TESTNET — AUTOMATIC EXIT VALIDATION\n\nScenario: "
                + self.scenario
                + "\n"
                + "\n".join(f"{k:30} {v}" for k, v in self.results.items())
                + "\n\nRESULT: "
                + result
                + "\n"
            )
            self.fixture_path.with_suffix(".txt").write_text(report, encoding="utf-8")
            print(report, flush=True)
        return 0 if result == "PASS" else 3 if result == "INCONCLUSIVE" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", choices=("stop-loss", "take-profit", "restart"), required=True
    )
    parser.add_argument("--confirm-testnet-scenario", action="store_true")
    parser.add_argument("--observe-seconds", type=int, default=45)
    parser.add_argument("--trigger-bps", default="1")
    args = parser.parse_args()
    if not args.confirm_testnet_scenario:
        parser.error("--confirm-testnet-scenario required; no network calls made")
    require(5 <= args.observe_seconds <= 120, "observation_window_out_of_bounds")
    bps = dec(args.trigger_bps)
    require(Decimal("0.1") <= bps <= Decimal("10"), "trigger_distance_out_of_bounds")
    logging.disable(logging.CRITICAL)
    from signal_bot.config import settings

    validate_environment(settings.delta_mode, settings.exec_mode, settings.delta_ws_url)
    output = (
        Path(__file__).resolve().parents[1]
        / "tests"
        / "fixtures"
        / ("delta_" + args.scenario.replace("-", "_") + ".json")
    )
    return asyncio.run(
        ExitProbe(make_guarded_client(), output, args.scenario, args.observe_seconds, bps).run()
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print("Scenario setup error: " + safe_error(exc), flush=True)
        raise SystemExit(1) from None
