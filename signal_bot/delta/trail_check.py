"""Testnet check of Delta's trailing-stop and stop-edit behaviour (places real testnet orders).

Delta documents ``bracket_trail_amount`` ("required if bracket stop price is empty")
and editing a stop order's ``stop_price`` with ``PUT /v2/orders``, but not the sign
convention of the trail amount, how a trailing stop shows up on the order and
position objects, or whether a bracket child's stop can be edited after the entry
fills.  The bot depends on all of these, so this script measures them:

  A. native trail:   market entry with ``bracket_trail_amount`` -> inspect the stop child
                     (type, stop_price, trail_amount sign) -> sample stop_price for a while
  B. stepped trail:  market entry with a fixed ``bracket_stop_loss_price`` -> edit the child's
                     stop_price with PUT /v2/orders -> read it back
  C. cleanup:        reduce-only market close, cancel leftover stop orders (always runs)

Run on testnet only, one contract per leg:

    DELTA_MODE=testnet python -m signal_bot.delta.trail_check --yes-place-testnet-orders

It prints one JSON report; record it in docs/runbook.md before enabling
``TRAIL_MODE=native`` or relying on stepped amendments in live mode.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from signal_bot.delta.client import DeltaClient


def _result(resp: Any) -> Any:
    return resp.get("result", resp) if isinstance(resp, dict) else resp


def _rows(resp: Any) -> list[dict]:
    """List payload of a Delta response (``result``, ``products``, ``orders`` or one object)."""
    body = resp
    if isinstance(body, dict):
        for key in ("result", "products", "orders", "positions"):
            if key in body:
                body = body[key]
                break
    if isinstance(body, dict):
        body = [body]
    return [r for r in (body or []) if isinstance(r, dict)]


def _round(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick


async def _stop_children(client: DeltaClient, product_id: int) -> list[dict]:
    return [
        o
        for o in _rows(await client.get_open_orders())
        if int(o.get("product_id") or 0) == product_id
        and "stop" in str(o.get("stop_order_type") or "")
    ]


async def _wait_filled(client: DeltaClient, coid: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        order = _result(await client.get_order_by_client_id(coid)) or {}
        if str(order.get("state")) in ("closed", "filled") or time.monotonic() > deadline:
            return order
        await asyncio.sleep(0.5)


async def run_check(
    client: DeltaClient,
    product_symbol: str = "BTCUSD",
    *,
    trail_pct: Decimal = Decimal("0.005"),
    sample_seconds: float = 60.0,
    sample_every: float = 5.0,
    sign: str = "positive",
) -> dict[str, Any]:
    if client.mode != "testnet":
        raise SystemExit("refusing: trail_check places orders and runs on DELTA_MODE=testnet only")
    report: dict[str, Any] = {"symbol": product_symbol, "sign_sent": sign, "steps": []}
    products = {p["symbol"]: p for p in _rows(await client.get_products())}
    product = products.get(product_symbol)
    if product is None:
        raise SystemExit(f"{product_symbol} not listed on Delta testnet")
    pid, tick = int(product["id"]), Decimal(str(product.get("tick_size", "0.5")))
    book = _result(await client.get_l2_orderbook(product_symbol)) or {}
    bid = Decimal(str((book.get("buy") or [{}])[0].get("price")))
    ask = Decimal(str((book.get("sell") or [{}])[0].get("price")))
    mid = (bid + ask) / 2
    trail = max(tick, _round(mid * trail_pct, tick))
    report["mid"], report["trail_amount"] = str(mid), str(trail)
    stamp = str(int(time.time()))
    try:
        # ---- A: exchange-native trailing bracket --------------------------------
        signed = -trail if sign == "negative_for_long" else trail
        body = {
            "product_id": pid,
            "size": 1,
            "side": "buy",
            "order_type": "market_order",
            "client_order_id": f"trailchkA{stamp}",
            "bracket_trail_amount": str(signed),
            "bracket_stop_trigger_method": "last_traded_price",
        }
        try:
            resp = await client.place_order(body)
            report["a_accepted"] = bool(isinstance(resp, dict) and resp.get("success", True))
            report["a_response"] = _result(resp)
        except Exception as exc:  # the sign or field may be refused: that is the finding
            report["a_accepted"] = False
            report["a_error"] = f"{type(exc).__name__}: {exc}"
        if report["a_accepted"]:
            entry = await _wait_filled(client, body["client_order_id"])
            report["a_entry"] = {
                k: entry.get(k)
                for k in (
                    "state",
                    "average_fill_price",
                    "bracket_trail_amount",
                    "bracket_stop_loss_price",
                )
            }
            children = await _stop_children(client, pid)
            report["a_children"] = [
                {
                    k: c.get(k)
                    for k in (
                        "id",
                        "stop_order_type",
                        "stop_price",
                        "trail_amount",
                        "side",
                        "client_order_id",
                        "bracket_order",
                    )
                }
                for c in children
            ]
            positions = _rows(await client.get_open_positions(product_id=pid))
            report["a_position_fields"] = sorted(positions[0]) if positions else []
            samples = []
            end = time.monotonic() + sample_seconds
            while time.monotonic() < end:
                kids = await _stop_children(client, pid)
                book = _result(await client.get_l2_orderbook(product_symbol)) or {}
                samples.append(
                    {
                        "t": round(time.monotonic() - end + sample_seconds, 1),
                        "bid": (book.get("buy") or [{}])[0].get("price"),
                        "stop_price": kids[0].get("stop_price") if kids else None,
                    }
                )
                await asyncio.sleep(sample_every)
            report["a_stop_samples"] = samples
            prices = {s["stop_price"] for s in samples if s["stop_price"] is not None}
            report["a_stop_moved"] = len(prices) > 1
            await _flatten(client, pid, report, "a")

        # ---- B: fixed bracket, then edit the child's stop price -----------------
        stop = _round(mid * (1 - 2 * trail_pct), tick)
        body = {
            "product_id": pid,
            "size": 1,
            "side": "buy",
            "order_type": "market_order",
            "client_order_id": f"trailchkB{stamp}",
            "bracket_stop_loss_price": str(stop),
            "bracket_stop_trigger_method": "last_traded_price",
        }
        resp = await client.place_order(body)
        report["b_accepted"] = bool(isinstance(resp, dict) and resp.get("success", True))
        await _wait_filled(client, body["client_order_id"])
        children = await _stop_children(client, pid)
        report["b_children_before"] = [
            {k: c.get(k) for k in ("id", "stop_order_type", "stop_price")} for c in children
        ]
        if children:
            tighter = _round(mid * (1 - trail_pct), tick)
            try:
                await client.edit_order(children[0]["id"], pid, stop_price=tighter)
                after = await _stop_children(client, pid)
                report["b_edit_requested"] = str(tighter)
                report["b_children_after"] = [
                    {k: c.get(k) for k in ("id", "stop_order_type", "stop_price")} for c in after
                ]
                report["b_edit_ok"] = any(
                    Decimal(str(c.get("stop_price"))) == tighter for c in after
                )
            except Exception as exc:
                report["b_edit_ok"] = False
                report["b_edit_error"] = f"{type(exc).__name__}: {exc}"
        await _flatten(client, pid, report, "b")
    finally:
        await _flatten(client, pid, report, "final")
    report["conclusion"] = {
        "native_trail_accepted_with_sign": sign if report.get("a_accepted") else None,
        "native_trail_moves": report.get("a_stop_moved"),
        "stepped_edit_works": report.get("b_edit_ok"),
    }
    return report


async def _flatten(client: DeltaClient, pid: int, report: dict, tag: str) -> None:
    try:
        positions = _rows(await client.get_open_positions(product_id=pid))
        size = int(Decimal(str(positions[0].get("size", 0)))) if positions else 0
        if size:
            await client.place_order(
                {
                    "product_id": pid,
                    "size": abs(size),
                    "side": "sell" if size > 0 else "buy",
                    "order_type": "market_order",
                    "reduce_only": True,
                }
            )
        for child in await _stop_children(client, pid):
            await client.cancel_order(str(child["id"]), product_id=pid)
        report[f"{tag}_cleanup"] = "ok"
    except Exception as exc:
        report[f"{tag}_cleanup"] = f"FAILED {type(exc).__name__}: {exc} — check the testnet account"


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--yes-place-testnet-orders", action="store_true", required=True)
    ap.add_argument("--symbol", default="BTCUSD")
    ap.add_argument("--sign", choices=("positive", "negative_for_long"), default="positive")
    ap.add_argument("--seconds", type=float, default=60.0)
    args = ap.parse_args()

    async def go() -> dict:
        async with DeltaClient(mode="testnet") as client:
            return await run_check(client, args.symbol, sample_seconds=args.seconds, sign=args.sign)

    report = asyncio.run(go())
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
