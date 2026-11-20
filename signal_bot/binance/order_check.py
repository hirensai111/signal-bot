"""Demo-only end-to-end check of the Binance adapter (places DEMO orders, then flattens).

Runs the same code path the bot uses: one-way mode, isolated margin and leverage, an IOC
entry of the smallest allowed size, the attached stop and target, moving the stop (new stop
first, old one cancelled), and a market close that sweeps the leftover target.

    BINANCE_TRADE_MODE=demo python -m signal_bot.binance.order_check --yes-place-demo-orders

Refuses to run unless BINANCE_TRADE_MODE=demo.  Prints a JSON report; any failure is
followed by a best-effort flatten and cancel on the symbol.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import uuid
from decimal import ROUND_UP, Decimal
from typing import Any

from signal_bot.binance.client import BinanceFuturesClient
from signal_bot.execution.adapter import OrderRequest
from signal_bot.execution.binance_adapter import BinanceFuturesAdapter


async def run(symbol: str, leverage: int) -> dict[str, Any]:
    report: dict[str, Any] = {"symbol": symbol}
    async with BinanceFuturesClient(symbols=[symbol]) as client:
        if client.mode != "demo":
            raise SystemExit(
                "refusing: order_check places orders and runs on BINANCE_TRADE_MODE=demo only"
            )
        adapter = BinanceFuturesAdapter(client)
        rules = client.rules[symbol]
        coid = "SBCK" + uuid.uuid4().hex[:28]
        try:
            await adapter.prepare()
            await adapter.set_leverage(symbol, leverage)
            report["equity_before"] = str(await adapter.account_equity())

            book = await adapter.order_book(symbol)
            ask = book.best_ask
            # the smallest size above both minimums (50% headroom on the notional)
            qty = max(rules.min_qty, rules.min_notional * Decimal("1.5") / ask)
            contracts = Decimal(math.ceil(qty / rules.step_size))
            report["entry"] = {
                "ask": str(ask),
                "contracts": str(contracts),
                "qty": str(contracts * rules.step_size),
            }

            order = await adapter.place_order(
                OrderRequest(
                    client_order_id=coid,
                    symbol=symbol,
                    side="buy",
                    size=contracts,
                    order_type="limit_ioc",
                    limit_price=(ask * Decimal("1.002")).quantize(rules.tick_size, ROUND_UP),
                    stop_loss_price=ask * Decimal("0.97"),
                    take_profit_price=ask * Decimal("1.05"),
                )
            )
            report["entry"].update(
                state=str(order.state),
                filled=str(order.filled_size),
                avg_price=str(order.avg_fill_price),
            )
            if order.filled_size == 0:
                report["result"] = "entry not filled (IOC found no liquidity); nothing else tested"
                return report

            pos = next(p for p in await adapter.positions() if p.symbol == symbol)
            report["after_entry"] = {
                "size": str(pos.size),
                "has_stop": pos.has_stop_loss,
                "stop": str(pos.stop_loss_price),
                "has_target": pos.has_take_profit,
            }

            moved = await adapter.amend_stop(symbol, coid, ask * Decimal("0.98"))
            report["after_amend"] = {
                "has_stop": bool(moved and moved.has_stop_loss),
                "stop": str(moved.stop_loss_price) if moved else None,
            }
            report["resting_after_amend"] = [
                o.client_order_id for o in await adapter.open_orders() if o.symbol == symbol
            ]

            closed = await adapter.close_position(symbol, coid)
            report["close"] = {
                "state": str(closed.state),
                "filled": str(closed.filled_size),
                "avg_price": str(closed.avg_fill_price),
            }
            await asyncio.sleep(1.0)
            report["flat"] = not any(p.symbol == symbol for p in await adapter.positions())
            report["leftover_orders"] = [
                o.client_order_id for o in await adapter.open_orders() if o.symbol == symbol
            ]
            report["entry_fills"] = len(await adapter.fills_for(coid))
            report["equity_after"] = str(await adapter.account_equity())
            ok = (
                report["after_entry"]["has_stop"]
                and report["after_amend"]["has_stop"]
                and report["flat"]
                and not report["leftover_orders"]
            )
            report["result"] = "PASS" if ok else "CHECK THE FIELDS ABOVE"
        except Exception as exc:
            report["error"] = f"{type(exc).__name__}: {exc}"
            try:
                await adapter.close_position(symbol, coid)
                await client.cancel_all_algo_orders(symbol)
                report["cleanup"] = "flattened and cancelled"
            except Exception as cleanup:
                report["cleanup"] = f"FAILED {cleanup!r}: close the demo position by hand"
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Binance demo order check (places DEMO orders)")
    ap.add_argument("--yes-place-demo-orders", action="store_true", required=True)
    ap.add_argument("--symbol", default="XRPUSDT")
    ap.add_argument("--leverage", type=int, default=5)
    args = ap.parse_args()
    print(json.dumps(asyncio.run(run(args.symbol, args.leverage)), indent=2, default=str))


if __name__ == "__main__":
    main()
