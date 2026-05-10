"""Integration test: place, verify, cancel, fill, and close orders on Delta India testnet."""

import asyncio
import os

# Trim watchlist so symbol_map.load() doesn't fail on missing testnet symbols
os.environ["SYMBOLS"] = '["BTCUSDT"]'

import httpx
from signal_bot.delta.client import DeltaClient


async def main():
    # Public ticker (no auth) to get current BTCUSD mark
    async with httpx.AsyncClient() as h:
        r = await h.get("https://cdn-ind.testnet.deltaex.org/v2/tickers/BTCUSD")
        mark = float(r.json()["result"]["mark_price"])
    print(f"BTCUSD mark price: {mark}")

    async with DeltaClient() as client:
        # symbol_map already loaded by __aenter__; explicit reload is harmless
        await client.symbol_map.load()
        pid = client.symbol_map.get_product_id("BTCUSDT")
        try:
            await client.set_margin_mode(pid, "isolated")
            print("Margin mode set: isolated")
        except Exception as e:
            print(f"set_margin_mode skipped ({type(e).__name__}: {e})")

        try:
            await client.set_leverage(pid, 5)
            print("Leverage set: 5x")
        except Exception as e:
            print(f"set_leverage skipped ({type(e).__name__}: {e})")

        # --- TEST 1: place a no-fill order, verify on UI, cancel ---
        order = await client.place_bracket_order(
            symbol="BTCUSDT",
            side="buy",
            size=1,
            entry_price=round(mark * 0.99, 1),       # 1% below market
            stop_loss_price=round(mark * 0.97, 1),
            take_profit_price=round(mark * 1.02, 1),
            client_order_id=f"test_{int(asyncio.get_event_loop().time())}_001",
            order_type="limit_order",
        )
        order_data = order.get("result") or order
        print(f"Placed (no-fill expected): order_id={order_data.get('id')}")
        print(f"Response: {order}")
        print("-> Open Delta demo UI now. Confirm order is visible WITH SL/TP attached.")
        await asyncio.sleep(8)

        cancelled = await client.cancel_order(str(order_data["id"]), product_id=pid)
        print(f"Cancelled: {cancelled}")
        await asyncio.sleep(2)

        # --- TEST 2: market order that fills immediately, then close ---
        fill = await client.place_bracket_order(
            symbol="BTCUSDT",
            side="buy",
            size=1,
            stop_loss_price=round(mark * 0.99, 1),
            take_profit_price=round(mark * 1.01, 1),
            client_order_id=f"test_{int(asyncio.get_event_loop().time())}_002",
            order_type="market_order",
        )
        fill_data = fill.get("result") or fill
        print(f"Placed aggressive: order_id={fill_data.get('id')}")
        print(f"Response: {fill}")
        await asyncio.sleep(5)

        positions = await client.get_open_positions(product_id=pid)
        print(f"Open positions: {positions}")

        # Close the position with a market reduce-only
        raw = None
        if isinstance(positions, dict):
            raw = positions.get("positions") or positions.get("result")
        pos_list = []
        if isinstance(raw, list):
            pos_list = raw
        elif isinstance(raw, dict):
            pos_list = [raw]

        if pos_list and any(int(p.get("size", 0)) for p in pos_list):
            close = await client.place_market_close(symbol="BTCUSD", product_id=pid)
            print(f"Closed: {close}")
        else:
            print("No position open — market order didn't fill, check balance or sizing")


if __name__ == "__main__":
    asyncio.run(main())
