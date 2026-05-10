import asyncio
import os

os.environ["SYMBOLS"] = '["BTCUSDT"]'

from signal_bot.delta.client import DeltaClient


async def main():
    async with DeltaClient() as client:
        await client.symbol_map.load()
        pid = client.symbol_map.get_product_id("BTCUSDT")

        # Cancel all open orders for BTCUSD
        try:
            cancelled = await client.cancel_all_orders(product_id=pid)
            print(f"cancel_all_orders: {cancelled}")
        except Exception as e:
            print(f"cancel_all_orders failed: {e}")

        # Also list open orders
        try:
            orders = await client.get_open_orders()
            print(f"Open orders: {orders}")
        except Exception as e:
            print(f"get_open_orders failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
