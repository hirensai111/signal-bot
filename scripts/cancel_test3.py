import asyncio
import os

os.environ["SYMBOLS"] = '["BTCUSDT"]'

from signal_bot.delta.client import DeltaClient


async def main():
    async with DeltaClient() as client:
        # id + product_symbol
        try:
            r = await client._request("DELETE", "/v2/orders", data={"id": 2160129488, "product_symbol": "BTCUSD"})
            print(f"id+symbol cancel OK: {r}")
            return
        except Exception as e:
            print(f"id+symbol cancel: {e}")

        # client_order_id + product_symbol
        try:
            r = await client._request("DELETE", "/v2/orders", data={"client_order_id": "test_001", "product_symbol": "BTCUSD"})
            print(f"client_id+symbol cancel OK: {r}")
            return
        except Exception as e:
            print(f"client_id+symbol cancel: {e}")


if __name__ == "__main__":
    asyncio.run(main())
