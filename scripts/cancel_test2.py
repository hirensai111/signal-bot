import asyncio
import os

os.environ["SYMBOLS"] = '["BTCUSDT"]'

from signal_bot.delta.client import DeltaClient


async def main():
    async with DeltaClient() as client:
        # Try cancel by product_symbol
        try:
            r = await client._request("DELETE", "/v2/orders", data={"product_symbol": "BTCUSD"})
            print(f"product_symbol cancel OK: {r}")
            return
        except Exception as e:
            print(f"product_symbol cancel: {e}")

        # Try cancel by product_id only
        try:
            r = await client._request("DELETE", "/v2/orders", data={"product_id": 84})
            print(f"product_id cancel OK: {r}")
            return
        except Exception as e:
            print(f"product_id cancel: {e}")


if __name__ == "__main__":
    asyncio.run(main())
