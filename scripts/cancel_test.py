import asyncio
import os

os.environ["SYMBOLS"] = '["BTCUSDT"]'

from signal_bot.delta.client import DeltaClient


async def main():
    async with DeltaClient() as client:
        oid = "2160129488"

        # Try 1: DELETE /v2/orders?id=xxx (query param)
        try:
            r = await client._request("DELETE", "/v2/orders", params={"id": oid})
            print(f"Query-param cancel OK: {r}")
            return
        except Exception as e:
            print(f"Query-param cancel: {e}")

        # Try 2: DELETE /v2/orders with body
        try:
            r = await client._request("DELETE", "/v2/orders", data={"id": int(oid)})
            print(f"Body cancel OK: {r}")
            return
        except Exception as e:
            print(f"Body cancel: {e}")

        # Try 3: POST /v2/orders/cancel
        try:
            r = await client._request("POST", "/v2/orders/cancel", data={"id": int(oid)})
            print(f"POST cancel OK: {r}")
            return
        except Exception as e:
            print(f"POST cancel: {e}")


if __name__ == "__main__":
    asyncio.run(main())
