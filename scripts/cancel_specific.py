import asyncio
import os

os.environ["SYMBOLS"] = '["BTCUSDT"]'

from signal_bot.delta.client import DeltaClient


async def main():
    async with DeltaClient() as client:
        cancelled = await client.cancel_order("2160129488")
        print(f"Cancelled: {cancelled}")


if __name__ == "__main__":
    asyncio.run(main())
