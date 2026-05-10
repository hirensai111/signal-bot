import asyncio
import json
import os

os.environ["SYMBOLS"] = '["BTCUSDT"]'

from signal_bot.delta.client import DeltaClient


async def main():
    async with DeltaClient() as client:
        await client.symbol_map.load()
        pid = client.symbol_map.get_product_id("BTCUSDT")

        positions = await client.get_open_positions(product_id=pid)
        print("Raw positions response:")
        print(json.dumps(positions, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
