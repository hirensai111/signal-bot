import asyncio
import os

os.environ["SYMBOLS"] = '["BTCUSDT"]'

from signal_bot.delta.client import DeltaClient


async def main():
    async with DeltaClient() as client:
        await client.symbol_map.load()
        pid = client.symbol_map.get_product_id("BTCUSDT")

        positions = await client.get_open_positions(product_id=pid)
        raw = None
        if isinstance(positions, dict):
            raw = positions.get("positions") or positions.get("result")

        pos_list = []
        if isinstance(raw, list):
            pos_list = raw
        elif isinstance(raw, dict):
            pos_list = [raw]

        print(f"Positions found: {len(pos_list)}")
        for p in pos_list:
            print(f"  {p}")

        if pos_list and any(int(p.get("size", 0)) for p in pos_list):
            close = await client.place_market_close(symbol="BTCUSD", product_id=pid)
            print(f"Closed: {close}")
        else:
            print("No position to close")


if __name__ == "__main__":
    asyncio.run(main())
