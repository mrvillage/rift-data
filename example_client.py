import asyncio
import aiohttp


async def main():
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect("ws://localhost:9093") as socket:
            async for message in socket:
                print(message.json())


asyncio.run(main())
