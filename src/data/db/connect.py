from asyncpg import create_pool
from asyncio import get_event_loop
from ...env import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME


async def _create_connection():
    return await create_pool(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )


loop = get_event_loop()
connection = loop.run_until_complete(_create_connection())
