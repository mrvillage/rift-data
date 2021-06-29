from datetime import datetime, timedelta
import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until
from ...env import GQLURL, UPDATE_TIMES
from ...data.db import execute_query_many, execute_read_query
from ...events import dispatch


@tasks.loop(minutes=5)
async def fetch_market_prices():
    time = datetime.utcnow()
    query = """
        {
            tradeprices(limit=1) {
                credit
                coal
                oil
                uranium
                lead
                iron
                bauxite
                gasoline
                munitions
                steel
                aluminum
                food
            }
        }
    """
    async with aiohttp.request("GET", GQLURL, json={"query": query}) as response:
        data = await response.json()
        data = data["tradeprices"]
        old = await execute_read_query(
            """
            SELECT credit, coal, oil, uranium,
            lead, iron, bauxite, gasoline, munitions,
            steel, aluminum, food FROM pricesUPDATE
            ORDER BY datetime DESC LIMIT 1;
            """,
        )
        old = dict(old)
        if old != data:
            await dispatch("market_prices_update", str(time), before=old, after=data)
        await execute_query_many(
            """
            INSERT INTO market_pricesUPDATE (datetime, credit, coal, oil, uranium,
            lead, iron, bauxite, gasoline, munitions, steel, aluminum, food)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
        """,
            str(time),
            *list(data.values()),
        )
        await UPDATE_TIMES.set_market_prices(time)


@fetch_market_prices.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(second=0)
    while wait < now:
        wait += timedelta(seconds=10)
    await sleep_until(wait)


fetch_market_prices.add_exception_type(Exception)
fetch_market_prices.start()
