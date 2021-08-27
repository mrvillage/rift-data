import sys
import traceback
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_read_query
from ...env import GQL_URL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(seconds=30)
async def fetch_market_prices():
    try:
        time = datetime.utcnow()
        query = """
            {
                tradeprices(limit: 1) {
                    credits
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
        async with aiohttp.request("GET", GQL_URL, json={"query": query}) as response:
            data = await response.json()
            data = data["data"]["tradeprices"][0]
            old = await execute_read_query(
                """
                SELECT credit, coal, oil, uranium,
                lead, iron, bauxite, gasoline, munitions,
                steel, aluminum, food FROM market_prices
                ORDER BY datetime DESC LIMIT 1;
                """,
            )
            old = dict(old[0])
            if old != data:
                await dispatch(
                    "market_prices_update", str(time), before=old, after=data
                )
            await execute_query(
                """
                INSERT INTO market_prices (datetime, credit, coal, oil, uranium,
                lead, iron, bauxite, gasoline, munitions, steel, aluminum, food)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
            """,
                str(time),
                *list(data.values()),
            )
            await UPDATE_TIMES.set_market_prices(time)
    except Exception as error:
        print("Ignoring exception in market_prices:", file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_market_prices.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(second=0)
    while wait < now:
        wait += timedelta(seconds=30)
    await sleep_until(wait)


fetch_market_prices.add_exception_type(Exception)
fetch_market_prices.start()
