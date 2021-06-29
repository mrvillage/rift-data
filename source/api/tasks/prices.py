import asyncio
from datetime import datetime, timedelta
from json import dumps
from time import perf_counter

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query
from ...env import APIKEY, BASEURL, UPDATE_TIMES


async def request(resource):
    async with aiohttp.request(
        "GET", f"{BASEURL}/tradeprice/?resource={resource}&key={APIKEY}"
    ) as response:
        return await response.json()


@tasks.loop(minutes=5)
async def fetch_prices():
    start = perf_counter()
    time = datetime.utcnow()
    credit, coal, oil, uranium = await asyncio.gather(
        request("credits"),
        request("coal"),
        request("oil"),
        request("uranium"),
    )
    await asyncio.sleep(1.5)
    lead, iron, bauxite, gasoline = await asyncio.gather(
        request("lead"),
        request("iron"),
        request("bauxite"),
        request("gasoline"),
    )
    await asyncio.sleep(1.5)
    munitions, steel, aluminum, food = await asyncio.gather(
        request("munitions"),
        request("steel"),
        request("aluminum"),
        request("food"),
    )
    data = (
        dumps(credit),
        dumps(coal),
        dumps(oil),
        dumps(uranium),
        dumps(lead),
        dumps(iron),
        dumps(bauxite),
        dumps(gasoline),
        dumps(munitions),
        dumps(steel),
        dumps(aluminum),
        dumps(food),
    )
    await execute_query(
        """
        INSERT INTO pricesUPDATE (datetime, credit, coal, oil, uranium,
        lead, iron, bauxite, gasoline, munitions, steel, aluminum, food)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
        """,
        str(time),
        *data,
    )
    await UPDATE_TIMES.set_prices(time)
    end = perf_counter()
    print(start, end, end - start)


@fetch_prices.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=5, second=10)
    while wait < now:
        wait += timedelta(minutes=5)
    await sleep_until(wait)


fetch_prices.add_exception_type(Exception)
fetch_prices.start()
