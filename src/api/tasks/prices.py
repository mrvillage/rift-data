import asyncio
import sys
import traceback
from datetime import datetime, timedelta
from json import dumps

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_read_query
from ...env import APIKEY, BASEURL, UPDATE_TIMES
from ...events import dispatch


async def request(resource):
    async with aiohttp.request(
        "GET", f"{BASEURL}/tradeprice/?resource={resource}&key={APIKEY}"
    ) as response:
        return await response.json()


@tasks.loop(minutes=5)
async def fetch_prices():
    try:
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
        data = {
            "credit": dumps(credit),
            "coal": dumps(coal),
            "oil": dumps(oil),
            "uranium": dumps(uranium),
            "lead": dumps(lead),
            "iron": dumps(iron),
            "bauxite": dumps(bauxite),
            "gasoline": dumps(gasoline),
            "munitions": dumps(munitions),
            "steel": dumps(steel),
            "aluminum": dumps(aluminum),
            "food": dumps(food),
        }
        old = await execute_read_query(
            """
            SELECT credit, coal, oil, uranium,
            lead, iron, bauxite, gasoline, munitions, steel,
            aluminum, food FROM prices ORDER BY datetime DESC LIMIT 1;
            """,
        )
        old = dict(old[0])
        await execute_query(
            """
            INSERT INTO prices (datetime, credit, coal, oil, uranium,
            lead, iron, bauxite, gasoline, munitions, steel, aluminum, food)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
            """,
            str(time),
            *list(data.values()),
        )
        await UPDATE_TIMES.set_prices(time)
        if old != data:
            await dispatch("prices_update", str(time), before=old, after=data)
    except Exception as error:
        print("Ignoring exception in prices:", file=sys.stderr, flush=True)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_prices.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=5, second=10)
    while wait < now:
        wait += timedelta(minutes=5)
    await sleep_until(wait)


fetch_prices.add_exception_type(Exception)
fetch_prices.start()
