import json
import sys
import traceback
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_read_query
from ...env import GQL_URL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(minutes=30)
async def fetch_colors():
    try:
        time = datetime.utcnow()
        query = """
            {
                colors {
                    color
                    bloc_name
                    turn_bonus
                }
            }
        """
        async with aiohttp.request("GET", GQL_URL, json={"query": query}) as response:
            data = await response.json()
            colors = data["data"]["colors"]
            old = await execute_read_query(
                "SELECT colors FROM colors ORDER BY datetime DESC LIMIT 1;"
            )
            old = json.loads(old[0]["colors"])
            await execute_query(
                """
                INSERT INTO colors (datetime, colors)
                VALUES ($1, $2);
            """,
                str(time),
                json.dumps(colors),
            )
            await UPDATE_TIMES.set_colors(time)
            if old != colors:
                await dispatch(
                    "colors_update",
                    str(time),
                    before=old,
                    after=colors,
                )
    except Exception as error:
        print("Ignoring exception in colors:", file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_colors.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=1, second=0)
    while wait < now:
        wait += timedelta(minutes=30)
    # await sleep_until(wait)


fetch_colors.add_exception_type(Exception)
fetch_colors.start()
