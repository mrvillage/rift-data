import sys
import traceback
from datetime import datetime, timedelta
from json import dumps, loads

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_read_query
from ...env import GQL_URL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(hours=1)
async def fetch_treasures():
    try:
        time = datetime.utcnow()
        query = """
            {
                treasures {
                    name
                    color
                    continent
                    bonus
                    spawndate
                    nation {
                        id
                    }
                }
            }
        """
        async with aiohttp.request("GET", GQL_URL, json={"query": query}) as response:
            data = await response.json()
            treasures = []
            for i in data["data"]["treasures"]:
                i["nation"] = i["nation"]["id"]
                treasures.append(i)
            old = await execute_read_query(
                """
                SELECT treasures FROM treasures
                ORDER BY datetime DESC LIMIT 1;
                """,
            )
            old = old[0]["treasures"]
            updated_dispatches = []
            if old != treasures:
                updated_dispatches.append({"before": old, "after": treasures})
            await execute_query(
                """
                INSERT INTO treasures (datetime, treasures)
                VALUES ($1, $2);
                """,
                str(time),
                dumps(treasures),
            )
            await UPDATE_TIMES.set_treasures(time)
            if updated_dispatches:
                await dispatch(
                    "bulk_treasures_update", str(time), data=updated_dispatches
                )
    except Exception as error:
        print("Ignoring exception in treasures:", file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


# @fetch_treasures.before_loop
# async def before_loop():
#     now = datetime.utcnow()
#     wait = now.replace(minute=2, second=6)
#     while wait < now:
#         wait += timedelta(hours=1)
#     await sleep_until(wait)


fetch_treasures.add_exception_type(Exception)
fetch_treasures.start()
