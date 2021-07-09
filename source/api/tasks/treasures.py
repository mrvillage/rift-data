from datetime import datetime, timedelta
from json import dumps, loads

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_read_query
from ...env import GQLURL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(hours=1)
async def fetch_treasures():
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
    async with aiohttp.request("GET", GQLURL, json={"query": query}) as response:
        data = await response.json()
        treasures = []
        for i in data["data"]["treasures"]:
            i["nation"] = i["nation"]["id"]
            treasures.append(i)
        treasures = dumps(treasures)
        old = await execute_read_query(
            """
            SELECT treasures FROM treasuresUPDATE
            ORDER BY datetime DESC LIMIT 1;
            """,
        )
        old = old[0]["treasures"]
        if old != treasures:
            await dispatch(
                "treasures_update", str(time), before=loads(old), after=loads(treasures)
            )
        await execute_query(
            """
            INSERT INTO treasuresUPDATE (datetime, treasures)
            VALUES ($1, $2);
            """,
            str(time),
            treasures,
        )
        await UPDATE_TIMES.set_treasures(time)


@fetch_treasures.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=2, second=6)
    while wait < now:
        wait += timedelta(hours=1)
    await sleep_until(wait)


fetch_treasures.add_exception_type(Exception)
fetch_treasures.start()
