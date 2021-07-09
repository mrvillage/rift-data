from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query_many, execute_read_query
from ...env import GQLURL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(minutes=30)
async def fetch_colors():
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
    async with aiohttp.request("GET", GQLURL, json={"query": query}) as response:
        data = await response.json()
        data = {
            i["color"]: (
                i["color"],
                i["bloc_name"],
                int(i["turn_bonus"]),
            )
            for i in data["data"]["colors"]
        }
        raw_colors = {i["color"]: i for i in data["data"]["colors"]}
        old = await execute_read_query("SELECT * FROM colorsupdate;")
        old = [dict(i) for i in old]
        old = {i["color"]: i for i in old}
        update = {}
        for after in data.values():
            before = tuple(old[after[0]].values())
            if before != after:
                await dispatch(
                    "color_update",
                    str(time),
                    before=old[after[0]],
                    after=raw_colors[after[0]],
                )
                update[after[0]] = after
        await execute_query_many(
            """
            UPDATE colorsUPDATE SET
            color = $1,
            bloc_name = $2,
            turn_bonus = $3;
        """,
            update.values(),
        )
        await UPDATE_TIMES.set_colors(time)


@fetch_colors.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=1, second=0)
    while wait < now:
        wait += timedelta(minutes=30)
    await sleep_until(wait)


fetch_colors.add_exception_type(Exception)
fetch_colors.start()
