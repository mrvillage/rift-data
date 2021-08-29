import sys
import traceback
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until


@tasks.loop(minutes=15)
async def api_keep_alive():
    try:
        async with aiohttp.request(
            "GET", "https://riftapi.mrvillage.dev/v1/treaties?id=3683"
        ) as req:
            pass
    except Exception as error:
        print("Ignoring exception in api:", file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@api_keep_alive.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=0)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


api_keep_alive.start()
