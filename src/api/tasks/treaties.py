import re
import sys
import traceback
from asyncio import sleep
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query_many, execute_read_query
from ...env import UPDATE_TIMES
from ...events import dispatch


async def scrape_treaties(alliance_id):
    async with aiohttp.request(
        "GET", f"https://politicsandwar.com/alliance/id={alliance_id}"
    ) as response:
        text = await response.text()
        matches = re.findall(
            r"'from':(\d*), 'to':(\d*), 'color':'\#[\d|\w]*', 'length':\d*, 'title':'(\w*)'",
            text,
        )
        return [(str(datetime.utcnow()), int(i[0]), int(i[1]), i[2]) for i in matches]


async def scrape_treaty_web():
    async with aiohttp.request(
        "GET", "https://politicsandwar.com/alliances/treatyweb/all"
    ) as response:
        text = await response.text()
        matches = re.findall(
            r"'from':(\d*), 'to':(\d*), 'color':'\#[\d|\w]*', 'length':\d*, 'title':'(\w*)'",
            text,
        )
        return [(str(datetime.utcnow()), int(i[0]), int(i[1]), i[2]) for i in matches]


@tasks.loop(hours=12)
async def fetch_treaties():
    try:
        time = datetime.utcnow()
        alliances = await execute_read_query(
            "SELECT id FROM alliances WHERE rank <= 250 AND rank > 50;"
        )
        alliances = [i["id"] for i in alliances]
        treaties = [*(await scrape_treaty_web())]
        for alliance in alliances:
            result = await scrape_treaties(alliance)
            treaties = [*treaties, *result]
            await sleep(2)
        old_treaties = await execute_read_query("SELECT * FROM treaties;")
        old_treaties = [(i[0], i[2], i[3], i[4]) for i in old_treaties if i[1] is None]
        short_old_treaties = [set(i[1:]) for i in old_treaties]
        purged_treaties = []
        short_purged_treaties = []
        for treaty in treaties:
            if set(treaty[1:]) not in short_purged_treaties:
                purged_treaties.append(treaty)
                short_purged_treaties.append(set(treaty[1:]))
        new_treaties = []
        new_dispatches = []
        for treaty in purged_treaties:
            if set(treaty[1:]) not in short_old_treaties:
                treaty = (treaty[0], None, treaty[1], treaty[2], treaty[3])
                new_dispatches.append({"treaty": treaty})
                new_treaties.append(treaty)
        expired_treaties = []
        expired_dispatches = []
        for treaty in old_treaties:
            if set(treaty[1:]) not in short_purged_treaties:
                treaty = (treaty[0], str(time), treaty[1], treaty[2], treaty[3])
                expired_dispatches.append({"treaty": treaty})
                expired_treaties.append(treaty)
        await execute_query_many(
            """
            INSERT INTO treaties (started, stopped, from_, to_,
            treaty_type) VALUES ($1, $2, $3, $4, $5);
        """,
            new_treaties,
        )
        await execute_query_many(
            """
            UPDATE treaties SET stopped = $2 WHERE started = $1
            AND from_ = $3 AND to_ = $4 AND treaty_type = $5;
        """,
            expired_treaties,
        )
        await UPDATE_TIMES.set_treaties(time)
        if new_dispatches:
            await dispatch("bulk_new_treaty", new_dispatches[0][0], data=new_dispatches)
        if expired_dispatches:
            await dispatch("bulk_treaty_expired", str(time), data=expired_dispatches)
    except Exception as error:
        print("Ignoring exception in treaties:", file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_treaties.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(hour=7, minute=17, second=0)
    while wait < now:
        wait += timedelta(hours=12)
    await sleep_until(wait)


fetch_treaties.add_exception_type(Exception)
fetch_treaties.start()
