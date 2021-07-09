import re
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
        "GET", "https://politicsandwar.com/alliances/treatyweb/"
    ) as response:
        text = await response.text()
        matches = re.findall(
            r"'from':(\d*), 'to':(\d*), 'color':'\#[\d|\w]*', 'length':\d*, 'title':'(\w*)'",
            text,
        )
        return [(str(datetime.utcnow()), int(i[0]), int(i[1]), i[2]) for i in matches]


@tasks.loop(hours=12)
async def fetch_treaties():
    time = datetime.utcnow()
    alliances = await execute_read_query(
        "SELECT id FROM alliancesUPDATE WHERE rank <= 250 AND rank > 50;"
    )
    alliances = [i["id"] for i in alliances]
    treaties = [*(await scrape_treaty_web())]
    for alliance in alliances:
        result = await scrape_treaties(alliance)
        treaties = [*treaties, *result]
    old_treaties = await execute_read_query(
        "SELECT started, from_, to_, treaty_type FROM treatiesUPDATE WHERE stopped = $1;",
        None,
    )
    old_treaties = [
        (i["started"], i["from"], i["to"], i["treaty_type"]) for i in old_treaties
    ]
    short_old_treaties = [(i["from"], i["to"], i["treaty_type"]) for i in old_treaties]
    purged_treaties = []
    short_purged_treaties = []
    for treaty in treaties:
        if treaty[1:] not in short_purged_treaties:
            purged_treaties.append(treaty)
            short_purged_treaties.append(treaty[1:])
    new_treaties = []
    for treaty in purged_treaties:
        if treaty[1:] not in short_old_treaties:
            treaty = (treaty[0], None, treaty[1], treaty[2], treaty[3])
            await dispatch("new_treaty", treaty[0], treaty=treaty)
            new_treaties.append(treaty)
            continue
    short_treaties = [i[1:] for i in new_treaties]
    expired_treaties = []
    for treaty in old_treaties:
        if treaty[1:] not in short_treaties:
            treaty = (treaty[0], str(time), treaty[1], treaty[2], treaty[3])
            await dispatch("treaty_expired", str(time), treaty=treaty)
            expired_treaties.append(treaty)
    await execute_query_many(
        """
        INSERT INTO treatiesUPDATE (started, stopped, from_, to_,
        treaty_type) VALUES ($1, $2, $3, $4, $5);
    """,
        new_treaties,
    )
    await execute_query_many(
        """
        UPDATE treatiesUPDATE SET stopped = $2 WHERE started = $1
        AND from_ = $3 AND to_ = $4 AND treaty_type = $5;
    """,
        expired_treaties,
    )
    await UPDATE_TIMES.set_treaties(time)


@fetch_treaties.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=5, second=0)
    while wait < now:
        wait += timedelta(minutes=5)
    await sleep_until(wait)


fetch_treaties.add_exception_type(Exception)
fetch_treaties.start()
