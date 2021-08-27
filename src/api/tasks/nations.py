import asyncio
import sys
import traceback
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_query_many, execute_read_query
from ...env import APIKEY, BASEURL, UPDATE_TIMES
from ...events import dispatch


async def city_one():
    async with aiohttp.request(
        "GET", f"{BASEURL}/v2/nations/{APIKEY}/&cities=1"
    ) as response:
        data = await response.json()
        return data["data"]


async def city_two():
    async with aiohttp.request(
        "GET", f"{BASEURL}/v2/nations/{APIKEY}/&min_cities=2"
    ) as response:
        data = await response.json()
        return data["data"]


@tasks.loop(minutes=4)
async def fetch_nations():
    try:
        time = datetime.utcnow()
        responses = await asyncio.gather(city_one(), city_two())
        fetched_data = [*responses[0], *responses[1]]
        raw_nations = {int(i["nation_id"]): i for i in fetched_data}
        data = {
            int(i["nation_id"]): (
                int(i["nation_id"]),
                i["nation"],
                i["leader"],
                int(i["continent"]),
                int(i["war_policy"]),
                int(i["domestic_policy"]),
                int(i["color"]),
                int(i["alliance_id"]),
                i["alliance"],
                int(i["alliance_position"]),
                int(i["cities"]),
                int(i["offensive_wars"]),
                int(i["defensive_wars"]),
                float(i["score"]),
                bool(i["v_mode"]),
                int(i["v_mode_turns"]),
                int(i["beige_turns"]),
                i["last_active"],
                i["founded"],
                int(i["soldiers"]),
                int(i["tanks"]),
                int(i["aircraft"]),
                int(i["ships"]),
                int(i["missiles"]),
                int(i["nukes"]),
            )
            for i in fetched_data
        }
        old = await execute_read_query("SELECT * FROM nations;")
        old = [dict(i) for i in old]
        old = {i["id"]: i for i in old}
        update = {}
        for i in old.values():
            i["score"] = round(i["score"], 2)
        for after in data.values():
            try:
                before = tuple(old[after[0]].values())
                if before != after:
                    await dispatch(
                        "nation_update",
                        str(time),
                        before=old[after[0]],
                        after=raw_nations[after[0]],
                    )
                    update[after[0]] = after
                del old[after[0]]
            except KeyError:
                await dispatch(
                    "nation_created", str(time), nation=raw_nations[after[0]]
                )
                update[after[0]] = after
        for deleted in old.values():
            await dispatch("nation_deleted", str(time), nation=deleted)
            await execute_query("DELETE FROM nations WHERE id = $1;", deleted["id"])
        await execute_query_many(
            """
            INSERT INTO nations (id, name, leader, continent, war_policy,
            domestic_policy, color, alliance_id, alliance, alliance_position, cities,
            offensive_wars, defensive_wars, score, v_mode, v_mode_turns, beige_turns,
            last_active, founded, soldiers, tanks, aircraft, ships, missiles, nukes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
            ON CONFLICT (id) DO UPDATE SET
            id = $1,
            name = $2,
            leader = $3,
            continent = $4,
            war_policy = $5,
            domestic_policy = $6,
            color = $7,
            alliance_id = $8,
            alliance = $9,
            alliance_position = $10,
            cities = $11,
            offensive_wars = $12,
            defensive_wars = $13,
            score = $14,
            v_mode = $15,
            v_mode_turns = $16,
            beige_turns = $17,
            last_active = $18,
            founded = $19,
            soldiers = $20,
            tanks = $21,
            aircraft = $22,
            ships = $23,
            missiles = $24,
            nukes = $25;
        """,
            update.values(),
        )
        await UPDATE_TIMES.set_nations(time)
    except Exception as error:
        print("Ignoring exception in nations:", file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_nations.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=0)
    while wait < now:
        wait += timedelta(minutes=4)
    await sleep_until(wait)


fetch_nations.add_exception_type(Exception)
fetch_nations.start()
