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
        nations = {
            int(i["nation_id"]): {
                "id": int(i["nation_id"]),
                "name": i["nation"],
                "leader": i["leader"],
                "continent": int(i["continent"]),
                "war_policy": int(i["war_policy"]),
                "domestic_policy": int(i["domestic_policy"]),
                "color": int(i["color"]),
                "alliance_id": int(i["alliance_id"]),
                "alliance": i["alliance"],
                "alliance_position": int(i["alliance_position"]),
                "cities": int(i["cities"]),
                "offensive_wars": int(i["offensive_wars"]),
                "defensive_wars": int(i["defensive_wars"]),
                "score": float(i["score"]),
                "v_mode": bool(i["v_mode"]),
                "v_mode_turns": int(i["v_mode_turns"]),
                "beige_turns": int(i["beige_turns"]),
                "last_active": i["last_active"],
                "founded": i["founded"],
                "soldiers": int(i["soldiers"]),
                "tanks": int(i["tanks"]),
                "aircraft": int(i["aircraft"]),
                "ships": int(i["ships"]),
                "missiles": int(i["missiles"]),
                "nukes": int(i["nukes"]),
            }
            for i in fetched_data
        }
        data = {key: tuple(value.values()) for key, value in nations.items()}
        old = await execute_read_query("SELECT * FROM nations;")
        old = [dict(i) for i in old]
        old = {i["id"]: i for i in old}
        update = {}
        for i in old.values():
            i["score"] = round(i["score"], 2)
        updated_dispatches = []
        created_dispatches = []
        for after in data.values():
            try:
                before = tuple(old[after[0]].values())
                if before != after:
                    updated_dispatches.append(
                        {"before": old[after[0]], "after": nations[after[0]]}
                    )
                    update[after[0]] = after
                del old[after[0]]
            except KeyError:
                created_dispatches.append(nations[after[0]])

                update[after[0]] = after
        deleted_dispatches = []
        for deleted in old.values():
            deleted_dispatches.append(deleted)
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
        if updated_dispatches:
            await dispatch(
                "bulk_nation_update",
                str(time),
                data=updated_dispatches,
            )
        if created_dispatches:
            await dispatch("bulk_nation_create", str(time), data=created_dispatches)
        if deleted_dispatches:
            await dispatch("bulk_nation_delete", str(time), data=deleted_dispatches)
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
