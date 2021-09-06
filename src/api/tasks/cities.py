import sys
import traceback
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_query_many, execute_read_query
from ...env import APIKEY, BASEURL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(minutes=2)
async def fetch_cities():
    try:
        time = datetime.utcnow()
        async with aiohttp.request(
            "GET", f"{BASEURL}/all-cities/key={APIKEY}"
        ) as response:
            data = await response.json()
            raw_cities = {int(i["city_id"]): i for i in data["all_cities"]}
            data = {
                int(i["city_id"]): (
                    int(i["city_id"]),
                    int(i["nation_id"]),
                    i["city_name"],
                    bool(int(i["capital"])),
                    float(i["infrastructure"]),
                    float(i["maxinfra"]),
                    float(i["land"]),
                )
                for i in data["all_cities"]
            }
            old = await execute_read_query("SELECT * FROM cities;")
            old = [dict(i) for i in old]
            old = {i["id"]: i for i in old}
            update = {}
            for i in old.values():
                i["infrastructure"] = round(i["infrastructure"], 2)
                i["max_infra"] = round(i["max_infra"], 2)
                i["land"] = round(i["land"], 2)
            updated_dispatches = []
            created_dispatches = []
            for after in data.values():
                try:
                    before = tuple(old[after[0]].values())
                    if before != after:
                        updated_dispatches.append(
                            {"before": old[after[0]], "after": raw_cities[after[0]]}
                        )
                        update[after[0]] = after
                    del old[after[0]]
                except KeyError:
                    created_dispatches.append({"city": raw_cities[after[0]]})
                    update[after[0]] = after
            if updated_dispatches:
                await dispatch(
                    "bulk_city_update",
                    str(time),
                    data=updated_dispatches,
                )
            if created_dispatches:
                await dispatch("bulk_city_created", str(time), data=created_dispatches)
            deleted_dispatches = []
            for deleted in old.values():
                deleted_dispatches.append({"city": deleted})
                await execute_query("DELETE FROM cities WHERE id = $1;", deleted["id"])
            if deleted_dispatches:
                await dispatch("bulk_city_deleted", str(time), data=deleted_dispatches)
            await execute_query_many(
                """
                INSERT INTO cities (id, nation_id, name, capital,
                infrastructure, max_infra, land) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO UPDATE SET
                id = $1,
                nation_id = $2,
                name = $3,
                capital = $4,
                infrastructure = $5,
                max_infra = $6,
                land = $7;
            """,
                update.values(),
            )
            await UPDATE_TIMES.set_cities(time)
    except Exception as error:
        print("Ignoring exception in cities:", file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_cities.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=0)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


fetch_cities.add_exception_type(Exception)
fetch_cities.start()
