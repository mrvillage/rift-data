from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_query_many, execute_read_query
from ...env import APIKEY, BASEURL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(minutes=2)
async def fetch_alliances():
    time = datetime.utcnow()
    async with aiohttp.request("GET", f"{BASEURL}/alliances/?key={APIKEY}") as response:
        data = await response.json()
        data = {
            int(i["id"]): (
                int(i["id"]),
                i["founddate"],
                i["name"],
                i["acronym"],
                i["color"],
                int(i["rank"]),
                int(i["members"]) if "members" in i else None,
                float(i["score"]) if "score" in i else None,
                str([int(j) for j in i["officerids"]]) if "officerids" in i else None,
                str([int(j) for j in i["heirids"]]) if "heirids" in i else None,
                str([int(j) for j in i["leaderids"]]) if "leaderids" in i else None,
                float(i["avgscore"]),
                str(i["flagurl"]) if i["flagurl"] != "" else None,
                str(i["forumurl"]) if i["forumurl"] != "" else None,
                str(i["ircchan"]) if i["ircchan"] != "" else None,
            )
            for i in data["alliances"]
        }
        raw_alliances = {int(i["id"]): i for i in data["alliances"]}
        old = await execute_read_query("SELECT * FROM alliancesupdate;")
        old = [dict(i) for i in old]
        old = {i["id"]: i for i in old}
        update = {}
        for i in old.values():
            i["score"] = round(i["score"], 2)
            i["avg_score"] = round(i["avg_score"], 4)
        for after in data.values():
            try:
                before = tuple(old[after[0]].values())
                del old[after[0]]
            except KeyError:
                await dispatch(
                    "alliance_created", str(time), alliance=raw_alliances[after[0]]
                )
                update[after[0]] = after
                continue
            if before != after:
                await dispatch(
                    "alliance_update",
                    str(time),
                    before=old[after[0]],
                    after=raw_alliances[after[0]],
                )
                update[after[0]] = after
        for deleted in old.values():
            await dispatch("alliance_deleted", str(time), alliance=deleted)
            await execute_query(
                "DELETE FROM alliancesUPDATE WHERE id = $1;", deleted["id"]
            )
        await execute_query_many(
            """
            INSERT INTO alliancesUPDATE (id, found_date, name, acronym, color, rank,
            members, score, officer_ids, heir_ids, leader_ids, avg_score,
            flag_url, forum_url, ircchan) VALUES ($1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (id) DO UPDATE SET
            id = $1,
            found_date = $2,
            name = $3,
            acronym = $4,
            color = $5,
            rank = $6,
            members = $7,
            score = $8,
            officer_ids = $9,
            heir_ids = $10,
            leader_ids = $11,
            avg_score = $12,
            flag_url = $13,
            forum_url = $14,
            ircchan = $15;
        """,
            update.values(),
        )
        await UPDATE_TIMES.set_alliances(time)


@fetch_alliances.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=0)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


fetch_alliances.add_exception_type(Exception)
fetch_alliances.start()
