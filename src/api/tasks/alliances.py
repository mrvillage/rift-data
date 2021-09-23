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
async def fetch_alliances():
    try:
        time = datetime.utcnow()
        async with aiohttp.request(
            "GET", f"{BASEURL}/alliances/?key={APIKEY}"
        ) as response:
            data = await response.json()
            alliances = {
                int(i["id"]): {
                    "id": int(i["id"]),
                    "found_date": i["founddate"],
                    "name": i["name"],
                    "acronym": i["acronym"],
                    "color": i["color"],
                    "rank": int(i["rank"]),
                    "members": int(i["members"]) if "members" in i else None,
                    "score": float(i["score"]) if "score" in i else None,
                    "officer_ids": [int(j) for j in i["officerids"]]
                    if "officerids" in i
                    else None,
                    "heir_ids": [int(j) for j in i["heirids"]]
                    if "heirids" in i
                    else None,
                    "leader_ids": [int(j) for j in i["leaderids"]]
                    if "leaderids" in i
                    else None,
                    "avg_score": float(i["avgscore"]),
                    "flag_url": str(i["flagurl"]) if i["flagurl"] != "" else None,
                    "forum_url": str(i["forumurl"]) if i["forumurl"] != "" else None,
                    "ircchan": str(i["ircchan"]) if i["ircchan"] != "" else None,
                }
                for i in data["alliances"]
            }
            data = {key: tuple(value.values()) for key, value in alliances.items()}
            old = await execute_read_query("SELECT * FROM alliances;")
            old = [dict(i) for i in old]
            old = {i["id"]: i for i in old}
            update = {}
            for i in old.values():
                i["score"] = round(i["score"], 2) if i["score"] is not None else None
                i["avg_score"] = round(i["avg_score"], 4)
            updated_dispatches = []
            created_dispatches = []
            for after in data.values():
                try:
                    before = tuple(old[after[0]].values())
                    if before != after:
                        updated_dispatches.append(
                            {"before": old[after[0]], "after": alliances[after[0]]}
                        )
                        update[after[0]] = after
                    del old[after[0]]
                except KeyError:
                    created_dispatches.append(alliances[after[0]])
                    update[after[0]] = after
            deleted_dispatches = []
            for deleted in old.values():
                deleted_dispatches.append(deleted)
                await execute_query(
                    "DELETE FROM alliances WHERE id = $1;", deleted["id"]
                )
            await execute_query_many(
                """
                INSERT INTO alliances (id, found_date, name, acronym, color, rank,
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
            if updated_dispatches:
                await dispatch(
                    "bulk_alliance_update",
                    str(time),
                    data=updated_dispatches,
                )
            if created_dispatches:
                await dispatch(
                    "bulk_alliance_create", str(time), data=created_dispatches
                )
            if deleted_dispatches:
                await dispatch(
                    "bulk_alliance_delete", str(time), data=deleted_dispatches
                )
    except Exception as error:
        print("Ignoring exception in alliances:", file=sys.stderr)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_alliances.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=0)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


fetch_alliances.start()
