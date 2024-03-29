import sys
import traceback
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_query_many, execute_read_query
from ...env import GQL_URL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(minutes=2)
async def fetch_pending_trades():
    try:
        time = datetime.utcnow()
        query = """
            {
                trades (accepted: false, limit: 1000) {
                    id
                    date
                    sid
                    rid
                    offer_resource
                    offer_amount
                    buy_or_sell
                    total
                    date_accepted
                }
            }
        """
        async with aiohttp.request("POST", GQL_URL, json={"query": query}) as response:
            data = await response.json()
            data = {
                int(i["id"]): {
                    "id": int(i["id"]),
                    "date": i["date"],
                    "sid": int(i["sid"]),
                    "rid": int(i["rid"]),
                    "offer_resource": i["offer_resource"],
                    "offer_amount": int(i["offer_amount"]),
                    "buy_or_sell": i["buy_or_sell"],
                    "total": int(i["total"]),
                    "date_accepted": i["date_accepted"],
                }
                for i in data["data"]["trades"]
            }
            old = await execute_read_query("SELECT * FROM pending_trades;")
            old = [dict(i) for i in old]
            old = {i["id"]: i for i in old}
            update = {}
            updated_dispatches = []
            created_dispatches = []
            for after in data.values():
                try:
                    before = dict(old[after["id"]])
                    if before != after:
                        updated_dispatches.append(
                            {"before": old[after["id"]], "after": data[after["id"]]}
                        )
                        update[after["id"]] = after
                    del old[after["id"]]
                except KeyError:
                    created_dispatches.append(data[after["id"]])
                    update[after["id"]] = after
            removed_dispatches = []
            for removed in old.values():
                removed_dispatches.append(removed)
                await execute_query(
                    "DELETE FROM pending_trades WHERE id = $1;", removed["id"]
                )
            await execute_query_many(
                """
                INSERT INTO pending_trades (id, date, sender, receiver, offer_resource,
                offer_amount, buy_or_sell, total, accepted) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (id) DO UPDATE SET
                id = $1,
                date = $2,
                sender = $3,
                receiver = $4,
                offer_resource = $5,
                offer_amount = $6,
                buy_or_sell = $7,
                total = $8,
                accepted = $9;
            """,
                [tuple(i.values()) for i in update.values()],
            )
            await UPDATE_TIMES.set_pending_trades(time)
            if updated_dispatches:
                await dispatch(
                    "bulk_pending_trade_update",
                    str(time),
                    data=updated_dispatches,
                )
            if created_dispatches:
                await dispatch(
                    "bulk_pending_trade_create",
                    str(time),
                    data=created_dispatches,
                )
            if removed_dispatches:
                await dispatch(
                    "bulk_pending_trade_delete", str(time), data=removed_dispatches
                )
    except Exception as error:
        print("Ignoring exception in pending_trades:", file=sys.stderr, flush=True)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_pending_trades.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=4)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


fetch_pending_trades.add_exception_type(Exception)
fetch_pending_trades.start()
