import sys
import traceback
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query_many, execute_read_query
from ...env import GQL_URL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(minutes=2)
async def fetch_trades():
    try:
        time = datetime.utcnow()
        query = """
            {
                trades (accepted: true, limit: 1000) {
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
            old = await execute_read_query(
                "SELECT id FROM completed_trades WHERE id > $1;", min(list(data))
            )
            old = [dict(i) for i in old]
            old: List[Dict[int, Dict[str, Any]]] = {i["id"]: i for i in old}  # type: ignore
            update = {}
            completed_dispatches = []
            for trade in data.values():
                if trade["id"] not in old:
                    completed_dispatches.append(data[trade["id"]])
                    update[trade["id"]] = trade
            await execute_query_many(
                """
                INSERT INTO completed_trades (id, date, sender, receiver, offer_resource,
                offer_amount, buy_or_sell, total, accepted) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (id) DO NOTHING;
            """,
                [tuple(i.values()) for i in update.values()],
            )
            await UPDATE_TIMES.set_completed_trades(time)
            if completed_dispatches:
                await dispatch(
                    "bulk_trade_complete", str(time), data=completed_dispatches
                )
    except Exception as error:
        print("Ignoring exception in completed_trades:", file=sys.stderr, flush=True)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_trades.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=2)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


fetch_trades.add_exception_type(Exception)
fetch_trades.start()
