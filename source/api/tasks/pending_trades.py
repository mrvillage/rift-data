from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query, execute_query_many, execute_read_query
from ...env import GQLURL, UPDATE_TIMES
from ...events import dispatch


@tasks.loop(minutes=2)
async def fetch_pending_trades():
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
    async with aiohttp.request("GET", GQLURL, json={"query": query}) as response:
        data = await response.json()
        data = {
            int(i["id"]): (
                int(i["id"]),
                i["date"],
                int(i["sid"]),
                int(i["rid"]),
                i["offer_resource"],
                int(i["offer_amount"]),
                i["buy_or_sell"],
                int(i["total"]),
                i["date_accepted"],
            )
            for i in data["data"]["trades"]
        }
        raw_trades = {int(i["id"]): i for i in data["data"]["trades"]}
        old = await execute_read_query("SELECT * FROM pending_tradesUPDATE;")
        old = [dict(i) for i in old]
        old = {i["id"]: i for i in old}
        update = {}
        for after in data.values():
            try:
                before = tuple(old[after[0]].values())
                del old[after[0]]
            except KeyError:
                await dispatch(
                    "pending_trade_created", str(time), trade=raw_trades[after[0]]
                )
                update[after[0]] = after
                continue
            if before != after:
                await dispatch(
                    "pending_trade_update",
                    str(time),
                    before=old[after[0]],
                    after=raw_trades[after[0]],
                )
                update[after[0]] = after
        for removed in old.values():
            await dispatch("pending_trade_removed", str(time), trade=removed)
            await execute_query(
                "DELETE FROM pending_tradesUPDATE WHERE id = $1;", removed["id"]
            )
        await execute_query_many(
            """
            INSERT INTO pending_tradesUPDATE (id, date, sender, receiver, offer_resource,
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
            update.values(),
        )
        await UPDATE_TIMES.set_pending_trades(time)


@fetch_pending_trades.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=4)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


fetch_pending_trades.add_exception_type(Exception)
fetch_pending_trades.start()
