from datetime import datetime, timedelta
import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until
from ...env import GQLURL, UPDATE_TIMES
from ...data.db import execute_read_query, execute_query_many
from ...events import dispatch


@tasks.loop(minutes=2)
async def fetch_trades():
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
        old = await execute_read_query("SELECT id FROM completed_tradesUPDATE;")
        old = [i["id"] for i in old]
        update = {}
        for trade in data.values():
            if trade[0] not in old:
                await dispatch("trade_completed", str(time), trade=raw_trades[trade[0]])
                update[trade[0]] = trade
        await execute_query_many(
            """
            INSERT INTO completed_tradesUPDATE (id, date, sender, receiver, offer_resource,
            offer_amount, buy_or_sell, total, accepted) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO NOTHING;
        """,
            update.values(),
        )
        await UPDATE_TIMES.set_completed_trades(time)


@fetch_trades.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=2)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


fetch_trades.add_exception_type(Exception)
fetch_trades.start()
