import asyncio
import sys
import traceback
from datetime import datetime, timedelta

import aiohttp
import pnwkit
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query_many, execute_read_query
from ...env import APIKEY, GQL_URL, UPDATE_TIMES
from ...events import dispatch

query = """
    id
    date
    reason
    war_type
    groundcontrol
    airsuperiority
    navalblockade
    winner
    turnsleft
    attid
    att_alliance_id
    defid
    def_alliance_id
    attpoints
    defpoints
    attpeace
    defpeace
    att_resistance
    def_resistance
    att_fortify
    def_fortify
    att_gas_used
    def_gas_used
    att_mun_used
    def_mun_used
    att_alum_used
    def_alum_used
    att_steel_used
    def_steel_used
    att_infra_destroyed
    def_infra_destroyed
    att_money_looted
    def_money_looted
    att_soldiers_killed
    def_soldiers_killed
    att_tanks_killed
    def_tanks_killed
    att_aircraft_killed
    def_aircraft_killed
    att_ships_killed
    def_ships_killed
    att_missiles_used
    def_missiles_used
    att_nukes_used
    def_nukes_used
    att_infra_destroyed_value
    def_infra_destroyed_value
"""
attack_query = """
    {
        wars(active: false, days_ago: 7) {
            id
            attacks {
                id
                date
                type
                victor
                success
                attcas1
                defcas1
                attcas2
                defcas2
                cityid
                infradestroyed
                improvementslost
                moneystolen
                loot_info
                resistance_eliminated
                city_infra_before
                infra_destroyed_value
                att_mun_used
                def_mun_used
                att_gas_used
                def_gas_used
            }
        }
    }
"""


async def request():
    async with aiohttp.request("GET", GQL_URL, json={"query": query}) as response:
        return await response.json()


async def request2():
    async with aiohttp.request(
        "GET",
        GQL_URL,
        json={
            "query": query.replace(
                "active: true, days_ago: 6", "active: false, days_ago: 1"
            )
        },
    ) as response:
        return await response.json()


async def request3(min_war_attack_id):
    async with aiohttp.request(
        "GET",
        f"https://politicsandwar.com/api/war-attacks/key={APIKEY}&min_war_attack_id={min_war_attack_id}",
    ) as response:
        return await response.json()


async def request4():
    async with aiohttp.request(
        "GET",
        GQL_URL,
        json={
            "query": attack_query.replace(
                "active: true, days_ago: 6", "active: false, days_ago: 1"
            )
        },
    ) as response:
        return await response.json()


@tasks.loop(minutes=2)
async def fetch_wars():
    try:
        min_war_attack_id = (await execute_read_query("SELECT max(id) FROM attacks;"))[
            0
        ][0]
        min_war_id = (
            await execute_read_query(
                "SELECT max(id) FROM wars WHERE date >= $1;",
                str(datetime.utcnow() - timedelta(days=7)),
            )
        )[0][0]
        time = datetime.utcnow()
        attacks = await request3(min_war_attack_id)
        # data2 = data2["data"]["wars"]
        # data = [*data, *data2]
        attacks = attacks["war_attacks"]
        # data4 = data4["data"]["wars"]
        # data3 = [*data3, *data4]
        wars = {
            int(war["id"]): {
                "id": int(war["id"]),
                "date": war["date"],
                "reason": war["reason"],
                "war_type": war["war_type"],
                "active": True,
                "ground_control": int(war["groundcontrol"]),
                "air_superiority": int(war["airsuperiority"]),
                "naval_blockade": int(war["navalblockade"]),
                "winner": int(war["winner"]),
                "turns_left": int(war["turnsleft"]),
                "attacker_id": int(war["attid"]),
                "attacker_alliance_id": int(war["att_alliance_id"]),
                "defender_id": int(war["defid"]),
                "defender_alliance_id": int(war["def_alliance_id"]),
                "attacker_points": int(war["attpoints"]),
                "defender_points": int(war["defpoints"]),
                "attacker_peace": war["attpeace"],
                "defender_peace": war["defpeace"],
                "attacker_resistance": int(war["att_resistance"]),
                "defender_resistance": int(war["def_resistance"]),
                "attacker_fortify": war["att_fortify"],
                "defender_fortify": war["def_fortify"],
                "attacker_gasoline_used": float(war["att_gas_used"]),
                "defender_gasoline_used": float(war["def_gas_used"]),
                "attacker_munitions_used": float(war["att_mun_used"]),
                "defender_munitions_used": float(war["def_mun_used"]),
                "attacker_aluminum_used": int(war["att_alum_used"]),
                "defender_aluminum_used": int(war["def_alum_used"]),
                "attacker_steel_used": int(war["att_steel_used"]),
                "defender_steel_used": int(war["def_steel_used"]),
                "attacker_infra_destroyed": float(war["att_infra_destroyed"]),
                "defender_infra_destroyed": float(war["def_infra_destroyed"]),
                "attacker_money_looted": float(war["att_money_looted"]),
                "defender_money_looted": float(war["def_money_looted"]),
                "attacker_soldiers_killed": int(war["att_soldiers_killed"]),
                "defender_soldiers_killed": int(war["def_soldiers_killed"]),
                "attacker_tanks_killed": int(war["att_tanks_killed"]),
                "defender_tanks_killed": int(war["def_tanks_killed"]),
                "attacker_aircraft_killed": int(war["att_aircraft_killed"]),
                "defender_aircraft_killed": int(war["def_aircraft_killed"]),
                "attacker_ships_killed": int(war["att_ships_killed"]),
                "defender_ships_killed": int(war["def_ships_killed"]),
                "attacker_missiles_used": int(war["att_missiles_used"]),
                "defender_missiles_used": int(war["def_missiles_used"]),
                "attacker_nukes_used": int(war["att_nukes_used"]),
                "defender_nukes_used": int(war["def_nukes_used"]),
                "attacker_infra_destroyed_value": float(
                    war["att_infra_destroyed_value"]
                ),
                "defender_infra_destroyed_value": float(
                    war["def_infra_destroyed_value"]
                ),
            }
            async for war in pnwkit.async_war_query(
                {"min_id": min_war_id}, query, paginator=True
            ).batch(5)
        }
        attacks = {
            int(attack["war_attack_id"]): {
                "id": int(attack["war_attack_id"]),
                "war_id": int(attack["war_id"]),
                "date": attack["date"],
                "attack_type": attack["attack_type"],
                "victor": int(attack["victor"]),
                "success": int(attack["success"]),
                "attcas1": int(attack["attcas1"]),
                "defcas1": int(attack["defcas1"]),
                "attcas2": int(attack["attcas2"]),
                "defcas2": int(attack["defcas2"]),
                "city_id": int(attack["city_id"]),
                "infra_destroyed": float(attack["infra_destroyed"]),
                "improvements_destroyed": int(attack["improvements_destroyed"]),
                "money_looted": float(attack["money_looted"]),
                "loot_info": attack["note"] if not attack["note"].isdigit() else None,
                "resistance_eliminated": int(attack["note"])
                if attack["note"].isdigit()
                else None,
                "city_infra_before": float(attack["city_infra_before"]),
                "infra_destroyed_value": float(attack["infra_destroyed_value"]),
                "attacker_munitions_used": float(attack["att_mun_used"]),
                "defender_munitions_used": float(attack["def_mun_used"]),
                "attacker_gasoline_used": float(attack["att_gas_used"]),
                "defender_gasoline_used": float(attack["def_gas_used"]),
                "aicraft_killed_by_tanks": int(attack["aircraft_killed_by_tanks"])
                if attack["aircraft_killed_by_tanks"] is not None
                else None,
            }
            for attack in attacks
        }
        attack_data = {}
        war_data = {}
        old_wars = await execute_read_query(
            "SELECT * FROM wars WHERE id >= $1;", min(wars)
        )
        old_wars = [dict(i) for i in old_wars]
        old_wars = {i["id"]: i for i in old_wars}
        attack_dispatches = []
        for attack in attacks.values():
            attack_dispatches.append(attacks[attack["id"]])
            attack_data[attack["id"]] = attack
        declaration_dispatches = []
        updated_dispatches = []
        for after in wars.values():
            try:
                before = dict(old_wars[after["id"]])
                if before != after:
                    updated_dispatches.append(
                        {"before": old_wars[after["id"]], "after": wars[after["id"]]}
                    )
                    war_data[after["id"]] = after
                del old_wars[after["id"]]
            except KeyError:
                declaration_dispatches.append(wars[after["id"]])
                war_data[after["id"]] = after
        await execute_query_many(
            """
            INSERT INTO wars (id, date, reason, war_type, active, ground_control,
            air_superiority, naval_blockade, winner, turns_left, attacker_id,
            attacker_alliance_id, defender_id, defender_alliance_id, attacker_points,
            defender_points, attacker_peace, defender_peace, attacker_resistance,
            defender_resistance, attacker_fortify, defender_fortify, attacker_gasoline_used,
            defender_gasoline_used, attacker_munitions_used, defender_munitions_used,
            attacker_aluminum_used, defender_aluminum_used, attacker_steel_used,
            defender_steel_used, attacker_infra_destroyed, defender_infra_destroyed,
            attacker_money_looted, defender_money_looted, attacker_soldiers_killed,
            defender_soldiers_killed, attacker_tanks_killed, defender_tanks_killed,
            attacker_aircraft_killed, defender_aircraft_killed, attacker_ships_killed,
            defender_ships_killed, attacker_missiles_used, defender_missiles_used,
            attacker_nukes_used, defender_nukes_used, attacker_infra_destroyed_value,
            defender_infra_destroyed_value) VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23,
            $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38,
            $39, $40, $41, $42, $43, $44, $45, $46, $47, $48) ON CONFLICT (id) DO UPDATE SET
            id = $1,
            date = $2,
            reason = $3,
            war_type = $4,
            active = $5,
            ground_control = $6,
            air_superiority = $7,
            naval_blockade = $8,
            winner = $9,
            turns_left = $10,
            attacker_id = $11,
            attacker_alliance_id = $12,
            defender_id = $13,
            defender_alliance_id = $14,
            attacker_points = $15,
            defender_points = $16,
            attacker_peace = $17,
            defender_peace = $18,
            attacker_resistance = $19,
            defender_resistance = $20,
            attacker_fortify = $21,
            defender_fortify = $22,
            attacker_gasoline_used = $23,
            defender_gasoline_used = $24,
            attacker_munitions_used = $25,
            defender_munitions_used = $26,
            attacker_aluminum_used = $27,
            defender_aluminum_used = $28,
            attacker_steel_used = $29,
            defender_steel_used = $30,
            attacker_infra_destroyed = $31,
            defender_infra_destroyed = $32,
            attacker_money_looted = $33,
            defender_money_looted = $34,
            attacker_soldiers_killed = $35,
            defender_soldiers_killed = $36,
            attacker_tanks_killed = $37,
            defender_tanks_killed = $38,
            attacker_aircraft_killed = $39,
            defender_aircraft_killed = $40,
            attacker_ships_killed = $41,
            defender_ships_killed = $42,
            attacker_missiles_used = $43,
            defender_missiles_used = $44,
            attacker_nukes_used = $45,
            defender_nukes_used = $46,
            attacker_infra_destroyed_value = $47,
            defender_infra_destroyed_value = $48;
        """,
            [tuple(i.values()) for i in war_data.values()],
        )
        await execute_query_many(
            """
            INSERT INTO attacks (id, war_id, date, type, victor, success, attcas1,
            defcas1, attcas2, defcas2, city_id, infra_destroyed, improvements_lost,
            money_stolen, loot_info, resistance_eliminated, city_infra_before,
            infra_destroyed_value, attacker_munitions_used, defender_munitions_used,
            attacker_gasoline_used, defender_gasoline_used, aircraft_killed_by_tanks) VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23)
            ON CONFLICT (id) DO NOTHING;
        """,
            [tuple(i.values()) for i in attack_data.values()],
        )
        await UPDATE_TIMES.set_wars(time)
        if attack_dispatches:
            await dispatch("bulk_attack", str(time), data=attack_dispatches)
        if declaration_dispatches:
            await dispatch("bulk_war_create", str(time), data=declaration_dispatches)
        if updated_dispatches:
            await dispatch(
                "bulk_war_update",
                str(time),
                data=updated_dispatches,
            )
    except Exception as error:
        print("Ignoring exception in wars:", file=sys.stderr, flush=True)
        traceback.print_exception(
            type(error), error, error.__traceback__, file=sys.stderr
        )


@fetch_wars.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=8)
    while wait < now:
        wait += timedelta(minutes=2)
    await sleep_until(wait)


fetch_wars.add_exception_type(Exception)
fetch_wars.start()
