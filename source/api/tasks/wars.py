import asyncio
from datetime import datetime, timedelta

import aiohttp
from discord.ext import tasks
from discord.utils import sleep_until

from ...data.db import execute_query_many, execute_read_query
from ...env import APIKEY, GQLURL, UPDATE_TIMES
from ...events import dispatch

query = """
    {
        wars(active: false, days_ago: 7) {
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
        }
    }
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
    async with aiohttp.request("GET", GQLURL, json={"query": query}) as response:
        return await response.json()


async def request2():
    async with aiohttp.request(
        "GET",
        GQLURL,
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
        GQLURL,
        json={
            "query": attack_query.replace(
                "active: true, days_ago: 6", "active: false, days_ago: 1"
            )
        },
    ) as response:
        return await response.json()


@tasks.loop(minutes=2)
async def fetch_wars():
    min_war_attack_id = (
        await execute_read_query("SELECT min(id) FROM attacksUPDATE;")
    )[0][0]
    time = datetime.utcnow()
    data, data3 = await asyncio.gather(request(), request3(min_war_attack_id))
    data = data["data"]["wars"]
    # data2 = data2["data"]["wars"]
    # data = [*data, *data2]
    data3 = data3["war_attacks"]
    # data4 = data4["data"]["wars"]
    # data3 = [*data3, *data4]
    raw_wars = {int(war["id"]): war for war in data}
    wars = {
        int(war["id"]): (
            int(war["id"]),
            war["date"],
            war["reason"],
            war["war_type"],
            True,
            int(war["groundcontrol"]),
            int(war["airsuperiority"]),
            int(war["navalblockade"]),
            int(war["winner"]),
            int(war["turnsleft"]),
            int(war["attid"]),
            int(war["att_alliance_id"]),
            int(war["defid"]),
            int(war["def_alliance_id"]),
            int(war["attpoints"]),
            int(war["defpoints"]),
            war["attpeace"],
            war["defpeace"],
            int(war["att_resistance"]),
            int(war["def_resistance"]),
            war["att_fortify"],
            war["def_fortify"],
            float(war["att_gas_used"]),
            float(war["def_gas_used"]),
            float(war["att_mun_used"]),
            float(war["def_mun_used"]),
            int(war["att_alum_used"]),
            int(war["def_alum_used"]),
            int(war["att_steel_used"]),
            int(war["def_steel_used"]),
            float(war["att_infra_destroyed"]),
            float(war["def_infra_destroyed"]),
            float(war["att_money_looted"]),
            float(war["def_money_looted"]),
            int(war["att_soldiers_killed"]),
            int(war["def_soldiers_killed"]),
            int(war["att_tanks_killed"]),
            int(war["def_tanks_killed"]),
            int(war["att_aircraft_killed"]),
            int(war["def_aircraft_killed"]),
            int(war["att_ships_killed"]),
            int(war["def_ships_killed"]),
            int(war["att_missiles_used"]),
            int(war["def_missiles_used"]),
            int(war["att_nukes_used"]),
            int(war["def_nukes_used"]),
            float(war["att_infra_destroyed_value"]),
            float(war["def_infra_destroyed_value"]),
        )
        for war in data
    }
    attacks = {
        int(attack["war_attack_id"]): (
            int(attack["war_attack_id"]),
            int(attack["war_id"]),
            attack["date"],
            attack["attack_type"],
            int(attack["victor"]),
            int(attack["success"]),
            int(attack["attcas1"]),
            int(attack["defcas1"]),
            int(attack["attcas2"]),
            int(attack["defcas2"]),
            int(attack["city_id"]),
            float(attack["infra_destroyed"]),
            int(attack["improvements_destroyed"]),
            float(attack["money_looted"]),
            attack["note"],
            int(attack["note"]) if attack["note"].isdigit() else attack["note"],
            float(attack["city_infra_before"]),
            float(attack["infra_destroyed_value"]),
            float(attack["att_mun_used"]),
            float(attack["def_mun_used"]),
            float(attack["att_gas_used"]),
            float(attack["def_gas_used"]),
            int(attack["aircraft_killed_by_tanks"])
            if attack["aircraft_killed_by_tanks"] is not None
            else None,
        )
        for attack in data3
    }
    raw_attacks = {
        attack[0]: {
            "id": attack[0],
            "war_id": attack[1],
            "date": attack[2],
            "type": attack[3],
            "victor": attack[4],
            "success": attack[5],
            "attcas1": attack[6],
            "defcas1": attack[7],
            "attcas2": attack[8],
            "defcas2": attack[9],
            "city_id": attack[10],
            "infra_destroyed": attack[11],
            "improvements_lost": attack[12],
            "money_stolen": attack[13],
            "loot_info": attack[14],
            "resistance_eliminated": attack[15],
            "city_infra_before": attack[16],
            "infra_destroyed_value": attack[17],
            "attacker_munitions_used": attack[18],
            "defender_munitions_used": attack[19],
            "attacker_gas_used": attack[20],
            "defender_gas_used": attack[21],
            "aircraft_killed_by_tanks": attack[22],
        }
        for attack in attacks.values()
    }
    old_attacks = await execute_read_query("SELECT id FROM attacksUPDATE;")
    old_attacks = [i["id"] for i in old_attacks]
    attack_data = {}
    war_data = {}
    old_wars = await execute_read_query("SELECT * FROM warsupdate;")
    old_wars = [dict(i) for i in old_wars]
    old_wars = {i["id"]: i for i in old_wars}
    for attack in attacks.values():
        if attack[0] not in old_attacks:
            await dispatch("attack", str(time), attack=raw_attacks[attack[0]])
            attack_data[attack[0]] = attack
    for after in wars.values():
        try:
            before = tuple(old_wars[after[0]])
        except KeyError:
            await dispatch("war_declaration", str(time), war=raw_wars[after[0]])
            war_data[after[0]] = after
            continue
        if before != after:
            await dispatch(
                "war_update",
                str(time),
                before=old_wars[after[0]],
                after=raw_wars[after[0]],
            )
            war_data[after[0]] = after
    await execute_query_many(
        """
        INSERT INTO warsUPDATE (id, date, reason, war_type, active, ground_control,
        air_superiority, naval_blockade, winner, turns_left, attacker_id,
        attacker_alliance_id, defender_id, defender_alliance_id, attacker_points,
        defender_points, attacker_peace, defender_peace, attacker_resistance,
        defender_resistance, attacker_fortify, defender_fortify, attacker_gas_used,
        defender_gas_used, attacker_munitions_used, defender_munitions_used,
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
        attacker_gas_used = $23,
        defender_gas_used = $24,
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
        war_data.values(),
    )
    await execute_query_many(
        """
        INSERT INTO attacksUPDATE (id, war_id, date, type, victor, success, attcas1,
        defcas1, attcas2, defcas2, city_id, infra_destroyed, improvements_lost,
        money_stolen, loot_info, resistance_eliminated, city_infra_before,
        infra_destroyed_value, attacker_munitions_used, defender_munitions_used,
        attacker_gas_used, defender_gas_used, aircraft_killed_by_tanks) VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        $17, $18, $19, $20, $21, $22, $23)
        ON CONFLICT (id) DO NOTHING;
    """,
        attack_data.values(),
    )
    await UPDATE_TIMES.set_wars(time)


@fetch_wars.before_loop
async def before_loop():
    now = datetime.utcnow()
    wait = now.replace(minute=0, second=8)
    while wait < now:
        wait += timedelta(minutes=2)
    print("wait", "wars", wait)
    await sleep_until(wait)


# fetch_wars.add_exception_type(Exception)
fetch_wars.start()
