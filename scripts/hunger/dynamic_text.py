"""Create dynamic text for the hunger topic"""

from bblocks.import_tools.world_bank import WorldBankData
from scripts.hunger.ipc import IPC
from scripts.hunger.common import get_insufficient_food, aggregate_insufficient_food
import datetime
from scripts.config import PATHS
import json
import os


def stunting() -> dict:
    """Stunting dynamic text"""

    wb = WorldBankData()
    wb.load_indicator("SH.STA.STNT.ME.ZS")

    df = (
        wb.get_data("SH.STA.STNT.ME.ZS")
        .dropna(subset=["value"])
        .assign(date=lambda d: d.date.dt.year)
        .round({"value": 0})
    )

    ssa_value = f'{df.loc[df.iso_code == "SSA", "value"].iloc[-1]:.0f}'
    ssa_date = f'{df.loc[df.iso_code == "SSA", "date"].iloc[-1]}'
    ssa_value_2000 = f'{df.loc[(df.iso_code == "SSA") & (df.date == 2000), "value"].iloc[-1]:.0f}'
    world_value = f'{df.loc[df.iso_code == "WLD", "value"].iloc[-1]:.0f}'

    return {
        "stunting_ssa_value": ssa_value,
        "stunting_ssa_date": ssa_date,
        "stunting_world_value": world_value,
        "stunting_ssa_2000_value": ssa_value_2000,
    }


def ipc_dynamic(ipc) -> dict:
    """IPC hunger phases dynamic text"""

    return {
        "phase3plus_world_value": f'{sum(ipc.phase_3plus)/ 1000000:.2f}',
        "phase5_world_millions": f'{sum(ipc.phase_5) / 1000000:.2f}',
    }


def insufficient_food_dynamic() -> dict:
    """Insufficient food dynamic text"""

    wfp_data = get_insufficient_food()
    latest_date = wfp_data["date"].max()
    month_date = latest_date - datetime.timedelta(days=30)

    latest_value = aggregate_insufficient_food(wfp_data, latest_date, "date")
    month_value = aggregate_insufficient_food(wfp_data, month_date, "date")
    change = ((latest_value - month_value) / month_value) * 100

    return {
        "insufficient_food_latest_value": f'{latest_value / 1000000:.2f}',
        "insufficient_food_month_change": f'{change:.2f}',
        "insufficient_food_date": latest_date.strftime("%d %b %Y"),
    }


def update_hunger_dynamic_text() -> None:
    """Update all dynamic text"""

    d = {}

    ipc = IPC(api_key=os.environ.get('IPC_API'))
    ipc_df = ipc.get_ipc_ch_data()
    d.update(ipc_dynamic(ipc_df))

    d.update(stunting())
    d.update(insufficient_food_dynamic())

    with open(f"{PATHS.charts}/hunger_topic/key_numbers.json", "w") as file:
        json.dump(d, file, indent=4)
