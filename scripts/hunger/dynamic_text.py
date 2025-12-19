"""Create dynamic text for the hunger topic"""

import datetime
import json

import pandas as pd
from bblocks import WorldBankData, set_bblocks_data_path

from scripts.config import PATHS
from scripts.hunger.common import aggregate_insufficient_food

set_bblocks_data_path(PATHS.bblocks_data)


def stunting() -> dict:
    """Stunting dynamic text"""

    wb = WorldBankData()
    wb.load_data("SH.STA.STNT.ME.ZS")

    df = pd.read_csv(
        f"{PATHS.raw_data}/hunger/SH.STA.STNT.ME.ZS.csv", parse_dates=["date"]
    )

    df = (
        df.dropna(subset=["value"])
        .assign(date=lambda d: d.date.dt.year)
        .round({"value": 0})
    )

    ssa_value = f"{df.loc[df.iso_code == 'SSA', 'value'].iloc[-1]:.0f}"
    ssa_date = f"{df.loc[df.iso_code == 'SSA', 'date'].iloc[-1]}"
    ssa_value_2000 = (
        f"{df.loc[(df.iso_code == 'SSA') & (df.date == 2000), 'value'].iloc[-1]:.0f}"
    )
    world_value = f"{df.loc[df.iso_code == 'WLD', 'value'].iloc[-1]:.0f}"

    return {
        "stunting_ssa_value": ssa_value,
        "stunting_ssa_date": ssa_date,
        "stunting_world_value": world_value,
        "stunting_ssa_2000_value": ssa_value_2000,
    }


def ipc_dynamic() -> dict:
    """IPC hunger phases dynamic text"""

    ipc = pd.read_csv(f"{PATHS.raw_data}/hunger/ipc.csv")

    return {
        "phase3plus_world_value": f"{sum(ipc.phase_3plus) / 1000000:.0f} million",
        "phase5_world_millions": f"{sum(ipc.phase_5) / 1000:.0f} thousand",
    }


def insufficient_food_dynamic() -> dict:
    """Insufficient food dynamic text"""

    wfp_data = pd.read_csv(f"{PATHS.raw_data}/hunger/wfp.csv", parse_dates=["date"])
    latest_date = wfp_data["date"].max()
    month_date = latest_date - datetime.timedelta(days=30)

    latest_value = aggregate_insufficient_food(wfp_data, latest_date, "date")
    month_value = aggregate_insufficient_food(wfp_data, month_date, "date")
    change = ((latest_value - month_value) / month_value) * 100

    return {
        "insufficient_food_latest_value": f"{latest_value / 1000000:.0f} million",
        "insufficient_food_month_change": f"{change:.1f}%",
        "insufficient_food_date": latest_date.strftime("%d %b %Y"),
    }


def update_hunger_dynamic_text() -> None:
    """Update all dynamic text"""

    d = {}

    d.update(ipc_dynamic())
    d.update(stunting())
    d.update(insufficient_food_dynamic())

    with open(f"{PATHS.charts}/hunger_topic/key_numbers.json", "w") as file:
        json.dump(d, file, indent=4)
