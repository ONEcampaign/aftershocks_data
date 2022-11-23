"""Update data chats and text for hunger topic"""

from scripts.hunger.dynamic_text import update_hunger_dynamic_text
from scripts.hunger.overview_charts import update_hunger_overview_charts
from scripts.hunger.topic_charts import update_hunger_topic_charts
from scripts.hunger.ipc import IPC
from scripts.config import PATHS
import os
from scripts.logger import logger
from bblocks.import_tools.world_bank import PinkSheet, WorldBankData
from scripts.hunger.common import get_insufficient_food, wb_indicators


# --- DAILY UPDATE ---


def update_daily_hunger_data() -> None:
    """Update daily data for hunger topic"""

    # update IPC data - temporarily switch off
    # ipc = IPC(api_key=os.environ.get("IPC_API"))
    # df = ipc.get_ipc_ch_data()
    # df.to_csv(f"{PATHS.raw_data}/hunger/ipc.csv", index=False)
    # logger.info("Updated IPC data")

    # update pink sheet
    pink_sheet = (
        PinkSheet(data_path=PATHS.bblocks_data)
        .load_indicator(indicator="prices")
        .get_data()
    )
    pink_sheet.to_csv(f"{PATHS.raw_data}/hunger/pink_sheet.csv", index=False)
    logger.info("Updated Pink Sheet data")

    # update WFP data
    wfp_data = get_insufficient_food()
    wfp_data.to_csv(f"{PATHS.raw_data}/hunger/wfp.csv", index=False)
    logger.info("Updated WFP data")


# --- Monthly update ---


def update_monthly_hunger_data() -> None:
    """Update monthly data for hunger topic"""

    # world bank
    wb = WorldBankData(data_path=PATHS.bblocks_data)
    for code in wb_indicators:
        (
            wb.load_indicator(code)
            .get_data(code)
            .to_csv(f"{PATHS.raw_data}/hunger/{code}.csv", index=False)
        )
        logger.info(f"Updated {code} data")


# --- Chart and text update ---


def update_charts_and_text() -> None:
    """Update all charts and text on hunger page"""

    update_hunger_topic_charts()
    logger.info("Updated hunger topic charts")
    update_hunger_overview_charts()
    logger.info("Updated hunger overview charts")
    update_hunger_dynamic_text()
    logger.info("Updated hunger dynamic text")
