from bblocks.import_tools.debt.common import get_dsa

from scripts.config import PATHS
from scripts.logger import logger


def update_weekly_data() -> None:

    _ = get_dsa(update=True, local_path=f"{PATHS.raw_data}/debt/dsa_list.pdf")
    logger.info("Updated DSA list data")
