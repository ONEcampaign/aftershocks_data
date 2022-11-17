from scripts.common import update_key_number
from scripts.config import PATHS
from scripts.logger import logger
from scripts.oda.ukraine_oda_tracker import oda
from scripts.oda.ukraine_oda_tracker import unhcr


def key_numbers() -> None:
    refugees = oda.refugees_in_countries()
    kn = {
        "refugee_date": unhcr.read_refugee_date(),
        "refugee_data": unhcr.read_refugee_data(),
        "oda": oda.total_reported_oda(),
        "idrc": oda.total_idrc(),
        "idrc_share": oda.idrc_share(),
        "poland_refugees": f"{refugees['POL']:,.0f}",
        "moldova_refugees": f"{refugees['MDA']:,.0f}",
        "romania_refugees": f"{refugees['ROU']:,.0f}",
        "slovakia_refugees": f"{refugees['SVK']:,.0f}",
        "germany_refugees": f"{refugees['DEU']:,.0f}",
        "czechia_refugees": f"{refugees['CZE']:,.0f}",
    }

    update_key_number(f"{PATHS.charts}/oda_topic/ukraine_oda_key_numbers.json", kn)
    logger.debug(
        f"Updated dynamic text Ukraine ODA tracker page ukraine_oda_key_numbers.json"
    )



