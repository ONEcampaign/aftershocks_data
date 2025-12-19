from scripts.common import update_key_number
from scripts.config import PATHS
from scripts.logger import logger
from scripts.oda.ukraine_oda_tracker.oda import refugee_data


def key_numbers() -> None:
    data = refugee_data()

    update_key_number(
        f"{PATHS.charts}/oda_topic/ukraine_tracker_key_numbers.json", data
    )
    logger.debug(
        f"Updated dynamic text Ukraine ODA tracker page ukraine_tracker_key_numbers.json"
    )


if __name__ == "__main__":
    key_numbers()
