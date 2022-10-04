from scripts.economy_picker.site_country_picker import (
    map_data,
    base_map_data,
    bubble_data,
    base_bubble_data,
)
from scripts.logger import logger


def update_map_charts() -> None:
    map_data(base_map_data())
    logger.debug("Updated economy picker base map")

    bubble_data(base_bubble_data())
    logger.debug("Updated economy picker base bubble")
