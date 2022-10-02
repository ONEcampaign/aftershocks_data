from .country_page import update as update_country_page
from .logger import logger


def country_page_weekly():
    """Update daily charts in country page"""

    # Run update scripts
    update_country_page.update_weekly()
    logger.info("Updated weekly charts in country page")


if __name__ == "__main__":
    ...
