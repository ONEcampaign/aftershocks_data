from .health import update as health_topic_update
from .logger import logger
from .country_page import update as update_country_page


def health_daily():
    """Update daily charts in health page"""

    # Run update scripts
    health_topic_update.update_daily()
    logger.info("Updated daily charts in health page")


def country_page_daily():
    """Update daily charts in country page"""

    # Run update scripts
    update_country_page.update_daily()
    logger.info("Updated daily charts in country page")


if __name__ == "__main__":
    health_daily()
    country_page_daily()
