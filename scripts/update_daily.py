from scripts.health.update import update_daily_health
from scripts.logger import logger
from scripts.country_page import update as update_country_page


def health_daily():
    """Update daily charts in health page"""

    # Run update scripts
    update_daily_health()
    logger.info("Updated daily charts in health page")


def country_page_daily():
    """Update daily charts in country page"""

    # Run update scripts
    update_country_page.update_daily()
    logger.info("Updated daily charts in country page")


if __name__ == "__main__":
    health_daily()
    country_page_daily()
