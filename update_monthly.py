from scripts.health import update as health_topic_update
from scripts.country_page import update as update_country_page
from scripts.logger import logger
from scripts.debt import update as update_debt


def health_monthly():
    """Update monthly charts in health page"""

    # Run update scripts
    health_topic_update.update_monthly()
    logger.info("Updated monthly charts in health page")


def country_page_monthly():
    """Update monthly charts in country page"""

    # Run update scripts
    update_country_page.update_monthly()
    logger.info("Updated monthly charts in country page")


def debt_monthly():
    """Update monthly charts in debt page"""

    # Run update scripts
    update_debt.update_debt_monthly()
    logger.info("Updated monthly charts in debt page")


if __name__ == "__main__":
    health_monthly()
    debt_monthly()
    country_page_monthly()
