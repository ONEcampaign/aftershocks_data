from scripts.country_page import update as update_country_page
from scripts.logger import logger

from scripts.debt import update as update_debt


def country_page_weekly():
    """Update daily charts in country page"""

    # Run update scripts
    update_country_page.update_weekly()
    logger.info("Updated weekly charts in country page")


def debt_page_weekly():
    """Update weekly charts in debt page"""

    # Run update scripts
    update_debt.update_debt_weekly()
    logger.info("Updated weekly charts in debt page")


if __name__ == "__main__":
    debt_page_weekly()
    country_page_weekly()
