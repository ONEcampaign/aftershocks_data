from scripts.economy_picker.update_economy_picker import update_map_charts
from scripts.explorers.economics import econ_explorer
from scripts.explorers.health import health_explorer
from scripts.health import update as health_topic_update
from scripts.hunger import update as hunger_topic_update
from scripts.logger import logger
from scripts.country_page import update as update_country_page
from scripts.oda.ukraine_oda_tracker import dynamic_text as ukraine_oda_text


def health_daily():
    """Update daily charts in health page"""

    # Run update scripts
    health_topic_update.update_daily()
    logger.info("Updated daily charts in health page")


def hunger_update():
    """Update daily data and all charts on hunger page"""

    hunger_topic_update.update_daily_hunger_data()
    hunger_topic_update.update_charts_and_text()
    logger.info("Updated daily hunger data and all charts and text")


def country_page_daily():
    """Update daily charts in country page"""

    # Run update scripts
    update_country_page.update_daily()
    logger.info("Updated daily charts in country page")


def update_economy_picker():
    """Update economy picker"""

    # Run update scripts
    update_map_charts()
    logger.info("Updated economy picker")


def update_explorers():
    econ_explorer()
    health_explorer()


def update_other_pages():
    ukraine_oda_text.key_numbers()


if __name__ == "__main__":
    health_daily()
    hunger_update()
    country_page_daily()
    update_economy_picker()
    update_explorers()
    update_other_pages()
