from scripts.health.dynamic_text import update_dynamic_text
from scripts.health.overview_charts import update_wb_health_data, wb_health_charts
from scripts.health.topic_charts import update_health_topic_charts
from scripts.logger import logger

from scripts.owid_covid import tools as ot


def update_daily_health_data() -> None:
    """Update the underlying OWID health data"""
    ot.download_owid_data()
    logger.info("Updated OWID data")


def update_monthly_health_data() -> None:
    """Update data which only changes infrequently"""
    update_wb_health_data()


def update_daily_health_charts() -> None:
    """Update the charts after having updated the underlying data"""
    ...


def update_monthly_health_charts() -> None:
    """Update health charts which change infrequently"""
    # World Bank charts
    # TODO: these should be split into individual functions
    wb_health_charts()


def update_health_topic() -> None:
    """Update all health charts and dynamic text"""
    update_health_topic_charts()
    update_dynamic_text()
