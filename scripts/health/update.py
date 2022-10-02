from scripts.health import dynamic_text as health_dynamic_text
from scripts.health import overview_charts as health_overview_charts

from scripts.health import topic_charts as health_topic
from scripts.health import common as health_common
from scripts.logger import logger

from scripts.owid_covid import tools as ot


# --- DAILY UPDATE ---
def update_daily_health_data() -> None:
    """Update the underlying OWID health data"""
    ot.download_owid_data()
    logger.info("Updated OWID data")


def update_daily_health_charts() -> None:
    """Update the charts after having updated the underlying data"""
    # OWID charts
    health_overview_charts.vaccination_chart()

    # Overview chart
    health_dynamic_text.update_dynamic_text()


# --- MONTHLY UPDATE ---
def update_monthly_health_data() -> None:
    """Update data which only changes infrequently"""
    # World Bank
    health_overview_charts.update_wb_health_data()
    health_topic.wb_spending_topic_chart()

    # WHO
    health_topic.update_dtp_data()
    health_common.update_malaria_data()

    # IHME
    health_topic.update_ihme_data()


def update_monthly_health_charts() -> None:
    """Update health charts which change infrequently"""
    # World Bank charts
    # TODO: these should be split into individual functions
    health_overview_charts.wb_health_charts()

    # TODO: Convert this to a automatic update
    health_topic.hiv_topic_chart()

    # WHO charts
    health_topic.dtp_topic_chart()
    health_overview_charts.malaria_chart()

    # IHME
    health_topic.ihme_spending_topic_chart()


if __name__ == "__main__":
    update_daily_health_data()
    update_daily_health_charts()
    update_monthly_health_data()
    update_monthly_health_charts()
