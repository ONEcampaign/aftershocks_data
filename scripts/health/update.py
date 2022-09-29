from scripts.health.dynamic_text import update_dynamic_text
from scripts.health.overview_charts import update_health_overview_charts
from scripts.health.topic_charts import update_health_topic_charts

from scripts.owid_covid import tools as ot


def update_daily_health() -> None:

    # Update data
    ot.download_owid_data()


def update_health_topic() -> None:
    """Update all health charts and dynamic text"""
    update_health_overview_charts()
    update_health_topic_charts()
    update_dynamic_text()
