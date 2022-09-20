"""Update health topic charts and dynamic text"""

from scripts.health.dynamic_text import update_dynamic_text
from scripts.health.overview_charts import update_health_overview_charts
from scripts.health.topic_charts import update_health_topic_charts


def update_health_topic() -> None:
    """Update all health charts and dynamic text"""
    update_health_overview_charts()
    update_health_topic_charts()
    update_dynamic_text()
