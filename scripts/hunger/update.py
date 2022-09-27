""" """

from scripts.hunger.dynamic_text import update_hunger_dynamic_text
from scripts.hunger.overview_charts import update_hunger_overview_charts
from scripts.hunger.topic_charts import update_hunger_topic_charts


def update_hunger_topic():
    """ """

    update_hunger_topic_charts()
    update_hunger_overview_charts()
    update_hunger_dynamic_text()