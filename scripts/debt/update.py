from bblocks.import_tools.debt.common import get_dsa
from scripts.debt.common import update_debt_world_bank

from scripts.debt import overview_charts as debt_overview
from scripts.debt import ids_data, topic_page, dashboard


from scripts.config import PATHS
from scripts.logger import logger


def update_weekly_data() -> None:

    # Update DSA list
    _ = get_dsa(update=True, local_path=f"{PATHS.raw_data}/debt/dsa_list.pdf")
    logger.info("Updated DSA list data")

    # Update IDS data
    ids_data.update_ids_data()

    # Update raw data for Tableau
    dashboard.export_tableau_database()

    # Update other charts
    ids_data.update_flourish_charts()


def update_weekly_charts() -> None:

    # update DSA chart
    debt_overview.debt_distress()

    # Update data from tracker
    debt_overview.debt_service_africa_trend()
    debt_overview.debt_stocks_africa_trend()
    debt_overview.debt_service_gov_spending()
    debt_overview.debt_to_gdp_trend()

    # Topic page
    topic_page.update_debt_country_charts()


def update_monthly_data() -> None:

    update_debt_world_bank()
    logger.info("Updated World Bank Health and Education spending data")


def update_debt_weekly() -> None:
    # Update data
    update_weekly_data()

    # Update charts
    update_weekly_charts()


def update_debt_monthly() -> None:
    # Update data
    update_monthly_data()
