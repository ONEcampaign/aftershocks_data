import pandas as pd
import requests
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.wfp import WFPData
from bblocks.import_tools.world_bank import WorldBankData

from scripts.common import CAUSES_OF_DEATH_YEAR
from scripts.config import PATHS
from scripts.country_page import (
    financial_security,
    food_security,
    health,
    health_update as hu,
)
from scripts.country_page.debt import debt_chart_country, debt_chart_region
from scripts.country_page.overview_text import build_summary
from scripts.explorers.common import base_africa_map
from scripts.logger import logger


def update_monthly_leading_causes_of_death() -> None:
    # Define year for data update
    request_year = CAUSES_OF_DEATH_YEAR

    dfs = []
    africa = base_africa_map().iso_code.to_list()

    for country in africa:
        d = requests.get(hu.get_ghe_url(country, request_year)).json()["value"]
        dfs.append(hu.unpack_ghe_country(country, d, request_year))

    df = pd.concat(dfs, ignore_index=True)

    df.to_csv(
        f"{PATHS.raw_data}/health/leading_causes_of_death_{request_year}.csv",
        index=False,
    )


def update_monthly_hiv_data() -> None:
    url = (
        "http://www.unaids.org/sites/default/files/media_asset/"
        "HIV_estimates_from_1990-to-present.xlsx"
    )

    files = pd.read_excel(url, sheet_name=[0, 1, 2, 3])

    # HIV country file
    df_hiv = hu.clean_hiv(files[0])
    df_hiv.to_csv(f"{PATHS.raw_data}/health/hiv_estimates.csv", index=False)

    # ART file
    df_art = hu.clean_art(files[2])
    df_art.to_csv(f"{PATHS.raw_data}/health/art_estimates.csv", index=False)


def update_monthly_malaria_data() -> None:
    indicator = "MALARIA_EST_DEATHS"
    indicator2 = "MALARIA_EST_MORTALITY"

    deaths = hu.unpack_malaria(indicator)
    mortality = hu.unpack_malaria(indicator2)

    df = pd.concat([deaths, mortality], ignore_index=True)

    df.to_csv(f"{PATHS.raw_data}/health/malaria_deaths.csv", index=False)


def update_daily_wfp_data() -> None:
    # Create a wfp object
    wfp = WFPData(data_path=PATHS.bblocks_data)

    # Load daily indicator
    wfp.load_indicator("insufficient_food")

    # Update data
    wfp.update()


def update_weekly_wfp_data() -> None:
    # Create a wfp object
    wfp = WFPData(data_path=PATHS.bblocks_data)

    # Load weekly indicator
    wfp.load_indicator("inflation")

    # Update data
    wfp.update()


def update_monthly_weo_data() -> None:
    """Update the WEO data. Monthly schedule though it updates twice a year"""

    # create object
    weo = WorldEconomicOutlook(data_path=PATHS.bblocks_data)

    # update data
    weo.update()


def update_monthly_wb_data() -> None:
    """Update the World Bank data. Monthly schedule"""
    import time

    # create object
    wb = WorldBankData(data_path=PATHS.bblocks_data)

    # Load indicators
    for _ in financial_security.WB_INDICATORS:
        wb.load_indicator(_)

    # update full timeseries data
    wb.update()

    time.sleep(180)

    # create new object
    wb_recent = WorldBankData(data_path=PATHS.bblocks_data)

    # Load only most recent data
    for _ in financial_security.WB_INDICATORS:
        wb_recent.load_indicator(_, most_recent_only=True)

    wb_recent.update()


def update_monthly_debt_data() -> None:
    url: str = (
        "https://onecampaign.github.io/project_covid-19_tracker/c07_debt_service_ts.csv"
    )

    debt = pd.read_csv(url, usecols=["year", "country_name", "Total"])

    debt.to_csv(f"{PATHS.raw_data}/debt/tracker_debt_service.csv", index=False)


def update_daily() -> None:
    """Update all data that is updated daily"""

    # Update underlying data
    update_daily_wfp_data()
    logger.info("Updated WFP data (insufficient food)")

    # Update related charts
    food_security.wfp_insufficient_food_single_measure()
    food_security.insufficient_food_chart()
    logger.info("Updated WFP insufficient food charts")

    # Update charts for which underlying data is updated elsewhere
    health.vaccination_rate_single_measure()
    logger.info("Updated vaccination rate chart")

    # Update live text
    build_summary()
    logger.info("Updated overview text (summary) json")


def update_weekly() -> None:
    """Update all data that is updated weekly"""
    update_weekly_wfp_data()

    # Update related charts
    financial_security.inflation_overview()
    financial_security.inflation_overview_regions()

    financial_security.inflation_ts_chart()
    food_security.food_inflation_chart()


def update_monthly() -> None:
    """Update all data that is updated monthly"""

    # ------- update underlying data-----------

    # Financial
    update_monthly_weo_data()
    update_monthly_wb_data()
    update_monthly_debt_data()

    # Health
    update_monthly_leading_causes_of_death()
    update_monthly_hiv_data()
    update_monthly_malaria_data()

    # ------- update related charts-----------

    # Financial security
    financial_security.gdp_growth_single_measure()
    financial_security.gdp_growth_regions_single_measure()
    financial_security.poverty_chart()
    financial_security.wb_poverty_single_measure()
    debt_chart_country()
    debt_chart_region()

    # Health
    health.leading_causes_of_death_chart()
    health.leading_causes_of_death_column_chart()
    health.life_expectancy_chart()
    health.art_chart()
    health.malaria_chart()
    health.dpt_chart()
