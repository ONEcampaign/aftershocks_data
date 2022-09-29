import pandas as pd
import requests
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.wfp import WFPData
from bblocks.import_tools.world_bank import WorldBankData

from scripts.common import CAUSES_OF_DEATH_YEAR
from scripts.config import PATHS
from scripts.country_page.debt import debt_chart
from scripts.country_page.financial_security import (
    inflation_overview,
    inflation_ts_chart,
    gdp_growth_single_measure,
    WB_INDICATORS,
    poverty_chart,
    wb_poverty_single_measure,
)
from scripts.country_page.food_security import (
    wfp_insufficient_food_single_measure,
    insufficient_food_chart,
    food_inflation_chart,
)
from scripts.country_page.health import (
    vaccination_rate_single_measure,
    leading_causes_of_death_chart,
    life_expectancy_chart,
    art_chart,
    malaria_chart,
    dpt_chart,
)
from scripts.country_page.health_update import (
    get_ghe_url,
    unpack_ghe_country,
    clean_hiv,
    clean_art,
    unpack_malaria,
)
from scripts.explorers.common import base_africa_map


def update_monthly_leading_causes_of_death() -> None:
    # Define year for data update
    request_year = CAUSES_OF_DEATH_YEAR

    dfs = []
    africa = base_africa_map().iso_code.to_list()

    for country in africa:
        d = requests.get(get_ghe_url(country, request_year)).json()["value"]
        dfs.append(unpack_ghe_country(country, d, request_year))

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
    df_hiv = clean_hiv(files[0])
    df_hiv.to_csv(f"{PATHS.raw_data}/health/hiv_estimates.csv", index=False)

    # ART file
    df_art = clean_art(files[2])
    df_art.to_csv(f"{PATHS.raw_data}/health/art_estimates.csv", index=False)


def update_monthly_malaria_data() -> None:
    indicator = "MALARIA_EST_DEATHS"
    indicator2 = "MALARIA_EST_MORTALITY"

    deaths = unpack_malaria(indicator)
    mortality = unpack_malaria(indicator2)

    df = pd.concat([deaths, mortality], ignore_index=True)

    df.to_csv(f"{PATHS.raw_data}/health/malaria_deaths.csv", index=False)


def update_daily_wfp_data() -> None:
    # Create a wfp object
    wfp = WFPData()

    # Load daily indicator
    wfp.load_indicator("insufficient_food")

    # Update data
    wfp.update()


def update_weekly_wfp_data() -> None:
    # Create a wfp object
    wfp = WFPData()

    # Load weekly indicator
    wfp.load_indicator("inflation")

    # Update data
    wfp.update()


def update_monthly_weo_data() -> None:
    """Update the WEO data. Monthly schedule though it updates twice a year"""

    # create object
    weo = WorldEconomicOutlook()

    # update data
    weo.update()


def update_monthly_wb_data() -> None:
    """Update the World Bank data. Monthly schedule"""
    import time

    # create object
    wb = WorldBankData()

    # Load indicators
    for _ in WB_INDICATORS:
        wb.load_indicator(_)

    # update full timeseries data
    wb.update()

    time.sleep(180)

    # create new object
    wb_recent = WorldBankData()

    # Load only most recent data
    for _ in WB_INDICATORS:
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

    # Update related charts
    wfp_insufficient_food_single_measure()
    insufficient_food_chart()

    # Update charts for which underlying data is updated elsewhere
    vaccination_rate_single_measure()


def update_weekly() -> None:
    """Update all data that is updated weekly"""
    update_weekly_wfp_data()

    # Update related charts
    inflation_overview()
    inflation_ts_chart()
    food_inflation_chart()


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
    gdp_growth_single_measure()
    poverty_chart()
    wb_poverty_single_measure()
    debt_chart()

    # Health
    leading_causes_of_death_chart()
    life_expectancy_chart()
    art_chart()
    malaria_chart()
    dpt_chart()
