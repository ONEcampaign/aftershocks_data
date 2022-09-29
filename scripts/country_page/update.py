from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.wfp import WFPData
from bblocks.import_tools.world_bank import WorldBankData

from scripts.country_page.financial_security import (
    inflation_overview,
    inflation_ts_chart,
    gdp_growth_single_measure,
    WB_INDICATORS,
    poverty_chart,
    wb_poverty_single_measure,
)


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


def update_daily() -> None:
    """Update all data that is updated daily"""

    # Update underlying data
    update_daily_wfp_data()

    # Update related charts
    inflation_overview()
    inflation_ts_chart()


def update_weekly() -> None:
    """Update all data that is updated weekly"""
    update_weekly_wfp_data()


def update_monthly() -> None:
    """Update all data that is updated monthly"""

    # Update underlying data
    update_monthly_weo_data()
    update_monthly_wb_data()

    # update related charts
    gdp_growth_single_measure()
    poverty_chart()
    wb_poverty_single_measure()
