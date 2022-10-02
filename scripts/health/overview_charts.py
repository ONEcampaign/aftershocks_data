import pandas as pd
from bblocks.import_tools.world_bank import WorldBankData

from scripts.common import clean_wb_overview
from scripts.config import PATHS
from scripts.health.common import get_malaria_data
from scripts.owid_covid import tools as owid_tools

from scripts.logger import logger

WORLD_BANK_INDICATORS = {
    "life_expectancy_overview": "SP.DYN.LE00.IN",
    "infant_mortality_overview": "SH.DYN.MORT",
    "health_expenditure_overview": "SH.XPD.CHEX.GD.ZS",
    "maternal_mortality_overview": "SH.STA.MMRT",
}


def update_wb_health_data() -> None:
    """Update World Bank health overview charts"""

    # Create object
    wb = WorldBankData()

    # Load indicators
    for code in WORLD_BANK_INDICATORS.values():
        wb.load_indicator(code)

    # Update charts
    wb.update()


def wb_health_charts() -> None:
    """Create World Bank overview charts"""

    wb = WorldBankData()

    for name, code in WORLD_BANK_INDICATORS.items():
        (
            wb.load_indicator(code)
            .get_data(code)
            .pipe(clean_wb_overview)
            .to_csv(f"{PATHS.charts}/health/{name}.csv", index=False)
        )
        logger.debug(f"Saved live version of {name}")


def vaccination_chart() -> None:
    """Create vaccination overview chart"""

    # covid vaccination overview
    (
        owid_tools.read_owid_data()
        .loc[lambda d: d["iso_code"].isin(["OWID_WRL", "OWID_AFR"])]
        .pipe(owid_tools.get_indicators_ts, "people_fully_vaccinated_per_hundred")
        .pivot(index="date", columns="iso_code", values="value")
        .round(2)
        .reset_index()
        .dropna(subset=["OWID_AFR", "OWID_WRL"])
        .rename(columns={"OWID_AFR": "Africa", "OWID_WRL": "World"})
        .to_csv(f"{PATHS.charts}/health/vaccination_overview.csv", index=False)
    )
    logger.debug("Saved live version of vaccination_overview.csv")


def malaria_chart() -> None:
    """Create malaria overview chart"""

    malaria_dict = get_malaria_data()
    df = pd.DataFrame(
        {
            "region": ["Africa", "Rest of world"],
            "total_deaths": [
                malaria_dict["malaria_africa_total"],
                malaria_dict["malaria_rest_of_world_total"],
            ],
            "proportion": [
                malaria_dict["malaria_africa_proportion"],
                malaria_dict["malaria_rest_of_world_proportion"],
            ],
            "year": [malaria_dict["malaria_year"], malaria_dict["malaria_year"]],
        }
    )

    # Save overview chart
    df.to_csv(f"{PATHS.charts}/health/malaria_overview.csv", index=False)

    logger.debug("Saved live version of malaria_overview.csv")
