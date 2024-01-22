import datetime

import pandas as pd
from bblocks import (
    add_short_names_column,
    convert_id,
    format_number,
    set_bblocks_data_path,
)
from bblocks.dataframe_tools.add import (
    add_gdp_column,
    add_gov_expenditure_column,
)
from bblocks.import_tools.debt.common import get_dsa

from scripts.common import update_key_number
from scripts.config import PATHS
from scripts.debt.common import read_dservice_data, read_dstocks_data
from scripts.logger import logger

set_bblocks_data_path(PATHS.bblocks_data)

KEY_NUMBERS: dict = {}

CURRENT_YEAR = datetime.datetime.now().year
STOCKS_YEAR = CURRENT_YEAR - 2


def debt_distress() -> None:
    """Update Debt Distress live number"""
    df = get_dsa(update=False, local_path=f"{PATHS.raw_data}/debt/dsa_list.pdf")

    df = df.assign(continent=lambda d: convert_id(d.country, to_type="continent")).loc[
        lambda d: d.risk_of_debt_distress.isin(["High", "In debt distress"])
    ]

    africa = df.loc[df.continent == "Africa"]

    # Dynamic text version
    kn = {
        "debt_distress_africa_share": (str(round(100 * len(africa) / len(df))) + "%"),
        "debt_distress_africa": str(len(africa)),
    }

    update_key_number(f"{PATHS.charts}/debt_topic/debt_key_numbers.json", kn)
    logger.debug("Updated debt file 'overview.json'")


def debt_service_africa_trend() -> None:
    """Debt Service trend overview chart"""

    df = (
        read_dservice_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .assign(Total=lambda d: d.Total * 1e6)
    )

    # Live version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_service_africa_trend.csv", index=False)
    logger.debug("Saved live debt file 'debt_service_africa_trend.csv'")

    # Dynamic text version
    number = format_number(
        df.loc[df.year == CURRENT_YEAR, "Total"], as_billions=True, decimals=1
    ).values[0]

    kn = {
        "debt_service_africa": f"{number} billion",
        "debt_service_year": f"{CURRENT_YEAR}",
    }
    update_key_number(f"{PATHS.charts}/debt_topic/debt_key_numbers.json", kn)
    logger.debug("Updated debt file 'overview.json'")


def debt_service_gov_spending() -> None:
    """Debt Service vs Government Spending debt chart"""

    df = (
        read_dservice_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .pipe(
            add_gov_expenditure_column,
            id_column="iso_code",
            date_column="year",
            usd=True,
            include_estimates=True,
        )
        .dropna(subset=["Total", "gov_exp"], how="any")
        .assign(Total=lambda d: d.Total * 1e6)
    )

    # Regional view
    africa = (
        df.groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .assign(iso_code="Africa")
    )

    df = (
        pd.concat([africa, df], ignore_index=True)
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(share=lambda d: d.Total / d.gov_exp)
        .filter(["year", "name_short", "share"], axis=1)
        .pivot(index="year", columns="name_short", values="share")
        .round(5)
        .reset_index()
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/dservice_to_gov_exp.csv", index=False)
    logger.debug("Saved live debt file 'dservice_to_gov_exp.csv'")

    # download version
    df.to_csv(f"{PATHS.download}/debt_topic/dservice_to_gov_exp.csv", index=False)
    logger.debug("Saved download debt file 'dservice_to_gov_exp.csv'")


def debt_to_gdp_trend() -> None:
    """Africa's debt to gdp overview chart"""

    df = (
        read_dstocks_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .loc[lambda d: d.year <= CURRENT_YEAR]
        .pipe(
            add_gdp_column,
            id_column="iso_code",
            id_type="ISO3",
            date_column="year",
            usd=True,
            include_estimates=True,
        )
        .groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .assign(
            Total=lambda d: d.Total * 1e6, gdp_share=lambda d: round(d.Total / d.gdp, 5)
        )
        .rename(columns={"gdp_share": "Debt to GDP ratio"})
    )

    # Live version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_gdp_africa_trend.csv", index=False)
    logger.debug("Saved live debt file 'debt_gdp_africa_trend.csv'")

    # Dynamic text version
    number = format_number(
        df.loc[df.year == df.year.max(), "Debt to GDP ratio"],
        as_percentage=True,
        decimals=1,
    ).values[0]

    kn = {"debt_to_gdp_africa": f"{number}"}
    update_key_number(f"{PATHS.charts}/debt_topic/debt_key_numbers.json", kn)
    logger.debug("Updated debt file 'overview.json'")


def debt_stocks_africa_trend() -> None:
    """Africa's debt to stock overview chart"""

    df = (
        read_dstocks_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .assign(Total=lambda d: d.Total * 1e6)
    )

    # Live version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_stocks_africa_trend.csv", index=False)
    logger.debug("Saved live debt file 'debt_stocks_africa_trend.csv'")

    # Dynamic text version
    number = format_number(
        df.loc[df.year == STOCKS_YEAR, "Total"], as_billions=True, decimals=1
    ).values[0]

    kn = {
        "debt_stocks_africa": f"{number} billion",
        "debt_stocks_year": f"{STOCKS_YEAR}",
    }

    update_key_number(f"{PATHS.charts}/debt_topic/debt_key_numbers.json", kn)
    logger.debug("Updated debt file 'overview.json'")


if __name__ == "__main__":
    debt_stocks_africa_trend()
