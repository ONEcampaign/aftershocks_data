import pandas as pd
from bblocks import add_short_names_column, convert_id, set_bblocks_data_path
from bblocks.dataframe_tools.add import add_gdp_column, add_gov_expenditure_column

from scripts.config import PATHS
from scripts.debt import common
from scripts.debt.common import (
    education_expenditure_share,
    health_expenditure_share,
    read_dservice_data,
    read_dstocks_data,
)
from scripts.debt.overview_charts import CURRENT_YEAR

set_bblocks_data_path(PATHS.bblocks_data)

SOURCE = "International Debt Statistics (IDS) Database"
DATE = " (December 2023)"


def debt_stocks_columns() -> None:
    """Bar chart of debt stocks by country"""

    df = (
        pd.read_feather(PATHS.raw_debt + r"/debt_stocks-ts.feather")
        .replace("C.A.R", "Central African Republic")
        .replace("D.R.C", "Democratic Republic of the Congo")
        .assign(
            iso_code=lambda d: convert_id(
                d.iso_code, from_type="regex", to_type="name_short"
            )
        )
    )

    africa = (
        df.groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .assign(iso_code="Africa")
    )

    df = pd.concat([africa, df], ignore_index=True).filter(
        [
            "iso_code",
            "year",
            "Bilateral (China)",
            "Bilateral (excl. China)",
            "Multilateral",
            "Private (China)",
            "Private (excl. China)",
            "Total",
        ],
        axis=1,
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_stocks_ts.csv", index=False)

    # download version
    df.assign(source=f"{SOURCE}{DATE}").to_csv(
        f"{PATHS.download}/debt_topic/debt_stocks_ts.csv", index=False
    )


def debt_service_columns() -> None:
    """Bar chart of debt stocks by country"""

    df = (
        pd.read_feather(PATHS.raw_debt + r"/debt_service_ts.feather")
        .replace("C.A.R", "Central African Republic")
        .replace("D.R.C", "Democratic Republic of the Congo")
        .assign(
            iso_code=lambda d: convert_id(
                d.iso_code, from_type="regex", to_type="name_short"
            )
        )
    )

    africa = (
        df.groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .assign(iso_code="Africa")
    )

    df = (
        pd.concat([africa, df], ignore_index=True)
        .filter(
            [
                "iso_code",
                "year",
                "Bilateral",
                "Multilateral",
                "Private",
            ],
            axis=1,
        )
        .loc[lambda d: d.year <= (CURRENT_YEAR + 5)]
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_service_ts.csv", index=False)

    # download version
    df.assign(source=f"{SOURCE}{DATE}").to_csv(
        f"{PATHS.download}/debt_topic/debt_service_ts.csv", index=False
    )


def debt_to_gdp_ts() -> None:
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
        .assign(Total=lambda d: d.Total * 1e6)
    )

    africa = (
        df.groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .assign(iso_code="Africa")
    )

    df = (
        pd.concat([africa, df], ignore_index=True)
        .assign(gdp_share=lambda d: round(d.Total / d.gdp, 5))
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .rename(columns={"gdp_share": "Debt to GDP ratio"})
        .filter(["name_short", "year", "Debt to GDP ratio"], axis=1)
        .pivot(index="year", columns="name_short", values="Debt to GDP ratio")
        .reset_index()
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_gdp_ratio_country_ts.csv", index=False)

    # download version
    df.assign(source=f"{SOURCE}{DATE}").to_csv(
        f"{PATHS.download}/debt_topic/debt_gdp_ratio_country_ts.csv", index=False
    )


def read_debt_chart_data() -> pd.DataFrame:
    return (
        pd.read_feather(f"{PATHS.raw_debt}/ids_tableau.feather")
        .assign(
            stocks_type=lambda d: d["Series Id"].map(common.DEBT_STOCKS),
            service_type=lambda d: d["Series Id"].map(common.DEBT_SERVICE),
        )
        .assign(
            continent=lambda d: convert_id(
                d.Country, from_type="regex", to_type="continent"
            ),
            Country=lambda d: convert_id(
                d.Country, from_type="regex", to_type="name_short"
            ),
        )
        .loc[lambda d: d.continent == "Africa"]
        .loc[lambda d: d.Creditors != "World"]
    )


def debt_composition_chart() -> None:
    df = (
        read_debt_chart_data()
        .loc[lambda d: d.time == 2022]
        .dropna(subset=["stocks_type"])
        .groupby(["time", "Country", "Creditors", "stocks_type"], as_index=False)
        .sum(numeric_only=True)
        .filter(["Country", "Creditors", "stocks_type", "value"], axis=1)
        .rename(
            columns={
                "stocks_type": "Creditor Type",
                "value": "US$",
                "Creditors": "Creditor",
            }
        )
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_composition_country.csv", index=False)

    # download version
    df.assign(source=f"{SOURCE}{DATE}").to_csv(
        f"{PATHS.download}/debt_topic/debt_composition_country.csv", index=False
    )


def debt_to_china_chart() -> None:
    df = (
        read_debt_chart_data()
        .dropna(subset=["stocks_type"])
        .groupby(["time", "Country", "Creditors", "stocks_type"], as_index=False)
        .sum(numeric_only=True)
        .filter(["time", "Country", "Creditors", "stocks_type", "value"], axis=1)
        .loc[lambda d: d.Creditors == "China"]
        .rename(
            columns={
                "time": "Year",
                "stocks_type": "Creditor Type",
                "value": "US$",
                "Creditors": "Creditor",
            }
        )
        .pivot(index=["Year", "Country"], columns="Creditor Type", values="US$")
        .reset_index()
    )

    africa = (
        df.groupby(["Year"], as_index=False)
        .sum(numeric_only=True)
        .assign(Country="Africa")
    )

    df = pd.concat([africa, df], ignore_index=True).loc[
        lambda d: d[["Bilateral", "Private"]].sum(axis=1) > 0
    ]

    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_to_china_country.csv", index=False)

    # download version
    df.assign(source=f"{SOURCE}{DATE}").to_csv(
        f"{PATHS.download}/debt_topic/debt_to_china_country.csv", index=False
    )


def debt_service_comparison_chart() -> None:
    edu = education_expenditure_share()
    health = health_expenditure_share()
    comparison = edu.merge(
        health, on=["iso_code", "year"], how="outer", suffixes=("_edu", "_health")
    )

    df = (
        read_dservice_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .pipe(
            add_gov_expenditure_column,
            id_column="iso_code",
            id_type="ISO3",
            date_column="year",
            usd=True,
            include_estimates=True,
        )
        .dropna(subset=["Total", "gov_exp"], how="any")
        .assign(Total=lambda d: d.Total * 1e6)
    )

    df = (
        df.merge(comparison, on=["year", "iso_code"], how="left")
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(share=lambda d: round(100 * d.Total / d.gov_exp, 2))
    )

    africa = (
        df.groupby(["year"], as_index=False)
        .median(numeric_only=True)
        .assign(name_short="Africa (median)")
    )

    df = (
        pd.concat([africa, df], ignore_index=True)
        .filter(["name_short", "year", "share", "value_edu", "value_health"], axis=1)
        .rename(
            columns={
                "share": "Debt service",
                "year": "Year",
                "value_edu": "Education expenditure",
                "value_health": "Health expenditure",
                "name_short": "Country",
            }
        )
    )
    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_service_comparison.csv", index=False)

    # download version
    df.assign(source=f"{SOURCE}{DATE}").to_csv(
        f"{PATHS.download}/debt_topic/debt_service_comparison.csv", index=False
    )


def update_debt_country_charts() -> None:
    debt_stocks_columns()
    debt_service_columns()
    debt_to_gdp_ts()
    debt_composition_chart()
    debt_to_china_chart()
    debt_service_comparison_chart()


if __name__ == "__main__":
    update_debt_country_charts()
