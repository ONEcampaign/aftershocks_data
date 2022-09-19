import pandas as pd
from bblocks.cleaning_tools.clean import convert_id
from bblocks.dataframe_tools.add import add_gdp_column, add_short_names_column

from scripts.config import PATHS
from scripts.debt import common
from scripts.debt.common import read_dstocks_data
from scripts.debt.overview_charts import CURRENT_YEAR

STOCKS_URL: str = (
    "https://onecampaign.github.io/project_covid-19_tracker/c08_debt_stocks-ts.csv"
)

SERVICE_URL: str = (
    "https://onecampaign.github.io/project_covid-19_tracker/c07_debt_service_ts.csv"
)


def debt_stocks_columns() -> None:
    """Bar chart of debt stocks by country"""

    df = (
        pd.read_csv(STOCKS_URL)
        .replace("C.A.R", "Central African Republic")
        .replace("D.R.C", "Democratic Republic of the Congo")
        .assign(
            iso_code=lambda d: convert_id(
                d.iso_code, from_type="regex", to_type="name_short"
            )
        )
    )

    africa = df.groupby(["year"], as_index=False).sum().assign(iso_code="Africa")

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

    df.to_csv(f"{PATHS.charts}/debt_topic/debt_stocks_ts.csv", index=False)


def debt_service_columns() -> None:
    """Bar chart of debt stocks by country"""

    df = (
        pd.read_csv(SERVICE_URL)
        .replace("C.A.R", "Central African Republic")
        .replace("D.R.C", "Democratic Republic of the Congo")
        .assign(
            iso_code=lambda d: convert_id(
                d.iso_code, from_type="regex", to_type="name_short"
            )
        )
    )

    africa = df.groupby(["year"], as_index=False).sum().assign(iso_code="Africa")

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
        .loc[lambda d: d.year <= (CURRENT_YEAR + 2)]
    )

    df.to_csv(f"{PATHS.charts}/debt_topic/debt_service_ts.csv", index=False)


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

    africa = df.groupby(["year"], as_index=False).sum().assign(iso_code="Africa")

    df = (
        pd.concat([africa, df], ignore_index=True)
        .assign(gdp_share=lambda d: round(d.Total / d.gdp, 5))
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .rename(columns={"gdp_share": "Debt to GDP ratio"})
        .filter(["name_short", "year", "Debt to GDP ratio"], axis=1)
        .pivot(index="year", columns="name_short", values="Debt to GDP ratio")
    )
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_gdp_ratio_country_ts.csv", index=False)


def read_debt_chart_data() -> pd.DataFrame:
    return (
        pd.read_csv(f"{PATHS.raw_data}/debt/ids_tableau.csv")
        .assign(
            stocks_type=lambda d: d["Series Id"].map(common.debt_stocks),
            service_type=lambda d: d["Series Id"].map(common.debt_service),
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
        .loc[lambda d: d.time == 2020]
        .dropna(subset=["stocks_type"])
        .groupby(["time", "Country", "Creditors", "stocks_type"], as_index=False)
        .sum()
        .filter(["Country", "Creditors", "stocks_type", "value"], axis=1)
        .rename(
            columns={
                "stocks_type": "Creditor Type",
                "value": "US$",
                "Creditors": "Creditor",
            }
        )
    )

    df.to_csv(f"{PATHS.charts}/debt_topic/debt_composition_country.csv", index=False)


def debt_to_china_chart() -> None:
    df = (
        read_debt_chart_data()
        .dropna(subset=["stocks_type"])
        .groupby(["time", "Country", "Creditors", "stocks_type"], as_index=False)
        .sum()
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

    africa = df.groupby(["Year"], as_index=False).sum().assign(Country="Africa")

    df = pd.concat([africa, df], ignore_index=True).loc[
        lambda d: d[["Bilateral", "Private"]].sum(axis=1) > 0
    ]

    df.to_clipboard(index=False)


def update_debt_country_charts() -> None:
    debt_stocks_columns()
    debt_service_columns()
    debt_to_gdp_ts()
    debt_composition_chart()
    debt_to_china_chart()
