import numpy as np
import requests

import pandas as pd
from bblocks.cleaning_tools.clean import format_number, clean_numeric_series
from bblocks.cleaning_tools.filter import filter_african_countries
from bblocks.import_tools.world_bank import WorldBankData

from scripts.config import PATHS
from scripts.explorers.common import base_africa_map
from bblocks.dataframe_tools.add import add_short_names_column

from scripts import common

CAUSES_OF_DEATH_YEAR = 2019
CAUSES_YEAR_COMPARISON = 2000

CAUSE_GROUPS = {
    1: "Communicable, maternal, neonatal, and nutritional diseases",
    2: "Noncommunicable diseases",
    3: "Injuries",
}

CAUSES_SOURCE = (
    "Global Health Estimates 2020: Deaths by Cause, Age, Sex,"
    " by Country and by Region, 2000-2019. "
    "Geneva, World Health Organization; 2020."
)


def get_url(country_code, year):
    return (
        "https://frontdoor-l4uikgap6gz3m.azurefd.net/"
        "DEX_CMS/GHE_FULL?&$orderby=VAL_DEATHS_RATE100K_NUMERIC"
        "%20desc&$select=DIM_COUNTRY_CODE,"
        "DIM_GHECAUSE_TITLE,DIM_YEAR_CODE,"
        "FLAG_CAUSEGROUP,"
        "VAL_DEATHS_COUNT_NUMERIC,"
        "ATTR_POPULATION_NUMERIC,"
        "VAL_DEATHS_RATE100K_NUMERIC&"
        "$filter=FLAG_RANKABLE%20eq%201%20and%20DIM_COUNTRY_CODE"
        f"%20eq%20%27{country_code}%27%20and%20DIM_SEX_CODE%20eq%20%27BTSX%27%20"
        "and%20DIM_AGEGROUP_CODE%20eq%20%27ALLAges%27%20and%20"
        f"DIM_YEAR_CODE%20eq%20%27{year}%27"
    )


def __unpack_country(country: str, country_data: list, year: int) -> pd.DataFrame:
    df = pd.DataFrame()

    for y in country_data:
        d = pd.DataFrame(
            {
                "iso_code": [country],
                "year": [year],
                "cause": [y["DIM_GHECAUSE_TITLE"]],
                "cause_group": [y["FLAG_CAUSEGROUP"]],
                "deaths": [y["VAL_DEATHS_COUNT_NUMERIC"]],
                "population": [y["ATTR_POPULATION_NUMERIC"]],
                "death_rate": [y["VAL_DEATHS_RATE100K_NUMERIC"]],
            }
        )
        df = pd.concat([df, d], ignore_index=True)

    return df


def _download_leading_causes_of_death(request_year: int) -> None:

    dfs = []
    africa = base_africa_map().iso_code.to_list()

    for country in africa:

        d = requests.get(get_url(country, request_year)).json()["value"]
        dfs.append(__unpack_country(country, d, request_year))

    df = pd.concat(dfs, ignore_index=True)

    df.to_csv(
        f"{PATHS.raw_data}/health/leading_causes_of_death_{request_year}.csv",
        index=False,
    )


def _read_leading_causes_of_death(year: int) -> pd.DataFrame:

    return pd.read_csv(
        f"{PATHS.raw_data}/health/leading_causes_of_death_{year}.csv"
    ).assign(cause_group=lambda d: d.cause_group.map(CAUSE_GROUPS))


def _get_x_largest_causes(df: pd.DataFrame, x: int = 5) -> pd.DataFrame:

    return df.groupby(["iso_code", "year"], as_index=False).apply(
        lambda d: d.nlargest(n=x, columns="death_rate")
    )


def _combined_causes_of_death_data(sort_indicator: str) -> pd.DataFrame:
    # get 2019 data and filter for top 10 causes
    df_latest = _read_leading_causes_of_death(CAUSES_OF_DEATH_YEAR).pipe(
        _get_x_largest_causes, x=10
    )

    causes = {}
    for country in df_latest.iso_code.unique():
        causes[country] = df_latest.query("iso_code == @country").cause.unique()

    # get 200 data
    df_comparison = _read_leading_causes_of_death(CAUSES_YEAR_COMPARISON)

    # combine and sort
    return (
        pd.concat([df_latest, df_comparison], ignore_index=True)
        .groupby(["iso_code", "year", "cause"], as_index=False)
        .apply(lambda d: d.loc[d.cause.isin(causes[d.iso_code.item()])])
        .sort_values(
            by=["iso_code", "year", sort_indicator], ascending=(True, True, True)
        )
        .reset_index(drop=True)
    )


def leading_causes_of_death_chart() -> None:

    dfc = (
        _combined_causes_of_death_data("death_rate")
        .merge(common.base_africa_df(), on="iso_code", how="outer")
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(deaths=lambda d: format_number(d.deaths, as_units=True, decimals=0))
        .filter(
            ["name_short", "cause", "cause_group", "year", "death_rate", "deaths"],
            axis=1,
        )
        .rename(
            columns={
                "death_rate": "Deaths per 100K people",
                "deaths": "Deaths",
                "name_short": "Country",
                "cause": "Cause",
                "year": "Year",
                "cause_group": "Type",
            }
        )
        .assign(missing=lambda d: np.where(d.Cause.isna(), True, False))
    )

    dfc.to_clipboard(index=False)

    # chart version
    dfc.to_csv(f"{PATHS.charts}/health/leading_causes_of_death.csv", index=False)

    # download version
    dfc.assign(source=CAUSES_SOURCE).to_csv(
        f"{PATHS.download}/health/leading_causes_of_death.csv", index=False
    )


def leading_causes_of_death_column_chart() -> None:

    dfc = (
        _combined_causes_of_death_data("death_rate")
        .merge(common.base_africa_df(), on="iso_code", how="outer")
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(deaths=lambda d: format_number(d.deaths, as_units=True, decimals=0))
        .fillna({"year": "missing"})
        .filter(
            ["name_short", "cause", "cause_group", "year", "death_rate", "deaths"],
            axis=1,
        )
        .pivot(
            index=["name_short", "cause", "cause_group"],
            columns="year",
            values=["death_rate", "deaths"],
        )
        .reset_index()
    )

    dfc.columns = [f"{a}_{b}".split(".")[0] for a, b in dfc.columns]

    dfc = (
        dfc.rename(
            columns={
                "name_short_": "Country",
                "cause_": "Cause",
                "cause_group_": "Type",
                "death_rate_2000": "2000",
                "death_rate_2019": "2019",
                "deaths_2000": "Deaths (2000)",
                "deaths_2019": "Deaths (2019)",
            }
        )
        .drop(["death_rate_missing", "deaths_missing"], axis=1)
        .assign(missing=lambda d: np.where(d.Cause.isna(), True, False))
    )

    dfc.to_clipboard(index=False)

    # chart version
    dfc.to_csv(f"{PATHS.charts}/health/leading_causes_of_death.csv", index=False)

    # download version
    dfc.assign(source=CAUSES_SOURCE).to_csv(
        f"{PATHS.download}/health/leading_causes_of_death.csv", index=False
    )


# -------------------  Life expectancy --------------- #


def _get_life_expectancy() -> pd.DataFrame:

    wb = WorldBankData()
    wb.load_indicator("SP.DYN.LE00.IN")

    return (
        wb.get_data()
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .copy()
        .replace({"SSA": "Sub-Saharan Africa", "WLD": "World"})
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(
            indicator="Life expectancy at birth (years)",
            year=lambda d: d.date.dt.year,
        )
        .filter(["year", "name_short", "indicator", "value"], axis=1)
    )


def life_expectancy_chart() -> None:

    df = _get_life_expectancy()

    chart = df.loc[lambda d: d.year.between(d.year.max() - 10, d.year.max())].pivot(
        index=["year"], columns="name_short", values="value"
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/health/life_expectancy.csv", index=False)

    # download version
    df.to_csv(f"{PATHS.download}/health/life_expectancy.csv", index=False)


if __name__ == "__main__":
    ...
    # leading_causes_of_death_chart()
