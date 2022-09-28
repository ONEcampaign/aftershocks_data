import numpy as np
import requests

import pandas as pd
from bblocks.cleaning_tools.clean import format_number, clean_numeric_series, convert_id
from bblocks.cleaning_tools.filter import filter_african_countries
from bblocks.import_tools.world_bank import WorldBankData

from scripts.config import PATHS
from scripts.explorers.common import base_africa_map
from bblocks.dataframe_tools.add import add_short_names_column, add_population_column

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


def get_ghe_url(country_code, year):
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


def get_url_malaria(indicator: str):

    return f"https://ghoapi.azureedge.net/api/{indicator}"


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

        d = requests.get(get_ghe_url(country, request_year)).json()["value"]
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


def __clean_hiv(df_hiv: pd.DataFrame) -> pd.DataFrame:
    df_hiv = df_hiv.rename(columns={"Unnamed: 0": "year", "Unnamed: 1": "iso_code"})
    df_hiv.columns = ["year", "iso_code"] + df_hiv.iloc[4, 2:].fillna("").to_list()
    df_hiv = (
        df_hiv.drop("", axis=1)
        .iloc[6:]
        .dropna(subset="iso_code")
        .set_index(["year", "iso_code"])
        .replace("...", "")
    )

    return df_hiv.pipe(
        clean_numeric_series, series_columns=df_hiv.columns, to=float
    ).reset_index()


def __clean_art(df_art: pd.DataFrame) -> pd.DataFrame:

    cols = df_art.columns

    columns = {
        cols[0]: "year",
        cols[1]: "iso_code",
        cols[2]: "name",
        cols[3]: (
            "Among people living with HIV, the percent who know"
            " their status (First 90 of 90-90-90 target) -All ages"
        ),
        cols[18]: "Among people living with HIV, the percent on ART -All ages",
        cols[33]: (
            "Among people who know their HIV status, the percent on ART "
            "(Second 90 of 90-90-90 target) -All ages"
        ),
        cols[48]: (
            "Among people living with HIV, the percent with suppressed "
            "viral load -All ages"
        ),
        cols[63]: (
            "Among people on ART, the percent with suppressed viral load "
            "(Third 90 of 90-90-90 target) -All ages"
        ),
        cols[78]: "Number who know their HIV status -All ages",
    }

    df_art = (
        df_art.rename(columns=columns)
        .dropna(subset=["name"])
        .loc[:, lambda d: ~d.columns.str.contains("named")]
        .set_index(["year", "iso_code", "name"])
        .replace("...", "")
    )
    return df_art.pipe(
        clean_numeric_series, series_columns=df_art.columns, to=float
    ).reset_index()


def _download_aids_data() -> None:
    url = (
        "http://www.unaids.org/sites/default/files/media_asset/"
        "HIV_estimates_from_1990-to-present.xlsx"
    )

    files = pd.read_excel(url, sheet_name=[0, 1, 2, 3])

    # HIV country file
    df_hiv = __clean_hiv(files[0])
    df_hiv.to_csv(f"{PATHS.raw_data}/health/hiv_estimates.csv", index=False)

    # ART file
    df_art = __clean_art(files[2])
    df_art.to_csv(f"{PATHS.raw_data}/health/art_estimates.csv", index=False)


def _read_art() -> pd.DataFrame:
    return pd.read_csv(f"{PATHS.raw_data}/health/art_estimates.csv")


def art_chart() -> None:

    indicator = {
        "Among people living with HIV, the percent on ART -All ages": "people_on_art"
    }

    mapping = {
        "Global": "Africa",
        "EAS": "Africa",
        "Middle East and North Africa": "Africa",
        "Western and central Africa": "Africa",
    }

    df = (
        _read_art()
        .filter(["iso_code", "year", "name"] + list(indicator), axis=1)
        .rename(columns=indicator)
        .replace("Eastern and southern Africa", "EAS")
        .assign(
            continent=lambda d: convert_id(
                d.name,
                from_type="regex",
                to_type="Continent",
                additional_mapping=mapping,
            )
        )
        .loc[lambda d: d.continent == "Africa"]
        .assign(
            name_short=lambda d: convert_id(
                d.name,
                from_type="regex",
                to_type="name_short",
                not_found=None,
                additional_mapping={"EAS": "Eastern and Southern Africa"},
            )
        )
        .filter(["year", "name_short", "people_on_art"], axis=1)
        .sort_values(["year", "name_short"])
        .drop_duplicates(["name_short", "year"], keep="last")
        .pivot(index="year", columns="name_short", values="people_on_art")
    )

    df.to_clipboard()


def __unpack_malaria(indicator: str) -> pd.DataFrame:

    url = get_url_malaria(indicator)

    df = pd.DataFrame()

    data = requests.get(url).json()["value"]

    for point in data:
        _ = pd.DataFrame(
            {
                "iso_code": [point["SpatialDim"]],
                "year": [point["TimeDim"]],
                "value": [point["NumericValue"]],
            }
        )
        df = pd.concat([df, _], ignore_index=True).assign(indicator=indicator)

    return df


def _download_malaria_data() -> None:
    indicator = "MALARIA_EST_DEATHS"
    indicator2 = "MALARIA_EST_MORTALITY"

    deaths = __unpack_malaria(indicator)
    mortality = __unpack_malaria(indicator2)

    df = pd.concat([deaths, mortality], ignore_index=True)

    df.to_csv(f"{PATHS.raw_data}/health/malaria_deaths.csv", index=False)


def _read_malaria_data() -> pd.DataFrame:
    return pd.read_csv(f"{PATHS.raw_data}/health/malaria_deaths.csv")


def malaria_chart() -> None:

    wb = WorldBankData()
    wb.load_indicator("SP.POP.TOTL")
    population = (
        wb.get_data().drop("indicator", axis=1).rename(columns={"value": "population"})
    )

    df = (
        _read_malaria_data()
        .dropna(subset=["value"])
        .pivot(
            index=["year", "iso_code"],
            columns="indicator",
            values="value",
        )
        .reset_index()
        .assign(
            continent=lambda d: convert_id(
                d.iso_code, from_type="ISO3", to_type="Continent", not_found="other"
            )
        )
        .loc[lambda d: d.continent != "other"]
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(date=lambda d: pd.to_datetime(d.year, format="%Y"))
        .merge(population, on=["iso_code", "date"], how="left")
        .filter(
            [
                "name_short",
                "continent",
                "year",
                "MALARIA_EST_MORTALITY",
                "MALARIA_EST_DEATHS",
                "population",
            ],
            axis=1,
        )
        .sort_values(["name_short", "year"])
    )


if __name__ == "__main__":
    ...
    # leading_causes_of_death_chart()
