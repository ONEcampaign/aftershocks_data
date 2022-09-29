import pandas as pd
import requests
from bblocks.cleaning_tools.clean import clean_numeric_series

from scripts.config import PATHS


def get_ghe_url(country_code, year):
    """Get a URL to the GHE API for a given country and year"""
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


def unpack_ghe_country(country: str, country_data: list, year: int) -> pd.DataFrame:
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


def clean_hiv(df_hiv: pd.DataFrame) -> pd.DataFrame:
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


def clean_art(df_art: pd.DataFrame) -> pd.DataFrame:
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


def get_url_malaria(indicator: str):
    return f"https://ghoapi.azureedge.net/api/{indicator}"


def unpack_malaria(indicator: str) -> pd.DataFrame:
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


def read_dpt_data() -> pd.DataFrame:
    file = "Diphtheria Tetanus Toxoid and Pertussis (DTP) vaccination coverage.xlsx"
    return pd.read_excel(f"{PATHS.raw_data}/health/{file}", sheet_name=0)
