import json
import os

import country_converter as coco
import pandas as pd
from country_converter import country_converter

from scripts.config import PATHS

WEO_YEAR: int = 2023
CAUSES_OF_DEATH_YEAR = 2019
DEBT_YEAR: int = 2023


def get_full_africa_iso3() -> list:
    africa = (
        coco.CountryConverter()
        .data[["ISO3", "continent"]]
        .query("continent == 'Africa'")
        .ISO3.to_list()
    )

    # Add sub saharan Africa
    africa.append("SSA")
    africa.append("SSF")
    africa.append("AFE")
    africa.append("AFW")

    # Add World
    africa.append("WLD")

    # Add Africa
    africa.append("AFR")

    # ADD Global
    africa.append("GLOBAL")

    return africa


def sort_name_first(
    df: pd.DataFrame,
    name: str,
    name_column: str,
    date_column: str,
    keep_current_sorting=True,
):
    if not keep_current_sorting:
        df = df.sort_values([date_column, name_column], ascending=[True, False])

    top = df.query(f"{name_column} == '{name}'").reset_index(drop=True)
    other = df.query(f"{name_column} != '{name}'").reset_index(drop=True)

    return pd.concat([top, other], ignore_index=True)


def base_africa_df():
    """Create a map with geometries for all african countries"""

    return (
        country_converter.CountryConverter()
        .data[["ISO3", "continent"]]
        .rename(columns={"ISO3": "iso_code"})
        .query("continent == 'Africa'")
        .drop(columns="continent")
    )


def _download_wb_regions():
    df = pd.read_excel(
        "http://databank.worldbank.org/" "data/download/site-content/CLASS.xlsx",
        sheet_name="Groups",
    ).rename(
        columns={
            "GroupCode": "group_code",
            "GroupName": "group_name",
            "CountryCode": "iso_code",
            "CountryName": "country_name",
        }
    )
    df.to_csv(f"{PATHS.raw_data}/wb_groupings.csv", index=False)


def read_wb_regions(region_code: str) -> dict:
    """Read World Bank regions from csv file"""

    df = pd.read_csv(f"{PATHS.raw_data}/wb_groupings.csv")

    data = df.groupby("group_code").apply(lambda d: d.iso_code.to_list()).to_dict()

    return data[region_code]


def regions() -> dict:
    north_africa_wb = [
        "DZA",
        "EGY",
        "LBY",
        "MAR",
        "TUN",
    ]

    un_mapping = {
        "Northern Africa": "NAF",
        "Middle Africa": "MAF",
        "Western Africa": "WAF",
        "Eastern Africa": "EAF",
        "Southern Africa": "SAF",
    }

    data = (
        country_converter.CountryConverter()
        .data[["ISO3", "continent", "UNregion"]]
        .rename(columns={"ISO3": "iso_code"})
        .query("continent == 'Africa'")
        .assign(un=lambda d: d.UNregion.map(un_mapping))
    )

    regions_dict = {
        "NAF_WB": north_africa_wb,
        "SSA_WB": data.query("iso_code not in @north_africa_wb").iso_code.to_list(),
        "AFR": data.iso_code.to_list(),
        "SSA_UN": data.query("un != 'NAF'").iso_code.to_list(),
        "AFE_WB": read_wb_regions("AFE"),
        "AFW_WB": read_wb_regions("AFW"),
    }

    # UN regions
    for region in data.un.unique():
        regions_dict[region] = data.query("un == @region").iso_code.to_list()

    return regions_dict


def region_names() -> dict:
    return {
        "NAF_WB": "North Africa (WB)",
        "SSA_WB": "Sub-Saharan Africa (WB)",
        "SSA": "Sub-Saharan Africa (WB exc. high income)",
        "SSF": "Sub-Saharan Africa (WB)",
        "AFE_WB": "Eastern and Southern Africa",
        "AFE": "Eastern and Southern Africa",
        "AFW_WB": "Western and Central Africa",
        "AFW": "Western and Central Africa",
        "AFR": "Africa",
        "SSA_UN": "Sub-Saharan Africa (UN)",
        "NAF": "Northern Africa",
        "MAF": "Middle Africa",
        "WAF": "Western Africa",
        "EAF": "Eastern Africa",
        "SAF": "Southern Africa",
        "WLD": "World",
    }


def clean_wb_overview(df: pd.DataFrame) -> pd.DataFrame:
    """Clean World Bank data for overview charts

    returns a dataframe for a line chart with values for World and SSA
    """

    return (
        df.loc[lambda d: d["iso_code"].isin(["WLD", "SSA"])]
        .pivot(index="date", columns="iso_code", values="value")
        .round(2)
        .reset_index()
        .dropna(subset=["SSA", "WLD"])
        .rename(columns={"SSA": "Sub-Saharan Africa", "WLD": "World"})
    )


def df_to_key_number(
    df: pd.DataFrame,
    indicator_name: str,
    id_column: str,
    value_columns: str | list[str],
) -> dict:
    if isinstance(value_columns, str):
        value_columns = [value_columns]

    return (
        df.assign(indicator=indicator_name)
        .filter(["indicator", id_column] + value_columns, axis=1)
        .groupby(["indicator"])
        .apply(
            lambda x: x.set_index(id_column)[value_columns]
            .astype(str)
            .to_dict(orient="index")
        )
        .to_dict()
    )


def update_key_number(path: str, new_dict: dict) -> None:
    """Update a key number json by updating it with a new dictionary"""

    # Check if the file exists, if not create
    if not os.path.exists(path):
        with open(path, "w") as f:
            json.dump({}, f)

    with open(path, "r") as f:
        data = json.load(f)

    for k in new_dict.keys():
        data[k] = new_dict[k]

    with open(path, "w") as f:
        json.dump(data, f, indent=4)
