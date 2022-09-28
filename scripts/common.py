import country_converter as coco
import pandas as pd
from country_converter import country_converter


def get_full_africa_iso3() -> list:
    africa = (
        coco.CountryConverter()
        .data[["ISO3", "continent"]]
        .query("continent == 'Africa'")
        .ISO3.to_list()
    )

    # Add sub saharan Africa
    africa.append("SSA")

    # Add World
    africa.append("WLD")

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


WEO_YEAR: int = 2022


def clean_wb_overview(df: pd.DataFrame) -> pd.DataFrame:
    """Clean World Bank data for overview charts

    returns a dataframe for a line chart with values for World and SSA
    """

    return (df
            .loc[lambda d: d['iso_code'].isin(['WLD', 'SSA'])]
            .pivot(index='date', columns='iso_code', values='value')
            .reset_index()
            .dropna(subset=['SSA', 'WLD'])
            .rename(columns={'SSA': 'Sub-Saharan Africa', 'WLD': 'World'})
            )