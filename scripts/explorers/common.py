import country_converter
import pandas as pd
import requests
from bblocks.cleaning_tools.clean import convert_id, clean_numeric_series
from bblocks.dataframe_tools.add import (
    add_flourish_geometries,
    add_short_names_column,
)
from bblocks.dataframe_tools.common import get_poverty_ratio_df, get_population_df
from bblocks.import_tools.imf import WorldEconomicOutlook

from scripts.config import PATHS

WEO_YEAR = 2022
HDI_YEAR = 2021

HDI_URL = (
    "https://hdr.undp.org/sites/default/files/"
    "2021-22_HDR/HDR21-22_Statistical_Annex_HDI_Table.xlsx"
)


class ExplorerSchema:
    ID = "iso_code"
    NAME = "Country Name"
    REGION = "UN Region"
    CONTINENT = "Continent"
    INCOME = "Income Group"
    LDC = "Least Developed Countries"
    GDP = "GDP per capita (USD)"
    POP = "Population"
    POVERTY = "Poverty rate (%)"
    GDP_GROWTH = "GDP growth (%)"
    UNEMPLOYMENT = "Unemployment rate (%)"
    GOV_REVENUE = "General Government Revenue (% of GDP)"
    GOV_EXPENDITURE = "General Government Expenditure (% of GDP)"
    HDI = "Human Development Index"


def base_africa_map():
    """Create a map with geometries for all african countries"""

    return (
        country_converter.CountryConverter()
        .data[["ISO3", "continent"]]
        .rename(columns={"ISO3": "iso_code"})
        .query("continent == 'Africa'")
        .drop(columns="continent")
        .pipe(add_flourish_geometries, id_column="iso_code", id_type="ISO3")
        .dropna(subset=["geometry"])
    )


def _download_hdi():
    # Get file
    r = requests.get(HDI_URL)

    df = (
        pd.read_excel(r.content, usecols=[1, 2], skiprows=5, skipfooter=46)
        .assign(
            iso_code=lambda d: convert_id(
                d.Country,
                from_type="regex",
                to_type="ISO3",
                additional_mapping={"Sub-Sharan Africa": "SSA"},
                not_found="not_found",
            )
        )
        .loc[lambda d: d.iso_code != "not_found"]
        .assign(
            value=lambda d: clean_numeric_series(d.Value.replace("..", "")), year=2021
        )
        .filter(["iso_code", "value", "year"], axis=1)
    )
    df.to_csv(f"{PATHS.raw_data}/hdi.csv", index=False)


def _read_hdi() -> pd.DataFrame:
    return pd.read_csv(f"{PATHS.raw_data}/hdi.csv")


def add_hdi_column(df: pd.DataFrame, iso_column="iso_code") -> pd.DataFrame:
    """Add the Human Development Index to a dataframe"""

    hdi = _read_hdi().rename(columns={"value": ExplorerSchema.HDI}).drop("year", axis=1)

    return df.merge(hdi, on=[iso_column], how="left")


LDC: list = [
    "AFG",
    "AGO",
    "BGD",
    "BEN",
    "BTN",
    "BFA",
    "BDI",
    "KHM",
    "CAF",
    "TCD",
    "COM",
    "COD",
    "DJI",
    "ERI",
    "ETH",
    "GMB",
    "GIN",
    "GNB",
    "HTI",
    "KIR",
    "LAO",
    "LSO",
    "LBR",
    "MDG",
    "MWI",
    "MLI",
    "MRT",
    "MOZ",
    "MMR",
    "NPL",
    "NER",
    "RWA",
    "STP",
    "SEN",
    "SLE",
    "SLB",
    "SOM",
    "SSD",
    "SDN",
    "TLS",
    "TGO",
    "TUV",
    "UGA",
    "TZA",
    "YEM",
    "ZMB",
]

ECONOMICS_INDICATORS = {
    "NGDP_RPCH": ExplorerSchema.GDP_GROWTH,
    "LUR": ExplorerSchema.UNEMPLOYMENT,
    "GGR_NGDP": ExplorerSchema.GOV_REVENUE,
    "GGX_NGDP": ExplorerSchema.GOV_EXPENDITURE,
}


def _weo_meta() -> pd.DataFrame:
    weo = WorldEconomicOutlook()

    for indicator in ECONOMICS_INDICATORS:
        weo.load_indicator(indicator)

    return (
        weo.get_data(keep_metadata=True)
        .loc[lambda d: d.year.dt.year == WEO_YEAR]
        .assign(
            indicator=lambda d: d.indicator.map(ECONOMICS_INDICATORS),
            year=lambda d: d.year.dt.year,
        )
        .filter(["iso_code", "year", "indicator", "estimate"], axis=1)
        .assign(source="IMF World Economic Outlook")
    )


def _wb_meta() -> pd.DataFrame:
    # population
    population = (
        get_population_df(most_recent_only=True, update_population_data=False)
        .filter(["iso_code", "year"])
        .assign(indicator=ExplorerSchema.POP, source="World Bank Open Data")
    )

    # poverty
    poverty = (
        get_poverty_ratio_df(most_recent_only=True, update_poverty_data=False)
        .filter(["iso_code", "year", "poverty_ratio"])
        .assign(indicator=ExplorerSchema.POVERTY, source="World Bank Open Data")
    )

    return pd.concat([population, poverty], ignore_index=True)


def indicators_metadata():
    """DataFrame with information on the indicators used by the explorers"""

    df_weo = _weo_meta()
    df_wb = _wb_meta()
    df_hdi = (
        _read_hdi()
        .assign(
            year=HDI_YEAR,
            source="UNDP Human Development Report",
            indicator=ExplorerSchema.HDI,
        )
        .drop("value", axis=1)
    )

    df = (
        pd.concat([df_weo, df_wb, df_hdi], ignore_index=True)
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .rename(
            columns={"short_name": ExplorerSchema.NAME, "iso_code": ExplorerSchema.ID}
        )
    )

    return df
