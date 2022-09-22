import country_converter
import pandas as pd
from bblocks.dataframe_tools.add import add_flourish_geometries, add_short_names_column
from bblocks.dataframe_tools.common import get_poverty_ratio_df, get_population_df
from bblocks.import_tools.imf import WorldEconomicOutlook

WEO_YEAR = 2022


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

    df = (
        pd.concat([df_weo, df_wb], ignore_index=True)
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .rename(
            columns={"short_name": ExplorerSchema.NAME, "iso_code": ExplorerSchema.ID}
        )
    )

    return df
