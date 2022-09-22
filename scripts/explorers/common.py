import country_converter
import pandas as pd
import requests
from bblocks.cleaning_tools.clean import convert_id, clean_numeric_series
from bblocks.dataframe_tools import add
from bblocks.dataframe_tools.add import (
    add_flourish_geometries,
    add_short_names_column,
)
from bblocks.dataframe_tools.common import get_poverty_ratio_df, get_population_df
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.world_bank import WorldBankData

from scripts.config import PATHS


from scripts.owid_covid.tools import (
    read_owid_data,
    get_indicators_ts,
    filter_countries_only,
)


# Data structure
class ExplorerSchema:
    ID = "iso_code"
    TIME = "date"
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
    CHILD_MORTALITY = "Under 5 mortality rate (per 1,000)"
    MATERNAL_MORTALITY = "Maternal mortality (per 100,000)"
    HEALTH_EXPENDITURE = "General Gov. Health expenditure (% of GDP)"
    COVID_VAX_SHARE = "Pop. vaccinated against COVID-19 (%)"
    AIDS_DEATHS = "AIDS related deaths"
    MALARIA_DEATHS = "Malaria deaths"
    RECEIVING_ART = "Share Receiving ART (of ppl living with HIV)"


# Constants
WEO_YEAR: int = 2022
HDI_YEAR: int = 2021

HDI_URL: str = (
    "https://hdr.undp.org/sites/default/files/"
    "2021-22_HDR/HDR21-22_Statistical_Annex_HDI_Table.xlsx"
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

ECONOMICS_WEO_INDICATORS: dict = {
    "NGDP_RPCH": ExplorerSchema.GDP_GROWTH,
    "LUR": ExplorerSchema.UNEMPLOYMENT,
    "GGR_NGDP": ExplorerSchema.GOV_REVENUE,
    "GGX_NGDP": ExplorerSchema.GOV_EXPENDITURE,
}

HEALTH_WB_INDICATORS: dict = {
    "SH.DYN.MORT": ExplorerSchema.CHILD_MORTALITY,
    "SH.STA.MMRT": ExplorerSchema.MATERNAL_MORTALITY,
    "SH.XPD.GHED.GD.ZS": ExplorerSchema.HEALTH_EXPENDITURE,
}

OWID_INDICATORS: dict = {
    "people_vaccinated_per_hundred": ExplorerSchema.COVID_VAX_SHARE,
}


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


def _base_df() -> pd.DataFrame:
    """A dataframe with iso3 codes, name, UN region and continent"""

    return country_converter.CountryConverter().data[
        ["ISO3", "name_short", "continent", "UNregion"]
    ]


def basic_info() -> pd.DataFrame:
    """Create a DataFrame with basic information"""

    return (
        _base_df()
        .pipe(add.add_income_level_column, id_column="ISO3", id_type="ISO3")
        .pipe(add.add_population_column, id_column="ISO3", id_type="ISO3")
        .pipe(
            add.add_gdp_column,
            id_column="ISO3",
            id_type="ISO3",
            usd=True,
            include_estimates=False,
        )
        .pipe(add.add_poverty_ratio_column, id_column="ISO3", id_type="ISO3")
        .assign(ldc=lambda d: d.ISO3.apply(lambda x: "LDC" if x in LDC else "Non-LDC"))
        .assign(gdp_per_capita=lambda d: d.gdp / d.population)
        .drop("gdp", axis=1)
        .dropna(thresh=6)
        .fillna({"income_level": "Not classified"})
        .rename(
            columns={
                "ISO3": ExplorerSchema.ID,
                "name_short": ExplorerSchema.NAME,
                "UNregion": ExplorerSchema.REGION,
                "continent": ExplorerSchema.CONTINENT,
                "income_level": ExplorerSchema.INCOME,
                "population": ExplorerSchema.POP,
                "poverty_ratio": ExplorerSchema.POVERTY,
                "ldc": ExplorerSchema.LDC,
                "gdp_per_capita": ExplorerSchema.GDP,
            }
        )
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


def _weo_meta() -> pd.DataFrame:
    weo = WorldEconomicOutlook()

    for indicator in ECONOMICS_WEO_INDICATORS:
        weo.load_indicator(indicator)

    return (
        weo.get_data(keep_metadata=True)
        .loc[lambda d: d.year.dt.year == WEO_YEAR]
        .assign(
            indicator=lambda d: d.indicator.map(ECONOMICS_WEO_INDICATORS),
            year=lambda d: d.year.dt.year,
        )
        .filter(["iso_code", "year", "indicator", "estimate"], axis=1)
        .assign(source="IMF World Economic Outlook")
        .rename(columns={"year": ExplorerSchema.TIME})
    )


def _wb_econ_meta() -> pd.DataFrame:
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

    return pd.concat([population, poverty], ignore_index=True).rename(
        columns={"year": ExplorerSchema.TIME}
    )


def _wb_health_meta() -> pd.DataFrame:
    wb = WorldBankData()

    for indicator in HEALTH_WB_INDICATORS:
        wb.load_indicator(indicator, most_recent_only=True)

    return (
        wb.get_data()
        .assign(
            year=lambda d: d.date.dt.year,
            source=lambda d: "World Bank Open Data: " + d["indicator_code"],
            indicator=lambda d: d.indicator.map(HEALTH_WB_INDICATORS),
        )
        .filter(["year", "iso_code", "indicator", "source"], axis=1)
        .rename(columns={"iso_code": ExplorerSchema.ID, "year": ExplorerSchema.TIME})
    )


def _owid_health_meta() -> pd.DataFrame:

    return (
        read_owid_data()
        .pipe(get_indicators_ts, indicators=OWID_INDICATORS)
        .pipe(filter_countries_only)
        .dropna(subset="value")
        .groupby(["iso_code", "indicator"], as_index=False)
        .last()
        .rename(columns={"date": ExplorerSchema.TIME})
        .assign(source="Our World in Data", indicator=ExplorerSchema.COVID_VAX_SHARE)
        .drop("value", axis=1)
    )


def _aids_health_meta() -> pd.DataFrame:
    deaths = (
        pd.read_csv(f"{PATHS.raw_data}/health/aids related deaths total.csv")
        .filter(["country", "year"], axis=1)
        .rename(columns={"country": ExplorerSchema.ID, "year": ExplorerSchema.TIME})
        .assign(indicator=ExplorerSchema.AIDS_DEATHS, source="UNAIDS")
    )

    art = (
        pd.read_csv(
            f"{PATHS.raw_data}/health/people living with HIV receiving ART percent.csv"
        )
        .filter(["country", "year"], axis=1)
        .rename(columns={"country": ExplorerSchema.ID, "year": ExplorerSchema.TIME})
        .assign(indicator=ExplorerSchema.RECEIVING_ART, source="UNAIDS")
    )

    df = pd.concat([deaths, art], ignore_index=True)

    return df


def _malaria_health_meta() -> pd.DataFrame:
    return (
        pd.read_csv(f"{PATHS.raw_data}/health/malaria_est_deaths_country.csv")
        .filter(["iso_code", "year"], axis=1)
        .rename(
            columns={
                "iso_code": ExplorerSchema.ID,
                "year": ExplorerSchema.TIME,
            }
        )
        .assign(indicator=ExplorerSchema.MALARIA_DEATHS, source="WHO")
    )


def _hdi_econ_meta() -> pd.DataFrame:
    return (
        _read_hdi()
        .assign(
            year=HDI_YEAR,
            source="UNDP Human Development Report",
            indicator=ExplorerSchema.HDI,
        )
        .drop("value", axis=1)
        .rename(columns={"iso_code": ExplorerSchema.ID, "year": ExplorerSchema.TIME})
    )


def indicators_metadata():
    """DataFrame with information on the indicators used by the explorers"""

    df_weo = _weo_meta()
    df_wb = _wb_econ_meta()
    df_hdi = _hdi_econ_meta()
    df_wb_health = _wb_health_meta()
    df_owid = _owid_health_meta()
    df_aids = _aids_health_meta()
    df_malaria = _malaria_health_meta()

    dataframes = [df_weo, df_wb, df_hdi, df_wb_health, df_owid, df_aids, df_malaria]

    df = (
        pd.concat(dataframes, ignore_index=True)
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .rename(
            columns={"short_name": ExplorerSchema.NAME, "iso_code": ExplorerSchema.ID}
        )
    )

    return df
