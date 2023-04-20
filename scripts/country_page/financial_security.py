import pandas as pd
from bblocks.cleaning_tools.filter import filter_african_countries
from bblocks.dataframe_tools.add import add_short_names_column, add_iso_codes_column
from bblocks import set_bblocks_data_path, WorldEconomicOutlook, WFPData, WorldBankData
from pydeflate import deflate, set_pydeflate_path

from scripts import common
from scripts.common import WEO_YEAR
from scripts.config import PATHS
from scripts.logger import logger

set_bblocks_data_path(PATHS.bblocks_data)
set_pydeflate_path(PATHS.raw_data)


# ------------------------------------------------------------------------------
# Country Page - Inflation
# ------------------------------------------------------------------------------


def _read_wfp() -> WFPData:
    """Read all available WFP indicators"""
    wfp = WFPData()

    for indicator in wfp.available_indicators:
        wfp.load_data(indicator)

    return wfp


def _wfp_inflation(wfp: WFPData, indicator="Inflation Rate") -> pd.DataFrame:
    """Read an inflation indicator from WFP and return a dataframe"""

    return (
        wfp.get_data("inflation")
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_type="ISO3")
        .loc[lambda d: d.date.dt.year.between(2018, 2023)]
        .loc[lambda d: d.indicator == indicator]
        .filter(["name_short", "date", "indicator", "value"], axis=1)
        .rename(
            columns={
                "indicator": "indicator_name",
            }
        )
    )


# ---------- OVERVIEW ----------
def inflation_overview() -> None:
    """Create a line chart with an overview of inflation data"""

    wfp = _read_wfp()

    inflation = _wfp_inflation(wfp)

    # Live chart version
    inflation.to_csv(f"{PATHS.charts}/country_page/overview_inflation.csv", index=False)
    logger.debug("Saved live version of 'overview_inflation.csv'")


def inflation_overview_regions() -> None:
    wfp = _read_wfp()

    inflation = _wfp_inflation(wfp).pipe(
        add_iso_codes_column, id_column="name_short", id_type="name_short"
    )

    incomplete = (
        inflation.groupby("date", as_index=False)
        .value.count()
        .loc[lambda d: d.value < 30]
    )

    inflation = inflation.loc[lambda d: ~d.date.isin(incomplete.date)]

    dfs = []

    for region in common.regions():
        _ = (
            inflation.loc[lambda d: d.iso_code.isin(common.regions()[region])]
            .groupby(["date"], as_index=False)
            .value.median()
            .assign(name_short=common.region_names()[region])
            .assign(indicator_name="Inflation Rate (median)")
        )
        dfs.append(_)

    inflation = pd.concat(dfs, ignore_index=True).filter(
        ["name_short", "date", "indicator_name", "value"]
    )

    # Live chart version
    inflation.to_csv(
        f"{PATHS.charts}/country_page/overview_inflation_regions.csv", index=False
    )
    logger.debug("Saved live version of 'overview_inflation_regions.csv'")

    # Dynamic text version
    kn = (
        inflation.sort_values("date")
        .dropna(subset=["value"])
        .drop_duplicates(["name_short"], keep="last")
        .assign(date=lambda d: d.date.dt.strftime("%d %B %Y"))
        .round(1)
        .pipe(
            common.df_to_key_number,
            indicator_name="inflation",
            id_column="name_short",
            value_columns=["value", "date"],
        )
    )

    common.update_key_number(f"{PATHS.charts}/country_page/region_overview.json", kn)
    logger.debug("Updated 'region_overview.json'")


# ---------- TIME SERIES ----------
def inflation_ts_chart() -> None:
    """Create a line chart with an overview of inflation data"""
    source = "Price inflation data from the WFP VAM resource centre"

    wfp = _read_wfp()

    inflation = _wfp_inflation(wfp)

    incomplete = (
        inflation.groupby("date", as_index=False)
        .value.count()
        .loc[lambda d: d.value < 30]
    )

    median = (
        inflation.loc[lambda d: ~d.date.isin(incomplete.date)]
        .groupby(["date"], as_index=False)
        .value.median()
        .assign(name_short="Africa (median)")
    )

    inflation = (
        pd.concat([median, inflation], ignore_index=True)
        .drop("indicator_name", axis=1)
        .pivot(index="date", columns="name_short", values="value")
        .reset_index()
    )

    # Live chart version
    inflation.to_csv(
        f"{PATHS.charts}/country_page/inflation_ts_by_country.csv", index=False
    )
    logger.debug("Saved live version of 'inflation_ts_by_country.csv'")

    # Download version
    inflation.assign(source=source).to_csv(
        f"{PATHS.download}/country_page/inflation_ts_by_country.csv", index=False
    )
    logger.debug("Saved download version of 'inflation_ts_by_country.csv'")

    # Dynamic text version
    kn = (
        inflation.melt(id_vars="date")
        .sort_values("date")
        .dropna(subset=["value"])
        .drop_duplicates(["name_short"], keep="last")
        .assign(date=lambda d: d.date.dt.strftime("%d %B %Y"))
        .pipe(
            common.df_to_key_number,
            indicator_name="inflation",
            id_column="name_short",
            value_columns=["value", "date"],
        )
    )

    common.update_key_number(f"{PATHS.charts}/country_page/overview.json", kn)
    logger.debug("Updated 'overview.json'")


# ------------------------------------------------------------------------------
# Country Page - Growth
# ------------------------------------------------------------------------------

# --------------- OVERVIEW ---------------

WEO_INDICATORS = {"NGDP_RPCH": "GDP Growth"}


def __weo_center(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the angle of the Single Measure template arrow"""
    return df.assign(
        center=lambda d: d.groupby(["iso_code", "indicator"]).value.transform(
            lambda g: g / g.abs().max()
        )
    )


def _read_weo() -> pd.DataFrame:
    """Read the WEO data and return a dataframe with the last 10 years of data"""
    weo = WorldEconomicOutlook()

    weo.load_data(indicator=list(WEO_INDICATORS))

    return (
        weo.get_data(indicators="all", keep_metadata=True)
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_column="iso_code", id_type="ISO3")
        .loc[lambda d: d.year.dt.year.between(WEO_YEAR - 10, WEO_YEAR)]
    )


def __single_weo_measure(indicator_code: str, comparison_year_difference: int = 1):
    """Create a dataframe for use with the Single Measure template"""

    # Read the data and add the arrow angle
    df = _read_weo().pipe(__weo_center)

    data = (
        df.loc[lambda d: d.indicator == indicator_code]
        .loc[
            lambda d: d.year.dt.year.isin(
                [WEO_YEAR - comparison_year_difference, WEO_YEAR]
            )
        ]
        .assign(year=lambda d: d.year.dt.year)
        .filter(["name_short", "indicator_name", "year", "value", "center"], axis=1)
    )

    latest = data.loc[lambda d: d.year == WEO_YEAR]
    previous = data.loc[
        lambda d: d.year == WEO_YEAR - comparison_year_difference
    ].filter(["name_short", "value"], axis=1)

    return (
        latest.merge(previous, on=["name_short"], suffixes=("", "_previous"))
        .assign(indicator_name=f"{WEO_YEAR} estimate")
        .drop("year", axis=1)
        .assign(lower=f"in {WEO_YEAR - comparison_year_difference}")
        .filter(
            [
                "name_short",
                "indicator_name",
                "value",
                "lower",
                "value_previous",
                "center",
            ],
            axis=1,
        )
    )


def gdp_growth_single_measure() -> None:
    # GDP Growth
    gdp_growth_chart = __single_weo_measure("NGDP_RPCH", comparison_year_difference=1)

    # chart version
    gdp_growth_chart.to_csv(
        f"{PATHS.charts}/country_page/overview_GDP_growth.csv", index=False
    )
    logger.debug("Saved live version of 'overview_GDP_growth.csv'")

    # dynamic text version
    kn = (
        gdp_growth_chart.filter(["name_short", "value"], axis=1)
        .assign(year=WEO_YEAR, value=lambda d: d.value.round(1).astype(str) + "%")
        .pipe(
            common.df_to_key_number,
            indicator_name="gdp_growth",
            id_column="name_short",
            value_columns=["value", "year"],
        )
    )

    common.update_key_number(f"{PATHS.charts}/country_page/overview.json", kn)
    logger.debug("Updated 'overview.json'")


def gdp_growth_regions_single_measure() -> None:
    gdp_growth = __single_weo_measure("NGDP_RPCH", comparison_year_difference=1).pipe(
        add_iso_codes_column, id_column="name_short", id_type="name_short"
    )

    dfs = []

    for region in common.regions():
        _ = (
            gdp_growth.loc[lambda d: d.iso_code.isin(common.regions()[region])]
            .groupby(["indicator_name", "lower"], as_index=False)
            .median(numeric_only=True)
            .assign(name_short=common.region_names()[region])
            .assign(indicator_name=f"{WEO_YEAR} estimate (median)")
        )
        dfs.append(_)

    gdp_growth = pd.concat(dfs, ignore_index=True).filter(
        ["name_short", "indicator_name", "value", "lower", "value_previous", "center"],
        axis=1,
    )

    # chart version
    gdp_growth.to_csv(
        f"{PATHS.charts}/country_page/overview_GDP_growth_regions.csv", index=False
    )
    logger.debug("Saved live version of 'overview_GDP_growth_regions.csv'")

    kn = (
        gdp_growth.filter(["name_short", "value"], axis=1)
        .assign(year=WEO_YEAR, value=lambda d: d.value.round(1).astype(str) + "%")
        .pipe(
            common.df_to_key_number,
            indicator_name="gdp_growth",
            id_column="name_short",
            value_columns=["value", "year"],
        )
    )

    common.update_key_number(f"{PATHS.charts}/country_page/region_overview.json", kn)
    logger.debug("Updated 'region_overview.json'")


# ------------------------------------------------------------------------------
# Country Page - Poverty
# ------------------------------------------------------------------------------

WB_INDICATORS = {
    "SI.POV.DDAY": "% of population below the poverty line",
    "SP.POP.TOTL": "Total Population",
    "SP.DYN.LE00.IN": "Life Expectancy",
}


def _read_wb_ts() -> dict:
    wb = WorldBankData()
    wb.load_data(list(WB_INDICATORS))

    dfs = {}
    for indicator in WB_INDICATORS:
        dfs[indicator] = (
            wb.get_data(indicator)
            .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
            .copy()
            .assign(iso_code=lambda d: d.iso_code.replace(common.region_names()))
            .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
            .assign(indicator=lambda d: d.indicator_code.map(WB_INDICATORS))
            .filter(["date", "name_short", "indicator", "value"], axis=1)
        )

    return dfs


# --------------- Time Series by country ---------------
def poverty_chart() -> None:
    indicator_names = {
        "value_poverty": "% of population below the poverty line",
        "people_in_poverty": "People below the poverty line",
    }

    source = "World Bank Open Data: SI.POV.DDAY"

    wb = WorldBankData()
    wb.load_data(list(WB_INDICATORS))

    cols = ["date", "iso_code", "value"]

    poverty_ratio = wb.get_data("SI.POV.DDAY").filter(cols, axis=1)
    population = wb.get_data("SP.POP.TOTL").filter(cols, axis=1)

    df = (
        poverty_ratio.merge(
            population,
            on=["date", "iso_code"],
            how="left",
            suffixes=("_poverty", "_population"),
        )
        .assign(
            people_in_poverty=lambda d: round(
                d.value_poverty / 100 * d.value_population, 0
            )
        )
        .dropna(subset=["value_poverty"])
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .replace(common.region_names())
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .drop("iso_code", axis=1)
        .melt(id_vars=["name_short", "date"], var_name="Indicator")
        .assign(
            Indicator=lambda d: d.Indicator.map(indicator_names),
            Year=lambda d: d.date.dt.year,
        )
        .drop("date", axis=1)
    )

    dfs = []

    for d in df.Indicator.unique():
        _ = (
            df.loc[lambda x: x.Indicator == d]
            .pivot(index=["Year", "Indicator"], columns="name_short", values="value")
            .reset_index()
        )
        dfs.append(_)

    data = pd.concat(dfs, ignore_index=True)

    # chart version
    data.to_csv(f"{PATHS.charts}/country_page/poverty_country_ts.csv", index=False)
    logger.debug("Saved live version of 'poverty_country_ts.csv'")

    # download version
    data_download = (
        data.melt(id_vars=["Year", "Indicator"], var_name="country")
        .assign(source=source)
        .pipe(add_iso_codes_column, id_column="country", id_type="regex")
        .filter(["iso_code", "country", "Year", "Indicator", "value", "source"], axis=1)
    )
    data_download.to_csv(
        f"{PATHS.download}/country_page/poverty_country_ts.csv", index=False
    )
    logger.debug("Saved download version of 'poverty_country_ts.csv'")

    # dynamic text version
    kn = (
        data_download.sort_values("Year")
        .dropna(subset=["value"])
        .drop_duplicates(["country", "Indicator"], keep="last")
        .loc[lambda d: d.Indicator == "% of population below the poverty line"]
        .pipe(
            common.df_to_key_number,
            indicator_name="poverty",
            id_column="country",
            value_columns=["value", "Year"],
        )
    )

    common.update_key_number(f"{PATHS.charts}/country_page/overview.json", kn)
    logger.debug("Updated 'overview.json'")


# --------------- Overview Single Measure ---------------
def wb_poverty_single_measure() -> None:
    data_dict = _read_wb_ts()

    df90s = (
        data_dict["SI.POV.DDAY"]
        .loc[lambda d: d.date.dt.year.between(1989, 2000)]
        .dropna(subset=["value"])
        .drop_duplicates(subset=["name_short"], keep="first")
    )
    df_mrnev = (
        data_dict["SI.POV.DDAY"]
        .dropna(subset=["value"])
        .drop_duplicates(subset=["name_short"], keep="last")
    )

    data = (
        df_mrnev.merge(
            df90s, on=["name_short", "indicator"], suffixes=["", "_90s"], how="left"
        )
        .assign(
            name=lambda d: "As of " + d.date.dt.year.astype(str),
            lower=lambda d: d.date_90s.apply(
                lambda x: f"In {x.year}"
                if not pd.isnull(x)
                else "Comparison not available"
            ),
            center=lambda d: (d.value - d.value_90s) / d.value_90s,
        )
        .filter(["name_short", "name", "value", "lower", "value_90s", "center"], axis=1)
    )

    # chart version
    data.to_csv(f"{PATHS.charts}/country_page/poverty_single_measure.csv", index=False)
    logger.debug("Saved live version of 'poverty_single_measure.csv'")


def _financial_weo() -> pd.DataFrame:
    indicators = ["GGX_NGDP"]

    return WorldEconomicOutlook().load_data(indicators).get_data()


def _financial_gdp_usd_current() -> pd.DataFrame:
    indicators = ["NGDPD"]

    return WorldEconomicOutlook().load_data(indicators).get_data()


def _financial_wb(update: bool = False) -> pd.DataFrame:
    wb_indicators = ["DT.ODA.ODAT.CD", "BX.TRF.PWKR.CD.DT", "BX.KLT.DINV.CD.WD"]

    wb = WorldBankData().load_data(wb_indicators)

    if update:
        wb.update_data(reload_data=True)

    return wb.get_data().rename(columns={"indicator_code": "indicator", "date": "year"})


def _financial_gdp_to_usd(df: pd.DataFrame) -> pd.DataFrame:
    gdp = _financial_gdp_usd_current()

    return (
        df.merge(
            gdp.drop("indicator", axis=1),
            on=["year", "iso_code"],
            suffixes=("", "_gdp"),
            how="left",
        )
        .assign(value=lambda d: (d.value / 100) * (d.value_gdp * 1e9))
        .drop("value_gdp", axis=1)
    )


def financial_overview() -> None:
    indicators = {
        "GGX_NGDP": "Government Expenditure",
        "DT.ODA.ODAT.CD": "ODA",
        "BX.TRF.PWKR.CD.DT": "Remittances",
        "BX.KLT.DINV.CD.WD": "FDI",
    }

    order = {
        "Government Expenditure": 1,
        "ODA": 2,
        "Remittances": 3,
        "FDI": 4,
    }

    gov_spending = _financial_weo().pipe(_financial_gdp_to_usd)
    wb_indicators = _financial_wb()

    data = pd.concat([gov_spending, wb_indicators], ignore_index=True)

    data = (
        data.query(f"year.dt.year >=2008 and year.dt.year <= {WEO_YEAR}")
        .pipe(
            deflate,
            base_year=WEO_YEAR,
            deflator_source="imf",
            deflator_method="gdp",
            exchange_source="imf",
            exchange_method="implied",
            date_column="year",
        )
        .assign(indicator=lambda d: d.indicator.map(indicators))
        .pipe(filter_african_countries, id_type="ISO3")
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .drop("iso_code", axis=1)
        .assign(value=lambda d: round(d.value, 1))
        .pivot(index=["year", "indicator"], columns="name_short", values="value")
        .reset_index()
        .assign(year=lambda d: d.year.dt.year, order=lambda d: d.indicator.map(order))
        .sort_values(["year", "order"])
        .drop("order", axis=1)
        .reset_index(drop=True)
    )

    # chart version
    data.to_csv(
        f"{PATHS.charts}/country_page/country_financial_overview.csv", index=False
    )

    # download version
    data.to_csv(
        f"{PATHS.download}/country_page/country_financial_overview_download.csv",
        index=False,
    )
