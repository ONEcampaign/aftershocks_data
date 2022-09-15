import datetime

import country_converter as coco
import pandas as pd
from bblocks.cleaning_tools.clean import clean_numeric_series
from bblocks.dataframe_tools.add import (
    add_population_share_column,
    add_flourish_geometries,
    add_population_column,
    add_iso_codes_column,
)
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.wfp import WFPData
from bblocks.import_tools.world_bank import WorldBankData

from scripts.config import PATHS
from scripts.owid_covid import tools as owid_tools
from scripts.schemas import MapDataSchema, BubbleDataSchema


def _core_data() -> pd.DataFrame:
    """Generate a basic table with African countries, formal names, short names,
    and geometries"""
    cc = coco.CountryConverter()

    return (
        cc.data[["ISO3", "name_short", "name_official", "continent"]]
        .rename(
            columns={
                "ISO3": MapDataSchema.ISO_CODE,
                "name_short": MapDataSchema.NAME,
                "name_official": MapDataSchema.FORMAL_NAME,
            }
        )
        .loc[lambda d: d.continent == "Africa"]
        .pipe(add_flourish_geometries, id_column=MapDataSchema.ISO_CODE, id_type="ISO3")
        .dropna(subset=[MapDataSchema.GEOMETRY])
    )


def base_map_data() -> pd.DataFrame:
    """Create the map data used for the homepage map. A structure and names must be
    respected in order for things to work well. Following the standard structure
    defined in the MapDataSchema class, additional indicators can be added"""

    df = _core_data().filter(
        [
            MapDataSchema.GEOMETRY,
            MapDataSchema.FORMAL_NAME,
            MapDataSchema.NAME,
            MapDataSchema.ISO_CODE,
        ],
        axis=1,
    )
    return df


def base_bubble_data() -> pd.DataFrame:
    """Create the base data used for the homepage bubble chart. A structure and names"""
    df = (
        _core_data()
        .assign(position=1)
        .rename(columns={"position": BubbleDataSchema.POSITION})
        .filter(
            [
                BubbleDataSchema.FORMAL_NAME,
                BubbleDataSchema.NAME,
                BubbleDataSchema.ISO_CODE,
                BubbleDataSchema.POSITION,
            ],
            axis=1,
        )
    )
    return df


def weo_indicators() -> pd.DataFrame:
    """Get a few indicators from WEO for the economy picker"""

    # The data will be filtered to keep only the current year
    current_year = datetime.datetime.now().year

    # These are the indicators that will be used and the name assigned
    indicators = {
        "NGDPDPC": "GDP per capita (US$)",
        "NGDP_RPCH": "GDP Growth (% change)",
    }

    # Create a WEO object to import the data
    weo = WorldEconomicOutlook()

    # Load the data for the relevant indicators
    for indicator in indicators:
        weo.load_indicator(indicator_code=indicator)

    # Return a dataframe which filters for the most recent year,
    # maps the right indicator names, and fixes the formatting and structure
    return (
        weo.get_data(keep_metadata=False)
        .query(f"year == {current_year}")
        .assign(
            indicator=lambda d: d.indicator.map(indicators),
            year=lambda d: d.year.dt.year,
        )
        .pivot(index=["iso_code", "year"], columns="indicator", values="value")
        .round(2)
        .reset_index()
        .rename({"iso_code": MapDataSchema.ISO_CODE})
    )


def owid_covid_indicators() -> pd.DataFrame:
    df = (
        owid_tools.read_owid_data()
        .pipe(
            owid_tools.get_indicators_ts,
            indicators=["people_fully_vaccinated_per_hundred"],
        )
        .pipe(owid_tools.filter_countries_only)
    )

    df = (
        df.dropna(subset="value")
        .groupby(["iso_code", "indicator"], as_index=False)
        .last()
    )

    return df.filter(["iso_code", "date", "value"], axis=1).rename(
        columns={
            "value": "Covid Vaccination Rate (%)",
            "iso_code": MapDataSchema.ISO_CODE,
        }
    )


def wb_indicators() -> pd.DataFrame:

    indicators = {
        "SP.DYN.LE00.IN": "Life Expectancy",
        "SP.DYN.IMRT.IN": "Infant Mortality Rate",
        "SE.ADT.LITR.ZS": "Literacy Rate",
        "SP.DYN.TFRT.IN": "Fertility Rate",
        "HD.HCI.OVRL": "Human Capital Index",
    }

    wb = WorldBankData()

    for indicator in indicators:
        wb.load_indicator(indicator_code=indicator, most_recent_only=True)

    return (
        wb.get_data()
        .assign(indicator=lambda d: d.indicator.map(indicators))
        .filter(["iso_code", "indicator", "value"], axis=1)
        .pivot(index="iso_code", columns="indicator", values="value")
        .round(2)
        .reset_index()
        .rename({"iso_code": MapDataSchema.ISO_CODE})
    )


def latest_inflation_data() -> pd.DataFrame:
    wfp = WFPData()

    for indicator in wfp.available_indicators:
        wfp.load_indicator(indicator)

    return (
        wfp.get_data("inflation")
        .loc[lambda d: d.date.dt.year.between(2018, 2022)]
        .loc[lambda d: d.indicator == "Inflation Rate"]
        .groupby(["iso_code"], as_index=False)
        .last()
        .filter(["iso_code", "value"], axis=1)
        .rename(
            columns={"value": "Inflation Rate (%)", "iso_code": MapDataSchema.ISO_CODE}
        )
    )


def latest_food_data() -> pd.DataFrame:
    wfp = WFPData()

    for indicator in wfp.available_indicators:
        wfp.load_indicator(indicator)

    food = wfp.get_data("insufficient_food")

    # calculate starting date

    return (
        food.filter(["iso_code", "date", "value"], axis=1)
        .groupby(["iso_code"], as_index=False)
        .last()
        .pipe(
            add_population_share_column,
            id_column="iso_code",
            id_type="ISO3",
            target_column="Population with Insufficient Food (%)",
        )
        .rename(columns={"iso_code": MapDataSchema.ISO_CODE})
        .drop(["value", "date"], axis=1)
    )


def latest_debt_service() -> pd.DataFrame:

    return (
        pd.read_csv(
            f"{PATHS.charts}/country_page/overview_debt.csv",
            usecols=["name_short", "value"],
        )
        .pipe(add_iso_codes_column, id_column="name_short")
        .assign(value=lambda d: clean_numeric_series(d.value, to=int))
        .rename(
            columns={
                "value": "Debt Service (US$ million)",
                "iso_code": MapDataSchema.ISO_CODE,
            }
        )
    )


def map_data(base_map: pd.DataFrame) -> None:
    df = base_map_data()

    # Add WEO indicators
    df = df.merge(
        weo_indicators().drop("year", axis=1), on=MapDataSchema.ISO_CODE, how="left"
    ).dropna(how="any")

    # Add population
    df = add_population_column(
        df, id_column=MapDataSchema.ISO_CODE, id_type="ISO3", target_column="Population"
    )

    # Add world bank indicators
    df = df.merge(wb_indicators(), on=MapDataSchema.ISO_CODE, how="left")

    # Add OWID covid indicators
    df = df.merge(
        owid_covid_indicators().drop("date", axis=1),
        on=MapDataSchema.ISO_CODE,
        how="left",
    )

    # Add WFP inflation data
    df = df.merge(latest_inflation_data(), on=MapDataSchema.ISO_CODE, how="left")

    # Add latest food data
    df = df.merge(latest_food_data(), on=MapDataSchema.ISO_CODE, how="left")

    # Add debt service data
    df = df.merge(latest_debt_service(), on=MapDataSchema.ISO_CODE, how="left")

    order = [
        MapDataSchema.GEOMETRY,
        MapDataSchema.FORMAL_NAME,
        MapDataSchema.NAME,
        MapDataSchema.ISO_CODE,
        "Income Level",
        "GDP Growth (% change)",
        "GDP per capita (US$)",
        "Inflation Rate (%)",
        "Population with Insufficient Food (%)",
        "Debt Service (US$ million)",
        "Covid Vaccination Rate (%)",
        "Population",
        "Fertility Rate",
        "Infant Mortality Rate",
        "Life Expectancy",
        "Literacy Rate",
        "Human Capital Index",
    ]

    df.filter(order, axis=1).to_csv(f"{PATHS.charts}/home_map_data.csv", index=False)


def bubble_data(base_bubble: pd.DataFrame) -> None:
    df = base_bubble

    base_map = pd.read_csv(f"{PATHS.charts}/home_map_data.csv").drop(
        ["geometry", "formal_name", "name"], axis=1
    )

    df = df.merge(base_map, on=MapDataSchema.ISO_CODE, how="left")

    order = [
        BubbleDataSchema.FORMAL_NAME,
        BubbleDataSchema.NAME,
        BubbleDataSchema.ISO_CODE,
        BubbleDataSchema.POSITION,
        "Income Level",
        "GDP Growth (% change)",
        "GDP per capita (US$)",
        "Inflation Rate (%)",
        "Population with Insufficient Food (%)",
        "Debt Service (US$ million)",
        "Covid Vaccination Rate (%)",
        "Population",
        "Fertility Rate",
        "Infant Mortality Rate",
        "Life Expectancy",
        "Literacy Rate",
        "Human Capital Index",
    ]

    df.filter(order, axis=1).to_csv(f"{PATHS.charts}/home_bubble_data.csv", index=False)


map_data(base_map_data())
bubble_data(base_bubble_data())
