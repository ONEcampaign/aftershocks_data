from functools import reduce

import pandas as pd
from bblocks.analysis_tools.get import change_from_date
from bblocks.cleaning_tools.clean import date_to_str
from bblocks.cleaning_tools.filter import filter_african_countries
from bblocks.dataframe_tools.add import (
    add_population_column,
    add_short_names_column,
    add_iso_codes_column,
)
from dateutil.relativedelta import relativedelta

from scripts import common
from scripts.config import PATHS
from scripts.country_page.financial_security import _read_wfp, _wfp_inflation


# ------------------------------------------------------------------------------
# Country Page - Insufficient Food
# ------------------------------------------------------------------------------


def _group_monthly_change(
    group: pd.DataFrame,
    value_columns: list,
    percentage: bool,
    months: int = 1,
) -> pd.DataFrame:
    sdate = group.date.max() - relativedelta(months=months)
    edate = group.date.max()

    return change_from_date(
        group,
        date_column="date",
        start_date=sdate,
        end_date=edate,
        value_columns=value_columns,
        percentage=percentage,
    )


def wfp_insufficient_food_single_measure() -> None:
    wfp = _read_wfp()
    food = wfp.get_data("insufficient_food")

    food = (
        food.pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_type="ISO3")
        .assign(indicator="People with insufficient food consumption")
        .filter(["name_short", "date", "indicator", "value"], axis=1)
    )

    change = (
        food.groupby(["name_short"])
        .apply(_group_monthly_change, value_columns=["value"], percentage=True)
        .reset_index(drop=True)
        .filter(["name_short", "value"], axis=1)
        .rename(columns={"value": "change"})
    )

    df = (
        food.merge(change, on=["name_short"], how="left")
        .groupby(["name_short"])
        .last()
        .reset_index()
        .assign(
            date=lambda d: "On " + date_to_str(d.date, "%d %B"),
            lower="Change in the last month",
            center=lambda d: d.change / d.change.max(),
        )
        .filter(["name_short", "date", "value", "lower", "change", "center"], axis=1)
    )

    df.to_csv(f"{PATHS.charts}/country_page/overview_food_sm.csv", index=False)

    # dynamic version
    kn = (
        df.assign(
            date=lambda d: d["date"].apply(lambda x: x.split("On")[1].strip()),
            value=lambda d: d.value.map(lambda x: f"{x:,.0f}"),
        )
        .filter(["name_short", "date", "value"], axis=1)
        .pipe(
            common.df_to_key_number,
            indicator_name="insufficient_food",
            id_column="name_short",
            value_columns=["value", "date"],
        )
    )

    common.update_key_number(f"{PATHS.charts}/country_page/overview.json", kn)

    # ---- REGIONS

    regions = []
    for region in common.regions():
        _ = (
            df.pipe(add_iso_codes_column, id_column="name_short", id_type="name_short")
            .loc[lambda d: d.iso_code.isin(common.regions()[region])]
            .assign(previous=lambda d: d.value * (d.change + 1))
            .groupby("lower", as_index=False)
            .agg(
                {
                    "value": "sum",
                    "previous": "sum",
                    "change": "mean",
                    "center": "median",
                    "date": pd.Series.mode,
                }
            )
            .assign(change=lambda d: d.value / d.previous - 1, name_short=region)
            .drop("previous", axis=1)
            .assign(name_short=lambda d: d.name_short.replace(common.region_names()))
            .filter(
                ["name_short", "date", "value", "lower", "change", "center"], axis=1
            )
        )
        regions.append(_)

    regions = pd.concat(regions, ignore_index=True)

    regions.to_csv(
        f"{PATHS.charts}/country_page/overview_food_sm_region.csv", index=False
    )

    # dynamic regions version
    kn_region = (
        regions.assign(
            date=lambda d: d["date"].apply(lambda x: x.split("On")[1].strip()),
            value=lambda d: d.value.map(lambda x: f"{x:,.0f}"),
        )
        .filter(["name_short", "date", "value"], axis=1)
        .pipe(
            common.df_to_key_number,
            indicator_name="insufficient_food",
            id_column="name_short",
            value_columns=["value", "date"],
        )
    )

    common.update_key_number(
        f"{PATHS.charts}/country_page/region_overview.json", kn_region
    )


def insufficient_food_chart() -> None:
    wfp = _read_wfp()
    source = "WFP HungerMapLive"

    food = (
        wfp.get_data("insufficient_food")
        .filter(["iso_code", "date", "value"], axis=1)
        .pipe(add_population_column, id_column="iso_code", id_type="ISO3")
        .assign(value=lambda d: round(100 * d.value / d.population, 2))
        .drop("population", axis=1)
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .loc[lambda d: d.date.dt.year >= 2022]
    )

    incomplete = (
        food.groupby("date", as_index=False).value.count().loc[lambda d: d.value < 30]
    )

    median = (
        food.loc[lambda d: ~d.date.isin(incomplete.date)]
        .groupby(["date"], as_index=False)
        .value.median()
        .assign(iso_code="Africa (median)")
    )

    food = food.loc[lambda d: ~d.date.isin(incomplete.date)]

    food = (
        pd.concat([median, food], ignore_index=True)
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .drop("iso_code", axis=1)
    )

    food_pivot = food.pivot(
        index="date", columns="name_short", values="value"
    ).reset_index()

    # Chart version
    food_pivot.to_csv(
        f"{PATHS.charts}/country_page/insufficient_food_ts.csv", index=False
    )

    # Download_version
    pd.concat([median, food], ignore_index=True).assign(source=source).to_csv(
        f"{PATHS.download}/country_page/insufficient_food_ts.csv", index=False
    )


# ------------------------------------------------------------------------------
# Country Page - Food inflation
# ------------------------------------------------------------------------------


def food_inflation_chart() -> None:
    wfp = _read_wfp()
    source = "Price inflation data from the WFP VAM resource centre"

    inflation = _wfp_inflation(wfp, "Food Inflation").dropna(subset=["value"])

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

    inflation_chart = (
        pd.concat([median, inflation], ignore_index=True)
        .drop("indicator_name", axis=1)
        .pivot(index="date", columns="name_short", values="value")
        .round(2)
        .reset_index()
    )

    # Chart version
    inflation_chart.to_csv(
        f"{PATHS.charts}/country_page/food_inflation_ts.csv", index=False
    )

    # Download version
    pd.concat([median, inflation], ignore_index=True).assign(source=source).to_csv(
        f"{PATHS.download}/country_page/food_inflation_ts.csv", index=False
    )

    # regions version
    regions = []

    for region in common.regions():
        _ = (
            inflation.copy()
            .loc[lambda d: ~d.date.isin(incomplete.date)]
            .pipe(add_iso_codes_column, id_column="name_short", id_type="name_short")
            .loc[lambda d: d.iso_code.isin(common.regions()[region])]
            .groupby(["date"], as_index=False)
            .value.median()
            .assign(name_short=common.region_names()[region])
            .pivot(index="date", columns="name_short", values="value")
            .round(2)
            .reset_index()
        )
        regions.append(_)

    regions_data = reduce(
        lambda left, right: pd.merge(left, right, on=["date"], how="left"), regions
    )

    # Chart version
    regions_data.to_csv(
        f"{PATHS.charts}/country_page/food_inflation_ts_regions.csv", index=False
    )

    # Download version
    regions_data.assign(source=source).to_csv(
        f"{PATHS.download}/country_page/food_inflation_ts_regions.csv", index=False
    )
    
