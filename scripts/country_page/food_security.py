import pandas as pd
from bblocks.dataframe_tools.add import add_population_column, add_short_names_column

from scripts import common
from scripts.country_page.site_country_hero import (
    _read_wfp,
    _wfp_food_sm,
    _wfp_inflation,
)


def insufficient_food_chart() -> pd.DataFrame:

    wfp = _read_wfp()

    food = _wfp_food_sm(wfp)

    food = (
        wfp.get_data("insufficient_food")
        .filter(["iso_code", "date", "value"], axis=1)
        .pipe(add_population_column, id_column="iso_code", id_type="ISO3")
        .assign(value=lambda d: round(100 * d.value / d.population, 2))
        .drop("population", axis=1)
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
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

    food = (
        pd.concat([median, food], ignore_index=True)
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .drop("iso_code", axis=1)
        .pivot(index="date", columns="name_short", values="value")
        .reset_index()
    )


def food_inflation_chart() -> pd.DataFrame:

    wfp = _read_wfp()

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

    inflation = (
        pd.concat([median, inflation], ignore_index=True)
        .drop("indicator_name", axis=1)
        .pivot(index="date", columns="name_short", values="value")
        .reset_index()
    )
