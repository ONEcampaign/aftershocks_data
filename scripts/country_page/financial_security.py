import pandas as pd
from bblocks.dataframe_tools.add import add_short_names_column
from bblocks.import_tools.world_bank import WorldBankData

from scripts import common
from scripts.country_page.site_country_hero import _read_wfp, _wfp_inflation


def poverty_chart() -> pd.DataFrame:
    indicators = {
        "SI.POV.DDAY": "% of population below the poverty line",
        "SP.POP.TOTL": "Population",
    }

    indicator_names = {
        "value_poverty": "% of population below the poverty line",
        "people_in_poverty": "People below the poverty line",
    }

    wb = WorldBankData()

    for _ in indicators:
        wb.load_indicator(_)

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
        .replace(
            {
                "SSA": "Sub-Saharan Africa (ex. high income)",
                "SSF": "Sub-Saharan Africa",
                "WLD": "World",
            }
        )
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


def inflation_chart() -> pd.DataFrame:

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
