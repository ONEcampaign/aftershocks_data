import pandas as pd
from bblocks.cleaning_tools.filter import filter_african_countries
from bblocks.dataframe_tools.add import (
    add_short_names_column,
    add_iso_codes_column,
    add_gov_exp_share_column,
    add_median_observation,
)
from bblocks.import_tools.world_bank import WorldBankData

from scripts.config import PATHS


def _group_interpolate(
    group: pd.DataFrame,
) -> pd.DataFrame:
    return group.filter(["value"]).interpolate(
        method="linear", limit_direction="forward"
    )


def _debt_chart() -> None:
    """Data for the Debt Service key number"""

    url: str = (
        "https://onecampaign.github.io/project_covid-19_tracker/c07_debt_service_ts.csv"
    )

    debt = pd.read_csv(url, usecols=["year", "country_name", "Total"])

    debt = (
        debt.replace("C.A.R", "Central African Republic")
        .pipe(add_short_names_column, id_column="country_name")
        .loc[lambda d: d.year == 2022]
        .filter(["name_short", "year", "Total"], axis=1)
        .assign(year=lambda d: d["year"].astype(str) + " estimate")
        .rename(columns={"year": "As of", "Total": "value"})
        .assign(value_units=lambda d: d.value * 1e6)
        .filter(["name_short", "As of", "indicator", "value", "value_units"], axis=1)
        .pipe(add_iso_codes_column, id_column="name_short", id_type="short_name")
        .pipe(
            add_gov_exp_share_column,
            id_column="iso_code",
            id_type="ISO3",
            value_column="value_units",
            target_column="note",
            usd=True,
            include_estimates=True,
        )
        .drop(columns=["value_units", "iso_code"])
        .assign(
            note=lambda d: d.note.round(1), center="", lower="of government spending"
        )
        .filter(["name_short", "As of", "value", "lower", "note", "center"], axis=1)
    )

    # Chart version
    debt.to_csv(f"{PATHS.charts}/country_page/overview_debt_sm.csv", index=False)


# ---------- WORLD BANK ------------ #


def _read_wb() -> dict:
    wb = WorldBankData()

    for code, name in WB_INDICATORs.items():
        wb.load_indicator(code, indicator_name=name, most_recent_only=True)

    dfs = {}
    for indicator in wb.indicators:
        dfs[indicator] = (
            wb.get_data(indicator)
            .pipe(filter_african_countries, id_column="iso_code", id_type="ISO3")
            .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
            .assign(indicator=lambda d: d.indicator_code.map(WB_INDICATORs))
            .filter(["date", "name_short", "indicator", "value"], axis=1)
        )

    return dfs


def _read_wb_ts() -> dict:
    wb = WorldBankData()

    for code, name in WB_INDICATORs.items():
        wb.load_indicator(code, indicator_name=name)

    dfs = {}
    for indicator in wb.indicators:
        dfs[indicator] = (
            wb.get_data(indicator)
            .pipe(filter_african_countries, id_column="iso_code", id_type="ISO3")
            .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
            .assign(indicator=lambda d: d.indicator_code.map(WB_INDICATORs))
            .filter(["date", "name_short", "indicator", "value"], axis=1)
        )

    return dfs


def _wb_poverty_single_measure(data_dict: dict) -> pd.DataFrame:
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

    return (
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


def _wb_population(data_dict: dict) -> pd.DataFrame:
    pop_source = (
        "( 1 ) United Nations Population Division. World Population Prospects:"
        " 2019 Revision. ( 2 ) Census reports and other statistical publications"
        " from national statistical offices, ( 3 ) Eurostat: Demographic"
        " Statistics, ( 4 ) United Nations Statistical Division. Population "
        "and Vital Statistics Reprot ( various years ), ( 5 ) U.S. Census"
        " Bureau: International Database, and ( 6 ) Secretariat of the Pacific"
        " Community: Statistics and Demography Programme."
    )
    return (
        data_dict["SP.POP.TOTL"]
        .assign(
            source=pop_source,
            value=lambda d: d.value.div(1e6).map("{:,.1f} million " "people".format),
            date=lambda d: d.date.dt.year,
        )
        .rename(columns={"date": "As of"})
        .filter(["name_short", "As of", "indicator", "value", "source"], axis=1)
    )


def _wb_life_exp(data_dict: dict) -> pd.DataFrame:
    life_source = (
        "( 1 ) United Nations Population Division. World Population"
        " Prospects: 2019 Revision, or derived from male and female life "
        "expectancy at birth from sources such as: ( 2 ) Census reports "
        "and other statistical publications from national statistical "
        "offices, ( 3 ) Eurostat: Demographic Statistics, ( 4 ) United "
        "Nations Statistical Division. Population and Vital Statistics "
        "Report ( various years ), ( 5 ) U.S. Census Bureau: International "
        "Database, and ( 6 ) Secretariat of the Pacific Community: Statistics "
        "and Demography Programme."
    )
    return (
        data_dict["SP.DYN.LE00.IN"]
        .assign(source=life_source)
        .assign(
            value=lambda d: d.value.map("{:,.1f} years".format),
            date=lambda d: d.date.dt.year,
        )
        .rename(columns={"date": "As of"})
        .filter(["name_short", "As of", "indicator", "value", "source"], axis=1)
    )


def _basic_info_chart() -> None:
    """Poverty headcount, total population, life expectancy, and age dependency"""

    dfs = _read_wb()

    poverty = _wb_poverty_single_measure(dfs)

    # Chart version
    poverty.to_csv(f"{PATHS.charts}/country_page/overview_poverty.csv", index=False)

    population = _wb_population(dfs)
    # Chart version
    population.drop("source", axis=1).to_csv(
        f"{PATHS.charts}/country_page/overview_population.csv", index=False
    )

    # Download version
    population.to_csv(
        f"{PATHS.download}/country_page/overview_population.csv", index=False
    )

    life = _wb_life_exp(dfs)
    # Chart version
    life.drop("source", axis=1).to_csv(
        f"{PATHS.charts}/country_page/overview_life_expectancy.csv", index=False
    )

    # Download version
    life.to_csv(
        f"{PATHS.download}/country_page/overview_life_expectancy.csv", index=False
    )


def _food_sec_chart() -> None:
    url = (
        "https://onecampaign.github.io/project_covid-19_tracker/"
        "insufficient_food-trend.csv"
    )

    df = pd.read_csv(url)

    value_cols = ["People with Insufficient Food Consumption (%)"]

    df = (
        df.melt(id_vars=["indicator", "date"], var_name="country", value_name="value")
        .astype({"date": "datetime64[ns]"})
        .replace("C.A.R", "Central African Republic")
        .pipe(
            add_short_names_column,
            id_column="country",
            id_type="regex",
        )
        .loc[lambda d: d.indicator.isin(value_cols)]
        .loc[lambda d: d.date.dt.year >= 2022]
        .dropna(subset=["value"])
        .filter(
            [
                "name_short",
                "date",
                "value",
            ],
            axis=1,
        )
        .pipe(
            add_median_observation,
            group_name="Africa (median)",
            value_columns="value",
            group_by=["date"],
        )
        .sort_values(["name_short", "date"])
        .reset_index(drop=True)
        .pivot(index="date", columns="name_short", values="value")
        .reset_index()
    )

    # Chart version
    df.to_csv(f"{PATHS.charts}/country_page/country_insufficient_food.csv", index=False)

    # Download version
    df.melt(id_vars="date", var_name="country").assign(
        source="WFP HungerMapLive",
        indicator="% of People with Insufficient food Consumption",
    ).to_csv(
        f"{PATHS.download}/country_page/country_insufficient_food.csv", index=False
    )


def key_indicators_chart() -> None:
    """Data for the Overview charts on the country pages"""

    # Create csvs for the WFP charts
    _wfp_charts()

    # Create csvs for the Vax charts
    _vax_chart()

    # Create csvs for the Debt charts
    _debt_chart()

    # Create csvs for the Basic Info charts
    # _basic_info_chart()

    # Create CSVs for Food Security charts
    _food_sec_chart()


if __name__ == "__main__":
    key_indicators_chart()
