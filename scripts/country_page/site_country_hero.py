import pandas as pd
from bblocks.analysis_tools.get import change_from_date
from bblocks.cleaning_tools.filter import filter_african_countries, filter_latest_by
from bblocks.dataframe_tools.add import (
    add_short_names_column,
    add_iso_codes_column,
    add_gov_exp_share_column,
    add_median_observation,
)
from bblocks.cleaning_tools.clean import date_to_str
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.wfp import WFPData
from bblocks.import_tools.world_bank import WorldBankData
from dateutil.relativedelta import relativedelta

from scripts.common import WEO_YEAR
from scripts.config import PATHS
from scripts.owid_covid import tools as ot

WEO_INDICATORs = {
    "NGDP_RPCH": "GDP Growth",
    "LUR": "Unemployment rate",
    "GGR_NGDP": "Government Revenue (% GDP)",
    "GGXWDN_NGDP": "Government Debt (% GDP)",
}

WB_INDICATORs = {
    "SI.POV.DDAY": "% of population below the poverty line",
    "SP.POP.TOTL": "Total Population",
    "SP.DYN.LE00.IN": "Life Expectancy",
}


def __weo_center(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        center=lambda d: d.groupby(["iso_code", "indicator"]).value.transform(
            lambda g: g / g.abs().max()
        )
    )


def _read_weo() -> pd.DataFrame:
    weo = WorldEconomicOutlook()

    for c, n in WEO_INDICATORs.items():
        weo.load_indicator(indicator_code=c, indicator_name=n)

    return (
        weo.get_data(indicators="all", keep_metadata=True)
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_column="iso_code", id_type="ISO3")
        .loc[lambda d: d.year.dt.year.between(WEO_YEAR - 10, WEO_YEAR)]
    )


def _weo_charts() -> None:

    df = _read_weo()

    for indicator in WEO_INDICATORs.values():
        df.loc[df.indicator_name == indicator].filter(
            ["name_short", "indicator_name", "year", "value"], axis=1
        ).to_csv(f"{PATHS.charts}/country_page/overview_{indicator}.csv", index=False)

        df.loc[df.indicator_name == indicator].to_csv(
            f"{PATHS.download}/country_page/overview_{indicator}.csv", index=False
        )


def __single_weo_measure(indicator_code: str, comparison_year_difference: int = 1):

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


def _gdp_growth_single_measure() -> None:
    # GDP Growth

    gdp_growth_chart = __single_weo_measure("NGDP_RPCH", comparison_year_difference=1)

    # chart version
    gdp_growth_chart.to_csv(
        f"{PATHS.charts}/country_page/overview_GDP_growth.csv", index=False
    )


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


def _group_interpolate(
    group: pd.DataFrame,
) -> pd.DataFrame:
    return group.filter(["value"]).interpolate(
        method="linear", limit_direction="forward"
    )


def _read_wfp() -> WFPData:
    wfp = WFPData()

    for indicator in wfp.available_indicators:
        wfp.load_indicator(indicator)

    return wfp


def _wfp_inflation(wfp: WFPData) -> pd.DataFrame:
    return (
        wfp.get_data("inflation")
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_type="ISO3")
        .loc[lambda d: d.date.dt.year.between(2018, 2022)]
        .loc[lambda d: d.indicator == "Inflation Rate"]
        .filter(["name_short", "date", "indicator", "value"], axis=1)
        .rename(
            columns={
                "indicator": "indicator_name",
            }
        )
    )


def _wfp_food_sm(wfp: WFPData) -> pd.DataFrame:

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


def _wfp_charts() -> None:
    """Data for the Food Security charts on the country pages"""

    source = "World Food Programme HungerMapLive"
    wfp = _read_wfp()

    inflation = _wfp_inflation(wfp)

    # Chart version
    inflation.to_csv(f"{PATHS.charts}/country_page/overview_inflation.csv", index=False)

    # Download version
    inflation.assign(source=source).to_csv(
        f"{PATHS.download}/country_page/overview_inflation.csv", index=False
    )

    _ = _wfp_food_sm(wfp)

    food = wfp.get_data("insufficient_food")

    # calculate starting date

    food = (
        food.pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_type="ISO3")
        .assign(indicator="People with insufficient food consumption")
        .filter(["name_short", "date", "indicator", "value"], axis=1)
    )

    change = (
        food.groupby(["name_short"])
        .apply(_group_monthly_change, value_columns=["value"], percentage=True)
        .assign(value=lambda d: d.value.map("Change in last month: {:+,.1%}".format))
        .reset_index(drop=True)
        .filter(["name_short", "value"], axis=1)
        .rename(columns={"value": "note"})
    )

    # For charts
    food = (
        food.pipe(
            filter_latest_by,
            date_column="date",
            value_columns="value",
            group_by=["name_short"],
        )
        .merge(change, on=["name_short"], how="left")
        .assign(
            date=lambda d: d.date.dt.strftime("%d %b %Y"),
            value=lambda d: d.value.map("{:,.0f}".format),
        )
        .rename(columns={"date": "As of"})
    )

    # Chart version
    food.to_csv(f"{PATHS.charts}/country_page/overview_food.csv", index=False)

    # Download version
    food.assign(source=source).to_csv(
        f"{PATHS.download}/country_page/overview_food.csv", index=False
    )


def _vax_chart() -> None:
    """Data for the Overview charts on the country pages"""
    data = ot.read_owid_data()

    indicator = "people_fully_vaccinated_per_hundred"
    chart_name = "overview_pct_fully_vaccinated"

    vax = (
        data.pipe(ot.get_indicators_ts, indicators=[indicator])
        .groupby(["iso_code", "indicator"], as_index=False)
        .apply(
            lambda d: d.set_index(["iso_code", "indicator", "date"]).interpolate(
                limit_direction="backward"
            )
        )
        .reset_index()
        .filter(["iso_code", "indicator", "date", "value"], axis=1)
        .dropna(subset=["value"])
    )

    change = (
        vax.groupby(["iso_code"])
        .apply(
            _group_monthly_change, value_columns=["value"], percentage=False, months=3
        )
        .reset_index(drop=True)
        .filter(["iso_code", "value"], axis=1)
        .rename(columns={"value": "note"})
        .assign(note=lambda d: round(d.note, 3))
    )

    vax = (
        vax.pipe(filter_latest_by, date_column="date", value_columns="value")
        .assign(date=lambda d: "As of " + d.date.dt.strftime("%d %B"))
        .pipe(filter_african_countries, id_type="ISO3")
        .merge(change, on=["iso_code"], how="left")
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .filter(["name_short", "date", "indicator", "value", "note"], axis=1)
        .rename(columns={"date": "As of"})
        .assign(
            lower="Change in the previous 3 months",
            center=lambda d: round(d.note / d.note.max(), 3),
        )
        .filter(["name_short", "As of", "value", "lower", "note", "center"], axis=1)
    )

    # Chart version
    vax.to_csv(f"{PATHS.charts}/country_page/{chart_name}.csv", index=False)


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

    # Create csvs for the WEO charts
    _weo_charts()

    _gdp_growth_single_measure()

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
