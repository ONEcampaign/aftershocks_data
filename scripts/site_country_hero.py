import pandas as pd
from bblocks.analysis_tools.get import change_from_date
from bblocks.cleaning_tools.filter import filter_african_countries, filter_latest_by
from bblocks.dataframe_tools.add import (
    add_short_names_column,
    add_iso_codes_column,
    add_gdp_share_column,
)
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.wfp import WFPData
from bblocks.import_tools.world_bank import WorldBankData
from dateutil.relativedelta import relativedelta

from scripts.config import PATHS
from scripts.owid_covid import tools as ot


def _weo_charts() -> None:
    weo = WorldEconomicOutlook()

    indicators = {
        "NGDP_RPCH": "GDP Growth",
        "LUR": "Unemployment rate",
        "GGR_NGDP": "Government Revenue (% GDP)",
        "GGXWDN_NGDP": "Government Debt (% GDP)",
    }

    for c, n in indicators.items():
        weo.load_indicator(indicator_code=c, indicator_name=n)

    df = (
        weo.get_data(indicators="all", keep_metadata=True)
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_column="iso_code", id_type="ISO3")
        .loc[lambda d: d.year.dt.year.between(2012, 2024)]
    )

    for indicator in indicators.values():
        df.loc[df.indicator_name == indicator].filter(
            ["name_short", "indicator_name", "year", "value"], axis=1
        ).to_csv(f"{PATHS.charts}/country_page/overview_{indicator}.csv", index=False)

        df.loc[df.indicator_name == indicator].to_csv(
            f"{PATHS.download}/country_page/overview_{indicator}.csv", index=False
        )


def _group_monthly_change(
    group: pd.DataFrame, value_columns: list, percentage: bool
) -> pd.DataFrame:
    """""" ""
    sdate = group.date.max() - relativedelta(months=1)
    edate = group.date.max()

    return change_from_date(
        group,
        date_column="date",
        start_date=sdate,
        end_date=edate,
        value_columns=value_columns,
        percentage=percentage,
    )


def _wfp_charts() -> None:
    """Data for the Food Security charts on the country pages"""

    wfp = WFPData()

    source = "World Food Programme HungerMapLive"

    for indicator in wfp.available_indicators:
        wfp.load_indicator(indicator)

    inflation = (
        wfp.get_data("inflation")
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_type="ISO3")
        .loc[lambda d: d.date.dt.year.between(2018, 2022)]
        .loc[lambda d: d.indicator == "Inflation Rate"]
        .filter(["name_short", "date", "indicator", "value"], axis=1)
    )

    # Chart version
    inflation.to_csv(f"{PATHS.charts}/country_page/overview_inflation.csv", index=False)

    # Download version
    inflation.assign(source=source).to_csv(
        f"{PATHS.download}/country_page/overview_inflation.csv", index=False
    )

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
        .dropna(subset="value")
        .pipe(filter_latest_by, date_column="date", value_columns="value")
        .assign(
            value=lambda d: d.value.map("{:,.1f}%".format),
            date=lambda d: d.date.dt.strftime("%d %b %Y"),
            indicator="Share of population fully vaccinated against COVID-19",
        )
        .pipe(filter_african_countries, id_type="ISO3")
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .filter(["name_short", "date", "indicator", "value"], axis=1)
        .rename(columns={"date": "As of"})
    )

    # Chart version
    vax.to_csv(f"{PATHS.charts}/country_page/{chart_name}.csv", index=False)

    # Download version
    vax.assign(source="Our World In Data").to_csv(
        f"{PATHS.download}/country_page/{chart_name}.csv", index=False
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
        .rename(columns={"year": "As of", "Total": "value"})
        .assign(indicator="Debt Service, Total (USD million)")
        .filter(["name_short", "As of", "indicator", "value"], axis=1)
        .assign(
            value_units=lambda d: d.value * 1e6,
            value=lambda d: d.value.map("{:,.0f}".format),
        )
        .pipe(add_iso_codes_column, id_column="name_short", id_type="short_name")
        .pipe(
            add_gdp_share_column,
            id_column="iso_code",
            id_type="ISO3",
            value_column="value_units",
            target_column="note",
            usd=True,
            include_estimates=True,
        )
        .drop(columns=["value_units", "iso_code"])
        .assign(note=lambda d: d.note.astype(str) + "% of GDP")
    )

    # Chart version
    debt.to_csv(f"{PATHS.charts}/country_page/overview_debt.csv", index=False)

    # Download version
    debt.assign(
        source="World Bank International Debt Statistics database, 2022"
    ).to_csv(f"{PATHS.download}/country_page/overview_debt.csv", index=False)


def _basic_info_chart() -> None:
    """Poverty headcount, total population, life expectancy, and age dependency"""

    wb = WorldBankData()

    indicators = {
        "SI.POV.DDAY": "% of population below the poverty line",
        "SP.POP.TOTL": "Total Population",
        "SP.DYN.LE00.IN": "Life Expectancy",
    }

    for code, name in indicators.items():
        wb.load_indicator(code, indicator_name=name, most_recent_only=True)

    dfs = {}
    for indicator in wb.indicators:
        dfs[indicator] = (
            wb.get_data(indicator)
            .pipe(filter_african_countries, id_column="iso_code", id_type="ISO3")
            .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
            .assign(indicator=lambda d: d.indicator_code.map(indicators))
            .filter(["date", "name_short", "indicator", "value"], axis=1)
        )

    poverty = (
        dfs["SI.POV.DDAY"]
        .assign(
            source="World Bank, Poverty and Inequality Platform.",
            value=lambda d: d.value.map("{:,.1f}%".format),
            date=lambda d: d.date.dt.year,
        )
        .rename(columns={"date": "As of"})
        .filter(["name_short", "As of", "indicator", "value", "source"], axis=1)
    )

    # Chart version
    poverty.drop("source", axis=1).to_csv(
        f"{PATHS.charts}/country_page/overview_poverty.csv", index=False
    )

    # Download version
    poverty.to_csv(f"{PATHS.download}/country_page/overview_poverty.csv", index=False)

    pop_source = (
        "( 1 ) United Nations Population Division. World Population Prospects:"
        " 2019 Revision. ( 2 ) Census reports and other statistical publications"
        " from national statistical offices, ( 3 ) Eurostat: Demographic"
        " Statistics, ( 4 ) United Nations Statistical Division. Population "
        "and Vital Statistics Reprot ( various years ), ( 5 ) U.S. Census"
        " Bureau: International Database, and ( 6 ) Secretariat of the Pacific"
        " Community: Statistics and Demography Programme."
    )

    population = (
        dfs["SP.POP.TOTL"]
        .assign(
            source=pop_source,
            value=lambda d: d.value.div(1e6).map("{:,.1f} million " "people".format),
            date=lambda d: d.date.dt.year,
        )
        .rename(columns={"date": "As of"})
        .filter(["name_short", "As of", "indicator", "value", "source"], axis=1)
    )
    # Chart version
    population.drop("source", axis=1).to_csv(
        f"{PATHS.charts}/country_page/overview_population.csv", index=False
    )

    # Download version
    population.to_csv(
        f"{PATHS.download}/country_page/overview_population.csv", index=False
    )

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

    life = (
        dfs["SP.DYN.LE00.IN"]
        .assign(source=life_source)
        .assign(
            value=lambda d: d.value.map("{:,.1f} years".format),
            date=lambda d: d.date.dt.year,
        )
        .rename(columns={"date": "As of"})
        .filter(["name_short", "As of", "indicator", "value", "source"], axis=1)
    )

    # Chart version
    life.drop("source", axis=1).to_csv(
        f"{PATHS.charts}/country_page/overview_life_expectancy.csv", index=False
    )

    # Download version
    life.to_csv(
        f"{PATHS.download}/country_page/overview_life_expectancy.csv", index=False
    )


def key_indicators_chart() -> None:
    """Data for the Overview charts on the country pages"""

    # Create csvs for the WEO charts
    _weo_charts()

    # Create csvs for the WFP charts
    _wfp_charts()

    # Create csvs for the Vax charts
    _vax_chart()

    # Create csvs for the Debt charts
    _debt_chart()

    # Create csvs for the Basic Info charts
    _basic_info_chart()


if __name__ == "__main__":
    key_indicators_chart()
