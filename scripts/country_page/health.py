import numpy as np
import pandas as pd
from bblocks import set_bblocks_data_path
from bblocks.cleaning_tools.clean import convert_id, format_number
from bblocks.cleaning_tools.filter import filter_african_countries, filter_latest_by
from bblocks.dataframe_tools.add import add_iso_codes_column, add_short_names_column
from bblocks.import_tools.world_bank import WorldBankData

from scripts import common
from scripts.common import CAUSES_OF_DEATH_YEAR
from scripts.config import PATHS
from scripts.country_page.food_security import _group_monthly_change
from scripts.country_page.health_update import read_dpt_data
from scripts.owid_covid import tools as ot

set_bblocks_data_path(PATHS.bblocks_data)

CAUSES_YEAR_COMPARISON = 2000

CAUSE_GROUPS = {
    1: "Communicable, maternal, neonatal, and nutritional diseases",
    2: "Noncommunicable diseases",
    3: "Injuries",
}

CAUSES_SOURCE = (
    "Global Health Estimates 2020: Deaths by Cause, Age, Sex,"
    " by Country and by Region, 2000-2019. "
    "Geneva, World Health Organization; 2020."
)


# ------------------------------------------------------------------------------
# Country Page - COVID Vaccination
# ------------------------------------------------------------------------------


def vaccination_rate_single_measure() -> None:
    """Data for the Overview charts on the country pages"""
    data = ot.read_owid_data()

    indicator = "people_fully_vaccinated_per_hundred"
    chart_name = "overview_pct_fully_vaccinated_single_measure"

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
        .reset_index(drop=False)
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

    # dynamic text version
    kn = (
        vax.assign(
            date=lambda d: d["As of"].apply(lambda x: x.split("As of")[1].strip()),
            value=lambda d: d.value.round(1).astype(str),
        )
        .filter(["name_short", "date", "value"], axis=1)
        .pipe(
            common.df_to_key_number,
            indicator_name="vaccination",
            id_column="name_short",
            value_columns=["value", "date"],
        )
    )

    common.update_key_number(f"{PATHS.charts}/country_page/overview.json", kn)


# ------------------------------------------------------------------------------
# Country Page - Leading Causes of Death
# ------------------------------------------------------------------------------


def _read_leading_causes_of_death(year: int) -> pd.DataFrame:
    return pd.read_csv(
        f"{PATHS.raw_data}/health/leading_causes_of_death_{year}.csv"
    ).assign(cause_group=lambda d: d.cause_group.map(CAUSE_GROUPS))


def _get_x_largest_causes(df: pd.DataFrame, x: int = 5) -> pd.DataFrame:
    idx = (
        df.groupby(["iso_code", "year"], group_keys=False)["death_rate"]
        .nlargest(x)
        .index.get_level_values(-1)
    )
    return df.loc[idx]


def _combined_causes_of_death_data(sort_indicator: str) -> pd.DataFrame:
    # get 2019 data and filter for top 10 causes
    df_latest = _read_leading_causes_of_death(CAUSES_OF_DEATH_YEAR).pipe(
        _get_x_largest_causes, x=10
    )

    causes = {}
    for country in df_latest.iso_code.unique():
        causes[country] = df_latest.query("iso_code == @country").cause.unique()

    def __causes(iso: str) -> list:
        try:
            return causes[iso]
        except KeyError:
            return []

    # get 2000 data
    df_comparison = _read_leading_causes_of_death(CAUSES_YEAR_COMPARISON)

    # combine and sort
    return (
        pd.concat([df_latest, df_comparison], ignore_index=True)
        .groupby(["iso_code", "year", "cause"], as_index=False)
        .apply(lambda d: d.loc[d.cause.isin(__causes(d.iso_code.item()))])
        .sort_values(
            by=["iso_code", "year", sort_indicator], ascending=(True, True, True)
        )
        .reset_index(drop=True)
    )


def leading_causes_of_death_chart() -> None:
    dfc = (
        _combined_causes_of_death_data("death_rate")
        .merge(common.base_africa_df(), on="iso_code", how="outer")
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(deaths=lambda d: format_number(d.deaths, as_units=True, decimals=0))
        .filter(
            ["name_short", "cause", "cause_group", "year", "death_rate", "deaths"],
            axis=1,
        )
        .rename(
            columns={
                "death_rate": "Deaths per 100K people",
                "deaths": "Deaths",
                "name_short": "Country",
                "cause": "Cause",
                "year": "Year",
                "cause_group": "Type",
            }
        )
        .assign(missing=lambda d: np.where(d.Cause.isna(), True, False))
    )

    # chart version
    dfc.to_csv(f"{PATHS.charts}/country_page/leading_causes_of_death.csv", index=False)

    # download version
    dfc.assign(source=CAUSES_SOURCE).to_csv(
        f"{PATHS.download}/country_page/leading_causes_of_death.csv", index=False
    )


def leading_causes_of_death_column_chart() -> None:
    dfc = (
        _combined_causes_of_death_data("death_rate")
        .merge(common.base_africa_df(), on="iso_code", how="outer")
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(deaths=lambda d: format_number(d.deaths, as_units=True, decimals=0))
        .fillna({"year": "missing"})
        .filter(
            ["name_short", "cause", "cause_group", "year", "death_rate", "deaths"],
            axis=1,
        )
        .pivot(
            index=["name_short", "cause", "cause_group"],
            columns="year",
            values=["death_rate", "deaths"],
        )
        .reset_index()
    )

    dfc.columns = [f"{a}_{b}".split(".")[0] for a, b in dfc.columns]

    dfc = dfc.rename(
        columns={
            "name_short_": "Country",
            "cause_": "Cause",
            "cause_group_": "Type",
            "death_rate_2000": "2000",
            "death_rate_2019": "2019",
            "deaths_2000": "Deaths (2000)",
            "deaths_2019": "Deaths (2019)",
        }
    ).drop(["death_rate_missing", "deaths_missing"], axis=1)

    # chart version
    dfc.to_csv(
        f"{PATHS.charts}/country_page/leading_causes_of_death_column.csv", index=False
    )

    # download version
    dfc.assign(source=CAUSES_SOURCE).to_csv(
        f"{PATHS.download}/country_page/leading_causes_of_death_column.csv", index=False
    )


# ------------------------------------------------------------------------------
# Country Page - Leading Causes of Death
# ------------------------------------------------------------------------------


def _get_life_expectancy() -> pd.DataFrame:
    wb = WorldBankData()
    wb.load_data("SP.DYN.LE00.IN")

    return (
        wb.get_data()
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .copy()
        .replace({"SSA": "Sub-Saharan Africa", "WLD": "World"})
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(
            indicator="Life expectancy at birth (years)",
            year=lambda d: d.date.dt.year,
        )
        .filter(["year", "name_short", "indicator", "value"], axis=1)
    )


def life_expectancy_chart() -> None:
    df = _get_life_expectancy()

    chart = (
        df.loc[lambda d: d.year.between(d.year.max() - 10, d.year.max())]
        .pivot(index=["year"], columns="name_short", values="value")
        .reset_index()
    )

    # chart version
    chart.to_csv(f"{PATHS.charts}/country_page/life_expectancy.csv", index=False)

    # download version
    chart.to_csv(f"{PATHS.download}/country_page/life_expectancy.csv", index=False)

    # dynamic version
    kn = (
        df.sort_values("year")
        .pipe(add_iso_codes_column, id_column="name_short", id_type="regex")
        .dropna(subset=["value"])
        .drop_duplicates("iso_code", keep="last")
        .assign(value=lambda d: round(d.value, 1))
        .pipe(
            common.df_to_key_number,
            id_column="iso_code",
            indicator_name="life_expectancy",
            value_columns="value",
        )
    )

    common.update_key_number(f"{PATHS.charts}/country_page/health.json", kn)


# ------------------------------------------------------------------------------
# Country Page - HIV ART
# ------------------------------------------------------------------------------


def _read_art() -> pd.DataFrame:
    return pd.read_csv(f"{PATHS.raw_data}/health/art_estimates.csv")


def art_chart() -> None:
    indicator = {
        "Among people living with HIV, the percent on ART -All ages": "people_on_art"
    }

    mapping = {
        "Global": "Africa",
        "EAS": "Africa",
        "Middle East and North Africa": "Africa",
        "Western and central Africa": "Africa",
    }

    df_art = _read_art()

    df = (
        df_art.filter(["iso_code", "year", "name"] + list(indicator), axis=1)
        .rename(columns=indicator)
        .replace("Eastern and southern Africa", "EAS")
        .assign(
            continent=lambda d: convert_id(
                d.name,
                from_type="regex",
                to_type="Continent",
                additional_mapping=mapping,
            )
        )
        .loc[lambda d: d.continent == "Africa"]
        .assign(
            name_short=lambda d: convert_id(
                d.name,
                from_type="regex",
                to_type="name_short",
                not_found=None,
                additional_mapping={"EAS": "Eastern and Southern Africa"},
            )
        )
        .filter(["year", "name_short", "people_on_art"], axis=1)
        .sort_values(["year", "name_short"])
        .drop_duplicates(["name_short", "year"], keep="last")
    )

    # chart version
    dfp = df.pivot(
        index="year", columns="name_short", values="people_on_art"
    ).reset_index()

    dfp.to_csv(f"{PATHS.charts}/country_page/people_on_art_ts.csv", index=False)

    # download version
    df.assign(source="UNAIDS").to_csv(
        f"{PATHS.download}/country_page/people_on_art.csv", index=False
    )


def _read_malaria_data() -> pd.DataFrame:
    return pd.read_csv(f"{PATHS.raw_data}/health/malaria_deaths.csv")


def malaria_chart() -> None:
    wb = WorldBankData()
    wb.load_data("SP.POP.TOTL")
    population = (
        wb.get_data()
        .drop("indicator_code", axis=1)
        .rename(columns={"value": "population"})
    )

    df = (
        _read_malaria_data()
        .dropna(subset=["value"])
        .pivot(
            index=["year", "iso_code"],
            columns="indicator",
            values="value",
        )
        .reset_index()
        .assign(
            continent=lambda d: convert_id(
                d.iso_code, from_type="ISO3", to_type="Continent", not_found="other"
            )
        )
        .loc[lambda d: d.continent != "other"]
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .assign(date=lambda d: pd.to_datetime(d.year, format="%Y"))
        .merge(population, on=["iso_code", "date"], how="left")
        .filter(
            [
                "name_short",
                "year",
                "MALARIA_EST_MORTALITY",
                "MALARIA_EST_DEATHS",
            ],
            axis=1,
        )
        .sort_values(["name_short", "year"])
        .rename(
            columns={
                "MALARIA_EST_MORTALITY": "Malaria mortality rate (per 100K people)",
                "MALARIA_EST_DEATHS": "Malaria deaths (estimated)",
            }
        )
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/country_page/malaria_deaths.csv", index=False)

    # download version
    df.assign(source="WHO").to_csv(
        f"{PATHS.download}/country_page/malaria_deaths.csv", index=False
    )


def dpt_chart() -> None:
    columns = {
        "CODE": "iso_code",
        "NAME": "name",
        "YEAR": "year",
        "COVERAGE_CATEGORY": "coverage_category",
        "COVERAGE": "coverage",
    }

    regions = {
        "African Region": "Africa",
        "Eastern Mediterranean Region": "Eastern Mediterranean",
        "European Region": "Europe",
        "South-East Asia Region": "South-East Asia",
        "Western Pacific Region": "Western Pacific",
        "Region of the Americas": "America",
        "Global": "Global",
    }

    df = read_dpt_data()

    countries = (
        df.loc[lambda d: d.GROUP == "COUNTRIES"]
        .filter(columns, axis=1)
        .rename(columns=columns)
        .loc[lambda d: d.coverage_category == "WUENIC"]
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .astype({"year": int})
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .filter(["name_short", "year", "coverage"], axis=1)
    )

    regions = (
        df.loc[lambda d: (d.GROUP == "WHO_REGIONS") | (d.GROUP == "GLOBAL")]
        .filter(columns, axis=1)
        .rename(columns=columns)
        .loc[lambda d: d.coverage_category == "WUENIC"]
        .astype({"year": int})
        .assign(name_short=lambda d: d.name.map(regions))
        .filter(["name_short", "year", "coverage"], axis=1)
    )

    data = (
        pd.concat([regions, countries], ignore_index=True)
        .pivot(index="year", columns="name_short", values="coverage")
        .reset_index()
    )

    # chart version
    data.to_csv(f"{PATHS.charts}/country_page/dpt_ts.csv", index=False)

    # download version
    pd.concat([regions, countries], ignore_index=True).assign(source="WHO").to_csv(
        f"{PATHS.download}/country_page/dpt_ts.csv", index=False
    )


if __name__ == "__main__":
    ...
    art_chart()
    # leading_causes_of_death_chart()
