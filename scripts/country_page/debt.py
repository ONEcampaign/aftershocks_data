import pandas as pd
from bblocks import set_bblocks_data_path, DebtIDS
from bblocks.cleaning_tools.clean import convert_id, format_number
from bblocks.dataframe_tools.add import (
    add_gov_exp_share_column,
    add_gov_expenditure_column,
    add_iso_codes_column,
    add_short_names_column,
)

from scripts.common import DEBT_YEAR, df_to_key_number, update_key_number
from scripts.config import PATHS
from scripts.logger import logger

set_bblocks_data_path(PATHS.bblocks_data)


def _update_debt_data() -> None:
    debt = DebtIDS()

    service = debt.debt_service_indicators()
    stocks = debt.debt_stocks_indicators()

    debt.load_data(
        indicators=list(service) + list(stocks), start_year=2009, end_year=DEBT_YEAR + 5
    )

    debt.update_data()


def _read_debt_service_total_data() -> pd.DataFrame:
    debt = DebtIDS()

    service = debt.debt_service_indicators()

    debt.load_data(list(service), start_year=2009, end_year=DEBT_YEAR + 5)

    df = debt.get_data()

    return (
        df.query("counterpart_area == 'World'")
        .groupby(["year", "country"])["value"]
        .sum()
        .reset_index()
        .assign(year=lambda d: d.year.dt.year)
        .rename(columns={"country": "country_name", "value": "Total"})
    )


def _clean_debt_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(add_short_names_column, id_column="country_name")
        .query(f"year == {DEBT_YEAR}")
        .filter(["name_short", "year", "Total"], axis=1)
        .assign(
            year=lambda d: d["year"].astype(str) + " estimate",
            value=lambda d: round(d.Total / 1e6, 1),
        )
        .rename(columns={"year": "As of", "Total": "value_units"})
        .filter(["name_short", "As of", "indicator", "value", "value_units"], axis=1)
        .pipe(add_iso_codes_column, id_column="name_short", id_type="short_name")
    )


def debt_chart_country() -> None:
    """Data for the Debt Service key number"""

    df = _read_debt_service_total_data()
    df = _clean_debt_data(df)

    debt = (
        df.pipe(
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
        .dropna(subset="note")
    )

    # Chart version
    debt.to_csv(f"{PATHS.charts}/country_page/overview_debt_sm.csv", index=False)
    logger.debug("Saved live version of 'overview_debt_sm.csv'")

    # Key number version
    kn = (
        debt.rename(columns={"value": "debt_service", "note": "debt_service_share"})
        .assign(
            debt_service=lambda d: format_number(
                d.debt_service, as_units=True, decimals=1
            )
        )
        .pipe(
            df_to_key_number,
            indicator_name="debt_service",
            id_column="name_short",
            value_columns=["debt_service", "debt_service_share"],
        )
    )

    update_key_number(f"{PATHS.charts}/country_page/overview.json", kn)
    logger.debug("Updated 'overview.json'")


def debt_chart_region() -> None:
    df = _read_debt_service_total_data()
    df = _clean_debt_data(df)

    debt = (
        df.pipe(
            add_gov_expenditure_column,
            id_column="iso_code",
            id_type="ISO3",
            target_column="gov_exp",
            usd=True,
            include_estimates=True,
        )
        .dropna(subset=["value"])
        .assign(
            continent=lambda d: convert_id(
                d.iso_code, from_type="ISO3", to_type="continent"
            )
        )
        .query("continent == 'Africa'")
        .drop(columns=["continent"])
        .groupby(["As of"], as_index=False)[["value", "value_units", "gov_exp"]]
        .sum()
        .assign(
            name_short="Africa",
            note_number=lambda d: 100 * d.value_units / d.gov_exp,
            note=lambda d: d.note_number.round(1),
            center="",
            lower="of government spending",
        )
        .filter(["name_short", "As of", "value", "lower", "note", "center"], axis=1)
    )

    # Chart version
    debt.to_csv(f"{PATHS.charts}/country_page/overview_debt_sm_region.csv", index=False)
    logger.debug("Saved live version of 'overview_debt_sm_region.csv'")

    # Key number version
    kn = debt.rename(
        columns={"value": "debt_service", "note": "debt_service_share"}
    ).pipe(
        df_to_key_number,
        indicator_name="debt_service_regions",
        id_column="name_short",
        value_columns=["debt_service", "debt_service_share"],
    )

    update_key_number(f"{PATHS.charts}/country_page/overview.json", kn)
    logger.debug("Updated 'overview.json'")
