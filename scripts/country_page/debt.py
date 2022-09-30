import pandas as pd
from bblocks.dataframe_tools.add import (
    add_short_names_column,
    add_iso_codes_column,
    add_gov_exp_share_column,
    add_gov_expenditure_column,
)

from scripts.config import PATHS
from scripts.common import update_key_number, df_to_key_number


def _read_debt_data() -> pd.DataFrame:
    return pd.read_csv(f"{PATHS.raw_data}/debt/tracker_debt_service.csv")


def _clean_debt_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.replace("C.A.R", "Central African Republic")
        .pipe(add_short_names_column, id_column="country_name")
        .loc[lambda d: d.year == 2022]
        .filter(["name_short", "year", "Total"], axis=1)
        .assign(year=lambda d: d["year"].astype(str) + " estimate")
        .rename(columns={"year": "As of", "Total": "value"})
        .assign(value_units=lambda d: d.value * 1e6)
        .filter(["name_short", "As of", "indicator", "value", "value_units"], axis=1)
        .pipe(add_iso_codes_column, id_column="name_short", id_type="short_name")
    )


def debt_chart_country() -> None:
    """Data for the Debt Service key number"""

    df = _read_debt_data()
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
    )

    # Chart version
    debt.to_csv(f"{PATHS.charts}/country_page/overview_debt_sm.csv", index=False)

    # Key number version
    kn = debt.rename(
        columns={"value": "debt_service", "note": "debt_service_share"}
    ).pipe(
        df_to_key_number,
        indicator_name="debt_service",
        id_column="name_short",
        value_columns=["debt_service", "debt_service_share"],
    )

    update_key_number(f"{PATHS.charts}/country_page/overview.json", kn)


def debt_chart_region() -> None:
    df = _read_debt_data()
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