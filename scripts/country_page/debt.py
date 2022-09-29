import pandas as pd
from bblocks.dataframe_tools.add import (
    add_short_names_column,
    add_iso_codes_column,
    add_gov_exp_share_column,
)

from scripts.config import PATHS


def debt_chart() -> None:
    """Data for the Debt Service key number"""

    debt = pd.read_csv(f"{PATHS.raw_data}/debt/tracker_debt_service.csv.csv")

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
