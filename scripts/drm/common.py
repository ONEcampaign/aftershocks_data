import pandas as pd
from bblocks import WorldEconomicOutlook, set_bblocks_data_path
from bblocks.dataframe_tools.add import add_gdp_column

from scripts.config import PATHS

set_bblocks_data_path(PATHS.bblocks_data)

UNU_NAME = "UNUWIDERGRD_2022_0.xlsx"


def gov_revenue(weo: WorldEconomicOutlook) -> pd.DataFrame:
    """Read government revenue data from the World Economic Outlook database."""

    rev: str = "GGR_NGDP"

    return (
        weo.load_data(rev)
        .get_data(keep_metadata=True)
        .filter(["iso_code", "indicator_name", "year", "value", "estimate"])
        .pipe(
            add_gdp_column,
            id_column="iso_code",
            id_type="ISO3",
            date_column="year",
            include_estimates=True,
        )
        .assign(
            value=lambda d: round(d.value / 100 * d.gdp, 1),
            indicator="Government revenue (current USD)",
        )
        .filter(["iso_code", "year", "indicator", "value", "estimate"], axis=1)
    )


north_africa: list = ["DZA", "DJI", "EGY", "LBY", "MAR", "TUN"]

revenue_indicators: list = [
    "total revenue_including grants_inc sc",
    "total revenue_excluding grants_inc_sc",
    "total resource revenue",
    "total non-resource revenue (inc sc)",
    "taxes_excluding sc",
    "non-tax revenue_total",
    "social contributions",
    "grants",
    "taxes on income, profits & capital gains_total",
]


def _read_unu() -> pd.DataFrame:
    """Read data from UNU WIDER database"""
    import re

    df = pd.read_excel(
        f"{PATHS.raw_drm}/{UNU_NAME}", sheet_name="Merged", header=[0, 1, 2]
    )

    df.columns = df.columns.map("_".join)

    df.columns = [
        re.sub(r"Unnamed: \d+_level_\d|_Unnamed: \d+_level_\d", "", c).lower()
        for c in df.columns
    ]

    return df.rename(columns={"iso": "iso_code"}).filter(
        ["iso_code", "year"] + revenue_indicators, axis=1
    )


def unu_gov_revenue() -> pd.DataFrame:

    return (
        _read_unu()
        .filter(["iso_code", "year", "total revenue_including grants_inc sc"], axis=1)
        .rename(columns={"total revenue_including grants_inc sc": "value"})
        .pipe(
            add_gdp_column,
            id_column="iso_code",
            id_type="ISO3",
            date_column="year",
            include_estimates=True,
        )
        .assign(
            value=lambda d: round(d.value * d.gdp, 1),
            indicator="Government revenue (current USD)",
        )
        .assign(year=lambda d: pd.to_datetime(d.year, format="%Y"))
        .filter(["iso_code", "year", "indicator", "value"], axis=1)
    )
