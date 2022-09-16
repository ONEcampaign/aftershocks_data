import datetime

from scripts.debt.common import read_dservice_data, read_dstocks_data
import pandas as pd
from scripts.config import PATHS
from bblocks.cleaning_tools.clean import format_number

KEY_NUMBERS: dict = {}

CURRENT_YEAR = datetime.datetime.now().year


def debt_service_africa_trend() -> None:

    df = (
        read_dservice_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .groupby(["year"], as_index=False)
        .sum()
        .assign(Total=lambda d: d.Total * 1e6)
    )

    KEY_NUMBERS["debt_service_africa"] = (
        format_number(
            df.loc[df.year == CURRENT_YEAR, "Total"], as_billions=True, decimals=1
        ).values[0]
        + " billion"
    )

    df.to_csv(f"{PATHS.charts}/debt_service_africa_trend.csv", index=False)


def debt_stocks_africa_trend() -> None:
    df = (
        read_dstocks_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .groupby(["year"], as_index=False)
        .sum()
        .assign(Total=lambda d: d.Total * 1e6)
    )

    KEY_NUMBERS["debt_stocks_africa"] = (
        format_number(
            df.loc[df.year == CURRENT_YEAR - 2, "Total"], as_billions=True, decimals=1
        ).values[0]
        + " billion"
    )

    df.to_csv(f"{PATHS.charts}/debt_stocks_africa_trend.csv", index=False)


debt_service_africa_trend()

debt_stocks_africa_trend()
