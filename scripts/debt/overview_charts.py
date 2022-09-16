from scripts.debt.common import read_dservice_data, read_dstocks_data
import pandas as pd
from scripts.config import PATHS

KEY_NUMBERS: dict = {}


def debt_service_africa_trend() -> None:

    df = (
        read_dservice_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .groupby(["year"], as_index=False)
        .sum()
        .assign(Total=lambda d: d.Total * 1e6)
    )

    df.to_csv(f"{PATHS.charts}/debt_service_africa_trend.csv", index=False)

