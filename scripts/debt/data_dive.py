import pandas as pd
from bblocks import convert_id

from scripts.config import PATHS
from scripts.debt.ids_data import (
    clean_ids_china_stocks,
    clean_ids_data,
    download_ids_stocks,
    flourish_pivot_debt,
    read_ids_stocks,
)
from scripts.debt.topic_page import DATE, SOURCE


def update_long_ids_stocks() -> None:
    download_ids_stocks(start_year=2000, end_year=2027, file_name="ids_stocks_raw_long")


def get_long_stocks_clean() -> pd.DataFrame:
    df = (
        read_ids_stocks(file_name="ids_stocks_raw_long")
        .pipe(clean_ids_data, detail=True)
        .pipe(clean_ids_china_stocks)
        .assign(value=lambda d: d.value / 1e6)  # in millions
        .pipe(flourish_pivot_debt)
        .round(1)
        .reset_index(drop=True)
    )

    for column in df.columns:
        if df[column].sum() == 0:
            df = df.drop(column, axis=1)

    return df


def debt_stocks_columns() -> None:
    """Bar chart of debt stocks by country"""

    df = (
        pd.read_feather(PATHS.raw_debt + r"/debt_stocks-ts.feather")
        .replace("C.A.R", "Central African Republic")
        .replace("D.R.C", "Democratic Republic of the Congo")
        .assign(
            iso_code=lambda d: convert_id(
                d.iso_code, from_type="regex", to_type="name_short"
            )
        )
    )

    africa = (
        df.groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .assign(iso_code="Africa")
    )

    df = pd.concat([africa, df], ignore_index=True).filter(
        [
            "iso_code",
            "year",
            "Bilateral (China)",
            "Bilateral (excl. China)",
            "Multilateral",
            "Private (China)",
            "Private (excl. China)",
            "Total",
        ],
        axis=1,
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/debt_stocks_ts.csv", index=False)

    # download version
    df.assign(source=f"{SOURCE}{DATE}").to_csv(
        f"{PATHS.download}/debt_topic/debt_stocks_ts.csv", index=False
    )


if __name__ == "__main__":
    update_long_ids_stocks()
    df = get_long_stocks_clean()
