import pandas as pd
from bblocks import add_short_names_column

from scripts import common
from scripts.config import PATHS
from scripts.debt.common import get_indicator_data, DEBT_SERVICE, DEBT_STOCKS
from scripts.logger import logger

START_YEAR: int = 2009
END_YEAR: int = 2028


# ---------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------


def _download_ids_service() -> None:
    """Use API to download IDS data"""

    d_ = [
        get_indicator_data(i, start_year=START_YEAR, end_year=END_YEAR)
        for i in DEBT_SERVICE
    ]

    df = (
        pd.concat(d_, ignore_index=True)
        .astype(
            {
                "time": "Int16",
                "country": "category",
                "series_code": "category",
                "counterpart-area": "category",
                "series": "category",
            }
        )
        .reset_index(drop=True)
    )

    df.to_feather(f"{PATHS.raw_debt}/ids_service_raw.feather")
    logger.info("Downloaded IDS debt service data")


def download_ids_stocks(
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    file_name: str = "ids_stocks_raw",
) -> None:
    """Use API to download IDS debt stocks"""

    d_ = [
        get_indicator_data(i, start_year=start_year, end_year=end_year)
        for i in DEBT_STOCKS
    ]

    df = (
        pd.concat(d_, ignore_index=True)
        .astype(
            {
                "time": "Int16",
                "country": "category",
                "series_code": "category",
                "counterpart-area": "category",
                "series": "category",
            }
        )
        .reset_index(drop=True)
    )

    df.to_feather(f"{PATHS.raw_debt}/{file_name}.feather")
    logger.info(f"Downloaded IDS debt stocks data: {file_name}")


def update_ids_data() -> None:
    _download_ids_service()
    download_ids_stocks()


# ---------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------


def add_ids_iso(
    df: pd.DataFrame, name_col: str = "country", target_col: str = "iso_code"
):
    codes = pd.read_csv(f"{PATHS.raw_data}/debt/ids_country_codes.csv")
    codes = codes.rename(columns={"iso_code": target_col})

    df = df.merge(codes, left_on=[name_col], right_on=["name"], how="left")

    return df.drop("name", axis=1)


def read_ids_service() -> pd.DataFrame:
    """Read IDS debt service data"""

    return pd.read_feather(f"{PATHS.raw_data}/debt/ids_service_raw.feather")


def read_ids_stocks(file_name: str = "ids_stocks_raw") -> pd.DataFrame:
    """Read IDS debt service data"""

    return pd.read_feather(f"{PATHS.raw_data}/debt/{file_name}.feather")


def clean_ids_data(df: pd.DataFrame, detail: bool = False) -> pd.DataFrame:
    """Avoid total duplication, add iso_codes, simplify series names"""

    dict_ = {**DEBT_STOCKS, **DEBT_SERVICE}

    if detail:
        df = df.loc[df["counterpart-area"] != "World"]
    else:
        df = df.loc[df["counterpart-area"] == "World"]

    return (
        df.rename(columns={"time": "year", "counterpart-area": "counterpart"})
        .pipe(add_ids_iso)
        .assign(indicator=lambda d: d.series_code.map(dict_))
        .groupby(
            ["iso_code", "year", "indicator", "counterpart"],
            as_index=False,
            observed=True,
            dropna=False,
        )["value"]
        .sum()
    )


def _flourish_clean_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe for Flourish"""

    return (
        df.loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .groupby(
            ["iso_code", "year", "indicator"],
            as_index=False,
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .pivot(index=["iso_code", "year"], columns="indicator", values="value")
        .assign(Total=lambda d: d.fillna(0).sum(axis=1))
        .reset_index(drop=False)
        .pipe(
            add_short_names_column,
            id_column="iso_code",
            id_type="ISO3",
            target_column="country_name",
        )
        .sort_values(by=["year", "iso_code"])
        .reset_index(drop=True)
    ).filter(
        [
            "year",
            "iso_code",
            "Bilateral",
            "Multilateral",
            "Private",
            "Total",
            "country_name",
        ],
        axis=1,
    )


def _clean_ids_china_service(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe of detailed debt service"""

    china = df.loc[df["counterpart"] == "China"].copy()
    df = df.loc[df["counterpart"] != "China"]

    df = df.groupby(
        ["iso_code", "year", "indicator"], as_index=False, dropna=False, observed=True
    ).sum(numeric_only=True)
    china = china.groupby(
        ["iso_code", "year", "indicator"], as_index=False, dropna=False, observed=True
    ).sum(numeric_only=True)

    indicators_other = {
        "Bilateral": "Bilateral (excl. China)",
        "Private": "Private (excl. China)",
        "Multilateral": "Multilateral",
    }

    indicators_china = {"Bilateral": "Bilateral (China)", "Private": "Private (China)"}

    df.indicator = df.indicator.map(indicators_other)
    china.indicator = china.indicator.map(indicators_china)

    order = {
        "Bilateral (excl. China)": 1,
        "Bilateral (China)": 2,
        "Multilateral": 3,
        "Private (excl. China)": 4,
        "Private (China)": 5,
    }

    return (
        pd.concat([df, china], ignore_index=True)
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .assign(order=lambda d: d.indicator.map(order))
        .sort_values(["iso_code", "year", "order"], ascending=(True, True, True))
        .drop("order", axis=1)
        .reset_index(drop=True)
    )


def clean_ids_china_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe for Flourish"""

    china = df.loc[df["counterpart"] == "China"].copy()
    df = df.loc[df["counterpart"] != "China"]

    df = df.groupby(["iso_code", "year", "indicator"], as_index=False).sum(
        numeric_only=True
    )
    china = china.groupby(["iso_code", "year", "indicator"], as_index=False).sum(
        numeric_only=True
    )

    indicators_other = {
        "Bilateral": "Bilateral (excl. China)",
        "Private": "Private (excl. China)",
        "Multilateral": "Multilateral",
    }

    indicators_china = {
        "Bilateral": "Bilateral (China)",
        "Private": "Private (China)",
        "Multilateral": "Multilateral (China)",
    }

    df.indicator = df.indicator.map(indicators_other)
    china.indicator = china.indicator.map(indicators_china)

    order = {
        "Bilateral (excl. China)": 1,
        "Bilateral (China)": 2,
        "Multilateral": 3,
        "Private (excl. China)": 4,
        "Private (China)": 5,
    }

    return (
        pd.concat([df, china], ignore_index=True)
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .assign(order=lambda d: d.indicator.map(order))
        .sort_values(["iso_code", "year", "order"], ascending=(True, True, True))
        .drop("order", axis=1)
        .reset_index(drop=True)
        .pipe(
            add_short_names_column,
            id_column="iso_code",
            id_type="ISO3",
            target_column="iso_code",
        )
    )


# ---------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------


def flourish_pivot_debt(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pivot(index=["iso_code", "year"], columns=["indicator"], values="value")
        .assign(Total=lambda d: d.fillna(0).sum(axis=1).round(1))
        .reset_index(drop=False)
        .assign(country_name=lambda d: d.iso_code)
    )


def flourish_ids_debt_stocks() -> None:
    """Debt Stocks data for Flourish in Millions"""
    df = (
        read_ids_stocks()
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

    df.to_feather(f"{PATHS.raw_debt}/debt_stocks-ts.feather")
    logger.debug("Saved debt file debt_stocks-ts.feather (tracker version)")


def flourish_ids_debt_service_china() -> None:
    df = (
        read_ids_service()
        .pipe(clean_ids_data, detail=True)
        .pipe(_clean_ids_china_service)
        .assign(value=lambda d: (d.value / 1e6))  # In millions
        .loc[lambda d: d.iso_code.isin(common.get_full_africa_iso3())]
        .groupby(
            ["year", "indicator"],
            as_index=False,
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .pivot(index=["year"], columns="indicator", values="value")
        .reset_index()
        .sort_values(["year"])
        .round(2)
        .reset_index(drop=True)
    )

    file_name = "debt_service_china"

    # chart version
    df.to_csv(f"{PATHS.charts}/debt_topic/{file_name}.csv", index=False)
    logger.debug("Saved debt file debt_service_ts.csv (tracker version)")

    # download version
    df = df.assign(source="World Bank IDS")
    df.to_csv(f"{PATHS.download}/debt_topic/{file_name}.csv", index=False)


def flourish_ids_debt_service() -> None:
    """Debt service data for Flourish, in millions"""

    df = (
        read_ids_service()
        .pipe(clean_ids_data, detail=False)
        .assign(value=lambda d: (d.value / 1e6))  # In millions
        .pipe(_flourish_clean_ids)
        .pipe(
            add_short_names_column,
            id_column="iso_code",
            id_type="ISO3",
            target_column="iso_code",
        )
        .round(1)
        .reset_index(drop=True)
    )
    for column in df.columns:
        if df[column].sum() == 0:
            df = df.drop(column, axis=1)

    df.to_feather(f"{PATHS.raw_debt}/debt_service_ts.feather")
    logger.debug("Saved debt file debt_service_ts.csv (tracker version)")


def update_flourish_charts() -> None:
    """Update Flourish charts with new data"""
    flourish_ids_debt_stocks()
    flourish_ids_debt_service()
    flourish_ids_debt_service_china()


if __name__ == "__main__":
    # update_ids_data()
    update_flourish_charts()
