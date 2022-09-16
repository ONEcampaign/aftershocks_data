from scripts.debt.common import get_indicator_data, debt_service, debt_stocks
import pandas as pd
from scripts.config import PATHS

START_YEAR: int = 2009
END_YEAR: int = 2026


def _download_ids_service() -> None:
    """Use API to download IDS data"""

    d_ = [
        get_indicator_data(i, start_year=START_YEAR, end_year=END_YEAR)
        for i in debt_service
    ]

    df = pd.concat(d_, ignore_index=True)

    df.to_csv(f"{PATHS.raw_data}/debt/ids_service_raw.csv", index=False)
    print("Downloaded IDS debt service data")


def _download_ids_stocks() -> None:
    """Use API to download IDS debt stocks"""

    d_ = [
        get_indicator_data(i, start_year=START_YEAR, end_year=END_YEAR)
        for i in debt_stocks
    ]

    df = pd.concat(d_, ignore_index=True)

    df.to_csv(f"{PATHS.raw_data}/debt/ids_stocks_raw.csv", index=False)
    print("Downloaded IDS debt stocks data")


def add_ids_iso(
    df: pd.DataFrame, name_col: str = "country", target_col: str = "iso_code"
):

    codes = pd.read_csv(f"{PATHS.raw_data}/debt/ids_country_codes.csv")
    codes = codes.rename(columns={"iso_code": target_col})

    df = df.merge(codes, left_on=[name_col], right_on=["name"], how="left")

    return df.drop("name", axis=1)


def read_ids_service() -> pd.DataFrame:
    """Read IDS debt service data"""

    return pd.read_csv(f"{PATHS.raw_data}/ids_service_raw.csv")


def ids_stocks() -> pd.DataFrame:
    """Read IDS debt service data"""

    return pd.read_csv(f"{PATHS.raw_data}/debt/ids_stocks_raw.csv")


def _clean_ids_data(df: pd.DataFrame, detail: bool = False) -> pd.DataFrame:

    """Avoid total duplication, add iso_codes, simplify series names"""

    dict_ = {**debt_stocks, **debt_service}

    if detail:
        df = df.loc[df["counterpart-area"] != "World"]
    else:
        df = df.loc[df["counterpart-area"] == "World"]

    return (
        df.rename(columns={"time": "year", "counterpart-area": "counterpart"})
        .pipe(add_ids_iso)
        .assign(indicator=lambda d: d.series_code.map(dict_))
        .groupby(["iso_code", "year", "indicator", "counterpart"], as_index=False)[
            "value"
        ]
        .sum()
    )


def _clean_ids_china_service(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe of detailed debt service"""

    china = df.loc[df["counterpart"] == "China"].copy()
    df = df.loc[df["counterpart"] != "China"]

    df = df.groupby(["iso_code", "year", "indicator"], as_index=False).sum()
    china = china.groupby(["iso_code", "year", "indicator"], as_index=False).sum()

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
        df.append(china, ignore_index=True)
        .assign(order=lambda d: d.indicator.map(order))
        .sort_values(["iso_code", "year", "order"], ascending=(True, True, True))
        .drop("order", axis=1)
        .reset_index(drop=True)
    )


def _clean_ids_china_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe for Flourish"""

    china = df.loc[df["counterpart"] == "China"].copy()
    df = df.loc[df["counterpart"] != "China"]

    df = df.groupby(["iso_code", "year", "indicator"], as_index=False).sum()
    china = china.groupby(["iso_code", "year", "indicator"], as_index=False).sum()

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
        df.append(china, ignore_index=True)
        .assign(order=lambda d: d.indicator.map(order))
        .sort_values(["iso_code", "year", "order"], ascending=(True, True, True))
        .drop("order", axis=1)
        .reset_index(drop=True)
    )


if __name__ == "__main__":

    _download_ids_service()
    _download_ids_stocks()

    pass
