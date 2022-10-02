import pandas as pd
import requests

from scripts.config import PATHS

WHO_API_URL = "https://ghoapi.azureedge.net/api/"


def query_who(code: str):
    """Query the WHO website for a given code.
    To be replaced in bblocks
    """

    request = requests.get(WHO_API_URL + code)
    data = request.json()
    df = pd.DataFrame.from_records(data["value"])

    return df


def update_malaria_data() -> None:
    """Update WHO data for malaria"""
    df = (
        query_who("MALARIA_EST_DEATHS")
        .loc[lambda d: d.SpatialDim.isin(["GLOBAL", "AFR"])]
        .filter(["SpatialDim", "TimeDim", "NumericValue"], axis=1)
    )

    df.to_csv(f"{PATHS.raw_data}/health/who_malaria_data.csv", index=False)


def read_malaria_data() -> pd.DataFrame:
    """Read malaria data from file"""
    return pd.read_csv(f"{PATHS.raw_data}/health/who_malaria_data.csv")


def get_malaria_data() -> dict:
    """Extract and clean malaria data for overview chart"""

    malaria_dict = (
        read_malaria_data()
        .astype({"TimeDim": "int64"})
        .sort_values("TimeDim")
        .groupby(["SpatialDim"], as_index=False)
        .last()
        .assign(NumericValue=lambda d: pd.to_numeric(d.NumericValue, errors="coerce"))
        .pivot(index="TimeDim", columns="SpatialDim", values="NumericValue")
        .reset_index()
        .assign(
            malaria_rest_of_world_total=lambda d: d["GLOBAL"] - d["AFR"],
            malaria_africa_proportion=lambda d: (d["AFR"] / d["GLOBAL"]) * 100,
            malaria_rest_of_world_proportion=lambda d: (
                d["malaria_rest_of_world_total"] / d["GLOBAL"]
            )
            * 100,
        )
        .rename(
            columns={
                "AFR": "malaria_africa_total",
                "GLOBAL": "malaria_world_total",
                "TimeDim": "malaria_year",
            }
        )
        .round(0)
        .astype(int)
        .to_dict(orient="records")[0]
    )
    return malaria_dict
