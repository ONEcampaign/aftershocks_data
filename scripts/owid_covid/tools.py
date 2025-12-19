import pandas as pd

# =============================================================================
# OWID Data constant
# =============================================================================
from scripts.config import PATHS


def download_owid_data() -> None:
    """Download OWID data from Github"""

    url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

    columns = {
        "iso_code": str,
        "continent": str,
        "date": str,
        "population": float,
        "total_cases": float,
        "new_cases": float,
        "new_cases_smoothed": float,
        "total_deaths": float,
        "new_deaths": float,
        "new_deaths_smoothed": float,
        "total_cases_per_million": float,
        "new_cases_per_million": float,
        "new_cases_smoothed_per_million": float,
        "total_deaths_per_million": float,
        "new_deaths_per_million": float,
        "new_deaths_smoothed_per_million": float,
        "total_tests": float,
        "new_tests": float,
        "total_tests_per_thousand": float,
        "new_tests_per_thousand": float,
        "new_tests_smoothed": float,
        "new_tests_smoothed_per_thousand": float,
        "total_vaccinations": float,
        "people_vaccinated": float,
        "people_fully_vaccinated": float,
        "total_boosters": float,
        "new_vaccinations": float,
        "new_vaccinations_smoothed": float,
        "total_vaccinations_per_hundred": float,
        "people_vaccinated_per_hundred": float,
        "people_fully_vaccinated_per_hundred": float,
        "total_boosters_per_hundred": float,
        "new_vaccinations_smoothed_per_million": float,
    }

    try:
        df = pd.read_csv(
            url,
            usecols=columns.keys(),
            dtype=columns,
            encoding=None,
            engine="python",
        )

        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

        print("Downloaded OWID data successfully")
        df.to_feather(PATHS.raw_data + r"/owid_data.feather")

    except ConnectionError:
        raise ConnectionError("Data could not be updated")

    except UnicodeError:
        raise UnicodeError("Wrong encoding. Data could not be updated")


def read_owid_data() -> pd.DataFrame:
    """Read OWID data"""

    return pd.read_feather(PATHS.raw_data + r"/owid_data.feather")


# =============================================================================
# Functions
# =============================================================================


def date_resample(df: pd.DataFrame, grouper=None) -> pd.DataFrame:
    """Resample dates from daily to weekly, always keeping the latest 2 values"""

    # get value for yesterday
    if grouper is None:
        grouper = ["iso_code", "indicator"]

    yesterday = pd.Timestamp("today").floor("D") + pd.Timedelta(-2, unit="D")

    # Split into latest and timeseries
    df_latest = df.loc[df.date >= yesterday]
    df = df.loc[df.date < yesterday]

    # Resample other data
    df = (
        df.groupby(grouper)
        .apply(
            lambda x: x.set_index("date").asfreq(freq="W-MON"), include_groups=False
        )["value"]
        .reset_index(drop=False)
    )

    # Append 'latest data' to resampled data and drop nans
    df = pd.concat([df_latest, df], ignore_index=True)
    df = df.dropna(subset=["value"])

    return df.reset_index(drop=True)


def interpolate(
    df: pd.DataFrame, start_date: str, end_date: str = None
) -> pd.DataFrame:
    """Interpolate based on each iso_code"""

    if end_date is None:
        end_date = df.date.max()

    frame = pd.DataFrame({"date": pd.date_range(start_date, end_date)})

    def __interp(df_, iso_code, frame):
        return (
            df.loc[lambda d: d.iso_code == iso_code]
            .merge(frame, how="right", on="date")
            .sort_values(by="date")
            .set_index("date")
            .interpolate(method="linear", limit_direction="forward")
            .reset_index(drop=False)
        )

    return pd.concat(
        [__interp(df, x, frame) for x in df.iso_code.unique()], ignore_index=True
    )


def get_indicators_ts_wide(
    owid_data: pd.DataFrame, indicators: list[str]
) -> pd.DataFrame:
    """Select indicators and return wide"""

    id_vars = ["iso_code", "date"]

    return owid_data.filter([*id_vars, *indicators], axis=1)


def get_indicators_ts(owid_data: pd.DataFrame, indicators: list[str]) -> pd.DataFrame:
    """Get indicators and return long"""

    if isinstance(indicators, str):
        indicators = [indicators]

    id_vars = ["iso_code", "date"]

    return get_indicators_ts_wide(owid_data, indicators).melt(
        id_vars=id_vars, var_name="indicator"
    )


# =============================================================================
# Filters
# =============================================================================


def filter_africa(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only OWID Africa"""
    return df.query('iso_code == "OWID_AFR"').reset_index(drop=True)


def filter_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only OWID regions"""
    regions = ["OWID_AFR", "OWID_EUR", "OWID_ASI", "OWID_OCE", "OWID_SAM", "OWID_NAM"]

    return df.loc[lambda d: d.iso_code.isin(regions)].reset_index(drop=True)


def filter_world(df: pd.DataFrame) -> pd.DataFrame:
    """keep only OWID world"""
    return df.query('iso_code == "OWID_WRL"').reset_index(drop=True)


def filter_countries_only(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only Countries"""
    return df.loc[lambda d: ~d.iso_code.str.contains("OWID")].reset_index(drop=True)
