import pandas as pd
from bblocks.cleaning_tools.clean import format_number, convert_id
from pydeflate import deflate

from scripts.config import PATHS

CONSTANT_YEAR: int = 2021
START_YEAR: int = 2010

DAC = [
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    18,
    20,
    21,
    22,
    40,
    50,
    61,
    68,
    69,
    75,
    76,
    301,
    302,
    701,
    742,
    801,
    820,
    918,
]


def read_total_oda(official_definition: bool = True) -> pd.DataFrame:
    """Read the csv containing total ODA data"""
    flow = pd.read_csv(f"{PATHS.raw_oda}/total_oda_flow.csv", parse_dates=["year"])
    ge = pd.read_csv(f"{PATHS.raw_oda}/total_oda_ge.csv", parse_dates=["year"])

    if official_definition:
        flow = flow.loc[flow.year.dt.year < 2018]
        ge = ge.loc[ge.year.dt.year >= 2018]

    return pd.concat([flow, ge], ignore_index=True).loc[
        lambda d: d.donor_code.isin(DAC)
    ]


def _read_oda_gni() -> pd.DataFrame:
    """Read the csv containing ODA/GNI data"""

    return pd.read_csv(f"{PATHS.raw_oda}/oda_gni.csv", parse_dates=["year"])


def read_oda_africa() -> pd.DataFrame:
    """Read the csv containing ODA to Africa data"""

    return pd.read_csv(
        f"{PATHS.raw_oda}/total_oda_to_africa.csv", parse_dates=["year"]
    ).loc[lambda d: d.donor_code.isin(DAC)]


def read_oda_by_income() -> pd.DataFrame:
    """Read the csv containing ODA by income group data"""

    return pd.read_csv(
        f"{PATHS.raw_oda}/total_oda_by_income.csv", parse_dates=["year"]
    ).loc[lambda d: d.donor_code.isin(DAC)]


def read_gni() -> pd.DataFrame:
    """Read the csv containing GNI data"""

    return pd.read_csv(f"{PATHS.raw_oda}/gni.csv", parse_dates=["year"]).loc[
        lambda d: d.donor_code.isin(DAC)
    ]


def read_sectors() -> pd.DataFrame:
    df = pd.read_csv(f"{PATHS.raw_oda}/sectors_view.csv", parse_dates=["year"]).loc[
        lambda d: d.donor_code.isin(DAC)
    ]

    # df = df.loc[
    #    lambda d: (d.currency == "usd")
    #    & (d.donor_code.isin(DAC))
    #    & (d.prices == "current")
    #    & (d.recipient.isin(["Africa", "LDCs", "All Developing Countries"]))
    # ].filter(
    #    ["year", "donor_code", "indicator", "sector", "recipient", "value", "share"],
    #    axis=1,
    # )

    return df


def append_DAC_total(df: pd.DataFrame, grouper=None) -> pd.DataFrame:
    """Append the "DAC Countries, Total" value to the dataframe.
    Identify with code 20001"""
    if grouper is None:
        grouper = ["year", "flows_code", "indicator"]

    df_dac = (
        df.loc[lambda d: d.donor_code != 918]
        .groupby(grouper, as_index=False)
        .sum()
        .assign(donor_code=20001)
    )

    return pd.concat([df, df_dac], ignore_index=True)


def add_constant_change_column(df: pd.DataFrame, base: int) -> pd.DataFrame:
    """Add a column with the change in constant terms"""

    df_constant = deflate(
        df,
        base_year=base,
        date_column="year",
        source="oecd_dac",
        id_column="donor_code",
        id_type="DAC",
        target_col="value_constant",
    )

    df_constant["pct_change"] = df_constant.groupby(["donor_code", "flows_code"])[
        "value_constant"
    ].pct_change()

    df_constant["pct_change"] = format_number(
        df_constant["pct_change"],
        as_percentage=True,
        decimals=1,
    ).replace("nan%", "")

    return df_constant


def add_short_names(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        name=lambda d: convert_id(
            d.donor_code,
            from_type="DACcode",
            to_type="name_short",
            additional_mapping={
                20001: "DAC Countries, Total",
                918: "EU Institutions",
            },
        ),
    )


def filter_health_sectors(df: pd.DataFrame) -> pd.DataFrame:
    health = [
        "Basic Health",
        "Health, General",
        "Non-communicable diseases (NCDs)",
        "Population Policies/Programmes & Reproductive Health",
    ]

    return df.loc[lambda d: d.sector.isin(health)].reset_index(drop=True)
