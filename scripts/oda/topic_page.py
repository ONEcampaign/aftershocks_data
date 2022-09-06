from scripts.config import PATHS
import pandas as pd
from pydeflate import deflate
from bblocks.cleaning_tools.clean import format_number, convert_id
from bblocks.cleaning_tools.filter import filter_latest_by

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


def _read_total_oda(official_definition: bool = True) -> pd.DataFrame:
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


def _read_oda_africa() -> pd.DataFrame:
    """Read the csv containing ODA to Africa data"""

    return pd.read_csv(
        f"{PATHS.raw_oda}/total_oda_to_africa.csv", parse_dates=["year"]
    ).loc[lambda d: d.donor_code.isin(DAC)]


def _read_gni() -> pd.DataFrame:
    """Read the csv containing GNI data"""

    return pd.read_csv(f"{PATHS.raw_oda}/gni.csv", parse_dates=["year"])


def _append_DAC_total(df: pd.DataFrame, grouper=None) -> pd.DataFrame:
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


def _add_constant_change_column(df: pd.DataFrame, base: int) -> pd.DataFrame:
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


def _add_short_names(df: pd.DataFrame) -> pd.DataFrame:

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


# ------------------------------------------------------------------------------
#                                   Charts
# ------------------------------------------------------------------------------


def global_aid_key_number() -> None:
    """Create an overview chart which contains the latest total ODA value and
    the change in constant terms."""

    df = (
        _read_total_oda(official_definition=True)
        .pipe(_append_DAC_total)
        .pipe(_add_constant_change_column, base=2021)
        .assign(
            pct_change=lambda d: "Real change from previous year: " + d["pct_change"]
        )
        .pipe(_add_short_names)
        .loc[lambda d: d.name == "DAC Countries, Total"]
        .assign(
            year=lambda d: d.year.dt.year,
            value=lambda d: format_number(d.value * 1e6, as_billions=True, decimals=1)
            + " billion",
        )
        .pipe(
            filter_latest_by,
            date_column="year",
            group_by=["name"],
            value_columns=["value", "pct_change"],
        )
        .filter(["name", "year", "value", "pct_change"], axis=1)
        .rename(
            columns={
                "year": "As of",
                "pct_change": "note",
            }
        )
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/key_number_total_oda.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/key_number_total_oda.csv", index=False
    )


def aid_gni_key_number() -> None:
    """Create an overview chart which contains the latest ODA/GNI value and
    the change in constant terms."""

    gni = (
        _read_gni()
        .pipe(
            filter_latest_by,
            date_column="year",
            group_by=["donor_code", "flows_code", "indicator"],
            value_columns=["value"],
        )
        .loc[lambda d: d.donor_code.isin(DAC)]
        .pipe(_append_DAC_total)
        .rename(columns={"value": "gni"})
        .filter(["year", "donor_code", "gni"], axis=1)
    )

    oda = (
        _read_total_oda(official_definition=True)
        .pipe(_append_DAC_total)
        .pipe(
            filter_latest_by,
            date_column="year",
            group_by=["donor_code"],
            value_columns=["value"],
        )
        .rename(columns={"value": "oda"})
        .filter(["year", "donor_code", "oda"], axis=1)
    )

    df = (
        oda.merge(gni, on=["year", "donor_code"], how="left")
        .assign(
            oda_gni=lambda d: d.oda / d.gni, distance=lambda d: d.gni * 0.007 - d.oda
        )
        .pipe(_add_short_names)
        .loc[lambda d: d.name == "DAC Countries, Total"]
        .assign(
            oda_gni=lambda d: format_number(d.oda_gni, decimals=2, as_percentage=True),
            distance=lambda d: "Additional required to get to 0.7%: US$"
            + format_number(d.distance * 1e6, decimals=0, as_billions=True)
            + " billion",
            year=lambda d: d.year.dt.year,
        )
        .filter(["name", "year", "oda_gni", "distance"], axis=1)
        .rename({"oda_gni": "value", "distance": "note", "year": "As of"}, axis=1)
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/key_number_oda_gni.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/key_number_oda_gni.csv", index=False
    )


def aid_to_africa_key_number() -> None:
    df = (
        _read_oda_africa()
        .pipe(_append_DAC_total, grouper=["year"])
        .pipe(_add_short_names)
        .loc[lambda d: d.name == "DAC Countries, Total"]
        .assign(
            year=lambda d: d.year.dt.year,
            value=lambda d: format_number(
                d.value_africa * 1e6, as_billions=True, decimals=1
            )
            + " billion",
            share=lambda d: format_number(
                d.value_africa / d.value_all, decimals=1, as_percentage=True
            )
            + " of total ODA",
        )
        .pipe(
            filter_latest_by,
            date_column="year",
            group_by=["name"],
            value_columns=["value", "value_africa", "value_all", "share"],
        )
        .filter(["name", "year", "value", "share"], axis=1)
        .rename(
            columns={
                "year": "As of",
                "share": "note",
            }
        )
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/key_number_aid_to_africa.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/key_number_aid_to_africa.csv", index=False
    )


if __name__ == "__main__":
    global_aid_key_number()
    aid_gni_key_number()
    aid_to_africa_key_number()
