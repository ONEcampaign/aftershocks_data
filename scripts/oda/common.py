import numpy as np
import pandas as pd
from bblocks import convert_id, format_number
from pydeflate import deflate, set_pydeflate_path
from oda_data import donor_groupings
from scripts.config import PATHS

set_pydeflate_path(PATHS.raw_data)

# Define a year for the constant price calculations
CONSTANT_YEAR: int = 2023

# Start year for the timeseries charts
START_YEAR: int = 2010

# DAC codes for the members of the DAC
DAC = list(donor_groupings()["dac_members"])

# How to group sectors into more aggregated categories
SECTORS_MAPPING: dict = {
    "Action Relating to Debt": ["Action Relating to Debt"],
    "Other Sectors": [
        "Other Commodity Assistance",
        np.nan,
        "nan",
        "Administrative Costs of Donors",
        "Disaster Risk Reduction",
        "Multi-Sector",
        "Other multi-sector Aid",
        "Rural Development",
        "Urban Development",
        "Government & Civil Society",
    ],
    "Government": [
        "General Budget Support",
        "Decentralization & Subnational government",
        "Domestic resource mobilisation",
        "Legal & Judicial Development",
        "Legislature & Political Parties",
        "Macroeconomic policy",
        "Migration",
        "Elections",
        "Public finance management",
        "Public procurement",
        "Public sector policy & management",
    ],
    "Agriculture & Forestry and Fishing": [
        "Agriculture",
        "Forestry & Fishing",
    ],
    "Other Economic Infrastructure": [
        "Banking & Financial Services",
        "Business & Other Services",
        "Transport & Storage",
        "Communications",
        "Industry, Mining, Construction",
        "Trade Policies & Regulations",
    ],
    "Conflict Peace & Security": [
        "Conflict Peace and Security",
    ],
    "Education": [
        "Education, Level Unspecified",
        "Basic Education",
        "Secondary Education",
        "Post-Secondary Education",
    ],
    "Health": [
        "Basic Health",
        "Health, General",
        "Non-communicable diseases (NCDs)",
        "Population Policies/Programmes & Reproductive Health",
    ],
    "Environment Protection": [
        "Bio-diversity",
        "Biosphere Protection",
        "Environment Education/Training",
        "Environmental Policy and Admin Management",
        "Environmental Research",
        "Site-Preservation",
        "Site- Preservation",
    ],
    "Civil Society": [
        "Anti-corruption organisations and institutions",
        "Democratic participation and civil society",
        "Media & Free Flow of Information",
        "Ending violence against women and girls",
        "Human Rights",
        "Women's rights organisations, movements, and institutions",
    ],
    "Water Supply & Sanitation": [
        "Water Supply & Sanitation",
    ],
    "Developmental Food Aid/Food Security Assistance": [
        "Developmental Food Aid/Food Security Assistance"
    ],
    "Humanitarian": [
        "Disaster Prevention & Preparedness",
        "Emergency Response",
        "Reconstruction, Relief & Rehabilitation",
    ],
    "Energy": [
        "Energy",
        "Energy Distribution",
        "Energy Generation, Non-renewable",
        "Energy Generation, Renewable",
        "Energy Policy",
        "Hybrid Energy Plants",
        "Nuclear Energy Plants",
    ],
    "Social Protection": [
        "Other Social Infrastructure & Services",
        "Multi-Sector Aid for Basic Social Services",
        "Social Protection",
    ],
    "Refugees in Donor Countries": ["Refugees in Donor Countries"],
    "Unallocated/ Unspecified": [
        "Unallocated/ Unspecified",
        "Unallocated/ Unspecificed",
    ],
}


def read_total_oda(official_definition: bool = True) -> pd.DataFrame:
    """Read the csv containing total ODA data.
    This CSV is generated in another repository. Eventually this should
    be replaced by a connection to our database
    """
    flow = pd.read_csv(f"{PATHS.raw_oda}/total_oda_flow.csv", parse_dates=["year"])
    ge = pd.read_csv(f"{PATHS.raw_oda}/total_oda_ge.csv", parse_dates=["year"])

    # Filter dataframe to keep only the 'official definition' of ODA in a given year
    # This means GE from 2018 and Flow before then.
    if official_definition:
        flow = flow.loc[flow.year.dt.year < 2018]
        ge = ge.loc[ge.year.dt.year >= 2018]

    return pd.concat([flow, ge], ignore_index=True).loc[
        lambda d: d.donor_code.isin(DAC)
    ]


def read_oda_gni() -> pd.DataFrame:
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
    ).loc[lambda d: d.donor_code.isin(DAC + [20001])]


def read_gni() -> pd.DataFrame:
    """Read the csv containing GNI data"""

    return pd.read_csv(f"{PATHS.raw_oda}/gni.csv", parse_dates=["year"]).loc[
        lambda d: d.donor_code.isin(DAC)
    ]


def read_sectors() -> pd.DataFrame:
    df = (
        pd.read_csv(f"{PATHS.raw_oda}/sectors_view.csv", parse_dates=["year"])
        .astype({"donor_code": "Int16"})
        .loc[lambda d: d.donor_code.isin(DAC)]
    )

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


def read_oda_by_region() -> pd.DataFrame:
    return pd.read_csv(
        f"{PATHS.raw_oda}/total_oda_by_region.csv", parse_dates=["year"]
    ).loc[lambda d: d.donor_code.isin(DAC + [20001])]


def total_by_region(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(
        ["year", "donor_code", "recipient_code", "recipient"], as_index=False
    )["value"].sum()


def append_dac_total(df: pd.DataFrame, grouper=None) -> pd.DataFrame:
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


def add_change(
    df: pd.DataFrame, grouper: list = None, as_formatted_str: bool = False
) -> pd.DataFrame:
    if grouper is None:
        grouper = ["donor_code", "indicator"]

    df["pct_change"] = df.groupby(grouper)["value"].pct_change()

    if not as_formatted_str:
        return df

    df["pct_change"] = format_number(
        df["pct_change"],
        as_percentage=True,
        decimals=1,
    ).replace("nan%", "")

    return df


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
    return df.loc[lambda d: d.sector.isin(SECTORS_MAPPING["Health"])].reset_index(
        drop=True
    )


def filter_humanitarian_sectors(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[lambda d: d.sector.isin(SECTORS_MAPPING["Humanitarian"])].reset_index(
        drop=True
    )


def filter_food_sectors(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[
        lambda d: d.sector.isin(
            SECTORS_MAPPING["Developmental Food Aid/Food Security Assistance"]
        )
    ].reset_index(drop=True)


def aid_to_sector_ts(filter_function: callable) -> pd.DataFrame:
    all_sectors = (
        read_sectors()
        .loc[
            lambda d: (d.donor_code != 918)
            & (d.recipient == "All Developing Countries")
        ]
        .groupby(["year", "recipient"], as_index=False)["value"]
        .sum()
        .filter(["year", "value"], axis=1)
    )

    return (
        read_sectors()
        .pipe(filter_function)
        .loc[lambda d: d.recipient == "All Developing Countries"]
        .groupby(["year", "donor_code"], as_index=False)["value"]
        .sum()
        .pipe(append_dac_total, grouper=["year"])
        .pipe(add_short_names)
        .loc[lambda d: d.name == "DAC Countries, Total"]
        .merge(all_sectors, on=["year"], how="left", suffixes=("", "_all"))
        .assign(
            year=lambda d: d.year.dt.year,
            share=lambda d: format_number(
                d.value / d.value_all, decimals=1, as_percentage=True
            ),
        )
        .assign(
            value=lambda d: deflate(
                d,
                base_year=CONSTANT_YEAR,
                date_column="year",
                deflator_source="oecd_dac",
                deflator_method="dac_deflator",
                exchange_source="oecd_dac",
                id_column="donor_code",
                id_type="DAC",
                source_col="value",
                target_col="value_constant",
            ).value_constant,
        )
        .assign(
            value=lambda d: format_number(d.value * 1e6, as_billions=True, decimals=1)
        )
        .filter(["name", "year", "value", "share"], axis=1)
    )


def filter_map_broad_sector(
    df: pd.DataFrame, sector_name, sectors_list: list
) -> pd.DataFrame:
    return (
        df.loc[lambda d: d.sector.isin(sectors_list)]
        .assign(sector=sector_name)
        .reset_index(drop=True)
    )


def check_sector_completeness(df_: pd.DataFrame) -> None:
    """Check that all sectors are in the mapping"""

    all_sectors = [
        sector for sectors_list in SECTORS_MAPPING.values() for sector in sectors_list
    ]

    not_included = set(df_.sector.unique()) - set(all_sectors)

    assert len(not_included) == 0


def sort_dac_first(df: pd.DataFrame, keep_current_sorting=True):
    if not keep_current_sorting:
        df = df.sort_values(["year", "name"], ascending=[True, False])

    dac = df.query("name == 'DAC Countries, Total'").reset_index(drop=True)
    other = df.query("name != 'DAC Countries, Total'").reset_index(drop=True)

    return pd.concat([dac, other], ignore_index=True)
