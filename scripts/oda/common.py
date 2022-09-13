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


SECTORS_MAPPING: dict = {
    "Other Sectors": [
        "Action Relating to Debt",
        "General Budget Support",
        "Other Commodity Assistance",
    ],
    "Administrative Costs of Donors": ["Administrative Costs of Donors"],
    "Agriculture & Forestry and Fishing": [
        "Agriculture",
        "Forestry & Fishing",
    ],
    "Other Economic Infrastructure": [
        "Banking & Financial Services",
        "Business & Other Services",
        "Communications",
        "Energy",
        "Industry, Mining & Construction",
        "Industry, Mining, Construction",
        "Trade Policies & Regulations",
        "Transport & Storage",
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
    "Other Social Infrastructure": [
        "Conflict, Peace & Security",
        "Conflict Peace and Security",
        "Government & Civil Society",
        "Other Social Infrastructure & Services",
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
    "Multisector": [
        "Disaster Risk Reduction",
        "Multi-Sector",
        "Other multi-sector Aid",
        "Rural Development",
        "Urban Development",
    ],
    "Social Protection": [
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
    """Read the csv containing total ODA data"""
    flow = pd.read_csv(f"{PATHS.raw_oda}/total_oda_flow.csv", parse_dates=["year"])
    ge = pd.read_csv(f"{PATHS.raw_oda}/total_oda_ge.csv", parse_dates=["year"])

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


def read_oda_by_region() -> pd.DataFrame:
    return pd.read_csv(
        f"{PATHS.raw_oda}/total_oda_by_region.csv", parse_dates=["year"]
    ).loc[lambda d: d.donor_code.isin(DAC)]


def total_by_region(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(
        ["year", "donor_code", "recipient_code", "recipient"], as_index=False
    )["value"].sum()


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

    return df.loc[lambda d: d.sector.isin(SECTORS_MAPPING["Health"])].reset_index(
        drop=True
    )


def filter_humanitarian_sectors(df: pd.DataFrame) -> pd.DataFrame:

    return df.loc[lambda d: d.sector.isin(SECTORS_MAPPING["Humanitarian"])].reset_index(
        drop=True
    )


def filter_food_sectors(df: pd.DataFrame) -> pd.DataFrame:
    food = ["Developmental Food Aid/Food Security Assistance"]

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
        .pipe(append_DAC_total, grouper=["year"])
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
                base_year=CONSTANT_YEAR - 1,
                date_column="year",
                source="oecd_dac",
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
