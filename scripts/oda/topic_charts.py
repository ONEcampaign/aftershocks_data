import pandas as pd
from bblocks.cleaning_tools.clean import format_number
from oda_data import set_data_path, ODAData
from oda_data.tools.groupings import donor_groupings
from pydeflate import deflate

from scripts.config import PATHS
from scripts.logger import logger
from scripts.oda import common


set_data_path(PATHS.raw_oda)
DacMembers = donor_groupings()["dac_members"] | {20001: "DAC Countries, Total"}


def _ge_filter(ge_indicator: str, flow_indicator: str) -> str:
    return (
        f"(indicator=='{ge_indicator}' and year >=2018) | "
        f"(indicator=='{flow_indicator}' and year <2018)"
    )


def global_aid_ts() -> None:
    """Create an overview chart which contains the latest total ODA value and
    the change in constant terms."""

    oda = ODAData(
        years=range(2000, 2024),
        donors=list(DacMembers),
        prices="constant",
        base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    oda.load_indicator(["total_oda_ge", "total_oda_flow_net"]).add_share_of_gni()

    data = (
        oda.get_data()
        .query(_ge_filter("total_oda_ge", "total_oda_flow_net"))
        .assign(
            value=lambda d: round(d.value, 2), gni_share=lambda d: round(d.gni_share, 2)
        )
        .sort_values(["donor_name", "year"])
        .filter(["donor_name", "year", "value", "gni_share"], axis=1)
        .rename(
            columns={
                "value": "ODA (left-axis)",
                "gni_share": "ODA/GNI (right-axis)",
                "donor_name": "name",
            }
        )
        .pipe(common.sort_dac_first, keep_current_sorting=True)
    )

    # chart version
    data.to_csv(f"{PATHS.charts}/oda_topic/oda_gni_ts.csv", index=False)
    logger.debug("Saved live chart oda_gni_ts.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    data.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/oda_gni_ts.csv", index=False
    )
    logger.debug("Saved download chart oda_gni_ts.csv")


def oda_gni_single_year() -> None:

    oda = ODAData(
        years=range(2000, 2024),
        donors=list(DacMembers),
        # prices="constant",
        # base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    oda.load_indicator(["total_oda_ge", "total_oda_flow_net", "gni"])

    data = (
        oda.get_data(["total_oda_ge", "total_oda_flow_net"])
        .query(_ge_filter("total_oda_ge", "total_oda_flow_net"))
        .merge(
            oda.get_data("gni").filter(["year", "donor_code", "value"], axis=1),
            on=["year", "donor_code"],
            how="left",
            suffixes=("", "_gni"),
        )
        .assign(
            oda_gni=lambda d: round(100 * d.value / d.value_gni, 3),
            missing=lambda d: round(d.value_gni * 0.007 - d.value, 1),
        )
        .assign(missing=lambda d: d.missing.apply(lambda v: v if v > 0 else 0))
        .assign(
            value=lambda df_: df_.value.apply(
                lambda d: f"{d / 1e3:.2f} billion" if d > 1e3 else f"{d:.1f} million"
            ),
            missing=lambda df_: df_.missing.apply(
                lambda d: f"{d / 1e3:.2f} billion" if d > 1e3 else f"{d:.1f} million"
            ),
        )
        .loc[lambda d: (d.donor_code != 918) & (d.year >= common.START_YEAR)]
        .filter(["donor_name", "year", "value", "missing", "oda_gni"], axis=1)
        .sort_values(["year", "donor_name"], ascending=[False, True])
        .rename(
            {
                "value": "Total ODA",
                "oda_gni": "ODA/GNI",
                "donor_name": "Donor",
                "year": "Year",
                "missing": "ODA short of 0.7% commitment",
            },
            axis=1,
        )
    )

    # chart version
    data.to_csv(f"{PATHS.charts}/oda_topic/oda_gni_single_year_ts.csv", index=False)
    logger.debug("Saved live chart oda_gni_single_year_ts.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    data.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/oda_gni_single_year_ts.csv", index=False
    )
    logger.debug("Saved download chart oda_gni_sing_year_ts.csv")


def _sectors_ts() -> tuple[pd.DataFrame, list]:
    df_ = common.read_sectors()
    common.check_sector_completeness(df_=df_)

    df = pd.DataFrame()

    for sn, sl in common.SECTORS_MAPPING.items():
        _ = df_.pipe(common.filter_map_broad_sector, sector_name=sn, sectors_list=sl)
        df = pd.concat([df, _], ignore_index=True)

    df = (
        df.groupby(["year", "donor_code", "sector", "recipient"], as_index=False)[
            ["value", "share"]
        ]
        .sum()
        .loc[lambda d: d.recipient == "All Developing Countries"]
        .assign(year=lambda d: d.year.dt.year)
        .pipe(
            common.append_dac_total,
            grouper=["year", "sector", "recipient"],
        )
        .pipe(common.add_short_names)
        .assign(
            share=lambda d: d.groupby(["year", "name", "recipient"])["value"].transform(
                lambda x: x / x.sum()
            )
        )
        .filter(["name", "year", "sector", "share", "value"], axis=1)
        .sort_values(["year", "name", "share"], ascending=[False, True, False])
    )

    order = (
        df.groupby(["sector"], as_index=False)["share"]
        .sum()
        .sort_values("share", ascending=False)
        .sector.to_list()
    )

    return df, order


def sector_totals() -> None:
    df, order = _sectors_ts()

    df = (
        df.pivot(index=["year", "name"], columns="sector", values="value")
        .round(1)
        .filter(order, axis=1)
        .reset_index()
        .pipe(common.sort_dac_first, keep_current_sorting=True)
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/sector_totals.csv", index=False)
    logger.debug("Saved live chart sector_totals.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/sector_totals.csv", index=False
    )
    logger.debug("Saved download chart sector_totals.csv")


def key_sector_shares() -> None:
    key_sectors = [
        "Humanitarian",
        "Education",
        "Health",
        "Refugees in Donor Countries",
        "Social Protection",
        "Environment Protection",
        "Agriculture & Forestry and Fishing",
    ]

    df, order = _sectors_ts()

    order = [s for s in order if s in key_sectors]

    df = (
        df.pivot(index=["year", "name"], columns="sector", values="share")
        .round(4)
        .filter(order, axis=1)
        .reset_index()
        .pipe(common.sort_dac_first, keep_current_sorting=True)
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/key_sector_shares.csv", index=False)
    logger.debug("Saved live chart key_sector_shares.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/key_sector_shares.csv", index=False
    )
    logger.debug("Saved download chart key_sector_shares.csv")


def aid_to_regions_ts() -> None:
    df = (
        common.read_oda_by_region()
        .loc[lambda d: d.recipient != "Middle East"]
        .pipe(common.total_by_region)
        # .pipe(common.append_dac_total, grouper=["year", "recipient", "recipient_code"])
        .pipe(common.add_short_names)
        .assign(year=lambda d: d.year.dt.year)
        .assign(
            share=lambda d: d.groupby(["year", "donor_code"])["value"].transform(
                lambda x: x / x.sum()
            )
        )
        .filter(["year", "name", "recipient", "value", "share"], axis=1)
        .assign(
            share_note=lambda d: format_number(
                d.share, decimals=1, as_percentage=True
            ).replace("nan%", "", regex=False)
        )
    )

    share_note = (
        df.sort_values(["name", "recipient"])
        .assign(
            value=lambda d: format_number(1e6 * d.value, decimals=1, as_millions=True)
            + " million"
        )
        .groupby(["year", "name"])[["value", "recipient"]]
        .apply(
            lambda d: "<br>".join("<b>" + d.recipient.fillna("") + ":</b> " + d.value)
        )
        .reset_index()
        .rename(columns={0: "note"})
    )

    df = (
        df.pivot(index=["year", "name"], columns="recipient", values="value")
        .round(2)
        .reset_index()
        .merge(share_note, on=["year", "name"])
    )

    df = common.sort_dac_first(df, keep_current_sorting=True)

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_regions_ts.csv", index=False)
    logger.debug("Saved live chart aid_to_regions_ts.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_regions_ts.csv", index=False
    )
    logger.debug("Saved download chart aid_to_regions_ts.csv")


def aid_to_incomes() -> None:
    df = (
        common.read_oda_by_income()
        # .pipe(common.append_dac_total, grouper=["year", "recipient", "recipient_code"])
        .pipe(common.add_short_names)
        .assign(
            year=lambda d: d.year.dt.year, value=lambda d: round((d.value / 1e3), 2)
        )
        .filter(
            [
                "name",
                "year",
                "recipient",
                "value",
            ],
            axis=1,
        )
        .groupby(["name", "year", "recipient"], as_index=False)
        .sum()
        .pivot(index=["name", "year"], columns="recipient", values="value")
    )

    df2 = df.copy(deep=True).rename(columns=lambda d: d + " (value)").round(2)

    df = df.merge(df2, left_index=True, right_index=True).reset_index()

    df = common.sort_dac_first(df, keep_current_sorting=True)

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_income_ts.csv", index=False)
    logger.debug("Saved chart version of aid_to_income_ts.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_income_ts.csv", index=False
    )
    logger.debug("Saved download version of aid_to_income_ts.csv")


def oda_covid():
    from oda_data import ODAData, set_data_path
    from oda_data.tools.groupings import donor_groupings

    set_data_path(PATHS.raw_oda)

    dg = donor_groupings()

    oda = ODAData(
        years=range(2015, 2022),
        donors=list(dg["dac_countries"]) + [20001],
        prices="constant",
        base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    indicators = ["total_covid_oda_ge_linked", "total_oda_ge", "total_oda_flow_net"]

    data = (
        oda.load_indicator(indicators)
        .get_data()
        .loc[
            lambda d: ~((d.year < 2018) & (d.indicator == "total_oda_ge"))
            & ~((d.year >= 2018) & (d.indicator == "total_oda_flow_net"))
        ]
    )

    data.indicator = data.indicator.replace(
        {"total_oda_ge": "Total ODA", "total_oda_flow_net": "Total ODA"}
    )

    data = (
        data.pivot(
            index=["year", "donor_code", "donor_name"],
            columns="indicator",
            values="value",
        )
        .round(1)
        .reset_index()
        .assign(
            other_oda=lambda d: round(
                d["Total ODA"].fillna(0) - d["total_covid_oda_ge_linked"].fillna(0), 1
            )
        )
    )

    dac_total = data.loc[lambda d: d.donor_code == 20001]
    other = data.loc[lambda d: d.donor_code != 20001]

    df = (
        pd.concat([dac_total, other], ignore_index=True)
        .rename(
            columns={
                "year": "Year",
                "donor_name": "Donor",
                "total_covid_oda_ge_linked": "COVID ODA",
                "other_oda": "Other ODA",
            }
        )
        .filter(["Year", "Donor", "COVID ODA", "Other ODA", "Total ODA"], axis=1)
    )

    # live version
    df.to_csv(f"{PATHS.charts}/oda_topic/oda_covid.csv", index=False)
    logger.debug("Saved live chart oda_covid.csv")

    # download version
    source = "OECD DAC Table1"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/oda_covid.csv", index=False
    )
    logger.debug("Saved download version of oda_covid.csv")


if __name__ == "__main__":
    global_aid_ts()
    oda_gni_single_year()
    sector_totals()
    key_sector_shares()
    aid_to_regions_ts()
    aid_to_incomes()
    oda_covid()
