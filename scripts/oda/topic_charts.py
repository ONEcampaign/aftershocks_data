import pandas as pd
from bblocks import format_number
from oda_data import ODAData, set_data_path
from oda_data.tools.groupings import donor_groupings

from scripts.config import PATHS
from scripts.logger import logger
from scripts.oda import common

set_data_path(PATHS.raw_oda)
DacMembers = (
    donor_groupings()["dac_members"]
    | {84: "Lithuania"}
    | {82: "Estonia"}
    | {20001: "DAC Countries, Total"}
)


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


def oda_covid_idrc():
    from oda_data import ODAData, set_data_path
    from oda_data.tools.groupings import donor_groupings

    set_data_path(PATHS.raw_oda)

    dg = donor_groupings()

    oda22 = ODAData(
        years=range(2015, 2024),
        donors=list(dg["dac_countries"]) + [20001],
        prices="constant",
        base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    oda23 = ODAData(
        years=range(2015, 2024),
        donors=list(dg["dac_countries"]) + [20001],
        prices="constant",
        base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    indicators22 = ["total_covid_oda_ge"]
    indicators23 = ["total_oda_ge", "total_oda_flow_net", "idrc_ge_linked"]

    d22 = oda22.load_indicator(indicators22).get_data()
    d23 = oda23.load_indicator(indicators23).get_data()

    data = pd.concat([d22, d23], ignore_index=True)

    data = data.loc[
        lambda d: ~((d.year < 2018) & (d.indicator == "total_oda_ge"))
        & ~((d.year >= 2018) & (d.indicator == "total_oda_flow_net"))
    ]

    dac = list(dg["dac_countries"])

    dac_covid = (
        data.loc[lambda d: d.donor_code.isin(dac)]
        .query("indicator in ['total_covid_oda_ge']")
        .groupby(
            ["year", "indicator", "currency", "prices"],
            as_index=False,
            observed=True,
            dropna=False,
        )["value"]
        .sum(numeric_only=True)
        .assign(donor_code=20001, donor_name="DAC Countries, Total")
    )

    ukraine_aid = (
        aid_to_ukraine()
        .assign(indicator="aid_to_ukraine")
        .loc[lambda d: d.year >= 2015]
    )

    data = pd.concat([data, dac_covid, ukraine_aid], ignore_index=True)

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
                d["Total ODA"].fillna(0)
                - d["total_covid_oda_ge"].fillna(0)
                - d["idrc_ge_linked"].fillna(0)
                - d["aid_to_ukraine"].fillna(0),
                1,
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
                "total_covid_oda_ge": "COVID ODA",
                "idrc_ge_linked": "IDRC",
                "aid_to_ukraine": "ODA to Ukraine",
                "other_oda": "Other ODA",
            }
        )
        .filter(
            [
                "Year",
                "Donor",
                "COVID ODA",
                "IDRC",
                "ODA to Ukraine",
                "Other ODA",
                "Total ODA",
            ],
            axis=1,
        )
    )

    # df["Preliminary"] = df.loc[lambda d: d.Year == 2023, "Other ODA"]
    # df.loc[lambda d: d.Year == 2023, "Other ODA"] = None

    # live version
    df.to_csv(f"{PATHS.charts}/oda_topic/oda_covid.csv", index=False)
    logger.debug("Saved live chart oda_covid.csv")

    # download version
    source = "OECD DAC Table1"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/oda_covid.csv", index=False
    )
    logger.debug("Saved download version of oda_covid.csv")


def oda_idrc_share():
    from oda_data import ODAData, set_data_path
    from oda_data.tools.groupings import donor_groupings

    set_data_path(PATHS.raw_oda)

    dg = donor_groupings()

    oda = ODAData(
        years=range(2008, 2024),
        donors=list(dg["dac_countries"]) + [20001],
        prices="constant",
        base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    indicators = [
        "total_oda_ge",
        "total_oda_flow_net",
        "idrc_ge_linked",
    ]

    data = (
        oda.load_indicator(indicators)
        .get_data()
        .loc[
            lambda d: ~((d.year < 2018) & (d.indicator == "total_oda_ge"))
            & ~((d.year >= 2018) & (d.indicator == "total_oda_flow_net"))
        ]
    )

    dac = list(dg["dac_countries"])

    # dac2022 = (
    #     data.loc[lambda d: d.donor_code.isin(dac) & (d.year == 2022)]
    #     .query("indicator in ['idrc_ge_linked']")
    #     .groupby(
    #         ["year", "indicator", "currency", "prices"],
    #         as_index=False,
    #         observed=True,
    #         dropna=False,
    #     )["value"]
    #     .sum(numeric_only=True)
    #     .assign(donor_code=20001, donor_name="DAC Countries, Total")
    # )
    #
    # data = pd.concat([data, dac2022], ignore_index=True)

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
    )

    dac_total = data.loc[lambda d: d.donor_code == 20001]
    other = data.loc[lambda d: d.donor_code != 20001]

    df = (
        pd.concat([dac_total, other], ignore_index=True)
        .rename(
            columns={
                "year": "Year",
                "donor_name": "Donor",
                "idrc_ge_linked": "IDRC",
            }
        )
        .filter(
            ["Year", "Donor", "COVID ODA", "IDRC", "Other ODA", "Total ODA"], axis=1
        )
    )

    df = df.query("Donor == 'DAC Countries, Total'").assign(
        share=lambda d: round(d.IDRC / d["Total ODA"] * 100, 1)
    )

    # live version
    df.to_csv(f"{PATHS.charts}/oda_topic/oda_idrc_share.csv", index=False)
    logger.debug("Saved live chart oda_covid.csv")

    # download version
    source = "OECD DAC Table1"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/oda_idrc_share.csv", index=False
    )
    logger.debug("Saved download version of oda_idrc_share.csv")


def flow_shares_idrc_covid():
    from oda_data import ODAData, set_data_path
    from oda_data.tools.groupings import donor_groupings

    set_data_path(PATHS.raw_oda)

    dg = donor_groupings()

    oda = ODAData(
        years=range(2010, 2023),
        donors=list(dg["dac_countries"]) + [20001],
        prices="constant",
        base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    indicators = [
        "total_covid_oda_ge",
        "total_oda_flow_net",
        "idrc_ge_linked",
    ]

    data = oda.load_indicator(indicators).get_data()

    ukraine = (
        oda.load_indicator("recipient_bilateral_flow_net")
        .get_data("recipient_bilateral_flow_net")
        .query("recipient_name == 'Ukraine' and donor_code == 20001")
    )

    # urkaine22 = ukraine.query("year == 2021").assign(value=16120.581863, year=2022)
    # ukraine = pd.concat([ukraine, urkaine22], ignore_index=True)

    dac = list(dg["dac_countries"])

    dac_covid = (
        data.loc[lambda d: d.donor_code.isin(dac)]
        .query("indicator in [ 'total_covid_oda_ge']")
        .groupby(
            ["year", "indicator", "currency", "prices"],
            as_index=False,
            observed=True,
            dropna=False,
        )["value"]
        .sum(numeric_only=True)
        .assign(donor_code=20001, donor_name="DAC Countries, Total")
    )

    data = pd.concat([data, dac_covid], ignore_index=True)

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
    )

    data = (
        data.query("donor_code == 20001")
        .merge(ukraine.filter(["year", "value"], axis=1), on="year", how="left")
        .rename(columns={"value": "Bilateral aid to Ukraine"})
    )

    data = data.assign(
        other_oda=lambda d: round(
            d["Total ODA"].fillna(0)
            - d["total_covid_oda_ge"].fillna(0)
            - d["idrc_ge_linked"].fillna(0)
            - d["Bilateral aid to Ukraine"].fillna(0),
            1,
        )
    )

    df = data.rename(
        columns={
            "year": "Year",
            "donor_name": "Donor",
            "total_covid_oda_ge": "COVID ODA",
            "idrc_ge_linked": "IDRC",
            "other_oda": "Other ODA",
        }
    ).filter(
        [
            "Year",
            "Donor",
            "COVID ODA",
            "IDRC",
            "Bilateral aid to Ukraine",
            "Other ODA",
            "Total ODA",
        ],
        axis=1,
    )

    # live version
    df.to_csv(f"{PATHS.charts}/oda_topic/oda_ukraine_covid_refugees.csv", index=False)
    logger.debug("Saved live chart oda_covid.csv")

    # download version
    source = "OECD DAC Table1"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/oda_ukraine_covid_refugees.csv", index=False
    )
    logger.debug("Saved download version of oda_covid.csv")


def aid_to_ukraine() -> pd.DataFrame:
    from oda_data import ODAData, set_data_path
    from oda_data.tools.groupings import donor_groupings

    set_data_path(PATHS.raw_oda)

    dg = donor_groupings()

    oda = ODAData(
        years=range(2015, 2024),
        donors=list(dg["dac_countries"]),
        prices="constant",
        base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    oda.load_indicator(
        [
            "recipient_imputed_multi_flow_net",
            "crs_bilateral_ge",
            "recipient_total_flow_net",
        ]
    )

    df_bilateral_ge = (
        oda.get_data("crs_bilateral_ge")
        .loc[lambda d: d.recipient_name == "Ukraine"]
        .loc[lambda d: d.year >= 2018]
    )

    df_bilateral_ge = (
        df_bilateral_ge.groupby(
            [
                "year",
                "indicator",
                "donor_code",
                "donor_name",
                "recipient_code",
                "recipient_name",
                "currency",
                "prices",
            ],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
    )

    df_multilateral = (
        oda.get_data("recipient_imputed_multi_flow_net")
        .loc[lambda d: d.recipient_name == "Ukraine"]
        .loc[lambda d: d.year >= 2018]
    )

    df_total_flow = (
        oda.get_data("recipient_total_flow_net")
        .loc[lambda d: d.recipient_name == "Ukraine"]
        .loc[lambda d: d.year < 2018]
    )

    df = (
        pd.concat([df_bilateral_ge, df_multilateral, df_total_flow], ignore_index=True)
        .groupby(
            [
                "year",
                "donor_code",
                "donor_name",
                "recipient_code",
                "recipient_name",
                "currency",
                "prices",
            ],
            dropna=False,
            observed=True,
        )["value"]
        .sum()
        .reset_index()
        .assign(indicator="aid_to_ukraine")
    )

    dac = (
        df.groupby(
            [
                "year",
                "indicator",
                "recipient_code",
                "recipient_name",
                "currency",
                "prices",
            ],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
        .assign(donor_code=20001, donor_name="DAC Countries, Total")
    )

    df = pd.concat([df, dac], ignore_index=True)

    return df


def aid_to_ukraine_comparison() -> None:
    data = (
        aid_to_ukraine()
        .filter(["year", "donor_name", "recipient_name", "value"])
        .loc[lambda d: d.year > 2020]
        .round(2)
        .loc[lambda d: d.donor_name != "DAC Countries, Total"]
    )

    # chart version
    data.to_csv(f"{PATHS.charts}/oda_topic/aid_to_ukraine_comparison.csv", index=False)

    # download version
    source = "OECD DAC Table2a and Credit Reporting System (CRS)"

    data.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_ukraine_comparison.csv", index=False
    )


if __name__ == "__main__":
    global_aid_ts()
    oda_gni_single_year()
    sector_totals()
    aid_to_regions_ts()
    aid_to_incomes()
    oda_covid_idrc()
    oda_idrc_share()
    flow_shares_idrc_covid()
    aid_to_ukraine_comparison()
    ...
