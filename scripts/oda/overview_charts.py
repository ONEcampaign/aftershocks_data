from bblocks.cleaning_tools.clean import format_number
from bblocks.cleaning_tools.filter import filter_latest_by
from pydeflate import deflate

from scripts.config import PATHS
from scripts.oda import common


# ------------------------------------------------------------------------------
#                                   Charts
# ------------------------------------------------------------------------------

KEY_NUMBERS: dict = {}


def global_aid_key_number() -> None:
    """Create an overview chart whi§ch contains the latest total ODA value and
    the change in constant terms."""

    df = (
        common.read_total_oda(official_definition=True)
        .pipe(common.append_dac_total)
        .pipe(common.add_constant_change_column, base=common.CONSTANT_YEAR)
        .assign(
            pct_change=lambda d: "Real change from previous year: " + d["pct_change"]
        )
        .pipe(common.add_short_names)
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

    # Total ODA key numberes

    KEY_NUMBERS["total_oda"] = df["value"].values[0]
    KEY_NUMBERS["total_oda_change"] = df["note"].values[0].split(": ")[1]
    KEY_NUMBERS["latest_year"] = str(df["As of"].values[0])

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
        common.read_gni()
        .pipe(
            filter_latest_by,
            date_column="year",
            group_by=["donor_code", "flows_code", "indicator"],
            value_columns=["value"],
        )
        .loc[lambda d: d.donor_code.isin(common.DAC)]
        .pipe(common.append_dac_total)
        .rename(columns={"value": "gni"})
        .filter(["year", "donor_code", "gni"], axis=1)
    )

    oda = (
        common.read_total_oda(official_definition=True)
        .pipe(common.append_dac_total)
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
        .pipe(common.add_short_names)
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

    # Key numbers
    KEY_NUMBERS["oda_gni"] = df["value"].values[0]
    KEY_NUMBERS["oda_gni_distance"] = df["note"].values[0].split(": ")[1]

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/key_number_oda_gni.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/key_number_oda_gni.csv", index=False
    )


def aid_to_africa_ts() -> None:
    df = (
        common.read_oda_africa()
        .pipe(common.append_dac_total, grouper=["year"])
        .pipe(common.add_short_names)
        .loc[lambda d: d.name == "DAC Countries, Total"]
        .pipe(
            deflate,
            base_year=common.CONSTANT_YEAR - 1,
            date_column="year",
            source="oecd_dac",
            id_column="donor_code",
            id_type="DAC",
            source_col="value_africa",
            target_col="africa_constant",
        )
        .assign(
            year=lambda d: d.year.dt.year,
            share=lambda d: format_number(
                d.value_africa / d.value_all, as_percentage=True, decimals=1
            ),
            value=lambda d: format_number(
                d.africa_constant * 1e6, as_billions=True, decimals=1
            ),
        )
        .loc[lambda d: d.year.isin(range(common.START_YEAR, 2030))]
        .filter(["name", "year", "value", "share"], axis=1)
        .rename(columns={"value": "Aid to Africa", "share": "Share of total ODA"})
    )

    # Aid to Africa key numbers
    KEY_NUMBERS["aid_to_africa"] = df["Aid to Africa"].values[-1] + " billion"
    KEY_NUMBERS["aid_to_africa_share"] = df["Share of total ODA"].values[-1]

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_africa_ts.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_africa_ts.csv", index=False
    )


def aid_to_incomes() -> None:
    df = (
        common.read_oda_by_income()
        .pipe(common.append_dac_total, grouper=["year", "recipient", "recipient_code"])
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

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_africa_ts.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_africa_ts.csv", index=False
    )


def aid_to_incomes_latest() -> None:
    df = (
        common.read_oda_by_income()
        .pipe(common.append_dac_total, grouper=["year", "recipient", "recipient_code"])
        .pipe(common.add_short_names)
        .loc[lambda d: d.name == "DAC Countries, Total"]
        .assign(
            year=lambda d: d.year.dt.year,
            value=lambda d: format_number(d.value * 1e6, as_billions=True, decimals=1),
        )
        .pipe(filter_latest_by, date_column="year", group_by=["name", "recipient"])
        .filter(
            [
                "name",
                "year",
                "recipient",
                "value",
            ],
            axis=1,
        )
        .astype({"value": "float"})
        .assign(
            share=lambda d: d.groupby("year")["value"].transform(lambda x: x / x.sum())
        )
        .assign(
            share=lambda d: format_number(d.share, decimals=1, as_percentage=True),
            lable=lambda d: d["recipient"] + ": " + d["share"],
        )
    )

    # Key numbers
    KEY_NUMBERS["aid_to_incomes"] = (
        df[["recipient", "value"]].set_index("recipient").to_dict()["value"]
    )

    KEY_NUMBERS["aid_to_incomes_share"] = (
        df[["recipient", "share"]].set_index("recipient").to_dict()["share"]
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_africa_ts.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_africa_ts.csv", index=False
    )


def aid_to_health_ts() -> None:
    df = common.aid_to_sector_ts(common.filter_health_sectors).rename(
        columns={"value": "Total aid to health", "share": "Share of total ODA"}
    )

    # Key numbers
    KEY_NUMBERS["aid_to_health"] = df["Total aid to health"].values[-1] + " billion"
    KEY_NUMBERS["aid_to_health_share"] = df["Share of total ODA"].values[-1]

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_health_ts.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_health_ts.csv", index=False
    )


def aid_to_humanitarian_ts() -> None:
    df = common.aid_to_sector_ts(common.filter_humanitarian_sectors).rename(
        columns={"value": "Total Humanitarian Aid", "share": "Share of total ODA"}
    )

    # Key numbers
    KEY_NUMBERS["aid_to_humanitarian"] = (
        df["Total Humanitarian Aid"].values[-1] + " billion"
    )
    KEY_NUMBERS["aid_to_humanitarian_share"] = df["Share of total ODA"].values[-1]

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_humanitarian_ts.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_humanitarian_ts.csv", index=False
    )


def aid_to_food() -> None:
    df = common.aid_to_sector_ts(common.filter_food_sectors).rename(
        columns={"value": "Total Food Aid", "share": "Share of total ODA"}
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_food_ts.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_food_ts.csv", index=False
    )


def aid_to_regions_ts() -> None:
    df = (
        common.read_oda_by_region()
        .pipe(common.total_by_region)
        .pipe(common.append_dac_total, grouper=["year", "recipient", "recipient_code"])
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
        .reset_index()
        .merge(share_note, on=["year", "name"])
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_regions_ts.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_regions_ts.csv", index=False
    )


def export_key_numbers_overview() -> None:
    """Export KEY_NUMBERS dictionary as json"""
    import json

    with open(f"{PATHS.charts}/oda_topic/key_numbers.json", "w") as f:
        json.dump(KEY_NUMBERS, f, indent=4)


if __name__ == "__main__":
    global_aid_key_number()
    aid_gni_key_number()
    aid_to_incomes_latest()
    aid_to_africa_ts()
    aid_to_health_ts()
    aid_to_food()
    aid_to_humanitarian_ts()
    export_key_numbers_overview()
