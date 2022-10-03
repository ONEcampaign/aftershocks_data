from bblocks.cleaning_tools.clean import format_number
from bblocks.cleaning_tools.filter import filter_latest_by
from pydeflate import deflate

from scripts.config import PATHS
from scripts.logger import logger
from scripts.oda import common

from scripts.common import update_key_number, df_to_key_number


def global_aid_key_number() -> None:
    """Create an overview chart whi§ch contains the latest total ODA value and
    the change in constant terms."""

    df = (
        common.read_total_oda(official_definition=True)
        .assign(value=lambda d: d.value * 1e6)
        .pipe(common.append_dac_total)
        .pipe(common.add_constant_change_column, base=common.CONSTANT_YEAR)
        .pipe(common.add_short_names)
        .loc[lambda d: d.name == "DAC Countries, Total"]
        .assign(
            year=lambda d: d.year.dt.year,
            pct_change=lambda d: d["pct_change"].str.replace("%", ""),
        )
        .pipe(
            filter_latest_by,
            date_column="year",
            group_by=["name"],
            value_columns=["value", "pct_change"],
        )
        .assign(
            first_line=lambda d: f"As of {d.year.item()}",
            second_line=lambda d: f"real change from {d.year.item() - 1}",
            centre=lambda d: round(d["pct_change"].astype(float) / 10, 2),
        )
        .filter(
            ["name", "first_line", "value", "second_line", "pct_change", "centre"],
            axis=1,
        )
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/sm_total_oda.csv", index=False)
    logger.debug("Saved chart version of sm_total_oda.csv")

    # Dynamic text version
    kn = {
        "total_oda": f"{df['value'].item()/1e9:,.1f} billion",
        "total_oda_change": f"{float(df['pct_change'].item()):.1f} %",
        "latest_year": f"{df['first_line'].item().split(' ')[-1]}",
    }

    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", kn)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")


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
            oda_gni=lambda d: d.oda / d.gni,
            distance=lambda d: round(d.gni * 0.007 - d.oda, 1),
        )
        .pipe(common.add_short_names)
        .loc[lambda d: d.name == "DAC Countries, Total"]
        .assign(
            year=lambda d: d.year.dt.year,
            first_line=lambda d: f"As of {d.year.item()}",
            second_line="Additional required to get to 0.7%",
            centre="",
        )
        .filter(
            ["name", "first_line", "oda_gni", "second_line", "distance", "centre"],
        )
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/oda_gni_sm.csv", index=False)
    logger.debug("Saved chart version of oda_gni_sm.csv")

    # Dynamic text version
    kn = {
        "oda_gni": f"{100*df.oda_gni.item():,.2f}%",
        "oda_gni_distance": f"{df.distance.item()/1e3:,.0f} billion",
    }

    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", kn)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")


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

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_africa_ts.csv", index=False)
    logger.debug("Saved chart version of aid_to_africa_ts.csv")

    # Dynamic text version
    kn = {
        "aid_to_africa": f"{df['Aid to Africa'].values[-1]} billion",
        "aid_to_africa_share": f"{df['Share of total ODA'].values[-1]}",
    }
    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", kn)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")

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

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_income_latest.csv", index=False)
    logger.debug("Saved chart version of aid_to_income_latest.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_income_latest.csv", index=False
    )
    logger.debug("Saved download version of aid_to_income_latest.csv")

    # Dynamic text version
    income_dict = df_to_key_number(
        df,
        indicator_name="aid_to_incomes",
        id_column="recipient",
        value_columns=["value", "share"],
    )

    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", income_dict)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")


def aid_to_health_ts() -> None:
    df = common.aid_to_sector_ts(common.filter_health_sectors).rename(
        columns={"value": "Total aid to health", "share": "Share of total ODA"}
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_health_ts.csv", index=False)
    logger.debug("Saved chart version of aid_to_health_ts.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_health_ts.csv", index=False
    )
    logger.debug("Saved download version of aid_to_health_ts.csv")

    # Dynamic text version
    kn = {
        "aid_to_health": f"{df['Total aid to health'].values[-1]} billion",
        "aid_to_health_share": f"{df['Share of total ODA'].values[-1]}",
    }
    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", kn)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")


def aid_to_humanitarian_ts() -> None:
    df = common.aid_to_sector_ts(common.filter_humanitarian_sectors).rename(
        columns={"value": "Total Humanitarian Aid", "share": "Share of total ODA"}
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_humanitarian_ts.csv", index=False)
    logger.debug("Saved chart version of aid_to_humanitarian_ts.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_humanitarian_ts.csv", index=False
    )
    logger.debug("Saved download version of aid_to_humanitarian_ts.csv")

    # Dynamic text version
    kn = {
        "aid_to_humanitarian": f"{df['Total Humanitarian Aid'].values[-1]} billion",
        "aid_to_humanitarian_share": f"{df['Share of total ODA'].values[-1]}",
    }
    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", kn)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")


def aid_to_food() -> None:
    df = common.aid_to_sector_ts(common.filter_food_sectors).rename(
        columns={"value": "Total Food Aid", "share": "Share of total ODA"}
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/aid_to_food_ts.csv", index=False)
    logger.debug("Saved chart version of aid_to_food_ts.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_food_ts.csv", index=False
    )
    logger.debug("Saved download version of aid_to_food_ts.csv")


if __name__ == "__main__":
    global_aid_key_number()
    aid_gni_key_number()
    aid_to_incomes_latest()
    aid_to_africa_ts()
    aid_to_health_ts()
    aid_to_food()
    aid_to_humanitarian_ts()
