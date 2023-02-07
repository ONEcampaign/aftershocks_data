from bblocks.cleaning_tools.clean import format_number
from bblocks import set_bblocks_data_path
from oda_data import ODAData, set_data_path
from oda_data.tools.groupings import donor_groupings

from scripts.common import update_key_number, df_to_key_number
from scripts.config import PATHS
from scripts.logger import logger
from scripts.oda import common

set_data_path(PATHS.raw_oda)
set_bblocks_data_path(PATHS.bblocks_data)

DacCountries = donor_groupings()["dac_countries"] | {20001: "DAC Countries, Total"}


def global_aid_key_number() -> None:
    oda = ODAData(
        years=range(2020, 2024),
        donors=20001,
        prices="constant",
        base_year=common.CONSTANT_YEAR,
        include_names=True,
    )

    data = (
        oda.load_indicator(indicators="total_oda_ge")
        .get_data()
        .pipe(common.add_change, as_formatted_str=True)
        .query("year == year.max()")
        .assign(
            name=lambda d: d.donor_name,
            value=lambda d: d.value * 1e6,
            pct_change=lambda d: d["pct_change"].str.replace("%", ""),
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
    data.to_csv(f"{PATHS.charts}/oda_topic/sm_total_oda.csv", index=False)
    logger.debug("Saved chart version of sm_total_oda.csv")

    # Dynamic text version
    kn = {
        "total_oda": f"{data['value'].item() / 1e9:,.1f} billion",
        "total_oda_change": f"{float(data['pct_change'].item()):.1f} %",
        "latest_year": f"{data['first_line'].item().split(' ')[-1]}",
    }

    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", kn)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")


def aid_gni_key_number() -> None:
    """Create an overview chart which contains the latest ODA/GNI value and
    the change in constant terms."""

    oda = ODAData(
        years=range(2020, 2024),
        donors=20001,
        include_names=True,
    )

    data = (
        oda.load_indicator(indicators=["total_oda_ge", "gni"])
        .get_data()
        .query("year == year.max()")
        .assign(value=lambda d: d.value * 1e6, name=lambda d: d.donor_name)
        .pivot(index=["year", "name"], columns="indicator", values="value")
        .reset_index()
        .assign(
            oda_gni=lambda d: round(d.total_oda_ge / d.gni, 6),
            distance=lambda d: round(d.gni * 0.007 - d.total_oda_ge, 1),
            first_line=lambda d: f"As of {d.year.item()}",
            second_line="Additional required to get to 0.7%",
            centre="",
        )
        .filter(
            ["name", "first_line", "oda_gni", "second_line", "distance", "centre"],
        )
        .assign(distance=lambda d: round(d.distance / 1e6, 2))
    )

    # chart version
    data.to_csv(f"{PATHS.charts}/oda_topic/oda_gni_sm.csv", index=False)
    logger.debug("Saved chart version of oda_gni_sm.csv")

    # Dynamic text version
    kn = {
        "oda_gni": f"{100 * data.oda_gni.item():,.2f}%",
        "oda_gni_distance": f"{data.distance.item() / 1e3:,.0f} billion",
    }

    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", kn)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")


def aid_to_africa_ts() -> None:
    oda = ODAData(
        years=range(common.START_YEAR, 2024),
        donors=20001,
        recipients=[10001, 10100],
        include_names=True,
        prices="constant",
        base_year=common.CONSTANT_YEAR,
    )

    data = (
        oda.load_indicator(indicators=["recipient_total_flow_net"])
        .add_share_of_total(True)
        .get_data()
        .pivot(index=["year", "donor_name"], columns="recipient_name", values="value")
        .reset_index()
        .assign(
            share=lambda d: format_number(
                (d["Africa, Total"] / d["Developing Countries, Total"]),
                as_percentage=True,
                decimals=1,
            )
        )
        .rename(columns={"Africa, Total": "value"})
        .filter(["year", "donor_name", "value", "share"], axis=1)
        .pipe(common.add_change, as_formatted_str=True, grouper="donor_name")
        .assign(
            value=lambda d: format_number(d.value * 1e6, as_billions=True, decimals=1),
            name=lambda d: d.donor_name,
        )
        .rename(columns={"value": "Aid to Africa", "share": "Share of total ODA"})
        .filter(["name", "year", "Aid to Africa", "Share of total ODA"], axis=1)
    )

    # chart version
    data.to_csv(f"{PATHS.charts}/oda_topic/aid_to_africa_ts.csv", index=False)
    logger.debug("Saved chart version of aid_to_africa_ts.csv")

    # Dynamic text version
    kn = {
        "aid_to_africa": f"{data['Aid to Africa'].values[-1]} billion",
        "aid_to_africa_share": f"{data['Share of total ODA'].values[-1]}",
    }
    update_key_number(f"{PATHS.charts}/oda_topic/oda_key_numbers.json", kn)
    logger.debug(f"Updated dynamic text ODA topic page oda_key_numbers.json")


def aid_to_incomes_latest() -> None:
    recipients = {
        10024: "Not classified by income",
        10045: "Low income",
        10046: "Lower-middle income",
        10047: "Upper-middle income",
        10048: "High income",
        10049: "Not classified by income",
        10100: "Developing Countries, Total",
    }
    oda = ODAData(
        years=range(common.START_YEAR, 2025),
        donors=20001,
        recipients=list(recipients),
        include_names=True,
    )
    oda.load_indicator("recipient_total_flow_net")

    data = (
        oda.get_data()
        .assign(recipient=lambda d: d.recipient_code.map(recipients))
        .groupby(["year", "donor_code", "donor_name", "recipient"], as_index=False)
        .sum(numeric_only=True)
        .pivot(index=["year", "donor_name"], columns="recipient", values="value")
        .reset_index()
        .melt(id_vars=["year", "donor_name", "Developing Countries, Total"])
        .query("year == year.max()")
        .rename(columns={"donor_name": "name"})
        .assign(
            share=lambda d: format_number(
                d.value / d["Developing Countries, Total"],
                decimals=1,
                as_percentage=True,
            ),
            value=lambda d: format_number(d.value * 1e6, as_billions=True, decimals=1),
            lable=lambda d: d["recipient"] + ": " + d["share"],
        )
        .filter(["name", "year", "recipient", "value", "share", "lable"], axis=1)
    )

    # chart version
    data.to_csv(f"{PATHS.charts}/oda_topic/aid_to_income_latest.csv", index=False)
    logger.debug("Saved chart version of aid_to_income_latest.csv")

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    data.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/aid_to_income_latest.csv", index=False
    )
    logger.debug("Saved download version of aid_to_income_latest.csv")

    # Dynamic text version
    income_dict = df_to_key_number(
        data,
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
    ...
