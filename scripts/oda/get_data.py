import pandas as pd
from oda_data import set_data_path, ODAData, recipient_groupings, read_crs
from pydeflate import set_pydeflate_path, update_dac1

from scripts import config

set_data_path(config.PATHS.raw_data)
set_pydeflate_path(config.PATHS.raw_data)


YEARS = range(2000, 2024)


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
    82,
    84,
    301,
    302,
    701,
    742,
    801,
    820,
    918,
]


def get_totals():
    oda = ODAData(years=YEARS, donors=DAC)
    oda.load_indicator(["total_oda_flow_net", "total_oda_ge"])

    flow = oda.get_data("total_oda_flow_net").assign(
        flows_code=1140, indicator="ODA (net flows)"
    )
    ge = oda.get_data("total_oda_ge").assign(
        flows_code=1160, indicator="ODA (grant equivalent)"
    )

    order = ["year", "donor_code", "flows_code", "value", "indicator"]

    # Save
    flow.filter(order, axis=1).to_csv(
        f"{config.PATHS.raw_oda}/total_oda_flow.csv", index=False
    )
    ge.filter(order, axis=1).to_csv(
        f"{config.PATHS.raw_oda}/total_oda_ge.csv", index=False
    )


def get_oda_gni():
    oda = ODAData(years=YEARS, donors=DAC)
    oda.add_share_of_gni().load_indicator(["total_oda_flow_net", "total_oda_ge"])

    flow = oda.get_data("total_oda_flow_net").loc[lambda d: d.year < 2018]
    ge = oda.get_data("total_oda_ge").loc[lambda d: d.year >= 2018]

    data = (
        pd.concat([flow, ge], ignore_index=True)
        .assign(value=lambda d: round(d.gni_share, 2), indicator="ODA GNI")
        .assign(flows_code=1140)
        .filter(["year", "donor_code", "flows_code", "value", "indicator"], axis=1)
    )

    data.to_csv(f"{config.PATHS.raw_oda}/oda_gni.csv", index=False)


def get_gni():
    oda = ODAData(years=YEARS, donors=DAC)
    oda.load_indicator("gni")
    data = (
        oda.get_data("gni")
        .assign(flows_code=1140, indicator="GNI")
        .filter(["year", "donor_code", "flows_code", "value", "indicator"], axis=1)
    )
    data.to_csv(f"{config.PATHS.raw_oda}/gni.csv", index=False)


def get_oda_by_income():
    recipients = [10024, 10045, 10046, 10047, 10048, 10049]
    oda = ODAData(years=YEARS, donors=DAC + [20001], recipients=recipients)
    oda.load_indicator("recipient_total_flow_net")
    data = oda.get_data()

    names = {
        10024: "Not classified by income",
        10045: "Low income",
        10046: "Lower-middle income",
        10047: "Upper-middle income",
        10048: "High income",
        10049: "Not classified by income",
    }

    data = data.assign(recipient=lambda d: d.recipient_code.map(names))

    data.filter(
        ["year", "donor_code", "recipient_code", "value", "recipient"], axis=1
    ).to_csv(f"{config.PATHS.raw_oda}/total_oda_by_income.csv", index=False)


def get_oda_to_africa() -> None:
    total = [10100]
    africa = [10001]

    oda = ODAData(years=YEARS, donors=DAC, recipients=total + africa)

    oda.load_indicator("recipient_total_flow_net")

    data = oda.get_data().filter(
        ["year", "donor_code", "recipient_code", "value"], axis=1
    )

    total_data = data.query(f"recipient_code in {total}").drop(
        ["recipient_code"], axis=1
    )
    africa_data = data.query(f"recipient_code in {africa}").drop(
        ["recipient_code"], axis=1
    )

    data = total_data.merge(
        africa_data, on=["year", "donor_code"], suffixes=("_all", "_africa")
    )

    data.to_csv(f"{config.PATHS.raw_oda}/total_oda_to_africa.csv", index=False)


def get_oda_to_regions():
    recipients = {
        9998: "Developing countries, unspecified",
        10001: "Africa",
        10004: "America",
        10007: "Asia",
        10010: "Europe",
        10011: "Middle East",
        10012: "Oceania",
    }

    indicators = {
        "recipient_imputed_multi_flow_net": "imputed_multilateral",
        "recipient_bilateral_flow_net": "bilateral",
    }

    oda = ODAData(years=YEARS, donors=DAC + [20001], recipients=list(recipients))

    oda.load_indicator(list(indicators))

    data = (
        oda.get_data()
        .assign(
            recipient=lambda d: d.recipient_code.map(recipients),
            indicator=lambda d: d.indicator.map(indicators),
        )
        .filter(
            ["year", "donor_code", "recipient_code", "value", "indicator", "recipient"],
            axis=1,
        )
    )

    data.to_csv(f"{config.PATHS.raw_oda}/total_oda_by_region.csv", index=False)


def get_ukraine_bilat() -> pd.DataFrame:
    recipients = {85: "Ukraine"}

    indicators = {"recipient_bilateral_flow_net": "bilateral"}

    oda = ODAData(
        years=YEARS,
        donors=DAC + [20001],
        recipients=list(recipients),
        include_names=True,
        prices="constant",
        base_year=2022,
    )

    oda.load_indicator(list(indicators))

    data = oda.get_data()

    return data


def get_ukraine_crs() -> pd.DataFrame:
    df = read_crs(years=range(2015, 2023))

    df = df.loc[lambda d: d.recipient_code == 85].loc[lambda d: d.donor_code.isin(DAC)]
    df = df.loc[lambda d: d.flow_name != "Other Official Flows (non Export Credit)"]

    df = (
        df.groupby(
            [
                "year",
                "donor_code",
                "donor_name",
                "recipient_name",
                # "flow_name",
            ],
            observed=True,
            dropna=False,
        )[["usd_disbursement", "usd_received", "usd_grant_equiv"]]
        .sum()
        .reset_index()
    )

    df["net_disbursement"] = df["usd_disbursement"] - df["usd_received"]

    dac = df.loc[lambda d: d.donor_code != 918]
    eu = df.loc[lambda d: d.donor_code == 918].drop(
        columns=["usd_disbursement", "usd_received"]
    )

    dac = (
        dac.groupby(["year", "recipient_name"], observed=True, dropna=False)[
            ["net_disbursement", "usd_grant_equiv"]
        ]
        .sum()
        .reset_index()
        .assign(donor_name="DAC Countries")
    )

    return pd.concat([dac], ignore_index=True)


def get_idrc() -> pd.DataFrame:

    indicators = {"idrc_ge_linked": "irdc"}

    oda = ODAData(
        years=YEARS,
        donors=DAC + [20001],
        include_names=True,
        prices="constant",
        base_year=2023,
    )

    oda.load_indicator(list(indicators))

    data = oda.get_data()

    return data


def get_total_official():
    oda = ODAData(
        years=YEARS,
        donors=DAC + [20001],
        include_names=True,
        prices="constant",
        base_year=2023,
    )
    oda.load_indicator(["total_oda_flow_net", "total_oda_ge"])

    flow = (
        oda.get_data("total_oda_flow_net")
        .assign(flows_code=1140, indicator="ODA (net flows)")
        .query("year < 2018")
    )
    ge = (
        oda.get_data("total_oda_ge")
        .assign(flows_code=1160, indicator="ODA (grant equivalent)")
        .query("year >= 2018")
    )

    df = pd.concat([flow, ge], ignore_index=True)

    return df


if __name__ == "__main__":
    get_totals()
    get_oda_gni()
    get_gni()
    get_oda_by_income()
    get_oda_to_africa()
    get_oda_to_regions()
    # df = get_ukraine_crs()
