import pandas as pd
from oda_data import set_data_path, ODAData, CRSData, provider_groupings, OECDClient
from pydeflate import set_pydeflate_path

from scripts import config

set_data_path(config.PATHS.raw_oda)
set_pydeflate_path(config.PATHS.raw_data)


YEARS = range(2000, 2025)
DAC = list(provider_groupings()["dac_members"])


def get_totals():
    oda = OECDClient(
        years=YEARS, providers=DAC, measure=["net_disbursement", "grant_equivalent"]
    )
    oda = oda.get_indicators(["DAC1.10.1010"])

    flow = oda.loc[lambda d: d.flows_code == 1140].assign(indicator="ODA (net flows)")
    ge = oda.loc[lambda d: d.flows_code == 1160].assign(
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
    oda = OECDClient(
        years=YEARS, providers=DAC, measure=["net_disbursement", "grant_equivalent"]
    )

    oda = oda.get_indicators(["ONE.40.1010_11010_1"])

    data = (
        oda.assign(value=lambda d: round(d.value * 100, 2), indicator="ODA GNI")
        .assign(flows_code=1140)
        .filter(["year", "donor_code", "flows_code", "value", "indicator"], axis=1)
    )

    data.to_csv(f"{config.PATHS.raw_oda}/oda_gni.csv", index=False)


def get_gni():
    oda = OECDClient(years=YEARS, providers=DAC, measure=["net_disbursement"])

    oda = oda.get_indicators(["DAC1.40.1"])

    data = oda.assign(indicator="GNI").filter(
        ["year", "donor_code", "flows_code", "value", "indicator"], axis=1
    )
    data.to_csv(f"{config.PATHS.raw_oda}/gni.csv", index=False)


def get_oda_by_income():
    recipients = [10024, 10045, 10046, 10047, 10048, 10049]
    oda = OECDClient(
        years=YEARS,
        providers=DAC + [20001],
        measure=["net_disbursement"],
        recipients=recipients,
    )
    oda = oda.get_indicators(["ONE.10.206_106"])

    names = {
        10024: "Not classified by income",
        10045: "Low income",
        10046: "Lower-middle income",
        10047: "Upper-middle income",
        10048: "High income",
        10049: "Not classified by income",
    }

    data = oda.assign(recipient=lambda d: d.recipient_code.map(names))

    data.filter(
        ["year", "donor_code", "recipient_code", "value", "recipient"], axis=1
    ).to_csv(f"{config.PATHS.raw_oda}/total_oda_by_income.csv", index=False)


def get_oda_to_africa() -> None:
    total = [10100]
    africa = [10001]

    recipients = total + africa

    oda = OECDClient(
        years=YEARS,
        providers=DAC + [20001],
        measure=["net_disbursement"],
        recipients=recipients,
    )
    oda = oda.get_indicators(["ONE.10.206_106"])

    data = oda.filter(["year", "donor_code", "recipient_code", "value"], axis=1)

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

    oda = OECDClient(
        years=YEARS,
        providers=DAC + [20001],
        measure=["net_disbursement"],
        recipients=list(recipients),
    )

    oda = oda.get_indicators(["DAC2A.10.106", "DAC2A.10.206"])

    indicators = {
        "DAC2A.10.106": "imputed_multilateral",
        "DAC2A.10.206": "bilateral",
    }

    data = oda.assign(
        recipient=lambda d: d.recipient_code.map(recipients),
        indicator=lambda d: d.one_indicator.map(indicators),
    ).filter(
        ["year", "donor_code", "recipient_code", "value", "indicator", "recipient"],
        axis=1,
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
        base_year=2023,
    )

    oda.load_indicator(list(indicators))

    data = oda.get_data()

    return data


def get_ukraine_crs() -> pd.DataFrame:
    df = CRSData(years=range(2015, 2025), recipients=[85], providers=DAC).read(
        using_bulk_download=True
    )

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
