import pandas as pd
from oda_data import set_data_path, ODAData

from scripts import config

set_data_path(config.PATHS.raw_data)

YEARS = range(2000, 2023)


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


if __name__ == "__main__":
    get_totals()
    get_oda_gni()
    get_gni()
