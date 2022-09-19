import datetime

from scripts.debt.common import read_dservice_data, read_dstocks_data
from bblocks.import_tools.debt.common import get_dsa
from bblocks.cleaning_tools.clean import convert_id

import pandas as pd
from scripts.config import PATHS
from bblocks.cleaning_tools.clean import format_number

KEY_NUMBERS: dict = {}

CURRENT_YEAR = datetime.datetime.now().year


def debt_distress() -> None:
    df = get_dsa(update=False, local_path=f"{PATHS.raw_data}/dsa_list.pdf")

    df = df.assign(continent=lambda d: convert_id(d.country, to_type="continent")).loc[
        lambda d: d.risk_of_debt_distress.isin(["High", "In debt distress"])
    ]

    africa = df.loc[df.continent == "Africa", :]

    number = len(africa)

    KEY_NUMBERS["debt_distress_africa_share"] = (
        str(round(100 * len(africa) / len(df))) + "%"
    )

    KEY_NUMBERS["debt_distress_africa"] = str(number)

    card = pd.DataFrame(
        {
            "name": ["African countries"],
            "Latest assessment": africa.latest_publication.max().strftime("%B %Y"),
            "value": [f"{number} African countries in, or at risk of, debt distress"],
            "note": [f"out of {len(df)} countries assessed"],
        }
    )

    # chart version
    card.to_csv(
        f"{PATHS.charts}/debt_topic/debt_distress_africa_key_number.csv", index=False
    )


def debt_service_africa_trend() -> None:

    df = (
        read_dservice_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .groupby(["year"], as_index=False)
        .sum()
        .assign(Total=lambda d: d.Total * 1e6)
    )

    KEY_NUMBERS["debt_service_africa"] = (
        format_number(
            df.loc[df.year == CURRENT_YEAR, "Total"], as_billions=True, decimals=1
        ).values[0]
        + " billion"
    )

    df.to_csv(f"{PATHS.charts}/debt_topic/debt_service_africa_trend.csv", index=False)


def debt_stocks_africa_trend() -> None:
    df = (
        read_dstocks_data()
        .filter(["year", "iso_code", "Total"], axis=1)
        .groupby(["year"], as_index=False)
        .sum()
        .assign(Total=lambda d: d.Total * 1e6)
    )

    KEY_NUMBERS["debt_stocks_africa"] = (
        format_number(
            df.loc[df.year == CURRENT_YEAR - 2, "Total"], as_billions=True, decimals=1
        ).values[0]
        + " billion"
    )

    df.to_csv(f"{PATHS.charts}/debt_topic/debt_stocks_africa_trend.csv", index=False)


def export_key_numbers_overview() -> None:
    """Export KEY_NUMBERS dictionary as json"""
    import json

    with open(f"{PATHS.charts}/debt_topic/key_numbers.json", "w") as f:
        json.dump(KEY_NUMBERS, f, indent=4)


def update_overview_charts_key_numbers() -> None:
    """Update key numbers for overview charts"""

    debt_distress()
    debt_service_africa_trend()
    debt_stocks_africa_trend()
    export_key_numbers_overview()


if __name__ == "__main__":
    update_overview_charts_key_numbers()