"""HIV charts for health topic page"""

import pandas as pd
import numpy as np
from scripts.config import PATHS
from scripts.logger import logger

HIV_DATA = pd.read_csv(f"{PATHS.raw_data}/health/unaids_hiv_data.csv")

INDICATORS = {
    "unaids_new_hiv_infections": "New HIV infections",
    "unaids_aids_related_deaths": "AIDS-related deaths",
    "unaids_people_living_with_hiv_receiving_art": "People accessing treatment",
}


def _significant_rounding(number: int, significance: int) -> int:
    """round a number to the nearest significance

    Args:
        number: number to round
        significance (int): significance to round to

    Returns:
        rounded number
    """

    return round(number, -(len(str(number)) - significance))


def unaids_rounding(number):
    """Simulate UNAIDS rounding

    returns a formatted string for a number rounded to a significance of 2 digits,
    either separated by commas or rounded as <100, <500 or <1000

    Args:
        number: number to round

    Returns:
        rounded number

    """

    # if number is nan, return nan
    if np.isnan(number):
        return number

    number = _significant_rounding(int(number), 2)
    if number <= 100:
        return 100
    if number <= 500:
        return 500
    if number <= 1000:
        return 1000

    return number


def create_topic_chart(df: pd.DataFrame) -> None:
    """Create HIV topic chart"""

    df = (
        df.loc[
            (df.indicator_code.isin(INDICATORS))
            & (df.gender == "all")
            & (df.age == "all ages")
            & (df.units == "people")
            & (df.value_type == "value"),

            ["entity_name", "indicator_code", "year", "value"],
        ]
        .dropna(subset=["value"]) # drop rows with no value
        .assign(value_rounded=lambda d: d.loc[
                d.indicator_code.isin(
                    ["unaids_new_hiv_infections", "unaids_aids_related_deaths"]
                )
            ]["value"].apply(unaids_rounding),
        )
        .assign(value_rounded=lambda d: d.value_rounded.fillna(d.value))

        .pivot(
            index=["year", "indicator_code"],
            columns="entity_name",
            values="value_rounded",
        )
        .reset_index()
        .rename(columns={"indicator_code": "indicator"})
        .assign(indicator=lambda x: x.indicator.map(INDICATORS))

    )

    df.to_csv(f"{PATHS.charts}/health/hiv_topic_chart_v2.csv", index=False)
    logger.debug("Saved live version of 'hiv_topic_chart_v2.csv'")


def create_topic_chart_download(df):
    """ """
    df = df.loc[
        (df.indicator_code.isin(INDICATORS))
        & (df.gender == "all")
        & (df.age == "all ages")
        & (df.units == "people")
        & (df.value_type == "value"),
        ["entity_name", "indicator_code", "year", "value"],
    ]

    df.to_csv(f"{PATHS.download}/health/hiv_topic_chart_v2.csv", index=False)
    logger.debug("Saved download version of 'hiv_topic_chart_v2.csv'")


if __name__ == "__main__":
    create_topic_chart(HIV_DATA)
    create_topic_chart_download(HIV_DATA)
