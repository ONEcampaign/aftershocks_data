import pandas as pd
from bblocks import WorldBankData, set_bblocks_data_path

from scripts.config import PATHS
from scripts.explorers.common import (
    ExplorerSchema,
    HEALTH_WB_INDICATORS,
    OWID_INDICATORS,
    basic_info,
    indicators_metadata,
)
from scripts.owid_covid.tools import (
    filter_countries_only,
    get_indicators_ts,
    read_owid_data,
)

set_bblocks_data_path(PATHS.bblocks_data)


def _base_wb_health() -> pd.DataFrame:
    wb = WorldBankData()
    wb.load_data(indicator=list(HEALTH_WB_INDICATORS), most_recent_only=True)

    return (
        wb.get_data()
        .assign(indicator=lambda d: d.indicator.map(HEALTH_WB_INDICATORS))
        .filter(["iso_code", "indicator", "value"], axis=1)
        .pivot(index=["iso_code"], columns="indicator", values="value")
        .reset_index()
        .rename(columns={"iso_code": ExplorerSchema.ID})
    )


def _base_owid_health() -> pd.DataFrame:
    return (
        read_owid_data()
        .pipe(get_indicators_ts, indicators=OWID_INDICATORS)
        .pipe(filter_countries_only)
        .dropna(subset="value")
        .groupby(["iso_code", "indicator"], as_index=False)
        .last()
        .filter(["iso_code", "indicator", "value"], axis=1)
        .pivot(index=["iso_code"], columns="indicator", values="value")
        .reset_index()
        .rename(columns={"iso_code": ExplorerSchema.ID})
        .rename(columns=OWID_INDICATORS)
    )


def _base_who_health() -> pd.DataFrame:
    return (
        pd.read_csv(f"{PATHS.raw_data}/health/malaria_est_deaths_country.csv")
        .filter(["iso_code", "value"], axis=1)
        .rename(
            columns={
                "iso_code": ExplorerSchema.ID,
                "value": ExplorerSchema.MALARIA_DEATHS,
            }
        )
    )


def _base_hiv_health() -> pd.DataFrame:
    deaths = (
        pd.read_csv(f"{PATHS.raw_data}/health/aids related deaths total.csv")
        .filter(["country", "All ages estimate"], axis=1)
        .rename(
            columns={
                "country": ExplorerSchema.ID,
                "All ages estimate": ExplorerSchema.AIDS_DEATHS,
            }
        )
    )

    art = (
        pd.read_csv(
            f"{PATHS.raw_data}/health/people living with HIV receiving ART percent.csv"
        )
        .filter(["country", "All ages estimate"], axis=1)
        .rename(
            columns={
                "country": ExplorerSchema.ID,
                "All ages estimate": ExplorerSchema.RECEIVING_ART,
            }
        )
    )

    return deaths.merge(art, on=ExplorerSchema.ID, how="outer")


def health_explorer() -> None:
    from functools import reduce

    data = [
        basic_info(),
        _base_wb_health(),
        _base_owid_health(),
        _base_who_health(),
        _base_hiv_health(),
    ]

    df = reduce(
        lambda left, right: pd.merge(left, right, on=[ExplorerSchema.ID], how="left"),
        data,
    )

    metadata = indicators_metadata().loc[lambda d: d.indicator.isin(df.columns)]

    # Export explorer
    df.to_csv(f"{PATHS.charts}/explorers/health.csv", index=False)

    # Export metadata
    metadata.to_excel(f"{PATHS.download}/explorers/health_metadata.xlsx", index=False)


if __name__ == "__main__":
    health_explorer()
