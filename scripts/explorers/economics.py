import pandas as pd
from bblocks import WorldEconomicOutlook, set_bblocks_data_path

from scripts.config import PATHS
from scripts.explorers.common import (
    ECONOMICS_WEO_INDICATORS,
    ExplorerSchema,
    WEO_YEAR,
    add_hdi_column,
    basic_info,
    indicators_metadata,
)

set_bblocks_data_path(PATHS.bblocks_data)


def _base_weo_economics() -> pd.DataFrame:
    weo = WorldEconomicOutlook(year=2025, release=1)
    weo.load_data(list(ECONOMICS_WEO_INDICATORS))

    return (
        weo.get_data(keep_metadata=False)
        .loc[lambda d: d.year.dt.year == WEO_YEAR]
        .drop("year", axis=1)
        .assign(indicator=lambda d: d.indicator.map(ECONOMICS_WEO_INDICATORS))
        .pivot(index=["iso_code"], columns="indicator", values="value")
        .reset_index()
        .rename(columns={"iso_code": ExplorerSchema.ID})
    )


def econ_explorer():
    base = basic_info()
    econ = _base_weo_economics()

    df = base.merge(econ, on=ExplorerSchema.ID, how="left").pipe(
        add_hdi_column, iso_column=ExplorerSchema.ID
    )

    metadata = indicators_metadata().loc[lambda d: d.indicator.isin(df.columns)]

    # Export explorer
    df.to_csv(f"{PATHS.charts}/explorers/economics.csv", index=False)

    # Export metadata
    metadata.to_excel(f"{PATHS.download}/explorers/econ_metadata.xlsx", index=False)


if __name__ == "__main__":
    econ_explorer()
