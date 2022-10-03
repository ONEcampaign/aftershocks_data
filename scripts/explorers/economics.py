import pandas as pd
from bblocks.import_tools.imf import WorldEconomicOutlook

from scripts.config import PATHS
from scripts.explorers.common import (
    ExplorerSchema,
    WEO_YEAR,
    add_hdi_column,
    basic_info,
    ECONOMICS_WEO_INDICATORS,
    indicators_metadata,
)


def _base_weo_economics() -> pd.DataFrame:
    weo = WorldEconomicOutlook(data_path=PATHS.bblocks_data)

    for indicator in ECONOMICS_WEO_INDICATORS:
        weo.load_indicator(indicator)

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
