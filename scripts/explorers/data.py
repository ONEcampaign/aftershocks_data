import country_converter
import pandas as pd
from bblocks.dataframe_tools import add
from bblocks.import_tools.imf import WorldEconomicOutlook

from scripts.config import PATHS
from scripts.explorers.common import (
    LDC,
    ExplorerSchema,
    WEO_YEAR,
    ECONOMICS_INDICATORS,
    indicators_metadata,
    add_hdi_column,
)


def _base_df() -> pd.DataFrame:
    """A dataframe with iso3 codes, name, UN region and continent"""

    return country_converter.CountryConverter().data[
        ["ISO3", "name_short", "continent", "UNregion"]
    ]


def basic_info() -> pd.DataFrame:
    """Create a DataFrame with basic information"""

    return (
        _base_df()
        .pipe(add.add_income_level_column, id_column="ISO3", id_type="ISO3")
        .pipe(add.add_population_column, id_column="ISO3", id_type="ISO3")
        .pipe(
            add.add_gdp_column,
            id_column="ISO3",
            id_type="ISO3",
            usd=True,
            include_estimates=False,
        )
        .pipe(add.add_poverty_ratio_column, id_column="ISO3", id_type="ISO3")
        .assign(ldc=lambda d: d.ISO3.apply(lambda x: "LDC" if x in LDC else "Non-LDC"))
        .assign(gdp_per_capita=lambda d: d.gdp / d.population)
        .drop("gdp", axis=1)
        .dropna(thresh=6)
        .fillna({"income_level": "Not classified"})
        .rename(
            columns={
                "ISO3": ExplorerSchema.ID,
                "name_short": ExplorerSchema.NAME,
                "UNregion": ExplorerSchema.REGION,
                "continent": ExplorerSchema.CONTINENT,
                "income_level": ExplorerSchema.INCOME,
                "population": ExplorerSchema.POP,
                "poverty_ratio": ExplorerSchema.POVERTY,
                "ldc": ExplorerSchema.LDC,
                "gdp_per_capita": ExplorerSchema.GDP,
            }
        )
    )


def _base_weo_economics() -> pd.DataFrame:
    weo = WorldEconomicOutlook()

    for indicator in ECONOMICS_INDICATORS:
        weo.load_indicator(indicator)

    return (
        weo.get_data(keep_metadata=False)
        .loc[lambda d: d.year.dt.year == WEO_YEAR]
        .assign(
            indicator=lambda d: d.indicator.map(ECONOMICS_INDICATORS),
            year=lambda d: d.year.dt.year,
        )
        .pivot(index=["iso_code", "year"], columns="indicator", values="value")
        .reset_index()
        .drop("year", axis=1)
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
