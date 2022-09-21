import pandas as pd
import country_converter
from bblocks.dataframe_tools import add

from scripts.explorers.common import LDC, ExplorerSchema


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


