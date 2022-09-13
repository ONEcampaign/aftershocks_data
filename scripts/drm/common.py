from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.dataframe_tools.add import add_gdp_column
import pandas as pd


def gov_revenue(weo: WorldEconomicOutlook) -> pd.DataFrame:
    """Read government revenue data from the World Economic Outlook database."""

    rev: str = "GGR_NGDP"

    return (
        weo.load_indicator(rev)
        .get_data(keep_metadata=True)
        .filter(["iso_code", "indicator_name", "year", "value", "estimate"])
        .pipe(
            add_gdp_column,
            id_column="iso_code",
            id_type="ISO3",
            date_column="year",
            include_estimates=True,
        )
        .assign(
            value=lambda d: round(d.value / 100 * d.gdp, 1),
            indicator="Government revenue (current USD)",
        )
        .filter(["iso_code", "year", "indicator", "value", "estimate"], axis=1)
    )


north_africa: list = ["DZA", "DJI", "EGY", "LBY", "MAR", "TUN"]
