import pandas as pd
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.dataframe_tools.add import add_short_names_column
from bblocks.cleaning_tools.filter import filter_african_countries, filter_latest_by

from scripts.config import PATHS

from bblocks.import_tools.wfp import WFPData


def _weo_charts() -> None:
    weo = WorldEconomicOutlook()

    indicators = {
        "NGDP_RPCH": "GDP Growth",
        "LUR": "Unemployment rate",
        "GGR_NGDP": "Government Revenue (% GDP)",
        "GGXWDN_NGDP": "Government Debt (% GDP)",
    }

    for c, n in indicators.items():
        weo.load_indicator(indicator_code=c, indicator_name=n)

    df = (
        weo.get_data(indicators="all", keep_metadata=True)
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_column="iso_code", id_type="ISO3")
        .loc[lambda d: d.year.dt.year.between(2012, 2022)]
    )

    for indicator in indicators.values():
        df.loc[df.indicator_name == indicator].filter(
            ["name_short", "indicator_name", "year", "value"], axis=1
        ).to_csv(f"{PATHS.charts}/country_page/overview_{indicator}.csv", index=False)

        df.loc[df.indicator_name == indicator].to_csv(
            f"{PATHS.download}/country_page/overview_{indicator}.csv", index=False
        )


def _wfp_charts() -> None:
    """Data for the Food Security charts on the country pages"""

    wfp = WFPData()

    source = "World Food Programme HungerMapLive"

    for indicator in wfp.available_indicators:
        wfp.load_indicator(indicator)

    inflation = (
        wfp.get_data("inflation")
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_type="ISO3")
        .loc[lambda d: d.date.dt.year.between(2018, 2022)]
        .loc[lambda d: d.indicator == "Inflation Rate"]
        .filter(["name_short", "date", "indicator", "value"], axis=1)
    )

    # Chart version
    inflation.to_csv(f"{PATHS.charts}/country_page/overview_inflation.csv", index=False)

    # Download version
    inflation.assign(source=source).to_csv(
        f"{PATHS.download}/country_page/overview_inflation.csv", index=False
    )

    food = (
        wfp.get_data("insufficient_food")
        .pipe(add_short_names_column, id_column="iso_code")
        .pipe(filter_african_countries, id_type="ISO3")
        .pipe(
            filter_latest_by,
            date_column="date",
            value_columns="value",
            group_by=["name_short"],
        )
        .assign(indicator="People with insufficient food consumption")
        .filter(["name_short", "date", "indicator", "value"], axis=1)
        .assign(
            date=lambda d: d.date.dt.strftime("%d %b %Y"),
            value=lambda d: d.value.map("{:,.0f}".format),
        )
    )

    # Chart version
    food.to_csv(f"{PATHS.charts}/country_page/overview_food.csv", index=False)

    # Download version
    food.assign(source=source).to_csv(
        f"{PATHS.download}/country_page/overview_food.csv", index=False
    )


def key_indicators_chart() -> None:
    """Data for the Overview charts on the country pages"""

    # Create csvs for the WEO charts
    _weo_charts()

    # Create csvs for the WFP charts
    _wfp_charts()


if __name__ == "__main__":
    key_indicators_chart()
