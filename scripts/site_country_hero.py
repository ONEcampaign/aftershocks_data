import pandas as pd
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.dataframe_tools.add import add_short_names_column
from bblocks.cleaning_tools.filter import filter_african_countries


def key_indicators_chart() -> None:
    """Data for the Overview charts on the country pages"""
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
        ).to_csv(f"../charts_live/country_page/overview_{indicator}.csv", index=False)

        df.loc[df.indicator_name == indicator].to_csv(
            f"../charts_download/country_page/overview_{indicator}.csv", index=False
        )


if __name__ == "__main__":
    key_indicators_chart()
