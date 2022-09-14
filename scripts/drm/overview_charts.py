import pandas as pd

from scripts.drm import common
from bblocks.import_tools.imf import WorldEconomicOutlook
from datetime import datetime
from bblocks.cleaning_tools.clean import convert_id, format_number
from scripts.config import PATHS


WEO = WorldEconomicOutlook()


def _latest_weo_ssa_with_yoy_change(
    df: pd.DataFrame, summary: bool = True
) -> pd.DataFrame:

    current_year = datetime.now().year

    exclude = ["value", "iso_code", "estimate"] if summary else ["value"]

    df_grouper = [c for c in df.columns if c not in exclude]
    change_grouper = "indicator" if summary else "iso_code"

    return (
        df.loc[lambda d: d.year.dt.year.isin([current_year - 1, current_year])]
        .assign(
            region=lambda d: convert_id(d.iso_code, "ISO3", "Continent"),
            estimate=lambda d: d.estimate.apply(lambda x: "IMF estimate" if x else ""),
        )
        .query("region == 'Africa' and iso_code not in @common.north_africa")
        .sort_values(["iso_code", "year"])
        .groupby(df_grouper, as_index=False)
        .sum()
        .assign(
            yoy_change=lambda d: d.groupby(change_grouper)["value"].pct_change(),
            region="Sub-Saharan Africa",
        )
        .loc[lambda d: d.year.dt.year == current_year]
    )


def _latest_unu_ssa_with_yoy_change(
    df: pd.DataFrame, summary: bool = True
) -> pd.DataFrame:

    latest_year = 2020

    exclude = ["value", "iso_code"] if summary else ["value"]

    df_grouper = [c for c in df.columns if c not in exclude]
    change_grouper = "indicator" if summary else "iso_code"

    return (
        df.loc[lambda d: d.year.dt.year.isin([latest_year - 1, latest_year])]
        .assign(
            region=lambda d: convert_id(d.iso_code, "ISO3", "Continent"),
        )
        .query("region == 'Africa' and iso_code not in @common.north_africa")
        .sort_values(["iso_code", "year"])
        .groupby(df_grouper, as_index=False)
        .sum()
        .assign(
            yoy_change=lambda d: d.groupby(change_grouper)["value"].pct_change(),
            region="Sub-Saharan Africa",
        )
        .loc[lambda d: d.year.dt.year == latest_year]
    )


def revenue_key_number(summary:bool=True) -> None:

    df = common.gov_revenue(WEO)
    df = (
        df.pipe(_latest_weo_ssa_with_yoy_change, summary=summary)
        .assign(
            note=lambda d: "Real change from previous year: "
            + format_number(d.yoy_change, decimals=1, as_percentage=True),
            value=lambda d: "US$"
            + format_number(d.value, decimals=1, as_billions=True)
            + " billion",
            as_of=lambda d: d.year.dt.year,
        )
        .rename(columns={"as_of": "As of", "region": "name"})
        .filter(["name", "As of", "value", "note"])
    )

    # live version
    # df.to_csv(f"{PATHS.charts}/drm_topic/revenue_key_number.csv", index=False)

    # download version
    # df.assign(source="IMF World Economic Outlook").to_csv(
    #    f"{PATHS.download}/drm_topic/revenue_key_number.csv", index=False
    # )

    df.to_clipboard(index=False)


if __name__ == "__main__":

    revenue_key_number(summary=False)
    ...
