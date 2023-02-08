import datetime

from bblocks import WFPData, set_bblocks_data_path

from scripts.config import PATHS

set_bblocks_data_path(PATHS.bblocks_data)


def get_insufficient_food():
    """ """
    wfp = WFPData()
    wfp.load_data("insufficient_food")
    # wfp.update()
    df = wfp.get_data("insufficient_food")

    return df


def aggregate_insufficient_food(df, date, date_col):
    """ """

    date_min = date - datetime.timedelta(days=7)

    return (
        df.dropna(subset=["value"])
        .loc[lambda d: (d[date_col] >= date_min) & (d[date_col] <= date)]
        .groupby("iso_code", as_index=False)
        .last()["value"]
        .sum()
    )


wb_indicators = {
    "SH.STA.STNT.ME.ZS": "Stunting prevalence, height for age (% of children under 5)",
    "SN.ITK.DEFC.ZS": "Prevalence of undernourishment",
    "SN.ITK.SVFI.ZS": "Prevalence of severe food insecurity",
}
