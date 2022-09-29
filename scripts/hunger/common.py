
from bblocks.import_tools.wfp import WFPData
import datetime


def get_insufficient_food():
    """ """
    wfp = WFPData()
    wfp.load_indicator('insufficient_food')
    # wfp.update()
    df = (wfp.get_data('insufficient_food'))

    return df


def aggregate_insufficient_food(df, date, date_col):
    """ """

    date_min = date - datetime.timedelta(days=7)

    return (df.dropna(subset=['value'])
            .loc[lambda d: (d[date_col] >= date_min) & (d[date_col] <= date)]
            .groupby('iso_code', as_index=False)
            .last()
            ['value']
            .sum()
            )