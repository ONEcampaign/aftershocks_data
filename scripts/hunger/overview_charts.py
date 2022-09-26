""" """
import pandas as pd

from bblocks.import_tools.world_bank import WorldBankData
from scripts.config import PATHS
from scripts.common import clean_wb_overview
from bblocks.import_tools.wfp import WFPData
import datetime

wb_indicators = {'SH.STA.STNT.ME.ZS': 'Stunting prevalence, height for age (% of children under 5)',
                 'SN.ITK.DEFC.ZS': 'Prevalence of undernourishment',
                 'SN.ITK.SVFI.ZS': 'Prevalence of severe food insecurity'
                 }


def wb_charts():
    """ """

    wb = WorldBankData()
    for code, name in wb_indicators.items():
        (wb.load_indicator(code)
         .get_data(code)
         .pipe(clean_wb_overview)
         .to_csv(f'{PATHS.charts}/hunger_topic/{name}.csv', index=False))


def __get_insufficient_food():
    """ """
    wfp = WFPData()
    wfp.load_indicator('insufficient_food')
    # wfp.update()
    df = (wfp.get_data('insufficient_food'))

    return df


def _aggregate(df, date, date_col):
    """ """

    date_min = date - datetime.timedelta(days=7)

    return (df.dropna(subset=['value'])
            .loc[lambda d: (d[date_col] >= date_min) & (d[date_col] <= date)]
            .groupby('iso_code', as_index=False)
            .last()
            ['value']
            .sum()
            )


def insufficient_food():
    """ """

    wfp_data = __get_insufficient_food()
    latest_date = wfp_data['date'].max()
    month_date = latest_date - datetime.timedelta(days=30)

    latest_value = _aggregate(wfp_data, latest_date, 'date')
    month_value = _aggregate(wfp_data, month_date, 'date')
    change = ((latest_value - month_value) / month_value) * 100
    arrow = ((latest_value - month_value) / month_value)

    d = {'value': latest_value / 1000000,
         'change': change,
         'top_label': f'People with insufficient food consumption as of {latest_date.strftime("%d %b %Y")}',
         'arrow': arrow,
         'bottom_label': 'in the last 30 days'

         }

    return pd.DataFrame.from_records([d])
