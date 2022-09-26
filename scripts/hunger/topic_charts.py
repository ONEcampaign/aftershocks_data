""" """

import pandas as pd
from scripts.hunger.ipc import IPC
from scripts.config import PATHS
from bblocks.import_tools.world_bank import WorldBankData, WorldBankPinkSheet
import country_converter as coco
from bblocks.import_tools.fao import get_fao_index


def ipc_chart(df: pd.DataFrame):
    """ """

    df = (df
          .drop(['phase_3plus', 'phase_1', 'iso_code'], axis=1)
          .rename(columns={
        'phase_2': 'Phase 2',
        'phase_3': 'Phase 3',
        'phase_4': 'Phase 4',
        'phase_5': 'Phase 5'})
          .assign(from_date=lambda d: pd.to_datetime(d.from_date).dt.strftime('%B %Y'),
                  to_date=lambda d: pd.to_datetime(df.to_date).dt.strftime('%B %Y'))
          )

    df.to_csv(f'{PATHS.charts}/hunger_topic/ipc_phases.csv', index=False)
    df.to_csv(f'{PATHS.download}/hunger_topic/ipc_phases.csv', index=False)



def stunting_chart():
    """ """

    country_list = list(coco.CountryConverter().data.loc[lambda d: d.continent == 'Africa', 'ISO3']) + ['SSA']

    wb = WorldBankData()
    wb.load_indicator('SH.STA.STNT.ME.ZS')
    df = (wb.get_data('SH.STA.STNT.ME.ZS')
          .dropna(subset='value')
          .loc[lambda d: d.iso_code.isin(country_list), ['date', 'iso_code', 'value']]
          .groupby('iso_code', as_index=False)
          )

    df = (pd.concat([df.first(), df.last()])
          .assign(date=lambda d: pd.to_datetime(d.date).dt.strftime('%Y'))
          .assign(country=lambda d: coco.convert(d.iso_code, to='name_short', not_found='Sub-Saharan Africa'))
          .sort_values(by='value')
          .replace({'Central African Republic': 'Central African Rep.', 'Sao Tome and Principe': 'Sao Tome'})
          )

    df.to_csv(f'{PATHS.charts}/hunger_topic/prevalence_of_stunting.csv', index=False)
    df.to_csv(f'{PATHS.download}/hunger_topic/prevalence_of_stunting.csv', index=False)



def prices():
    """ """

    commodities = ['Coconut oil',
                   'Palm oil',
                   'Palm kernel oil',
                   'Soybean oil',
                   'Rapeseed oil',
                   'Sunflower oil',
                   'Barley',
                   'Maize',
                   'Sorghum',
                   'Rice, Thai 5% ',
                   'Wheat, US HRW',
                   'Beef',
                   'Meat, chicken',
                   'Sugar, world']

    pink_sheet = WorldBankPinkSheet(sheet = 'Monthly Prices')
    df =  (pink_sheet.get_data()
    .melt(id_vars = 'period')
        .loc[lambda d: d.variable.isin(commodities)])



    return df






def update_topic_charts():
    """ """

    ipc = IPC(api_key='bac2a4d1-1274-4526-9065-0502ce9d4d5e')
    df = ipc.get_ipc_ch_data()
    ipc_chart(df)