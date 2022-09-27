""" """

import pandas as pd
from scripts.hunger.ipc import IPC
from scripts.config import PATHS
from bblocks.import_tools.world_bank import WorldBankData, WorldBankPinkSheet
import country_converter as coco
import datetime


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


def price_table():
    """ """
    commodities = {'Coconut oil': ['oil', 'USD/mt'],
                   'Groundnut oil': ['oil', 'USD/mt'],
                   'Palm oil': ['oil', 'USD/mt'],
                   'Palm kernel oil': ['oil', 'USD/mt'],
                   'Soybean oil': ['oil', 'USD/mt'],
                   'Rapeseed oil': ['oil', 'USD/mt'],
                   'Sunflower oil': ['oil', 'USD/mt'],

                   'Groundnuts': ['meals', 'USD/mt'],
                   'Soybeans': ['meal', 'USD/mt'],

                   'Maize': ['grains', 'USD/mt'],
                   'Rice, Thai 5% ': ['grains', 'USD/mt'],
                   'Wheat, US HRW': ['grains', 'USD/mt'],
                   'Wheat, US SRW': ['grains', 'USD/mt'],
                   'Rice, Thai 25%': ['grains', 'USD/mt'],
                   'Rice, Thai A.1': ['grains', 'USD/mt'],
                   'Rice, Viet Namese 5%': ['grains', 'USD/mt'],

                   'Beef': ['meat', 'USD/kg'],
                   'Meat, chicken': ['meat', 'USD/kg'],
                   'Shrimps, Mexican': ['meat', 'USD/kg'],

                   'Sugar, world': ['sugar', 'USD/kg']
                   }

    pink_sheet = WorldBankPinkSheet(sheet='Monthly Prices')
    df = (pink_sheet.get_data()
          .assign(period=lambda d: pd.to_datetime(d.period))
          .melt(id_vars='period')
          .dropna(subset=['value'])
          .loc[lambda d: (d.variable.isin(commodities))
                         &
                         (d.period >= d.period.max() - datetime.timedelta(days=365))]
          .reset_index(drop=True)
          )

    main_values_df = (df
                      .groupby('variable')
                      .agg(['first', 'last'])
                      .assign(change=lambda d: ((d['value']['last'] - d['value']['first']) / d['value']['first']) * 100)
                      .reset_index()
                      .loc[:, [('variable', ''),
                               ('period', 'last'),
                               ('value', 'last'),
                               ('change', '')]]
                      .droplevel(0, axis=1)
                      )

    main_values_df.columns = ['variable', 'latest_date', 'latest_value', 'change']
    main_values_df = (main_values_df.assign(category=lambda d: d.variable.map(lambda x: commodities[x][0]),
                                            units=lambda d: d.variable.map(lambda x: commodities[x][1]),
                                            latest_date=lambda d: d.latest_date.dt.strftime('%B %Y'))
                      .round({'latest_value': 2, 'change': 0})
                      )

    line_chart_df = (df.pivot(index='variable', columns='period', values='value'))
    line_chart_df.columns = range(len(line_chart_df.columns))
    line_chart_df = line_chart_df.reset_index()

    final = (pd.merge(main_values_df, line_chart_df, on='variable', how='left')
             .rename(columns={'variable': 'commodity', 'latest_date': 'as of',
                              'latest_value': 'price', 'change': '1 year change'})

             )
    final.insert(1, 'category', final.pop('category'))
    final.insert(3, 'as of', final.pop('as of'))
    final.insert(4, 'units', final.pop('units'))

    final.to_csv(f'{PATHS.charts}/hunger_topic/price_table.csv', index=False)


def update_hunger_topic_charts():
    """ """

    ipc = IPC(api_key='bac2a4d1-1274-4526-9065-0502ce9d4d5e')
    df = ipc.get_ipc_ch_data()
    ipc_chart(df)

    price_table()
    stunting_chart()
