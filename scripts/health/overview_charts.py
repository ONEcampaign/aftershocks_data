""" """

from bblocks.import_tools.world_bank import WorldBankData
import pandas as pd
from scripts.config import PATHS
from scripts.owid_covid import tools as owid_tools
from scripts.health.common import query_who
from datetime import datetime
import json


def __clean_overview_chart(df: pd.DataFrame) -> pd.DataFrame:
    """Create overview line chart with SSA and WLD data"""

    return (df
            .loc[lambda d: d['iso_code'].isin(['WLD', 'SSA'])]
            .pivot(index='date', columns='iso_code', values='value')
            .reset_index()
            .dropna(subset=['SSA', 'WLD'])
            .rename(columns={'SSA': 'Sub-Saharan Africa', 'WLD': 'World'})
            )


def wb_charts():
    """ """

    chart_indicators = {'life_expectancy_overview': 'SP.DYN.LE00.IN',
                        'infant_mortality_overview': 'SH.DYN.MORT',
                        'health_expenditure_overview': 'SH.XPD.CHEX.GD.ZS',
                        'maternal_mortality_overview': 'SH.STA.MMRT'
                        }

    wb = WorldBankData(update_data=True)
    for _, code in chart_indicators.items():
        wb.load_indicator(code)

    for name, code in chart_indicators.items():
        (wb.get_data(code)
         .pipe(__clean_overview_chart)
         .to_csv(f'{PATHS.charts}/health/{name}.csv', index=False))


def vccination_dynamic():
    """ """

    df = (owid_tools.read_owid_data()
          .pipe(owid_tools.get_indicators_ts, 'people_fully_vaccinated_per_hundred')
          .loc[lambda d: d['iso_code'].isin(['OWID_WRL', 'OWID_AFR'])]
          .sort_values('date')
          .groupby('iso_code', as_index=False)
          .last()
          .round(1)
          )
    afr_value = df.loc[df.iso_code == 'OWID_AFR', 'value'].item()
    wrl_value = df.loc[df.iso_code == 'OWID_WRL', 'value'].item()
    vaccination_date = df.loc[df.iso_code == 'OWID_WRL', 'date'].item()
    vaccination_date = vaccination_date.strftime('%d %B %Y')

    return {'vaccination_full_global': wrl_value, 'vaccination_rate_africa': afr_value, 'vaccination_date': vaccination_date}


def vaccination_chart():
    """ """

    # covid vaccination overview
    (owid_tools.read_owid_data()
     .loc[lambda d: d['iso_code'].isin(['OWID_WRL', 'OWID_AFR'])]
     .pipe(owid_tools.get_indicators_ts, 'people_fully_vaccinated_per_hundred')
     .pivot(index='date', columns='iso_code', values='value')
     .reset_index()
     .dropna(subset=['OWID_AFR', 'OWID_WRL'])
     .rename(columns={'OWID_AFR': 'Africa', 'OWID_WRL': 'World'})
     .to_csv(f'{PATHS.charts}/health/vaccination_overview.csv', index=False)
     )


def get_malaria_data():
    """ """

    malaria_dict = (query_who('MALARIA_EST_DEATHS')
        .loc[lambda d: d.SpatialDim.isin(['GLOBAL', 'AFR']), ['SpatialDim', 'TimeDim', 'NumericValue']]
        .sort_values('TimeDim')
        .groupby(['SpatialDim'], as_index=False)
        .last()
        .assign(NumericValue=lambda d: pd.to_numeric(d.NumericValue))
        .pivot(index='TimeDim', columns='SpatialDim', values='NumericValue')
        .reset_index()
        .assign(malaria_rest_of_world_total=lambda d: d['GLOBAL'] - d['AFR'])
        .assign(malaria_africa_proportion=lambda d: (d['AFR'] / d['GLOBAL']) * 100)
        .assign(malaria_rest_of_world_proportion=lambda d: (d['malaria_rest_of_world_total'] / d['GLOBAL']) * 100)
        .rename(columns={'AFR': 'malaria_africa_total', 'GLOBAL': 'malaria_world_total', 'TimeDim': 'malaria_year'})
        .round(0)
        .astype(int)
        .to_dict(orient='records')[0]
        )
    return malaria_dict


def malaria_chart(malaria_dict):
    """ """
    (pd.DataFrame({'region': ['Africa', 'Rest of world'],
                   'total_deaths': [malaria_dict['malaria_africa_total'],
                                    malaria_dict['malaria_rest_of_world_total']],
                   'proportion': [malaria_dict['malaria_africa_proportion'],
                                  malaria_dict['malaria_rest_of_world_proportion']],
                   'year': [malaria_dict['malaria_year'], malaria_dict['malaria_year']]})

     .to_csv(f'{PATHS.charts}/health/malaria_overview.csv', index=False)
     )


def update_charts_and_json():
    """ """

    malaria_dict = get_malaria_data()

    #wb_charts()
    vaccination_chart()
    malaria_chart(malaria_dict)

    dynamic_text = {}
    dynamic_text.update(vccination_dynamic())
    dynamic_text.update(malaria_dict)

    with open(f"{PATHS.charts}/health/key_numbers.json", "w") as file:
        json.dump(dynamic_text, file, indent=4)
