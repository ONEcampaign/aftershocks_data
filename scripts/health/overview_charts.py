""" """

from bblocks.import_tools.world_bank import WorldBankData
import pandas as pd
from scripts.config import PATHS
from scripts.owid_covid import tools as owid_tools
from scripts.health.common import get_malaria_data


def __clean_wb_overview(df: pd.DataFrame) -> pd.DataFrame:
    """Clean World Bank data for overview charts"""

    return (df
            .loc[lambda d: d['iso_code'].isin(['WLD', 'SSA'])]
            .pivot(index='date', columns='iso_code', values='value')
            .reset_index()
            .dropna(subset=['SSA', 'WLD'])
            .rename(columns={'SSA': 'Sub-Saharan Africa', 'WLD': 'World'})
            )


def wb_charts() -> None:
    """Create World Bank overview charts"""

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
         .pipe(__clean_wb_overview)
         .to_csv(f'{PATHS.charts}/health/{name}.csv', index=False))


def vaccination_chart() -> None:
    """Create vaccination overview chart"""

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


def malaria_chart(malaria_dict) -> None:
    """Create malaria overview chart"""
    (pd.DataFrame({'region': ['Africa', 'Rest of world'],
                   'total_deaths': [malaria_dict['malaria_africa_total'],
                                    malaria_dict['malaria_rest_of_world_total']],
                   'proportion': [malaria_dict['malaria_africa_proportion'],
                                  malaria_dict['malaria_rest_of_world_proportion']],
                   'year': [malaria_dict['malaria_year'], malaria_dict['malaria_year']]})

     .to_csv(f'{PATHS.charts}/health/malaria_overview.csv', index=False)
     )


def update_health_overview_charts():
    """Update health overview charts"""

    malaria_chart(get_malaria_data())
    wb_charts()
    vaccination_chart()


