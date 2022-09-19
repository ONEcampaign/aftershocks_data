""" """

from bblocks.import_tools.world_bank import WorldBankData
import pandas as pd
from scripts.config import PATHS

chart_indicators = {'life_expectancy_overview': 'SP.DYN.LE00.IN',
                    'infant_mortality_overview': 'SH.DYN.MORT',
                    'health_expenditure_overview': 'SH.XPD.CHEX.GD.ZS',
                    }


def __clean_overview_chart(df: pd.DataFrame) -> pd.DataFrame:
    """Create overview line chart with SSA and WLD data"""

    return (df
            .loc[lambda d: d['iso_code'].isin(['WLD', 'SSA'])]
            .pivot(index='date', columns='iso_code', values='value')
            .reset_index()
            .dropna(subset=['SSA', 'WLD'])
            .rename(columns={'SSA': 'Sub-Saharan Africa', 'WLD': 'World'})
            )


def create_overview_charts():
    """Create overview charts"""

    wb = WorldBankData(update_data=True)
    for _, code in chart_indicators.items():
        wb.load_indicator(code)

    for name, code in chart_indicators.items():
        (wb.get_data(code)
         .pipe(__clean_overview_chart)
         .to_csv(f'{PATHS.charts}/health/{name}.csv', index=False))
