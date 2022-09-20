""" """
import pandas as pd
import json
from scripts.config import PATHS
from scripts.owid_covid import tools as owid_tools
from bblocks.import_tools.world_bank import WorldBankData
import country_converter as coco
from scripts.health.common import get_malaria_data


def _format_wb_df(df: pd.DataFrame, indicator_name: str):
    """ """

    df = (df
          .dropna(subset=['value']).sort_values('date')
          .groupby('iso_code', as_index=False)
          .last()
          .loc[:, ['iso_code', 'value']]
          .assign(continent=lambda d: coco.convert(d.iso_code, to='continent', not_found=None))
          .loc[lambda d: d.continent == 'Africa', :]
          .assign(indicator=indicator_name)
          .reset_index(drop=True)
          )
    return df


def spending_dynamic() -> dict:
    """ """

    wb = WorldBankData()
    wb.load_indicator('SH.XPD.CHEX.PC.CD')
    wb.load_indicator('SH.XPD.CHEX.GD.ZS')

    pc = (wb
          .get_data('SH.XPD.CHEX.PC.CD')
          .pipe(_format_wb_df, 'pc')
          .loc[lambda d: d.value >= 86, :])

    gdp = (wb
           .get_data('SH.XPD.CHEX.GD.ZS')
           .pipe(_format_wb_df, 'gdp')
           .loc[lambda d: d.value >= 5, :])

    dff = pd.concat([pc, gdp])
    dff = dff.pivot(index='iso_code', columns='indicator', values='value')
    dff = dff.dropna()

    return {'count_86_target': len(pc), 'count_86_5_target': len(dff)}


def vaccination_dynamic() -> dict:
    """Create dynamic text for vaccination"""

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

    return {'vaccination_full_global': wrl_value,
            'vaccination_rate_africa': afr_value,
            'vaccination_date': vaccination_date}


def update_dynamic_text() -> None:
    """"Update dynamic text for health topic page"""
    dynamic_text = {}

    dynamic_text.update(vaccination_dynamic())
    dynamic_text.update(get_malaria_data())
    dynamic_text.update(spending_dynamic())

    with open(f"{PATHS.charts}/health/key_numbers.json", "w") as file:
        json.dump(dynamic_text, file, indent=4)
