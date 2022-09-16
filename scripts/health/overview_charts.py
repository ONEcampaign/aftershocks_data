""" """

from bblocks.import_tools.world_bank import WorldBankData
import pandas as pd

indicators = {'Life expectancy at birth, total (years)': 'SP.DYN.LE00.IN',
              'Mortality rate, under-5 (per 1,000 live births)': 'SH.DYN.MORT',
              

              }


wb = WorldBankData(update_data=True)
for _, code in indicators.items():
    wb.load_indicator(code)
