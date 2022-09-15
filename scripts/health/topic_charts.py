""" """

import pandas as pd
from scripts.config import PATHS


def hiv_topic_chart():
    """ """

    deaths = pd.read_csv(f'{PATHS.raw_data}/health/aids_region_AIDS-related deaths - All ages.csv')
    treatment = pd.read_csv(f'{PATHS.raw_data}/health/aids_region_People living with HIV receiving ART (%).csv')

    regions = {'UNAAP': 'Asia and the Pacific', 'UNACAR': 'Caribbean', 'UNAESA': 'East and Southern Africa',
               'UNAEECA': 'Eastern Europe and Central Asia', 'UNALA': 'Latin America', 'UNAMENA': 'Middle East and North Africa',
               'UNAWCA': 'West and Central Africa', 'UNAWCENA':'Western & Central Europe and North America'

               }

    deaths = deaths.assign(indicator = 'AIDS-related deaths')
    treatment = treatment.assign(indicator = 'People living with HIV receiving ART')

    df = (pd.concat([deaths, treatment])
          .assign(region = lambda d: d['country'].map(regions))
          .pivot(index=['region', 'year'], columns = 'indicator', values='All ages estimate')
          .dropna(subset=['People living with HIV receiving ART', 'AIDS-related deaths'])
          .reset_index()
          )

    df.to_csv(f'{PATHS.charts}/health/hiv_topic_chart.csv', index=False)
    df.to_csv(f'{PATHS.download}/health/hiv_topic_chart.csv', index=False)





if __name__ == "__main__":
    hiv_topic_chart()
