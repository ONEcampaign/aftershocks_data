""" """
import pandas as pd
import requests


def query_who(code: str):
    """Query the WHO website for a given code.
    To be replaced in bblocks
    """

    URL = 'https://ghoapi.azureedge.net/api/'

    request = requests.get(URL + code)
    data = request.json()
    df = pd.DataFrame.from_records(data['value'])

    return df


def get_malaria_data() -> dict:
    """Extract and clean malaria data for overview chart"""

    malaria_dict = (query_who('MALARIA_EST_DEATHS')
        .loc[lambda d: d.SpatialDim.isin(['GLOBAL', 'AFR']), ['SpatialDim', 'TimeDim', 'NumericValue']]
        .astype({'TimeDim': 'int64'})
        .sort_values('TimeDim')
        .groupby(['SpatialDim'], as_index=False)
        .last()
        .assign(NumericValue=lambda d: pd.to_numeric(d.NumericValue, errors='coerce'))
        .pivot(index='TimeDim', columns='SpatialDim', values='NumericValue')
        .reset_index()
        .assign(malaria_rest_of_world_total=lambda d: d['GLOBAL'] - d['AFR'],
                malaria_africa_proportion=lambda d: (d['AFR'] / d['GLOBAL']) * 100,
                malaria_rest_of_world_proportion=lambda d: (d['malaria_rest_of_world_total'] / d['GLOBAL']) * 100)
        .rename(columns={'AFR': 'malaria_africa_total', 'GLOBAL': 'malaria_world_total', 'TimeDim': 'malaria_year'})
        .round(0)
        .astype(int)
        .to_dict(orient='records')[0]
        )
    return malaria_dict
