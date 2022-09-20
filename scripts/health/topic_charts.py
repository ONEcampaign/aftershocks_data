""" """

import pandas as pd
from scripts.config import PATHS
import requests
from zipfile import ZipFile
import io
from scripts.health.common import query_who
import country_converter as coco

from bblocks.import_tools.world_bank import WorldBankData
from bblocks.dataframe_tools import add


def hiv_topic_chart() -> None:
    """Create HIV topic chart"""

    deaths = pd.read_csv(f'{PATHS.raw_data}/health/aids_region_AIDS-related deaths - All ages.csv')
    treatment = pd.read_csv(f'{PATHS.raw_data}/health/aids_region_People living with HIV receiving ART (%).csv')

    regions = {'UNAAP': 'Asia and the Pacific', 'UNACAR': 'Caribbean', 'UNAESA': 'East and Southern Africa',
               'UNAEECA': 'Eastern Europe and Central Asia', 'UNALA': 'Latin America',
               'UNAMENA': 'Middle East and North Africa',
               'UNAWCA': 'West and Central Africa', 'UNAWCENA': 'Western & Central Europe and North America'

               }

    deaths = deaths.assign(indicator='AIDS-related deaths')
    treatment = treatment.assign(indicator='People living with HIV receiving ART')

    df = (pd.concat([deaths, treatment])
          .assign(region=lambda d: d['country'].map(regions))
          .pivot(index=['region', 'year'], columns='indicator', values='All ages estimate')
          .dropna(subset=['People living with HIV receiving ART', 'AIDS-related deaths'])
          .reset_index()
          )

    df.to_csv(f'{PATHS.charts}/health/hiv_topic_chart.csv', index=False)
    df.to_csv(f'{PATHS.download}/health/hiv_topic_chart.csv', index=False)


def malaria_topic_chart() -> None:
    """Create Malaria topic chart"""

    code = 'MALARIA_EST_DEATHS'

    df = query_who(code)
    regions = {'NGA': 'Nigeria', 'COD': 'DRC', 'AFR': 'Africa',
               'GLOBAL': 'Global'}
    df = (df.loc[df.SpatialDim.isin(regions), ['SpatialDim', 'TimeDim', 'NumericValue']]
          .pivot(index='TimeDim', columns='SpatialDim', values='NumericValue')
          .reset_index()
          .assign(rest=lambda d: d['GLOBAL'] - d['AFR'])
          .rename(columns=regions)
          .rename(columns={'rest': 'Rest of the world', 'TimeDim': 'year'})
          )

    df.to_csv(f'{PATHS.charts}/health/malaria_topic_chart.csv', index=False)
    df.to_csv(f'{PATHS.download}/health/malaria_topic_chart.csv', index=False)


def dtp_topic_chart() -> None:
    """Create DTP topic chart"""

    code = 'WHS4_100'
    df = query_who(code)

    df = (df.loc[df.SpatialDimType == 'WORLDBANKINCOMEGROUP', ['SpatialDim', 'TimeDim', 'NumericValue']]
          .pivot(index='TimeDim', columns='SpatialDim', values='NumericValue')
          .reset_index()
          .rename(columns={'TimeDim': 'year', 'WB_HI': 'High income', 'WB_LI': 'Low income',
                           'WB_LMI': 'Lower-middle income', 'WB_UMI': 'Upper-middle income'})
          )

    df.to_csv(f'{PATHS.charts}/health/DTP_topic_chart.csv', index=False)
    df.to_csv(f'{PATHS.download}/health/DTP_topic_chart.csv', index=False)


# IHME spending
def __extract_data() -> pd.DataFrame:
    """Read IHME data from zip file"""

    zip_url = 'https://ghdx.healthdata.org/sites/default/files/record-attached-files/IHME_HEALTH_SPENDING_1995_2018_CSV.zip'

    try:
        response = requests.get(zip_url)
        folder = ZipFile(io.BytesIO(response.content))
        file_name = list(folder.NameToInfo.keys())[0]
        df = pd.read_csv(folder.open(file_name), low_memory=False, encoding='ISO-8859-1')
        return df

    except ConnectionError:
        raise ConnectionError('Could not connect to IHME website')


def __extract_codes() -> dict:
    """Extract codes from IHME website"""

    code_url = 'https://ghdx.healthdata.org/sites/default/files/record-attached-files/IHME_HEALTH_SPENDING_1995_2018_CODEBOOK_Y2021M09D22.CSV'
    try:
        response = requests.get(code_url)
        codes_dict = pd.read_csv(io.StringIO(response.text), sep=",").iloc[0, :].to_dict()
        return codes_dict

    except ConnectionError:
        raise ConnectionError('Could not connect to IHME website')


def get_ihme_spending() -> pd.DataFrame:
    """Extract IHME data qnd convert codes"""
    df = __extract_data()
    codes = __extract_codes()

    df = (df
          .melt(id_vars=['location_id', 'location_name', 'iso3', 'level', 'year', ])
          .assign(variable_name=lambda d: d.variable.map(codes))
          )
    return df


def ihme_spending_topic_chart() -> None:
    """Create IHME spending topic chart"""

    indicators = {'Out-of-pocket Health Spending (2020 USD) ': 'Total',
                  'Government Health Spending (2020 USD)': 'Total',
                  'Prepaid Private Health Spending (2020 USD)': 'Total',
                  'DAH (2020 USD)': 'Total',

                  'Total Health Spending per person (2020 USD)': 'Per person',
                  'Government Health Spending per person (2020 USD)': 'Per person',
                  'Prepaid Private Health Spending for per person (2020 USD)': 'Per person',
                  'DAH per person (2020 USD)': 'Per person'
                  }

    df = get_ihme_spending()

    query = (df.variable_name.isin(indicators) & (df.location_name == 'Sub-Saharan Africa'))

    df = (df[query]
          .assign(category=lambda d: d.variable_name.map(indicators))
          .assign(variable_name=lambda d: d.variable_name.str.replace('per person', '', regex=False))
          .assign(variable_name=lambda d: d.variable_name.str.replace('(2020 USD)', '', regex=False))
          .assign(variable_name=lambda d: d.variable_name.str.strip())
          .pivot(index=['year', 'category'], columns='variable_name', values='value')
          .reset_index()
          .sort_values(by='category', ascending=False)
          )

    df.to_csv(f'{PATHS.charts}/health/health_spending_topic_chart.csv', index=False)
    df.to_csv(f'{PATHS.download}/health/health_spending_topic_chart.csv', index=False)


def wb_spending_topic_chart() -> None:
    """Create World Bank health spending topic chart"""

    cc = coco.CountryConverter()

    wb = WorldBankData()
    wb.load_indicator('SH.XPD.CHEX.PC.CD')

    df = (wb
              .get_data()
              .dropna(subset='value')
              .sort_values('date')
              .groupby('iso_code', as_index=False)
              .last()
              .loc[:, ['iso_code', 'indicator', 'value']]
              .assign(country_name=lambda d: coco.convert(d.iso_code, to='name_short', not_found=None))
              .loc[lambda d: d.iso_code.isin(cc.data.ISO3), :]
              .assign(continent=lambda d: coco.convert(d.iso_code, to='continent', not_found=None))
              .pipe(add.add_income_level_column, 'iso_code')
              .loc[lambda d: (d.continent == 'Africa') & (d.income_level.isin(['Lower middle income', 'Low income'])),
                   ['value', 'country_name']]
              )

    df.to_csv(f'{PATHS.charts}/health/health_expenditure_per_person.csv', index=False)
    df.to_csv(f'{PATHS.download}/health/health_expenditure_per_person.csv', index=False)


def update_health_topic_charts() -> None:
    """" """

    hiv_topic_chart()
    malaria_topic_chart()
    dtp_topic_chart()
    ihme_spending_topic_chart()
    wb_spending_topic_chart()
