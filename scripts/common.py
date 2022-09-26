""" """
import pandas as pd


def clean_wb_overview(df: pd.DataFrame) -> pd.DataFrame:
    """Clean World Bank data for overview charts

    returns a dataframe for a line chart with values for World and SSA
    """

    return (df
            .loc[lambda d: d['iso_code'].isin(['WLD', 'SSA'])]
            .pivot(index='date', columns='iso_code', values='value')
            .reset_index()
            .dropna(subset=['SSA', 'WLD'])
            .rename(columns={'SSA': 'Sub-Saharan Africa', 'WLD': 'World'})
            )