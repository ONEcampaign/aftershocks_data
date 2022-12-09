import pandas as pd

from scripts import config


def export_tableau_database() -> None:
    """Export debt service and stocks data as CSV for Tableau"""

    service = pd.read_feather(config.PATHS.raw_debt + r"/ids_service_raw.feather")
    stocks = pd.read_feather(config.PATHS.raw_debt + r"/ids_stocks_raw.feather")

    df = pd.concat([service, stocks], ignore_index=True)

    df = df.rename(
        columns={
            "country": "Country",
            "counterpart-area": "Creditors",
            "series": "Series",
            "series_code": "Series Id",
        }
    )

    df.to_feather(config.PATHS.raw_debt + r"/ids_tableau.feather")
