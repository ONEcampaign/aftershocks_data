import time

import pandas as pd
import requests
from bblocks import WorldBankData, add_iso_codes_column, set_bblocks_data_path
from pyjstat import pyjstat
import bblocks_data_importers as bbdata

from scripts.config import PATHS
from scripts.logger import logger

set_bblocks_data_path(PATHS.bblocks_data)

DEBT_SERVICE = {
    "DT.AMT.BLAT.CD": "Bilateral",
    "DT.AMT.MLAT.CD": "Multilateral",
    "DT.AMT.PBND.CD": "Private",
    "DT.AMT.PCBK.CD": "Private",
    "DT.AMT.PROP.CD": "Private",
    "DT.INT.BLAT.CD": "Bilateral",
    "DT.INT.MLAT.CD": "Multilateral",
    "DT.INT.PBND.CD": "Private",
    "DT.INT.PCBK.CD": "Private",
    "DT.INT.PROP.CD": "Private",
}

DEBT_STOCKS = {
    "DT.DOD.BLAT.CD": "Bilateral",
    "DT.DOD.MLAT.CD": "Multilateral",
    "DT.DOD.PBND.CD": "Private",
    "DT.DOD.PCBK.CD": "Private",
    "DT.DOD.PROP.CD": "Private",
}

WORLD_BANK_INDICATORS = {
    "SH.XPD.GHED.GE.ZS": "health_expenditure_share",
    "SE.XPD.TOTL.GB.ZS": "education_expenditure_share",
}


def update_debt_world_bank() -> None:
    wb = WorldBankData()
    wb.load_data(indicator=list(WORLD_BANK_INDICATORS))

    wb.update_data(reload_data=False)


def _time_period(start_year: int, end_year: int) -> str:
    """Take a period range and convert it to an API compatible string"""

    time_period = ""

    for y in range(start_year, end_year + 1):
        if y < end_year:
            time_period += f"yr{y};"
        else:
            time_period += f"yr{y}"

    return time_period


def _country_list(countries: str | list) -> str:
    """Take a country list amd convert it to an API compatible string"""

    country_list = ""

    if isinstance(countries, str):
        return countries

    for c in countries:
        country_list += f"{c};"

    return country_list[:-1]


def _api_url(
    indicator: str,
    countries: str | list,
    start_year: int,
    end_year: int,
    source: int,
) -> str:
    """Query string for API for IDS data. One indicator at a time"""

    if not isinstance(indicator, str):
        raise TypeError("Must pass single indicator (as string) at a time")

    countries = _country_list(countries)
    time_period = _time_period(start_year, end_year)

    return (
        "http://api.worldbank.org/v2/"
        f"sources/{source}/country/{countries}/"
        f"series/{indicator}/time/{time_period}/"
        f"data?format=jsonstat"
    )


def get_indicator_data(
    indicator: str,
    countries: str | list = "all",
    start_year: int = 2017,
    end_year: int = 2025,
    source: int = 6,
    try_again: bool = True,
) -> pd.DataFrame:
    # Get API url
    url = _api_url(indicator, countries, start_year, end_year, source)

    # Get data
    try:
        data = pyjstat.Dataset.read(url).write(output="dataframe")
        logger.debug(f"Got data for {indicator}")

        return (
            data.loc[data.value.notna()]
            .assign(series_code=indicator)
            .reset_index(drop=True)
        )

    except requests.exceptions.HTTPError:
        logger.debug(f"Failed to get data for {indicator}")

    except requests.exceptions.JSONDecodeError:
        logger.debug(f"Failed to get data for {indicator}")

    except Exception as e:
        print("Ran into other trouble: ", e)

    if try_again:
        time.sleep(300)
        get_indicator_data(
            indicator=indicator,
            countries=countries,
            start_year=start_year,
            end_year=end_year,
            source=source,
            try_again=False,
        )


def read_dservice_data() -> pd.DataFrame:
    file: str = "debt_service_ts.feather"

    return (
        pd.read_feather(f"{PATHS.raw_data}/debt/{file}")
        .replace("C.A.R", "Central African Republic")
        .replace("D.R.C", "Democratic Republic of the Congo")
        .pipe(add_iso_codes_column, id_column="iso_code", id_type="regex")
    )


def read_dstocks_data() -> pd.DataFrame:
    file: str = "debt_stocks-ts.feather"

    return (
        pd.read_feather(f"{PATHS.raw_data}/debt/{file}")
        .replace("C.A.R", "Central African Republic")
        .replace("D.R.C", "Democratic Republic of the Congo")
        .pipe(add_iso_codes_column, id_column="iso_code", id_type="regex")
    )


def education_expenditure_share() -> pd.DataFrame:
    indicator = "SE.XPD.TOTL.GB.ZS"

    return (
        WorldBankData()
        .load_data(indicator)
        .get_data()
        .dropna(subset="value")
        .assign(year=lambda d: d.date.dt.year)
        .filter(["year", "iso_code", "value"])
    )


def health_expenditure_share_wb() -> pd.DataFrame:
    """Get gov health expenditure share from World Bank"""

    indicator = "SH.XPD.GHED.GE.ZS"

    return (
        WorldBankData()
        .load_data(indicator)
        .get_data()
        .dropna(subset="value")
        .assign(year=lambda d: d.date.dt.year)
        .filter(["year", "iso_code", "value"])
    )


def health_expenditure_share_ghed() -> pd.DataFrame:
    """Get gov health expenditure share from GHED database"""

    df = (bbdata.GHED().get_data()
            .loc[lambda d: d.indicator_code == "gghed_gge", ['iso3_code', 'year', 'value']]
            .rename(columns = {'iso3_code': 'iso_code'})
          )

    return df
