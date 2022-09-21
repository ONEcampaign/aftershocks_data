import pandas as pd
import country_converter
from bblocks.dataframe_tools.add import add_income_level_column


def _base_df() -> pd.DataFrame:
    """A dataframe with iso3 codes, name, UN region and continent"""

    return country_converter.CountryConverter().data[
        ["ISO3", "name_short", "continent", "UNregion"]
    ]


def basic_info() -> pd.DataFrame:
    """Create a DataFrame with basic information"""

    base = (
        _base_df()
        .pipe(add_income_level_column, id_column="ISO3", id_type="ISO3")
        .fillna({"income_level": "Not classified"})
    )

import requests
url = requests.get("http://unctad.org/topic/least-developed-countries/list")



df = pd.read_html(url.text)
