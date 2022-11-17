import datetime

import requests


def _get_data() -> dict:
    url = (
        "https://data.unhcr.org/population/"
        "?widget_id=314816&sv_id=54&population_group=5478"
    )

    return requests.get(url).json()["data"][0]


UN_DATA = _get_data()


def read_refugee_data() -> str:
    """Read json file from UNHCR website and return dataframe"""
    return f"{int(UN_DATA['individuals']):,.0f}"


def read_refugee_date() -> str:
    """Read json file from UNHCR website and return dataframe"""
    try:
        date = datetime.date(*[int(d) for d in UN_DATA["date"].split("-")])
    except AttributeError:
        data = _get_data()
        date = datetime.date(*[int(d) for d in data["date"].split("-")])

    return date.strftime("%d %B")
