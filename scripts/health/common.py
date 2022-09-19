""" """
import pandas as pd
import requests


def query_who(code: str):
    """To be replaced in bblocks"""

    URL = 'https://ghoapi.azureedge.net/api/'

    request = requests.get(URL + code)
    data = request.json()
    df = pd.DataFrame.from_records(data['value'])

    return df

