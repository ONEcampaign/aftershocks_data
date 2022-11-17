import pandas as pd

URL: str = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSqAIxjSZ78fE93CP1K9K0t8rLM2wi0z_nc60ezrUeDEIOPz-vr01SmmS_5nNnq"
    "_uPE0dM26m0V3rQK/pub?gid=388051861&single=true&output=csv"
)

URL_REF = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSqAIxjSZ78fE93CP1K9K0t8rLM2wi0z_nc60ezrUeDEIOPz-vr01SmmS"
    "_5nNnq_uPE0dM26m0V3rQK/pub?gid=967116072&single=true&output=csv"
)

SHEET: pd.DataFrame = pd.read_csv(URL, header=None)

REFUGEES_SHEET: pd.DataFrame = pd.read_csv(URL_REF)


def total_reported_oda() -> str:
    """Get our estimated total ODA to Ukraine"""

    return f"{round(float(SHEET.iloc[0,1]),1)}"


def total_idrc() -> str:
    """Get our estimated total ODA to Ukraine"""

    return f"{round(float(SHEET.iloc[1,1]),1)}"


def refugees_in_countries() -> dict:
    """Get our estimated total ODA to Ukraine"""
    return REFUGEES_SHEET.set_index("iso_code")[
        "Individual refugees from Ukraine recorded across Europe"
    ].to_dict()


def idrc_share() -> str:
    """Get our estimated total IDRC as a share of ODA"""

    return f"{round(100*float(SHEET.iloc[2,1]),1)}"
