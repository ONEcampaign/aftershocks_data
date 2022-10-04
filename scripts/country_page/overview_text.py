import json

from bblocks.dataframe_tools.add import add_short_names_column

from scripts.common import update_key_number, base_africa_df
from scripts.config import PATHS


def read_dictionary() -> dict:
    path = f"{PATHS.charts}/country_page/overview.json"

    with open(path, "r") as f:
        t = json.load(f)

    return t


def build_summary() -> None:
    df = (
        base_africa_df()
        .pipe(add_short_names_column, id_column="iso_code", id_type="ISO3")
        .name_short.unique()
    )

    data = read_dictionary()

    for indicator, country_data in data.items():
        for country in df:
            if country not in country_data.keys():
                data[indicator][country] = {"info": "display-none"}
            else:
                data[indicator][country]["info"] = ""

    update_key_number(
        path=f"{PATHS.charts}/country_page/overview_summary.json", new_dict=data
    )
