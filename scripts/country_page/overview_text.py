import json

import pandas as pd
from bblocks import convert_id
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
        .set_index("name_short")["iso_code"]
        .to_dict()
    )

    data = read_dictionary()

    for indicator, country_data in data.items():
        for country in df:
            if country not in country_data.keys():
                try:
                    data[indicator][df.get(country, None)] = data[indicator][country]
                except KeyError:
                    data[indicator][df.get(country, None)] = {"info": "display-none"}
                try:
                    del data[indicator][country]
                except KeyError:
                    pass
            else:
                data[indicator][df.get(country, None)] = data[indicator][country]
                data[indicator][df.get(country, None)]["info"] = ""
                try:
                    del data[indicator][country]
                except KeyError:
                    pass

    update_key_number(
        path=f"{PATHS.charts}/country_page/overview_summary.json", new_dict=data
    )


if __name__ == "__main__":
    build_summary()
