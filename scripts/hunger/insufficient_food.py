import pandas as pd
from bblocks import WFPData, set_bblocks_data_path, add_short_names_column
from bblocks.dataframe_tools.add import add_population_share_column

from scripts.config import PATHS

set_bblocks_data_path(PATHS.bblocks_data)


def read_world_insufficient_food() -> pd.DataFrame:
    wfp = WFPData()

    wfp.load_data("insufficient_food")

    return wfp.get_data()


def insufficient_food_map() -> None:
    data = read_world_insufficient_food()

    data = data.sort_values(["iso_code", "date"]).drop_duplicates(
        subset=["iso_code"], keep="last"
    )

    data = (
        data.pipe(
            add_short_names_column,
            id_column="iso_code",
            id_type="ISO3",
            target_column="country",
        )
        .pipe(
            add_population_share_column,
            id_column="iso_code",
            id_type="ISO3",
        )
        .filter(["iso_code", "date", "population_share", "country"])
    )

    data = data.rename(
        columns={"population_share": "People with insufficient food consumption (%)"}
    )

    # chart version
    data.to_csv(PATHS.charts + rf"/hunger_topic/insufficient_food.csv", index=False)


if __name__ == "__main__":
    insufficient_food_map()
