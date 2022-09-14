from bblocks.dataframe_tools.add import add_short_names_column, add_flourish_geometries
from bblocks.other_tools.dictionaries import flourish_geometries

from scripts.config import PATHS

import pandas as pd

import country_converter as coco


class MapDataSchema:
    GEOMETRY = "geometry"
    FORMAL_NAME = "formal_name"
    NAME = "name"
    ISO_CODE = "iso_code"


class BubbleDataSchema:
    FORMAL_NAME = "formal_name"
    NAME = "name"
    ISO_CODE = "iso_code"
    POSITION = "equal_position"


def map_data() -> None:
    """Create the map data used for the homepage map. A structure and names must be
    respected in order for things to work well. Following the standard structure
    defined in the MapDataSchema class, additional indicators can be added"""

    cc = coco.CountryConverter()

    df = (
        cc.data[["ISO3", "name_short", "name_official", "continent"]]
        .rename(
            columns={
                "ISO3": MapDataSchema.ISO_CODE,
                "name_short": MapDataSchema.NAME,
                "name_official": MapDataSchema.FORMAL_NAME,
            }
        )
        .loc[lambda d: d.continent == "Africa"]
        .pipe(add_flourish_geometries, id_column=MapDataSchema.ISO_CODE, id_type="ISO3")
        .filter(
            [
                MapDataSchema.GEOMETRY,
                MapDataSchema.FORMAL_NAME,
                MapDataSchema.NAME,
                MapDataSchema.ISO_CODE,
            ],
            axis=1,
        )
        .dropna(subset=[MapDataSchema.GEOMETRY])
    )

    df.to_csv(f"{PATHS.charts}/home_map_data.csv", index=False)
