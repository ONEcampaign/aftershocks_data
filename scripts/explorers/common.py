import country_converter
from bblocks.dataframe_tools.add import add_flourish_geometries


class ExplorerSchema:
    ID = "iso_code"
    NAME = "Country Name"
    REGION = "UN Region"
    CONTINENT = "Continent"
    INCOME = "Income Group"
    LDC = "Least Developed Countries"
    GDP = "GDP per capita"
    POP = "Population"
    POVERTY = "Poverty Headcount Ratio"


def base_africa_map():
    """Create a map with geometries for all african countries"""

    return (
        country_converter.CountryConverter()
        .data[["ISO3", "continent"]]
        .rename(columns={"ISO3": "iso_code"})
        .query("continent == 'Africa'")
        .drop(columns="continent")
        .pipe(add_flourish_geometries, id_column="iso_code", id_type="ISO3")
        .dropna(subset=["geometry"])
    )


LDC: list = [
    "AFG",
    "AGO",
    "BGD",
    "BEN",
    "BTN",
    "BFA",
    "BDI",
    "KHM",
    "CAF",
    "TCD",
    "COM",
    "COD",
    "DJI",
    "ERI",
    "ETH",
    "GMB",
    "GIN",
    "GNB",
    "HTI",
    "KIR",
    "LAO",
    "LSO",
    "LBR",
    "MDG",
    "MWI",
    "MLI",
    "MRT",
    "MOZ",
    "MMR",
    "NPL",
    "NER",
    "RWA",
    "STP",
    "SEN",
    "SLE",
    "SLB",
    "SOM",
    "SSD",
    "SDN",
    "TLS",
    "TGO",
    "TUV",
    "UGA",
    "TZA",
    "YEM",
    "ZMB",
]
