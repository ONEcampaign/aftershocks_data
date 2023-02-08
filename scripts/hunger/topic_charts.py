"""Create hunger topic charts"""

import datetime

import pandas as pd
from country_converter import CountryConverter

from scripts.config import PATHS

coco = CountryConverter()


def ipc_chart() -> None:
    """Create IPC bar chart"""

    df = pd.read_csv(f"{PATHS.raw_data}/hunger/ipc.csv")

    df = (
        df.drop(["phase_3plus", "phase_1", "iso_code"], axis=1)
        .rename(
            columns={
                "phase_2": "Phase 2",
                "phase_3": "Phase 3",
                "phase_4": "Phase 4",
                "phase_5": "Phase 5",
            }
        )
        .assign(
            from_date=lambda d: pd.to_datetime(d.from_date).dt.strftime("%B %Y"),
            to_date=lambda d: pd.to_datetime(df.to_date).dt.strftime("%B %Y"),
        )
        .loc[lambda d: d.country_name != "LAC"]
    )

    df.to_csv(f"{PATHS.charts}/hunger_topic/ipc_phases.csv", index=False)
    df.to_csv(f"{PATHS.download}/hunger_topic/ipc_phases.csv", index=False)


def stunting_chart() -> None:
    """Create stunting connected dot chart"""

    country_list = list(coco.data.loc[lambda d: d.continent == "Africa", "ISO3"]) + [
        "SSA"
    ]

    df = pd.read_csv(f"{PATHS.raw_data}/hunger/SH.STA.STNT.ME.ZS.csv")
    df = (
        df.dropna(subset="value")
        .loc[lambda d: d.iso_code.isin(country_list), ["date", "iso_code", "value"]]
        .groupby("iso_code", as_index=False)
    )

    df = (
        pd.concat([df.first(), df.last()])
        .assign(date=lambda d: pd.to_datetime(d.date).dt.strftime("%Y"))
        .assign(
            country=lambda d: coco.convert(
                d.iso_code, to="name_short", not_found="Sub-Saharan Africa"
            )
        )
        .sort_values(by="value")
        .replace(
            {
                "Central African Republic": "Central African Rep.",
                "Sao Tome and Principe": "Sao Tome",
            }
        )
    )

    df.to_csv(f"{PATHS.charts}/hunger_topic/prevalence_of_stunting.csv", index=False)
    df.to_csv(f"{PATHS.download}/hunger_topic/prevalence_of_stunting.csv", index=False)


def price_table() -> None:
    """Create commodity price table"""

    commodities = {
        "Coconut oil": ["oil", "USD/mt"],
        "Groundnut oil": ["oil", "USD/mt"],
        "Palm oil": ["oil", "USD/mt"],
        "Palm kernel oil": ["oil", "USD/mt"],
        "Soybean oil": ["oil", "USD/mt"],
        "Rapeseed oil": ["oil", "USD/mt"],
        "Sunflower oil": ["oil", "USD/mt"],
        "Groundnuts": ["meals", "USD/mt"],
        "Soybeans": ["meal", "USD/mt"],
        "Maize": ["grains", "USD/mt"],
        "Rice, Thai 5% ": ["grains", "USD/mt"],
        "Wheat, US HRW": ["grains", "USD/mt"],
        "Wheat, US SRW": ["grains", "USD/mt"],
        "Rice, Thai 25%": ["grains", "USD/mt"],
        "Rice, Thai A.1": ["grains", "USD/mt"],
        "Rice, Viet Namese 5%": ["grains", "USD/mt"],
        "Beef": ["meat", "USD/kg"],
        "Meat, chicken": ["meat", "USD/kg"],
        "Shrimps, Mexican": ["meat", "USD/kg"],
        "Sugar, world": ["sugar", "USD/kg"],
    }

    pink_sheet = pd.read_csv(f"{PATHS.raw_data}/hunger/pink_sheet.csv")
    df = (
        pink_sheet.drop(
            columns="units"
        )  # temporary solutions: dropping to avoid having to extensively reformat the pipeline
        # and the commodities list above
        .dropna(subset=["value"])
        .assign(
            period=lambda d: pd.to_datetime(d.period),
        )
        .dropna(subset=["value"])
        .loc[
            lambda d: (d.indicator.isin(commodities))
            & (d.period >= d.period.max() - datetime.timedelta(days=365))
        ]
        .reset_index(drop=True)
    )

    main_values_df = (
        df.groupby(["indicator"])
        .agg(["first", "last"])
        .assign(
            change=lambda d: (
                (d["value"]["last"] - d["value"]["first"]) / d["value"]["first"]
            )
            * 100
        )
        .reset_index()
        .loc[
            :,
            [("indicator", ""), ("period", "last"), ("value", "last"), ("change", "")],
        ]
        .droplevel(0, axis=1)
    )

    main_values_df.columns = ["indicator", "latest_date", "latest_value", "change"]
    main_values_df = main_values_df.assign(
        category=lambda d: d.indicator.map(lambda x: commodities[x][0]),
        units=lambda d: d.indicator.map(lambda x: commodities[x][1]),
        latest_date=lambda d: d.latest_date.dt.strftime("%B %Y"),
    ).round({"latest_value": 2, "change": 0})

    line_chart_df = df.pivot(index="indicator", columns="period", values="value")
    line_chart_df.columns = range(len(line_chart_df.columns))
    line_chart_df = line_chart_df.reset_index()

    final = pd.merge(main_values_df, line_chart_df, on="indicator", how="left").rename(
        columns={
            "indicator": "commodity",
            "latest_date": "as of",
            "latest_value": "price",
            "change": "1 year change",
        }
    )
    final.insert(1, "category", final.pop("category"))
    final.insert(3, "as of", final.pop("as of"))
    final.insert(4, "units", final.pop("units"))

    final.to_csv(f"{PATHS.charts}/hunger_topic/price_table.csv", index=False)


def update_hunger_topic_charts() -> None:
    """Update all charts for the hunger topic"""

    ipc_chart()
    stunting_chart()
    price_table()
