"""Create hunger overview charts for the topic carrousel"""

import pandas as pd
from scripts.config import PATHS
from scripts.common import clean_wb_overview
from scripts.hunger.common import wb_indicators
from scripts.hunger.common import aggregate_insufficient_food
import datetime


def wb_charts(indicators: dict) -> None:
    """Create world bank overview charts"""

    for code, name in indicators.items():
        (
            pd.read_csv(f"{PATHS.raw_data}/hunger/{code}.csv")
            .pipe(clean_wb_overview)
            .to_csv(f"{PATHS.charts}/hunger_topic/{name}.csv", index=False)
        )


def insufficient_food_single_measure() -> None:
    """Create insufficient food single measure chart"""

    wfp_data = pd.read_csv(f"{PATHS.raw_data}/hunger/wfp.csv", parse_dates=["date"])
    latest_date = wfp_data["date"].max()
    month_date = latest_date - datetime.timedelta(days=30)

    latest_value = aggregate_insufficient_food(wfp_data, latest_date, "date")
    month_value = aggregate_insufficient_food(wfp_data, month_date, "date")
    change = ((latest_value - month_value) / month_value) * 100
    arrow = (latest_value - month_value) / month_value

    d = {
        "value": latest_value / 1000000,
        "change": change,
        "top_label": f'as of {latest_date.strftime("%d %b %Y")}',
        "arrow": arrow,
        "bottom_label": "in the last 30 days",
    }

    df = pd.DataFrame.from_records([d])
    df.to_csv(
        f"{PATHS.charts}/hunger_topic/insufficient_food_single_measure.csv", index=False
    )


def update_hunger_overview_charts() -> None:
    """Update overview charts"""

    wb_charts(wb_indicators)
    insufficient_food_single_measure()
