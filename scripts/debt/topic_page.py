import pandas as pd
from bblocks.cleaning_tools.clean import convert_id

from scripts.config import PATHS

STOCKS_URL: str = (
    "https://onecampaign.github.io/project_covid-19_tracker/c08_debt_stocks-ts.csv"
)


def debt_stocks_columns() -> None:
    """Bar chart of debt stocks by country"""

    df = (
        pd.read_csv(STOCKS_URL)
        .replace("C.A.R", "Central African Republic")
        .replace("D.R.C", "Democratic Republic of the Congo")
        .assign(
            iso_code=lambda d: convert_id(
                d.iso_code, from_type="regex", to_type="name_short"
            )
        )
    )

    africa = df.groupby(["year"], as_index=False).sum().assign(iso_code="Africa")

    df = pd.concat([africa, df], ignore_index=True).filter(
        [
            "iso_code",
            "year",
            "Bilateral (China)",
            "Bilateral (excl. China)",
            "Multilateral",
            "Private (China)",
            "Private (excl. China)",
            "Total",
        ],
        axis=1,
    )

    df.to_csv(f"{PATHS.charts}/debt_topic/debt_stocks_ts.csv", index=False)


if __name__ == "__main__":
    debt_stocks_columns()
