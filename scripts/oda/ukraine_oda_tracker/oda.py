import pandas as pd
from bblocks import add_short_names_column, format_number


YEARLY_COSTS_URL: str = (
    "https://raw.githubusercontent.com/ONEcampaign/"
    "ukraine_oda_tracker/main/output/ukraine_refugee_cost_estimates.csv"
)

ODA_URL: str = (
    "https://raw.githubusercontent.com/ONEcampaign/ukraine_oda_tracker/"
    "main/output/latest_oda.csv"
)

TOTAL_IDRC: str = (
    "https://raw.githubusercontent.com/ONEcampaign/ukraine_oda_tracker/main/"
    "output/idrc_over_time_constant.csv"
)

HCR_TOTALS: str = (
    "https://raw.githubusercontent.com/ONEcampaign/ukraine_oda_tracker/"
    "main/output/hcr_data.csv"
)


def total_refugees() -> dict:
    from scripts.oda.ukraine_oda_tracker.unhcr import (
        read_refugee_data,
        read_refugee_date,
    )

    return {
        "total_refugees_total": read_refugee_data(),
        "total_refugees_date": read_refugee_date(),
    }


def hcr_totals() -> dict:
    countries = [
        "Poland",
        "Romania",
        "Slovakia",
        "Germany",
        "Czech Republic",
        "Republic of Moldova",
    ]
    return (
        pd.read_csv(HCR_TOTALS)
        .assign(date=lambda d: pd.to_datetime(d["Data Date"], format="%m-%Y"))
        .sort_values(["Country", "date"])
        .drop_duplicates(subset=["Country"], keep="last")
        .filter(
            ["Country", "Individual refugees from Ukraine recorded across Europe"],
            axis=1,
        )
        .rename(
            columns={
                "Individual refugees from Ukraine recorded across Europe": "latest_refugees"
            }
        )
        .assign(
            latest_refugees=lambda d: format_number(
                d.latest_refugees, as_units=True, decimals=0
            )
        )
        .loc[lambda d: d.Country.isin(countries)]
        .set_index("Country")
        .to_dict()
    )


def _clean_cost_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.pipe(add_short_names_column, id_column="iso_code", id_type="ISO3").drop(
        columns="iso_code", axis=1
    )


def _add_dac_total(df: pd.DataFrame) -> pd.DataFrame:
    dac_total = (
        pd.read_csv(TOTAL_IDRC)
        .astype({"year": "Int16", "DAC Countries, Total": "float"})
        .filter(["year", "DAC Countries, Total"])
        .rename(columns={"DAC Countries, Total": "value"})
        .assign(name_short="DAC Countries", value=lambda d: d.value * 1e3)
        .loc[lambda d: d.year > 2021]
        .pivot(index="name_short", columns="year", values="value")
        .rename(columns={2022: "cost22", 2023: "cost23", 2024: "cost24"})
        .reset_index(drop=False)
    )

    oda_total = (
        df.assign(name_short="DAC Countries")
        .groupby("name_short", as_index=False)
        .agg({"total_refugees": "sum", "oda": "sum", "year": "max"})
        .filter(["name_short", "total_refugees", "oda", "year"])
    )

    dac_total = dac_total.merge(oda_total, on="name_short", how="left")

    return pd.concat([df, dac_total], ignore_index=True).drop(
        columns=["indicator", "currency", "prices"]
    )


def _add_oda_shares(df: pd.DataFrame) -> pd.DataFrame:
    dac = df.loc[lambda d: d.name_short == "DAC Countries"]
    df = df.loc[lambda d: d.name_short != "DAC Countries"]

    dac = dac.assign(
        total_refugees=lambda d: format_number(
            d.total_refugees, as_units=True, decimals=0
        ),
        share22=lambda d: round(100 * (d.cost22 / 1e3) / d.oda, 1),
        share23=lambda d: round(100 * (d.cost23 / 1e3) / d.oda, 1),
        share24=lambda d: round(100 * (d.cost24 / 1e3) / d.oda, 1),
    )

    df = df.assign(
        total_refugees=lambda d: format_number(
            d.total_refugees, as_units=True, decimals=0
        ),
        share22=lambda d: round(100 * (d.cost22 / 1e6) / d.oda, 1),
        share23=lambda d: round(100 * (d.cost23 / 1e6) / d.oda, 1),
        share24=lambda d: round(100 * (d.cost24 / 1e6) / d.oda, 1),
    )

    return pd.concat([df, dac], ignore_index=True)


def _format_cost(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        cost22=lambda d: format_number(d.cost22, as_millions=True, decimals=2),
        cost23=lambda d: format_number(d.cost23, as_millions=True, decimals=2),
        cost24=lambda d: format_number(d.cost24, as_millions=True, decimals=2),
    )


def latest_oda() -> pd.DataFrame:
    return (
        pd.read_csv(ODA_URL)
        .pipe(add_short_names_column, id_column="donor_name", id_type="regex")
        .drop(columns=["donor_name", "donor_code"])
        .loc[lambda d: d.year == d.year.max()]
        .rename(columns={"value": "oda"})
    )


def refugee_data() -> dict:
    df = pd.read_csv(YEARLY_COSTS_URL).pipe(_clean_cost_data)

    oda = latest_oda()

    # merge datasets
    data = df.merge(oda, on="name_short", how="outer").astype(
        {"total_refugees": "Int32"}
    )

    # add DAC
    data = _add_dac_total(data)

    # calculate shares
    data = _add_oda_shares(data)

    # format costs
    data = _format_cost(data)

    # Fill in missing values
    data = data.fillna("").replace({"nan": "", "<NA>": ""})

    # convert to dict
    data = data.set_index("name_short").to_dict()

    # Add total data
    data = data | total_refugees() | hcr_totals()

    return data
