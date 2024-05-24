import pandas as pd
import requests
from bblocks import (
    add_iso_codes_column,
    add_short_names_column,
    filter_african_countries,
)

from scripts import config


def _wb_update_api(db, filt, cols, start):
    # Limit call to 1.1m rows
    limit = "1100000"

    end = f"end_of_period>='{start-1}-12-01T00:00:00.000'"

    # Create url
    url = db + "SELECT " + cols + " WHERE " + end + " LIMIT " + limit

    # Get file
    file = requests.get(url)
    json = file.json()

    # Create file from returned json
    df = pd.DataFrame.from_dict(json)

    df.end_of_period = pd.to_datetime(df.end_of_period)
    df.board_approval_date = pd.to_datetime(df.board_approval_date)

    df = df.rename(
        columns={
            "end_of_period": "period",
            "repaid_to_ida": "repayment",
            "repaid_to_ibrd": "repayment",
        }
    )

    return df


def __clean_types(df):
    df[["country_code", "country"]] = df[["country_code", "country"]].astype("category")
    df[["disbursed_amount", "repayment"]] = df[
        ["disbursed_amount", "repayment"]
    ].astype(float)
    return df


def _wbg_update_data(ibrd=True, ida=True, start=2017) -> None:
    if ida:
        db = "https://finances.worldbank.org/resource/tdwh-3krx.json?$query="
        filt = (
            'credit_status = "Fully Disbursed" '
            'or credit_status = "Disbursing" '
            'or credit_status = "Disbursing%26Repaying" '
            'or credit_status = "Approved" '
            'or credit_status = "Effective" '
        )
        cols = (
            "end_of_period, country_code, country, disbursed_amount, "
            "repaid_to_ida, board_approval_date"
        )

        df = _wb_update_api(db, filt, cols, start)

        df = __clean_types(df)

        df.reset_index(drop=True).to_feather(
            config.PATHS.raw_data + r"/ida_full_historical_data.feather"
        )

    if ibrd:
        db = "https://finances.worldbank.org/resource/zucq-nrc3.json?$query="
        filt = (
            'loan_status = "Fully Disbursed" '
            'or loan_status = "Disbursing" '
            'or loan_status = "Disbursing%26Repaying" '
            'or loan_status = "Approved" '
            'or loan_status = "Effective" '
        )
        cols = (
            "end_of_period, country_code, country,disbursed_amount, "
            "repaid_to_ibrd, board_approval_date"
        )

        df = _wb_update_api(db, filt, cols, start)

        df = __clean_types(df)

        df.reset_index(drop=True).to_feather(
            config.PATHS.raw_data + r"/ibrd_full_historical_data.feather"
        )


def wb_financial_summary(
    start_year=2017, yearly: bool = False, **kwargs
) -> pd.DataFrame:
    """Create a summary of financial support from IDA + IBRD."""

    # Load data
    ida = pd.read_feather(config.PATHS.raw_data + r"/ida_full_historical_data.feather")
    ibrd = pd.read_feather(
        config.PATHS.raw_data + r"/ibrd_full_historical_data.feather"
    )
    df = pd.concat([ida, ibrd], ignore_index=True)

    # clean data
    columns = ["period", "disbursed_amount", "country"]

    df = (
        df.filter(columns, axis=1)
        .loc[lambda d: d.period >= f"{start_year-1}-12-01"]
        .assign(
            disbursed_amount=lambda d: d.disbursed_amount.astype(float),
        )
        .pipe(add_iso_codes_column, id_column="country", id_type="regex")
        .dropna(subset=["iso_code"])
        .loc[lambda d: d.iso_code != "Africa"]
        .groupby(
            ["iso_code", "country", "period"],
            as_index=False,
            observed=True,
            dropna=False,
        )
        .sum()
        .rename(columns={"period": "date"})
    )

    # filter African countries
    df = filter_african_countries(df, id_type="ISO3")

    # Calculate monthly disbursement
    df["value"] = df.groupby(["iso_code", "country"], observed=True, dropna=False)[
        "disbursed_amount"
    ].diff()
    df = df.drop("disbursed_amount", axis=1)

    # keep only data starting on selected year
    df = df.loc[df.date.dt.year >= start_year]

    if yearly:
        df.date = df.date.dt.year
        return df.groupby(["date", "iso_code"], as_index=False).sum()

    return df


def wb_support_chart(download: bool = False) -> None:
    """Create a CSV table for Flourish with Monthly and Cumulative indicators"""

    if download:
        _wbg_update_data()

    # load monthly data
    monthly = wb_financial_summary(start_year=2017)

    # add indicator name
    monthly["indicator"] = "Monthly Disbursements"

    # create cumulative since april
    cumulative = monthly.loc[monthly.date >= "2020-04-01"].copy()
    cumulative.value = cumulative.groupby(
        ["iso_code", "country"], observed=True, dropna=False
    )["value"].cumsum()
    cumulative.indicator = "Cumulative Disbursements Since April 2020"

    # Append datasets and transform to million
    df = pd.concat([monthly, cumulative], ignore_index=True)
    df.value = round(df.value / 1e6, 2)

    # Reshape for Flourish
    df = df.drop(columns=["iso_code"]).pivot(
        index=["date", "indicator"], columns=["country"]
    )
    df.columns = [x[1] for x in df.columns]
    df = df.reset_index(drop=False)

    # Sort
    df = df.sort_values(["indicator", "date"], ascending=(False, True)).reset_index(
        drop=True
    )

    # Export
    df.to_csv(config.PATHS.charts + r"/country_page/c06_wb_support_ts.csv", index=False)


if __name__ == "__main__":
    wb_support_chart(download=True)
