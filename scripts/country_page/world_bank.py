import pandas as pd
import requests
from bblocks import (
    add_iso_codes_column,
    add_short_names_column,
    filter_african_countries,
)

from scripts import config
from urllib.parse import urlencode, quote


def api_query(
    base_url: str,
    dataset_id: str,
    resource_id: str,
    select_str: str,
    filter_str: str,
    data_type: str = "json",
    max_records: int = 1000,
):
    skip = 0  # Initialize skip for pagination
    all_data = []  # Initialize an empty list to store all data
    while True:
        # Construct the full API URL with parameters
        url = (
            f"{base_url}?datasetId={dataset_id}&resourceId={resource_id}"
            f"&select={select_str}&filter={filter_str}&type={data_type}&skip={skip}&top={max_records}"
        )

        # Make the API request
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        result = response.json()

        # Add the retrieved data to all_data
        data_batch = result.get("data", [])
        all_data.extend(data_batch)

        # Break if no more records are found
        if len(data_batch) < max_records:
            break

        # Increment skip by max_records to fetch the next batch
        skip += max_records

    # Convert to DataFrame for easy manipulation

    df = pd.DataFrame(all_data)
    return df


def fetch_ida_records(
    base_url: str,
    dataset_id: str,
    resource_id: str,
    select_fields: list,
    end_period_start: str,
    status_in: tuple,
    max_records: int = 1000,
) -> pd.DataFrame:
    """
    Fetches all records from the API, bypassing the 1000 record limit per call.

    Args:
        base_url (str): The base URL of the API.
        dataset_id (str): The dataset ID for the API query.
        resource_id (str): The resource ID for the API query.
        select_fields (list): A list of fields to select.
        max_records (int): Maximum number of records per call (default is 1000).

    Returns:
        pd.DataFrame: A DataFrame containing all records from the API.
    """

    # Encode select fields and filters into URL format
    select_str = "%2C".join([quote(field) for field in select_fields])
    filter_str = f"end_of_period>='{end_period_start}'"

    return api_query(
        base_url=base_url,
        dataset_id=dataset_id,
        resource_id=resource_id,
        select_str=select_str,
        filter_str=filter_str,
        max_records=max_records,
    )


def fetch_ibrd_records(
    base_url: str,
    dataset_id: str,
    resource_id: str,
    select_fields: list,
    end_period_start: str,
    status_in: tuple,
    max_records: int = 1000,
) -> pd.DataFrame:
    """
    Fetches all records from the API, bypassing the 1000 record limit per call.

    Args:
        base_url (str): The base URL of the API.
        dataset_id (str): The dataset ID for the API query.
        resource_id (str): The resource ID for the API query.
        select_fields (list): A list of fields to select.
        max_records (int): Maximum number of records per call (default is 1000).

    Returns:
        pd.DataFrame: A DataFrame containing all records from the API.
    """

    # Encode select fields and filters into URL format
    select_str = "%2C".join([quote(field) for field in select_fields])
    filter_str = f"end_of_period>='{end_period_start}'"

    return api_query(
        base_url=base_url,
        dataset_id=dataset_id,
        resource_id=resource_id,
        select_str=select_str,
        filter_str=filter_str,
        max_records=max_records,
    )


def clean_wb_response(df: pd.DataFrame) -> pd.DataFrame:
    df["end_of_period"] = pd.to_datetime(df["end_of_period"], format="%d-%b-%Y")
    df["board_approval_date"] = pd.to_datetime(
        df["board_approval_date"], format="%d-%b-%Y"
    )

    df = df.rename(
        columns={
            "end_of_period": "period",
            "repaid_to_ida_us_": "repayment",
            "repaid_to_ibrd": "repayment",
            "disbursed_amount_us_": "disbursed_amount",
        }
    )

    return df


def _wbg_update_data(ibrd=True, ida=True, start=2017) -> None:
    base_url = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"

    credit_status_in = (
        "Fully Disbursed",
        "Disbursing",
        "Approved",
        "Effective",
        "Disbursing&Repaying",
    )
    end_period_start = "01-Jan-2016"
    if ida:
        dataset_id = "DS00976"
        resource_id = "RS00906"

        select_fields = [
            "end_of_period",
            "country_code",
            "country",
            "disbursed_amount_us_",
            "repaid_to_ida_us_",
            "board_approval_date",
            "credit_status",
        ]

        df = fetch_ida_records(
            base_url=base_url,
            dataset_id=dataset_id,
            resource_id=resource_id,
            select_fields=select_fields,
            end_period_start=end_period_start,
            status_in=credit_status_in,
            max_records=100_000,
        )

        df = clean_wb_response(df)

        df.reset_index(drop=True).to_feather(
            config.PATHS.raw_data + r"/ida_full_historical_data.feather"
        )

    if ibrd:
        dataset_id = "DS00975"
        resource_id = "RS00905"

        select_fields = [
            "end_of_period",
            "country_code",
            "country",
            "disbursed_amount",
            "repaid_to_ibrd",
            "board_approval_date",
            "loan_status",
        ]

        df = fetch_ibrd_records(
            base_url=base_url,
            dataset_id=dataset_id,
            resource_id=resource_id,
            select_fields=select_fields,
            end_period_start=end_period_start,
            status_in=credit_status_in,
            max_records=100_000,
        )

        df = clean_wb_response(df)

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
        .loc[lambda d: d.period >= f"{start_year - 1}-12-01"]
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
