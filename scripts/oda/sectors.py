from pathlib import Path

import pandas as pd
from oda_data import (
    set_data_path,
    sector_imputations,
    add_sectors,
    add_broad_sectors,
    add_names_columns,
    provider_groupings,
)
from oda_data.clean_data.channels import add_channel_names

from scripts.config import PATHS

set_data_path(PATHS.raw_oda)

START_YEAR: int = 2012
END_YEAR: int = 2024

BILATERAL_PROVIDERS: list = list(provider_groupings()["all_bilateral"])


def groupby_purpose(
    data: pd.DataFrame, value_column: str, group_by: str, by_recipient: bool = False
) -> pd.DataFrame:
    """Group the data by purpose codes and recipient, if requested.

    Args:
        data (pd.DataFrame): The data to group.
        value_column (str): The name of the column with the values to sum.
        group_by (str): The level of aggregation for the data. Options are "purpose", "sector",
        or "broad_sector".
        by_recipient (bool): Whether to group the data by recipient. The default is False.

    Returns:
        pd.DataFrame: The grouped data.
    """
    # Drop the existing purpose names, unless the data is requested at that level
    if group_by != "purpose" and "purpose_name" in data.columns:
        data = data.drop(columns="purpose_name")

    # Group the data by the requested level of aggregation, if "sector" or "broad_sector"
    # are requested.
    if group_by == "sector":
        data = add_sectors(data)
    elif group_by == "broad_sector":
        data = add_broad_sectors(data)
    elif group_by != "purpose":
        raise ValueError(
            "Invalid value for group_by. Must be 'purpose', 'sector', or 'broad_sector'"
        )

    # For that we need to exclude the value columns and, if needed, the recipient
    exclude = (
        ["share", "value"]
        if by_recipient
        else ["share", "value", "recipient_code", "recipient_name"]
    )
    # Get the columns to group by
    grouper = [c for c in data.columns if c not in exclude]

    # Group the data
    data = (
        data.groupby(grouper, observed=True, dropna=False)[value_column]
        .sum()
        .reset_index()
    )

    return data


def add_names(data: pd.DataFrame, by_recipient: bool) -> pd.DataFrame:
    if "channel_code" in data.columns:
        data = data.pipe(add_channel_names)

    if "donor_code" in data.columns:
        data = data.pipe(
            add_names_columns,
            ["donor_code"],
        )
        data.loc[lambda d: d.donor_code == 765, "donor_name"] = "Timor-Leste"

    if by_recipient and "recipient_code" in data.columns:
        data = data.pipe(
            add_names_columns,
            ["recipient_code"],
        )

    return data


def get_multilateral_spending_shares(
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    group_by: str = "purpose",
    by_recipient: bool = False,
) -> pd.DataFrame:
    """Get sector spending shares for multilateral agencies.

    The data can be grouped by purpose codes (very detailed), by sector (less detailed), or
    by broad sector (least detailed). The default is to group by purpose codes.

    Args:
        start_year (int): The start year for the data.
        end_year (int): The end year for the data.
        group_by (str): The level of aggregation for the data. Options are "purpose", "sector",
        or "broad_sector". The default is "purpose".
        by_recipient (bool): Whether to show the data by recipient country. The default is False,
        which means the data will be shown for all recipients, total.

    Returns:
        pd.DataFrame: A DataFrame with the spending shares for each sector.

    """

    shares = (
        sector_imputations.multilateral_spending_shares_by_channel_and_purpose_smoothed(
            years=range(start_year - 2, end_year + 1), oda_only=False
        )
    )

    shares = groupby_purpose(
        shares, value_column="share", group_by=group_by, by_recipient=by_recipient
    ).dropna(subset=["share", "channel_code"], how="any")

    shares = add_names(shares, by_recipient)

    return shares


def get_imputed_multilateral_disbursements_by_sector(
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    currency: str = "USD",
    base_year: int | None = None,
    group_by: str = "purpose",
    by_recipient: bool = False,
) -> pd.DataFrame:
    """Get the imputed multilateral disbursements by sector.

    The data can be grouped by purpose codes (very detailed), by sector (less detailed), or
    by broad sector (least detailed). The default is to group by purpose codes.

    Args:
        start_year (int): The start year for the data.
        end_year (int): The end year for the data.
        currency (str): The currency for the data. The default is "USD".
        prices (str): The price type for the data. The default is "current".
        base_year (int): The base year for the prices. The default is None.
        group_by (str): The level of aggregation for the data. Options are "purpose", "sector",
        or "broad_sector". The default is "purpose".
        by_recipient (bool): Whether to show the data by recipient country. The default is False,
        which means the data will be shown for all recipients, total.

    Returns:
        pd.DataFrame: A DataFrame with the imputed multilateral disbursements by sector.

    """

    # Get the spending data
    spending = sector_imputations.imputed_multilateral_by_purpose(
        providers=BILATERAL_PROVIDERS,
        years=range(start_year - 2, end_year + 1),
        currency=currency,
        base_year=base_year,
    )

    # Group the data by the requested level of aggregation, if "sector" or "broad_sector"
    # are requested.
    spending = groupby_purpose(
        spending, value_column="value", group_by=group_by, by_recipient=by_recipient
    )

    spending = add_names(spending, by_recipient)

    return spending


def get_bilateral_disbursements_by_sector(
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    currency: str = "USD",
    base_year: int | None = None,
    group_by: str = "purpose",
    by_recipient: bool = False,
) -> pd.DataFrame:
    """Get the bilateral disbursements by sector.

    The data can be grouped by purpose codes (very detailed), by sector (less detailed), or
    by broad sector (least detailed). The default is to group by purpose codes.

    Args:
        start_year (int): The start year for the data.
        end_year (int): The end year for the data.
        currency (str): The currency for the data. The default is "USD".
        prices (str): The price type for the data. The default is "current".
        base_year (int): The base year for the prices. The default is None.
        group_by (str): The level of aggregation for the data. Options are "purpose", "sector",
        or "broad_sector". The default is "purpose".
        by_recipient (bool): Whether to show the data by recipient country. The default is False,
        which means the data will be shown for all recipients, total.

    Returns:
        pd.DataFrame: A DataFrame with the bilateral disbursements by sector.
    """

    # Get the spending data
    spending = sector_imputations.spending_by_purpose(
        years=range(start_year - 2, end_year + 1),
        providers=BILATERAL_PROVIDERS,
        currency=currency,
        base_year=base_year,
    )

    # Group the data by the requested level of aggregation, if "sector" or "broad_sector"
    # are requested.
    spending = groupby_purpose(
        spending, value_column="value", group_by=group_by, by_recipient=by_recipient
    )

    spending = add_names(spending, by_recipient)

    return spending


def pipeline(
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
    currency: str = "USD",
    by_recipient: bool = False,
    base_year: int | None = None,
    include_bilateral: bool = False,
) -> pd.DataFrame:
    if include_bilateral:
        bilateral = get_bilateral_disbursements_by_sector(
            start_year=start_year,
            end_year=end_year,
            currency=currency,
            by_recipient=by_recipient,
            base_year=base_year,
        ).assign(indicator="bilateral_flow_disbursement_gross")
    else:
        bilateral = pd.DataFrame()

    data = get_imputed_multilateral_disbursements_by_sector(
        start_year=start_year,
        end_year=end_year,
        currency=currency,
        by_recipient=by_recipient,
        base_year=base_year,
    ).assign(indicator="imputed_multi_flow_disbursement_gross")

    if include_bilateral:
        data = pd.concat([bilateral, data], ignore_index=True)

    return (
        data.pipe(add_names_columns, ["purpose_code"])
        .groupby(
            [
                "year",
                "donor_code",
                "donor_name",
                "recipient_code",
                "recipient_name",
                "purpose_code",
                "purpose_name",
                "indicator",
            ],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
    )


if __name__ == "__main__":
    df = pipeline(base_year=2024, include_bilateral=True, by_recipient=True)
    df.to_csv(PATHS.raw_oda + r"/sectors_view.csv", index=False)
