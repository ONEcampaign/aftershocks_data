import json
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import requests
from country_converter import CountryConverter
from dateutil.relativedelta import relativedelta

convert = CountryConverter()

BASE_URL: str = "https://api.ipcinfo.org/"
WEB_URL: str = "https://fsr2av3qi2.execute-api.us-east-1.amazonaws.com/ch/"

IPC_VALIDITY = -3
CH_VALIDITY = -5


def _build_country_df(data: dict, variables: list):
    """Take dictionary and build dataframe"""
    return (
        pd.DataFrame.from_dict(data, orient="index", columns=["value"])
        .loc[variables]
        .reset_index()
        .rename(columns={"index": "indicator"})
        .assign(country=lambda d: d.loc[d.indicator == "country", "value"][0])
        .loc[lambda d: d.indicator != "country"]
        .reset_index(drop=True)
    )


def _build_table(data: list):
    """Build a table on IPC levels for all available countries"""
    # Empty dataframe to hold data
    df = pd.DataFrame()

    for r, country in enumerate(data):
        data_: dict = {
            "iso2": country["country"],
            "from_date": country["from"],
            "to_date": country["to"],
            "year": country["year"],
            "source": "IPC" if "Acute" in country["title"] else "CH",
            "phase_1": country["phases"][0]["population"],
            "phase_2": country["phases"][1]["population"],
            "phase_3": country["phases"][2]["population"],
            "phase_4": country["phases"][3]["population"],
            "phase_5": country["phases"][4]["population"],
            "condition": country["condition"],
        }
        df = pd.concat([df, pd.DataFrame(data_, index=[r])], ignore_index=False)

    df = df.assign(
        country_name=convert.pandas_convert(df.iso2, to="name_short", not_found=None),
        iso_code=convert.pandas_convert(df.iso2, to="ISO3", not_found=None),
        from_date=pd.to_datetime(df.from_date, format="%b %Y"),
        to_date=pd.to_datetime(df.to_date, format="%b %Y"),
    )
    return df


@dataclass
class IPC:
    api_key: str = None
    data: pd.DataFrame = None

    def __post_init__(self):
        if self.api_key is None:
            # self.api_key = os.environ["IPC_WEB_API"]
            raise ValueError("IPC API key not provided")

    def _get_request_url(
        self, call_type: str = "population", format: str = "csv", **parameters
    ) -> str:
        """Get the WHO_API_URL for the API request"""

        # Create a parameters string
        params_str = "&".join(f"{k}={v}" for k, v in parameters.items())

        return f"{BASE_URL}{call_type}?format={format}&{params_str}&key={self.api_key}"

    def _get_web_url(self) -> str:
        return f"{WEB_URL}country?key={self.api_key}"

    def get_website_table(self) -> list:

        url = self._get_web_url()
        return requests.get(url).json()

    def get_ipc_ch_data(
        self, latest: bool = True, only_valid: bool = False
    ) -> pd.DataFrame:

        raw_table = self.get_website_table()
        df = _build_table(data=raw_table)
        current_date = datetime(datetime.today().year, datetime.today().month, 1)

        if latest:
            df = (
                df.sort_values(["iso_code", "year", "to_date"])
                .drop_duplicates(["iso_code"], keep="last")
                .reset_index(drop=True)
            )

        if only_valid:
            df = (
                df.assign(
                    validity=lambda d: d.apply(
                        lambda r: current_date
                        + relativedelta(
                            months=CH_VALIDITY if r.source == "CH" else IPC_VALIDITY
                        ),
                        axis=1,
                    )
                )
                .loc[lambda d: d.to_date >= d.validity]
                .drop(["validity"], axis=1)
                .reset_index(drop=True)
            )

        return df.assign(
            phase_3plus=lambda d: d.phase_3 + d.phase_4 + d.phase_5
        ).filter(
            [
                "iso_code",
                "country_name",
                "phase_1",
                "phase_2",
                "phase_3",
                "phase_4",
                "phase_5",
                "phase_3plus",
                "from_date",
                "to_date",
                "source",
            ],
            axis=1,
        )

    def get_population(
        self, start_year: int = 2022, end_year: int = 2022, countries: list = None
    ) -> json:
        """Get IPC classification population data"""

        raw_data: list = []

        if countries is not None:
            for country in countries:
                url = self._get_request_url(
                    call_type="population",
                    format="json",
                    start=start_year,
                    end=end_year,
                    country=country,
                )
                try:
                    raw_data.append(*requests.get(url).json())
                except json.decoder.JSONDecodeError:
                    print(f"Data for {country} is not available")

        else:
            url = self._get_request_url(
                call_type="population", format="json", start=start_year, end=end_year
            )
            _ = requests.get(url).json()
            for c in _:
                raw_data.append(c)

        # Empty dataframe to hold results
        df = pd.DataFrame()

        # Analysis variables
        variables: list = ["country", "projected_period_dates", "population"] + [
            f"phase{n}_population_projected" for n in range(1, 6)
        ]

        for _ in raw_data:
            _ = _build_country_df(data=_, variables=variables)
            df = pd.concat([_, df], ignore_index=True)

        return df
