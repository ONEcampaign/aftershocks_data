import json

from scripts.common import WEO_YEAR, update_key_number
from scripts.config import PATHS


def read_dictionary() -> dict:
    path = f"{PATHS.charts}/country_page/overview.json"

    with open(path, "r") as f:
        t = json.load(f)

    return t


def _unpack_inflation(d: dict) -> dict:

    new_data = {}

    source_link = (
        '<a href="https://dataviz.vam.wfp.org/economic_explorer/'
        'macro-economics/inflation" target="_blank" rel="noopener">'
    )

    for country in d:
        new_data[country] = (
            "<li>"
            f"On {d[country]['date']}, inflation "
            f"in {country} was "
            f"{source_link}{d[country]['value']}</a>."
            "</li>"
        )

    return {"inflation": new_data}


def _unpack_growth(d: dict) -> dict:

    new_data = {}

    source_link = (
        '<a href="https://www.imf.org/en/Publications/WEO" '
        'target="_blank" rel="noopener">'
    )

    for country in d:
        new_data[country] = (
            "<li>"
            f"The IMF estimates a GDP growth of"
            f"{source_link}{d[country]['value']}</a> in"
            f"in {d[country]['year']}."
            "</li>"
        )

    return {"growth": new_data}


def _unpack_poverty(d: dict) -> dict:
    source_link = (
        '<a href="http://iresearch.worldbank.org/PovcalNet/'
        'povOnDemand.aspx" target="_blank" rel="noopener">'
    )

    new_data = {}

    for country in d:
        new_data[country] = (
            "<li>"
            f"{d[country]['value']} of the population of {country} "
            f" lives in extreme poverty, according to the latest"
            f"{source_link}World Bank data</a> ({d[country]['Year']})."
            f"</li>"
        )

    return {"poverty": new_data}


def _unpack_food(d: dict) -> dict:
    source_link = '<a href="https://hungermap.wfp.org/" target="_blank" rel="noopener">'

    new_data = {}

    for country in d:
        new_data[country] = (
            "<li>"
            f'On {d[country]["date"]}, an estimated'
            f' {d[country]["value"]} experienced insufficient food'
            f" consumption (according to {source_link}"
            f"WFP’s HungerMap Live</a>)"
            f"</li>"
        )

    return {"insufficient_food": new_data}


def _unpack_debt(d: dict) -> dict:

    new_data = {}

    source_link = (
        '<a href="https://www.worldbank.org/en/programs/debt-statistics/ids/'
        'products" target="_blank" rel="noopener">'
    )

    for country in d:
        new_data[country] = (
            "<li>"
            f"In {WEO_YEAR}, {country} will pay an estimated "
            f"{source_link}US${source_link}{d[country]['debt_service']} million</a>"
            f" in debt service. That is"
            f"about {d[country]['debt_service_share']} of the country's total budget."
        )

    return {"debt": new_data}


def _unpack_vaccinated(d: dict) -> dict:
    source_link = (
        '<a href="https://ourworldindata.org/covid-vaccinations"'
        ' target="_blank" rel="noopener">'
    )

    new_data = {}

    for country in d:
        new_data[country] = (
            "<li>"
            f'As of {d[country]["date"]}, {source_link}{d[country]["value"]}% of '
            f"the population</a>"
            f"of {country} are fully vaccinated against COVID-19"
            "</li>"
        )

    return {"vaccinated": new_data}


def build_summary() -> None:

    data = read_dictionary()

    inflation = _unpack_inflation(data["inflation"])
    growth = _unpack_growth(data["gdp_growth"])
    poverty = _unpack_poverty(data["poverty"])
    food = _unpack_food(data["insufficient_food"])
    debt = _unpack_debt(data["debt_service"])
    vaccinated = _unpack_vaccinated(data["vaccination"])

    countries = [
        country
        for country in (
            list(inflation["inflation"].keys()),
            list(growth["growth"].keys()),
            list(poverty["poverty"].keys()),
            list(food["insufficient_food"].keys()),
            list(debt["debt"].keys()),
            list(vaccinated["vaccinated"].keys()),
        )
    ]

    countries = list(set([item for sublist in countries for item in sublist]))

    summary = {**inflation, **growth, **poverty, **food, **debt, **vaccinated}

    text_summary = {country: {"indicator": ""} for country in countries}
    for indicator, country in summary.items():
        for country_name in country.keys():
            text_summary[country_name][indicator] = country[country_name]

    for country, indicators in text_summary.items():
        for individual_indicator, text in indicators.items():
            text_summary[country]["indicator"] += text

        text_summary[country] = text_summary[country]["indicator"]

    update_key_number(
        path=f"{PATHS.charts}/country_page/overview_summary.json", new_dict=text_summary
    )
