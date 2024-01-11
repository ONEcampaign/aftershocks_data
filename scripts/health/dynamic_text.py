""" """
import json

import country_converter as coco
import pandas as pd
from bblocks import (
    WorldBankData,
    set_bblocks_data_path,
    format_number,
    add_income_level_column,
)

from scripts.common import update_key_number
from scripts.config import PATHS
from scripts.health.common import get_malaria_data
from scripts.owid_covid import tools as owid_tools

set_bblocks_data_path(PATHS.bblocks_data)


def _format_wb_df(df: pd.DataFrame, indicator_name: str):
    df = (
        df.dropna(subset=["value"])
        .sort_values("date")
        .groupby("iso_code", as_index=False)
        .last()
        .loc[:, ["iso_code", "value"]]
        .assign(
            continent=lambda d: coco.convert(d.iso_code, to="continent", not_found=None)
        )
        .loc[lambda d: d.continent == "Africa", :]
        .assign(indicator=indicator_name)
        .reset_index(drop=True)
    )
    return df


def spending_dynamic() -> dict:
    wb = WorldBankData()
    wb.load_data(["SH.XPD.CHEX.PC.CD", "SH.XPD.CHEX.GD.ZS"])

    # TODO: Explain the cut-offs
    pc = (
        wb.get_data("SH.XPD.CHEX.PC.CD")
        .pipe(_format_wb_df, "pc")
        .loc[lambda d: d.value >= 86, :]
    )

    # TODO: Explain the cut-offs
    gdp = (
        wb.get_data("SH.XPD.CHEX.GD.ZS")
        .pipe(_format_wb_df, "gdp")
        .loc[lambda d: d.value >= 5, :]
    )

    dff = pd.concat([pc, gdp])
    dff = dff.pivot(index="iso_code", columns="indicator", values="value")
    dff = dff.dropna()

    return {"count_86_target": len(pc), "count_86_5_target": len(dff)}


def vaccination_dynamic() -> dict:
    """Create dynamic text for vaccination"""

    df = (
        owid_tools.read_owid_data()
        .pipe(owid_tools.get_indicators_ts, "people_fully_vaccinated_per_hundred")
        .loc[lambda d: d["iso_code"].isin(["OWID_WRL", "OWID_AFR"])]
        .sort_values("date")
        .groupby("iso_code", as_index=False)
        .last()
        .round(1)
    )
    afr_value = df.loc[df.iso_code == "OWID_AFR", "value"].item()
    wrl_value = df.loc[df.iso_code == "OWID_WRL", "value"].item()
    vaccination_date = df.loc[df.iso_code == "OWID_WRL", "date"].item()
    vaccination_date = vaccination_date.strftime("%d %B %Y")

    return {
        "vaccination_full_global": wrl_value,
        "vaccination_rate_africa": afr_value,
        "vaccination_date": vaccination_date,
    }


def _get_indicator_item(
    data: pd.DataFrame, iso_code: str, indicator: str, as_type: str, decimals: int
):
    return format_number(
        data.loc[data.iso_code == iso_code, indicator],
        as_units=True if as_type == "units" else False,
        as_millions=True if as_type == "millions" else False,
        as_billions=True if as_type == "billions" else False,
        decimals=decimals,
    ).item()


def doses_dynamic() -> None:
    df = owid_tools.read_owid_data().filter(
        [
            "date",
            "continent",
            "iso_code",
            "total_vaccinations",
            "total_vaccinations_per_hundred",
            "people_fully_vaccinated_per_hundred",
            "population",
        ]
    )

    df = (
        df.sort_values(by=["date"])
        .dropna(subset=["total_vaccinations"])
        .drop_duplicates(["iso_code"], keep="last")
        .pipe(add_income_level_column, id_column="iso_code", id_type="ISO3")
    )

    latest_date = df["date"].max()

    world_doses = _get_indicator_item(
        data=df,
        iso_code="OWID_WRL",
        indicator="total_vaccinations",
        as_type="billions",
        decimals=1,
    )

    world_share_pop = _get_indicator_item(
        data=df,
        iso_code="OWID_WRL",
        indicator="people_fully_vaccinated_per_hundred",
        as_type="units",
        decimals=1,
    )

    africa_doses = _get_indicator_item(
        data=df,
        iso_code="OWID_AFR",
        indicator="total_vaccinations",
        as_type="millions",
        decimals=0,
    )

    africa_share_pop = _get_indicator_item(
        data=df,
        iso_code="OWID_AFR",
        indicator="people_fully_vaccinated_per_hundred",
        as_type="units",
        decimals=1,
    )

    lic_doses = _get_indicator_item(
        data=df,
        iso_code="OWID_LIC",
        indicator="total_vaccinations",
        as_type="millions",
        decimals=0,
    )

    lic_share_pop = _get_indicator_item(
        data=df,
        iso_code="OWID_LIC",
        indicator="people_fully_vaccinated_per_hundred",
        as_type="units",
        decimals=1,
    )

    lmic_doses = _get_indicator_item(
        data=df,
        iso_code="OWID_LMC",
        indicator="total_vaccinations",
        as_type="billions",
        decimals=1,
    )

    lmic_share_pop = _get_indicator_item(
        data=df,
        iso_code="OWID_LMC",
        indicator="people_fully_vaccinated_per_hundred",
        as_type="units",
        decimals=1,
    )

    umic_doses = _get_indicator_item(
        data=df,
        iso_code="OWID_UMC",
        indicator="total_vaccinations",
        as_type="billions",
        decimals=1,
    )

    umic_share_pop = _get_indicator_item(
        data=df,
        iso_code="OWID_UMC",
        indicator="people_fully_vaccinated_per_hundred",
        as_type="units",
        decimals=1,
    )

    hic_doses = _get_indicator_item(
        data=df,
        iso_code="OWID_HIC",
        indicator="total_vaccinations",
        as_type="billions",
        decimals=1,
    )

    hic_share_pop = _get_indicator_item(
        data=df,
        iso_code="OWID_HIC",
        indicator="people_fully_vaccinated_per_hundred",
        as_type="units",
        decimals=1,
    )

    numbers = {
        "latest_date": latest_date.strftime("%d %B %Y"),
        "world_total_doses": world_doses,
        "lic_total_doses": lic_doses,
        "lmic_total_doses": lmic_doses,
        "umic_total_doses": umic_doses,
        "hic_total_doses": hic_doses,
        "africa_total_doses": africa_doses,
        "africa_share_fully_vaccinated": africa_share_pop,
        "world_share_fully_vaccinated": world_share_pop,
        "lic_share_fully_vaccinated": lic_share_pop,
        "lmic_share_fully_vaccinated": lmic_share_pop,
        "umic_share_fully_vaccinated": umic_share_pop,
        "hic_share_fully_vaccinated": hic_share_pop,
    }

    update_key_number(f"{PATHS.charts}/health/key_numbers_doses.json", numbers)


def malaria_dynamic() -> dict:
    """Create dynamic text for malaria"""

    malaria = get_malaria_data()
    malaria[
        "malaria_africa_total"
    ] = f"{malaria['malaria_africa_total'] / 1000:.0f} thousand"
    malaria[
        "malaria_world_total"
    ] = f"{malaria['malaria_world_total'] / 1000:.0f} thousand"
    malaria[
        "malaria_rest_of_world_total"
    ] = f"{malaria['malaria_rest_of_world_total'] / 1000:.0f} thousand"

    return malaria


def update_dynamic_text() -> None:
    """ "Update dynamic text for health topic page"""
    dynamic_text = {}

    dynamic_text.update(vaccination_dynamic())
    dynamic_text.update(malaria_dynamic())
    dynamic_text.update(spending_dynamic())

    with open(f"{PATHS.charts}/health/key_numbers.json", "w") as file:
        json.dump(dynamic_text, file, indent=4)
