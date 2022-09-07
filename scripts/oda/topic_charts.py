from bblocks.cleaning_tools.clean import format_number
from bblocks.cleaning_tools.filter import filter_latest_by
from pydeflate import deflate

from scripts.config import PATHS
from scripts.oda import common


def global_aid_ts() -> None:
    """Create an overview chart which contains the latest total ODA value and
    the change in constant terms."""

    gni = common.read_gni().filter(["year", "donor_code", "value"], axis=1)

    df = (
        common.read_total_oda(official_definition=True)
        .merge(gni, on=["year", "donor_code"], how="left", suffixes=("", "_gni"))
        .pipe(common.append_DAC_total)
        # .pipe(common.add_constant_change_column, base=common.CONSTANT_YEAR)
        .assign(
            #    pct_change=lambda d: "Real change from previous year: " + d["pct_change"],
            oda_gni=lambda d: round(100 * d.value / d.value_gni, 2),
        )
        .assign(
            value=lambda d: deflate(
                d,
                base_year=common.CONSTANT_YEAR,
                date_column="year",
                source="oecd_dac",
                id_column="donor_code",
                id_type="DAC",
                source_col="value",
                target_col="const",
            ).const
        )
        .pipe(common.add_short_names)
        .assign(
            year=lambda d: d.year.dt.year,
        )
        .filter(["name", "year", "value", "oda_gni", "pct_change"], axis=1)
        .sort_values(["name", "year"])
        .rename(columns={"value": "ODA (left-axis)", "oda_gni": "ODA/GNI (right-axis)"})
    )

    # chart version
    df.to_csv(f"{PATHS.charts}/oda_topic/oda_gni_ts.csv", index=False)

    # download version
    source = "OECD DAC Creditor Reporting System (CRS)"
    df.assign(source=source).to_csv(
        f"{PATHS.download}/oda_topic/oda_gni_ts.csv", index=False
    )


if __name__ == "__main__":
    global_aid_ts()
