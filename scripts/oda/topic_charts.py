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


def oda_gni_single_year() -> None:

    gni = common.read_gni().filter(["year", "donor_code", "value"], axis=1)

    df = (
        common.read_total_oda(official_definition=True)
        .merge(gni, on=["year", "donor_code"], how="left", suffixes=("", "_gni"))
        .pipe(common.append_DAC_total, grouper=["year"])
        .assign(
            missing=lambda d: round(d.value_gni * 0.007 - d.value, 1),
            oda_gni=lambda d: round(100 * d.value / d.value_gni, 2),
            year=lambda d: d.year.dt.year,
        )
        .assign(missing=lambda d: d.missing.apply(lambda v: v if v > 0 else 0))
        .assign(
            value=lambda df_: df_.value.apply(
                lambda d: f"{d/1e3:.2f} billion" if d > 1e3 else f"{d:.1f} million"
            ),
            missing=lambda df_: df_.missing.apply(
                lambda d: f"{d/1e3:.2f} billion" if d > 1e3 else f"{d:.1f} million"
            ),
        )
        .loc[lambda d: (d.donor_code != 918) & (d.year >= common.START_YEAR)]
        .pipe(common.add_short_names)
        .filter(["name", "year", "value", "missing", "oda_gni"], axis=1)
        .sort_values(["year", "name"], ascending=[False, True])
        .rename(
            {
                "value": "Total ODA",
                "oda_gni": "ODA/GNI",
                "name": "Donor",
                "year": "Year",
                "missing": "ODA short of 0.7% commitment",
            },
            axis=1,
        )
    )
    df.to_clipboard(index=False)


if __name__ == "__main__":
    global_aid_ts()

    oda_gni_single_year()
