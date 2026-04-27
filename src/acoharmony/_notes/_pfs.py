# © 2025 HarmonyCares
# All rights reserved.

"""
Medicare PFS (Physician Fee Schedule) payment-rate analytics.

Backs ``notebooks/pfs_2026_rates.py`` (and supporting work for
``notebooks/pfs.py``): loads PUF-driven GPCI / PPRVU / ZIP-locality
data for a given year, calculates per-office / per-HCPCS Medicare
payment rates, and runs the year-over-year + scenario comparison
matrix used by the 2026 final-rule analysis.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ._base import PluginRegistry

CONVERSION_FACTOR_2025 = 32.3465
CONVERSION_FACTOR_2026_NON_APM = 33.4009
CONVERSION_FACTOR_2026_APM = 33.567


HARMONYCARES_OFFICES = (
    ("48108", "ANN ARBOR"),
    ("44685", "AKRON"),
    ("30309", "ATLANTA"),
    ("78757", "AUSTIN"),
    ("43235", "COLUMBUS"),
    ("78411", "CORPUS CHRISTI"),
    ("75038", "DALLAS"),
    ("45439", "DAYTON"),
    ("45245", "EAST CINCINNATI"),
    ("48108", "FLINT"),
    ("33322", "FT.LAUDERDALE"),
    ("49525", "GRAND RAPIDS"),
    ("77024", "HOUSTON"),
    ("46268", "INDIANAPOLIS"),
    ("32216", "JACKSONVILLE"),
    ("49002", "KALAMAZOO"),
    ("40509", "LEXINGTON"),
    ("48864", "LANSING"),
    ("53718", "MADISON"),
    ("48059", "MARYSVILLE"),
    ("44130", "WEST CLEVELAND"),
    ("53227", "MILWAUKEE"),
    ("70422", "MONTCLAIR"),
    ("23462", "NORFOLK"),
    ("23226", "NORTH VIRGINIA"),
    ("11375", "NEW YORK"),
    ("32751", "ORLANDO"),
    ("19312", "PHILADELPHIA"),
    ("23226", "RICHMOND"),
    ("23226", "ROANOKE"),
    ("48706", "SAGINAW"),
    ("78229", "SAN ANTONIO"),
    ("46628", "SOUTH BEND"),
    ("98057", "SEATTLE"),
    ("60527", "CHICAGO"),
    ("63144", "ST. LOUIS"),
    ("43537", "TOLEDO"),
    ("33637", "TAMPA"),
    ("48084", "TROY"),
    ("23185", "WILLIAMSBURG"),
    ("44512", "YOUNGSTOWN"),
)


class PfsPlugins(PluginRegistry):
    """PFS payment-rate calculator."""

    # ---- offices ------------------------------------------------------

    def offices(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "office_zip": [r[0] for r in HARMONYCARES_OFFICES],
                "office_name": [r[1] for r in HARMONYCARES_OFFICES],
            }
        )

    # ---- PUF loaders --------------------------------------------------

    def load_zip_to_locality_2026(
        self,
        pufs_dir: Path,
        offices: pl.DataFrame,
    ) -> pl.DataFrame:
        path = next(
            iter(
                Path(pufs_dir).glob(
                    "zipcarrier_2026_final_locality_ZIP5_JAN2026.xlsx"
                )
            )
        )
        raw = pl.read_excel(path, sheet_id=1, read_options={"skip_rows": 1})
        zips = set(offices["office_zip"].to_list())
        return (
            raw.rename(
                {
                    "ZIP CODE": "geo_zip_5",
                    "STATE": "geo_state_cd",
                    "CARRIER": "carrier",
                    "LOCALITY": "locality",
                }
            )
            .select("geo_zip_5", "geo_state_cd", "carrier", "locality")
            .filter(pl.col("geo_zip_5").is_in(zips))
            .unique()
        )

    def load_gpci_2026(self, pufs_dir: Path) -> pl.DataFrame:
        path = next(
            iter(Path(pufs_dir).glob("pfs_2026_final_addenda_Addendum E*.xlsx"))
        )
        raw_with_header = pl.read_excel(
            path, sheet_id=1, read_options={"skip_rows": 1}
        )
        header_row = raw_with_header.row(0)
        body = raw_with_header.slice(4)
        body.columns = list(header_row)
        return (
            body.rename(
                {
                    "Medicare Administrative Contractor (MAC)": "carrier",
                    "State": "state_name",
                    "Locality Number": "locality",
                    "Locality Name": "locality_name",
                    "2026 PW GPCI (without 1.0 Floor)": "pw_gpci",
                    "2026 PE GPCI": "pe_gpci_without_floor",
                    "2026 MP GPCI": "mp_gpci",
                }
            )
            .with_columns(
                pl.col("pw_gpci").cast(pl.Float64),
                pl.col("pe_gpci_without_floor").cast(pl.Float64),
                pl.col("mp_gpci").cast(pl.Float64),
            )
            .with_columns(
                pl.max_horizontal("pe_gpci_without_floor", pl.lit(1.0)).alias(
                    "pe_gpci_with_floor"
                )
            )
        )

    def load_pprvu_2026(
        self,
        pufs_dir: Path,
        visit_codes: list[str],
    ) -> pl.DataFrame:
        path = next(
            iter(Path(pufs_dir).glob("pfs_2026_final_addenda_Addendum B*.xlsx"))
        )
        raw_with_header = pl.read_excel(path, sheet_id=1)
        header_row = raw_with_header.row(0)
        clean_headers = [
            (
                str(val).replace("\r\n", " ").replace("\n", " ").replace("  ", " ").strip()
                if val
                else f"col_{i}"
            )
            for i, val in enumerate(header_row)
        ]
        body = raw_with_header.slice(2)
        body.columns = clean_headers
        return (
            body.rename(
                {
                    "CPT1/ HCPCS": "hcpcs",
                    "DESCRIPTION": "description",
                    "Work RVUs2": "work_rvu",
                    "Non- Facility PE RVUs2": "nf_pe_rvu",
                    "Facility PE RVUs2": "f_pe_rvu",
                    "Mal- Practice RVUs2": "mp_rvu",
                }
            )
            .with_columns(
                pl.col("work_rvu").cast(pl.Float64, strict=False),
                pl.col("nf_pe_rvu").cast(pl.Float64, strict=False),
                pl.col("f_pe_rvu").cast(pl.Float64, strict=False),
                pl.col("mp_rvu").cast(pl.Float64, strict=False),
            )
            .filter(pl.col("hcpcs").is_in(visit_codes))
        )

    def load_gpci_2025(self, pufs_dir: Path) -> pl.DataFrame:
        path = next(
            iter(Path(pufs_dir).glob("rvu_2025_q4_rvu_quarterly_GPCI2025.xlsx"))
        )
        raw = pl.read_excel(path, sheet_id=1, read_options={"skip_rows": 1})
        raw.columns = [
            "Medicare Administrative Contractor (MAC)",
            "State",
            "Locality Number",
            "Locality Name",
            "2025 PW GPCI (with 1.0 Floor)",
            "2025 PE GPCI",
            "2025 MP GPCI",
        ]
        return (
            raw.rename(
                {
                    "Medicare Administrative Contractor (MAC)": "carrier",
                    "Locality Number": "locality",
                    "2025 PW GPCI (with 1.0 Floor)": "pw_gpci",
                    "2025 PE GPCI": "pe_gpci",
                    "2025 MP GPCI": "mp_gpci",
                }
            )
            .select("carrier", "locality", "pw_gpci", "pe_gpci", "mp_gpci")
            .with_columns(
                pl.col("pw_gpci").cast(pl.Float64, strict=False),
                pl.col("pe_gpci").cast(pl.Float64, strict=False),
                pl.col("mp_gpci").cast(pl.Float64, strict=False),
            )
        )

    def load_pprvu_2025(
        self,
        pufs_dir: Path,
        visit_codes: list[str],
    ) -> pl.DataFrame:
        path = next(
            iter(
                Path(pufs_dir).glob("rvu_2025_q4_rvu_quarterly_PPRRVU2025_Oct.xlsx")
            )
        )
        data = pl.read_excel(path, sheet_id=1, read_options={"skip_rows": 9})
        cols = data.columns
        raw = data.select(
            pl.col(cols[0]).alias("hcpcs"),
            pl.col(cols[5]).alias("work_rvu"),
            pl.col(cols[6]).alias("nf_pe_rvu"),
            pl.col(cols[8]).alias("f_pe_rvu"),
            pl.col(cols[10]).alias("mp_rvu"),
        )
        filtered = raw.filter(pl.col("hcpcs").is_in(visit_codes))
        cast_exprs = [
            pl.col(c).cast(pl.Float64, strict=False)
            for c in ("work_rvu", "nf_pe_rvu", "f_pe_rvu", "mp_rvu")
            if c in filtered.columns
        ]
        return filtered.with_columns(cast_exprs)

    # ---- payment-rate calculation -------------------------------------

    def select_gpci_2026(
        self,
        gpci_2026_raw: pl.DataFrame,
        with_floor: bool,
    ) -> pl.DataFrame:
        col = "pe_gpci_with_floor" if with_floor else "pe_gpci_without_floor"
        return gpci_2026_raw.select(
            "carrier",
            "state_name",
            "locality",
            "locality_name",
            "pw_gpci",
            pl.col(col).alias("pe_gpci"),
            "mp_gpci",
        )

    def calculate_rates(
        self,
        offices: pl.DataFrame,
        zip_to_locality: pl.DataFrame,
        gpci: pl.DataFrame,
        pprvu: pl.DataFrame,
        conversion_factor: float,
        pe_floor: float = 0.0,
    ) -> pl.DataFrame:
        """
        Calculate per-(office, HCPCS) Medicare payment rates.

        ``pe_floor`` is the minimum value applied to PE GPCI; defaults
        to 0.0 because the PUF source already has the floor when needed.
        """
        from acoharmony._expressions._pfs_rate_calc import PFSRateCalcExpression

        offices_with_locality = offices.join(
            zip_to_locality, left_on="office_zip", right_on="geo_zip_5", how="left"
        )
        offices_with_gpci = offices_with_locality.join(
            gpci.select("carrier", "locality", "pw_gpci", "pe_gpci", "mp_gpci"),
            on=["carrier", "locality"],
            how="left",
        )
        offices_with_gpci = offices_with_gpci.with_columns(
            PFSRateCalcExpression.validate_gpci("pw_gpci", 1.0).alias("pw_gpci"),
            PFSRateCalcExpression.validate_gpci("pe_gpci", pe_floor).alias("pe_gpci"),
            PFSRateCalcExpression.validate_gpci("mp_gpci", 1.0).alias("mp_gpci"),
        )
        pprvu_columns = ["hcpcs", "work_rvu", "nf_pe_rvu", "mp_rvu"]
        pprvu_rename = {"hcpcs": "hcpcs_code"}
        if "description" in pprvu.columns:
            pprvu_columns.append("description")
            pprvu_rename["description"] = "hcpcs_description"
        rates_base = offices_with_gpci.join(
            pprvu.select(pprvu_columns).rename(pprvu_rename), how="cross"
        )
        payment_calcs = PFSRateCalcExpression.build_payment_calculation(
            work_rvu="work_rvu",
            pe_rvu="nf_pe_rvu",
            mp_rvu="mp_rvu",
            pw_gpci="pw_gpci",
            pe_gpci="pe_gpci",
            mp_gpci="mp_gpci",
            conversion_factor="conversion_factor",
        )
        return rates_base.with_columns(
            pl.lit(conversion_factor).alias("conversion_factor")
        ).with_columns(**payment_calcs)

    def comparison(
        self,
        rates_2026: pl.DataFrame,
        rates_2025: pl.DataFrame,
    ) -> pl.DataFrame:
        """Per-(office, HCPCS) 2026 vs 2025 comparison with $ and % changes."""
        rates_2026_select = rates_2026.select(
            "office_zip",
            "office_name",
            "hcpcs_code",
            "hcpcs_description",
            "payment_rate",
        ).rename({"payment_rate": "payment_2026"})
        rates_2025_select = rates_2025.select(
            "office_zip", "hcpcs_code", "payment_rate"
        ).rename({"payment_rate": "payment_2025"})
        return (
            rates_2026_select.join(
                rates_2025_select, on=["office_zip", "hcpcs_code"], how="left"
            )
            .with_columns(
                (pl.col("payment_2026") - pl.col("payment_2025")).alias("dollar_change"),
                (
                    (pl.col("payment_2026") - pl.col("payment_2025"))
                    / pl.col("payment_2025")
                    * 100
                ).alias("percent_change"),
            )
        )

    def comparison_summary(self, comparison: pl.DataFrame) -> pl.DataFrame:
        """Per-HCPCS averages of 2025 / 2026 / change / pct change."""
        return (
            comparison.filter(pl.col("payment_2025").is_not_null())
            .group_by("hcpcs_code")
            .agg(
                pl.col("hcpcs_description").first(),
                pl.col("payment_2025").mean().alias("avg_payment_2025"),
                pl.col("payment_2026").mean().alias("avg_payment_2026"),
                pl.col("dollar_change").mean().alias("avg_dollar_change"),
                pl.col("percent_change").mean().alias("avg_percent_change"),
            )
            .sort("avg_percent_change", descending=True)
        )

    def all_scenarios(
        self,
        offices: pl.DataFrame,
        zip_to_locality: pl.DataFrame,
        gpci_2026_raw: pl.DataFrame,
        pprvu: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        8-way scenario matrix: APM/Non-APM × Sequestration/none × PE-floor/no-floor.

        Each combo is a separate rate column on the joined output, keyed
        by ``(office_zip, hcpcs_code)``.
        """
        cf_apm = CONVERSION_FACTOR_2026_APM
        cf_nonapm = CONVERSION_FACTOR_2026_NON_APM
        cfs = {
            "apm_no_seq": cf_apm,
            "apm_seq": cf_apm * 0.98,
            "nonapm_no_seq": cf_nonapm,
            "nonapm_seq": cf_nonapm * 0.98,
        }
        gpci_floor = self.select_gpci_2026(gpci_2026_raw, with_floor=True)
        gpci_no_floor = self.select_gpci_2026(gpci_2026_raw, with_floor=False)

        out: pl.DataFrame | None = None
        for cf_name, cf_value in cfs.items():
            for floor_label, gpci in (("floor", gpci_floor), ("no_floor", gpci_no_floor)):
                col = f"2026_{cf_name}_{floor_label}"
                rates = self.calculate_rates(
                    offices, zip_to_locality, gpci, pprvu, cf_value
                ).select(
                    "office_zip",
                    "office_name",
                    "hcpcs_code",
                    "hcpcs_description",
                    "payment_rate",
                ).rename({"payment_rate": col})
                if out is None:
                    out = rates
                else:
                    out = out.join(
                        rates.drop("office_name", "hcpcs_description"),
                        on=["office_zip", "hcpcs_code"],
                        how="left",
                    )
        return out
