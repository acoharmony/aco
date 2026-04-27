# © 2025 HarmonyCares
# All rights reserved.

"""
Wound-care / skin-substitute claims analytics.

Backs ``notebooks/wound_care.py``: multi-dimensional claim filtering
(claim_type × date_range × cohort × source), summary statistics,
top NPI / HCPCS rollups, plus pattern detection
(high-frequency, high-cost, clustered, same-day duplicates,
identical billing) and NPI cross-dimension comparisons.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

DATE_RANGE_PRESETS = {
    "2026": (date(2026, 1, 1), date(2026, 12, 31)),
    "2025": (date(2025, 1, 1), date(2025, 12, 31)),
    "2024": (date(2024, 1, 1), date(2024, 12, 31)),
    "2023": (date(2023, 1, 1), date(2023, 12, 31)),
    "2023-2025": (date(2023, 1, 1), date(2025, 12, 31)),
    "2024-2026": (date(2024, 1, 1), date(2026, 12, 31)),
}

HDAI_EXCEL_FILENAME = "A2671_D0259_WoundClaim_Ids_20251117.xlsx"


class WoundCarePlugins(PluginRegistry):
    """Wound-care / skin-substitute claims analytics."""

    # ---- date / cohort helpers ---------------------------------------

    def resolve_date_range(
        self, date_range: str | tuple[date, date]
    ) -> tuple[date, date]:
        if isinstance(date_range, tuple):
            return date_range
        if date_range in DATE_RANGE_PRESETS:
            return DATE_RANGE_PRESETS[date_range]
        raise ValueError(f"Invalid date_range: {date_range}")

    def cohort_mbis(
        self, cohort: str, gold_path: Path
    ) -> list[str] | None:
        """Returns MBI list for the cohort, or ``None`` for the all-patients case."""
        if cohort == "all":
            return None
        alignment_df = pl.read_parquet(
            Path(gold_path) / "consolidated_alignment.parquet"
        )
        if cohort == "reach_current":
            latest = alignment_df.select(pl.col("observable_end").max()).item()
            ym_col = f"ym_{latest.strftime('%Y%m')}_reach"
            return (
                alignment_df.filter(pl.col(ym_col) == True)
                .select("current_mbi")
                .unique()["current_mbi"]
                .to_list()
            )
        if cohort == "reach_ever":
            return (
                alignment_df.filter(pl.col("ever_reach") == True)
                .select("current_mbi")
                .unique()["current_mbi"]
                .to_list()
            )
        raise ValueError(f"Invalid cohort: {cohort}")

    # ---- filtered claim loading --------------------------------------

    def filtered_claims(
        self,
        claim_type: str,
        date_range: str | tuple[date, date],
        cohort: str,
        source: str,
        gold_path: Path,
        bronze_path: Path,
        skin_substitute_codes: list[str] | tuple[str, ...] | None = None,
    ) -> pl.DataFrame | None:
        """Load filtered claims for the requested (type, date, cohort, source)."""
        date_start, date_end = self.resolve_date_range(date_range)
        cohort_list = self.cohort_mbis(cohort, gold_path)

        cclf_df: pl.DataFrame | None = None
        hdai_df: pl.DataFrame | None = None

        if source in ("cclf", "matched"):
            file_name = (
                "skin_substitute_claims.parquet"
                if claim_type == "skin_substitute"
                else "wound_care_claims.parquet"
            )
            cclf = pl.scan_parquet(Path(gold_path) / file_name).filter(
                (pl.col("claim_end_date") >= date_start)
                & (pl.col("claim_end_date") <= date_end)
            )
            if cohort_list is not None:
                cclf = cclf.filter(pl.col("member_id").is_in(cohort_list))
            cclf_df = cclf.collect()

        if source in ("hdai", "matched"):
            excel_file = Path(bronze_path) / HDAI_EXCEL_FILENAME
            hdai = pl.read_excel(excel_file, sheet_name="Claim IDs").select(
                pl.col("MBI NUM").cast(pl.Utf8).alias("mbi"),
                pl.col("Claim ID").cast(pl.Utf8).alias("claim_id"),
                pl.col("Claim Through Date").alias("claim_through_date"),
                pl.col("HCPCS Code").cast(pl.Utf8).alias("hcpcs_code"),
                pl.col("Line Payment Amount").cast(pl.Float64).alias("line_payment_amount"),
                pl.col("Rendering Provider NPI").cast(pl.Utf8).alias("rendering_npi"),
                pl.col("Claim Status").cast(pl.Utf8).alias("claim_status"),
            )
            if claim_type == "skin_substitute" and skin_substitute_codes is not None:
                hdai = hdai.filter(
                    pl.col("hcpcs_code").is_in(list(skin_substitute_codes))
                )
            hdai = hdai.filter(
                (pl.col("claim_through_date") >= date_start)
                & (pl.col("claim_through_date") <= date_end)
            )
            if cohort_list is not None:
                hdai = hdai.filter(pl.col("mbi").is_in(cohort_list))
            hdai_df = hdai

        if source == "cclf":
            return cclf_df
        if source == "hdai":
            return hdai_df
        # matched
        if hdai_df is None or cclf_df is None:
            return None
        hdai_prepared = hdai_df.with_columns(
            pl.col("line_payment_amount").round(2).alias("line_payment_amount_rounded"),
            pl.col("mbi")
            .cum_count()
            .over(
                [
                    "mbi",
                    "claim_through_date",
                    "line_payment_amount",
                    "rendering_npi",
                    "hcpcs_code",
                ]
            )
            .alias("hdai_seq"),
        )
        cclf_prepared = cclf_df.with_columns(
            pl.col("paid_amount").cast(pl.Float64).round(2).alias("paid_amount_rounded"),
            pl.col("member_id")
            .cum_count()
            .over(
                [
                    "member_id",
                    "claim_end_date",
                    "paid_amount",
                    "rendering_npi",
                    "hcpcs_code",
                ]
            )
            .alias("cclf_seq"),
        )
        return hdai_prepared.join(
            cclf_prepared,
            left_on=[
                "mbi",
                "claim_through_date",
                "line_payment_amount_rounded",
                "rendering_npi",
                "hcpcs_code",
                "hdai_seq",
            ],
            right_on=[
                "member_id",
                "claim_end_date",
                "paid_amount_rounded",
                "rendering_npi",
                "hcpcs_code",
                "cclf_seq",
            ],
            how="inner",
            suffix="_cclf",
        )

    # ---- column-name helpers ------------------------------------------

    def column_names(self, source: str) -> dict[str, str]:
        if source in ("hdai", "matched"):
            return {
                "patient": "mbi",
                "amount": "line_payment_amount",
                "date": "claim_through_date",
            }
        return {
            "patient": "member_id",
            "amount": "paid_amount",
            "date": "claim_end_date",
        }

    # ---- summaries ----------------------------------------------------

    def claim_summary(
        self,
        claim_type: str,
        date_range: str | tuple[date, date],
        cohort: str,
        source: str,
        gold_path: Path,
        bronze_path: Path,
        skin_substitute_codes: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        claims = self.filtered_claims(
            claim_type,
            date_range,
            cohort,
            source,
            gold_path,
            bronze_path,
            skin_substitute_codes,
        )
        if claims is None or claims.height == 0:
            return {
                "claims_df": None,
                "summary_stats": None,
                "top_npis": None,
                "top_hcpcs": None,
            }
        cols = self.column_names(source)
        amount, patient, dt = cols["amount"], cols["patient"], cols["date"]

        summary_stats = pl.DataFrame(
            {
                "Metric": [
                    "Total Claim Lines",
                    "Total Amount Paid",
                    "Average Amount per Line",
                    "Unique Patients",
                    "Unique HCPCS Codes",
                    "Unique NPIs",
                    "Date Range",
                ],
                "Value": [
                    f"{claims.height:,}",
                    f"${claims.select(pl.col(amount).sum()).item():,.2f}",
                    f"${claims.select(pl.col(amount).mean()).item():,.2f}",
                    f"{claims.select(pl.col(patient).n_unique()).item():,}",
                    f"{claims.select(pl.col('hcpcs_code').n_unique()).item():,}",
                    f"{claims.select(pl.col('rendering_npi').n_unique()).item():,}",
                    (
                        f"{claims.select(pl.col(dt).min()).item()} to "
                        f"{claims.select(pl.col(dt).max()).item()}"
                    ),
                ],
            }
        )
        top_npis = (
            claims.group_by("rendering_npi")
            .agg(
                pl.len().alias("claim_lines"),
                pl.col(amount).sum().alias("total_paid"),
                pl.col(patient).n_unique().alias("unique_patients"),
                pl.col("hcpcs_code").n_unique().alias("unique_hcpcs"),
            )
            .sort("total_paid", descending=True)
            .head(20)
        )
        top_hcpcs = (
            claims.group_by("hcpcs_code")
            .agg(
                pl.len().alias("claim_lines"),
                pl.col(amount).sum().alias("total_paid"),
                pl.col(amount).mean().alias("avg_paid"),
                pl.col(patient).n_unique().alias("unique_patients"),
            )
            .sort("claim_lines", descending=True)
            .head(20)
        )
        return {
            "claims_df": claims,
            "summary_stats": summary_stats,
            "top_npis": top_npis,
            "top_hcpcs": top_hcpcs,
        }

    # ---- pattern detection -------------------------------------------

    def high_frequency_providers(
        self,
        claims: pl.DataFrame | None,
        source: str,
        min_applications: int = 15,
    ) -> dict[str, pl.DataFrame] | None:
        if claims is None or claims.height == 0:
            return None
        cols = self.column_names(source)
        patient, dt = cols["patient"], cols["date"]
        patient_level = (
            claims.group_by(["rendering_npi", patient])
            .agg(
                pl.len().alias("application_count"),
                pl.col(dt).min().alias("first_application"),
                pl.col(dt).max().alias("last_application"),
                pl.col("hcpcs_code").n_unique().alias("unique_products"),
            )
            .filter(pl.col("application_count") >= min_applications)
            .with_columns(
                (pl.col("last_application") - pl.col("first_application"))
                .dt.total_days()
                .alias("span_days")
            )
            .sort("application_count", descending=True)
        )
        npi_summary = (
            patient_level.group_by("rendering_npi")
            .agg(
                pl.len().alias("patients_with_15plus_apps"),
                pl.col("application_count").sum().alias("total_applications"),
                pl.col("application_count").max().alias("max_apps_single_patient"),
                pl.col("application_count").mean().alias("avg_apps_per_patient"),
            )
            .sort("patients_with_15plus_apps", descending=True)
        )
        return {"patient_level": patient_level, "npi_summary": npi_summary}

    def high_cost_patients(
        self,
        claims: pl.DataFrame | None,
        source: str,
        min_cost: float = 1_000_000.0,
    ) -> pl.DataFrame | None:
        if claims is None or claims.height == 0:
            return None
        cols = self.column_names(source)
        patient, amount, dt = cols["patient"], cols["amount"], cols["date"]
        return (
            claims.group_by(patient)
            .agg(
                pl.col(amount).sum().alias("total_cost"),
                pl.len().alias("claim_count"),
                pl.col("rendering_npi").n_unique().alias("unique_providers"),
                pl.col("hcpcs_code").n_unique().alias("unique_products"),
                pl.col(dt).min().alias("first_claim"),
                pl.col(dt).max().alias("last_claim"),
            )
            .filter(pl.col("total_cost") >= min_cost)
            .with_columns(
                (pl.col("last_claim") - pl.col("first_claim"))
                .dt.total_days()
                .alias("treatment_span_days")
            )
            .sort("total_cost", descending=True)
        )

    def clustered_claims(
        self,
        claims: pl.DataFrame | None,
        source: str,
        min_claims_in_week: int = 3,
    ) -> dict[str, pl.DataFrame] | None:
        if claims is None or claims.height == 0:
            return None
        cols = self.column_names(source)
        patient, amount, dt = cols["patient"], cols["amount"], cols["date"]
        patient_dates = claims.select(
            ["rendering_npi", patient, dt, amount]
        ).sort(["rendering_npi", patient, dt])
        cluster_details = (
            patient_dates.join(
                patient_dates,
                on=["rendering_npi", patient],
                suffix="_next",
            )
            .filter(
                (pl.col(f"{dt}_next") >= pl.col(dt))
                & (pl.col(f"{dt}_next") <= pl.col(dt) + pl.duration(days=7))
            )
            .group_by(["rendering_npi", patient, dt])
            .agg(
                pl.col(f"{dt}_next").n_unique().alias("claims_in_week"),
                pl.col(f"{dt}_next").min().alias("week_start"),
                pl.col(f"{dt}_next").max().alias("week_end"),
                pl.col(f"{amount}_next").sum().alias("week_total_paid"),
            )
            .filter(pl.col("claims_in_week") >= min_claims_in_week)
            .sort("claims_in_week", descending=True)
        )
        npi_summary = (
            cluster_details.group_by("rendering_npi")
            .agg(
                pl.col(patient).n_unique().alias("patients_with_clusters"),
                pl.col("claims_in_week").sum().alias("total_clustered_claims"),
                pl.col("claims_in_week").max().alias("max_claims_in_week"),
                pl.col("week_total_paid").sum().alias("total_paid_in_clusters"),
            )
            .sort("patients_with_clusters", descending=True)
        )
        return {"cluster_details": cluster_details, "npi_summary": npi_summary}

    def same_day_duplicates(
        self, claims: pl.DataFrame | None, source: str
    ) -> dict[str, pl.DataFrame] | None:
        if claims is None or claims.height == 0:
            return None
        cols = self.column_names(source)
        patient, amount, dt = cols["patient"], cols["amount"], cols["date"]
        details = (
            claims.group_by(["rendering_npi", patient, dt, "hcpcs_code"])
            .agg(
                pl.len().alias("claim_count"),
                pl.col(amount).sum().alias("total_paid"),
                pl.col(amount).mean().alias("avg_paid_per_claim"),
            )
            .filter(pl.col("claim_count") > 1)
            .sort("claim_count", descending=True)
        )
        npi_summary = (
            details.group_by("rendering_npi")
            .agg(
                pl.len().alias("duplicate_instances"),
                pl.col("claim_count").sum().alias("total_duplicate_claims"),
                pl.col("total_paid").sum().alias("total_paid_duplicates"),
                pl.col(patient).n_unique().alias("unique_patients_affected"),
            )
            .sort("duplicate_instances", descending=True)
        )
        return {"duplicate_details": details, "npi_summary": npi_summary}

    def identical_billing_patterns(
        self, claims: pl.DataFrame | None, source: str
    ) -> dict[str, pl.DataFrame] | None:
        if claims is None or claims.height == 0:
            return None
        cols = self.column_names(source)
        patient, amount, dt = cols["patient"], cols["amount"], cols["date"]
        details = (
            claims.group_by(["rendering_npi", "hcpcs_code", amount])
            .agg(
                pl.len().alias("claim_count"),
                pl.col(patient).n_unique().alias("unique_patients"),
                pl.col(dt).n_unique().alias("unique_dates"),
                pl.col(dt).min().alias("first_claim_date"),
                pl.col(dt).max().alias("last_claim_date"),
            )
            .filter(
                (pl.col("claim_count") >= 10)
                & (pl.col("unique_patients") >= 3)
            )
            .with_columns(
                (pl.col("last_claim_date") - pl.col("first_claim_date"))
                .dt.total_days()
                .alias("pattern_span_days")
            )
            .sort(["rendering_npi", "claim_count"], descending=[False, True])
        )
        npi_summary = (
            details.group_by("rendering_npi")
            .agg(
                pl.len().alias("unique_billing_patterns"),
                pl.col("claim_count").sum().alias("total_identical_claims"),
                pl.col("unique_patients").sum().alias("total_patients_affected"),
                pl.col("claim_count").max().alias("max_identical_claims"),
                pl.col(amount).n_unique().alias("unique_payment_amounts"),
            )
            .sort("total_identical_claims", descending=True)
        )
        return {"pattern_details": details, "npi_summary": npi_summary}

    # ---- NPI cross-dim comparison ------------------------------------

    def npi_comparison(
        self,
        all_summaries: dict[str, dict[str, Any]],
        configs: list[tuple[str, str]],
        top_n: int = 20,
    ) -> pl.DataFrame | None:
        """Top-N NPIs across the selected dimensions, with paid/lines/patients metrics.

        Returns a wide DataFrame keyed by ``rendering_npi`` with one column per
        dimension label plus a delta/largest-value summary column. ``None`` if
        none of the configs have data.
        """
        parts: list[pl.DataFrame] = []
        for key, label in configs:
            summary = all_summaries.get(key) or {}
            top_npis = summary.get("top_npis")
            if top_npis is not None and top_npis.height > 0:
                parts.append(
                    top_npis.with_columns(
                        pl.lit(label).alias("dimension"),
                        pl.col("total_paid").cast(pl.Float64),
                    )
                )
        if not parts:
            return None
        combined = pl.concat(parts)
        top = (
            combined.group_by("rendering_npi")
            .agg(pl.col("total_paid").sum().alias("overall_total_paid"))
            .sort("overall_total_paid", descending=True)
            .head(top_n)["rendering_npi"]
            .to_list()
        )
        filtered = combined.filter(pl.col("rendering_npi").is_in(top))
        pivot_data = filtered.select(
            ["rendering_npi", "dimension", "total_paid", "claim_lines", "unique_patients"]
        )
        labels = [label for _, label in configs]
        if len(configs) == 2:
            return self._npi_two_way(pivot_data, labels)
        return self._npi_multi_way(pivot_data, combined)

    def _npi_two_way(self, pivot_data: pl.DataFrame, labels: list[str]) -> pl.DataFrame:
        dim1, dim2 = labels
        paid_by_dim = (
            pivot_data.select(["rendering_npi", "dimension", "total_paid"]).pivot(
                index="rendering_npi",
                on="dimension",
                values="total_paid",
                aggregate_function="first",
            )
        )
        metrics_pivoted = (
            pivot_data.with_columns(
                pl.concat_str(
                    [
                        pl.lit("Paid: $"),
                        pl.col("total_paid").cast(pl.Int64).cast(pl.Utf8),
                        pl.lit(" | Lines: "),
                        pl.col("claim_lines").cast(pl.Utf8),
                        pl.lit(" | Pts: "),
                        pl.col("unique_patients").cast(pl.Utf8),
                    ]
                ).alias("metrics")
            ).pivot(
                index="rendering_npi",
                on="dimension",
                values="metrics",
                aggregate_function="first",
            )
        )
        renamed = paid_by_dim.rename(
            {dim1: f"{dim1}_paid", dim2: f"{dim2}_paid"}
        )
        return (
            metrics_pivoted.join(renamed, on="rendering_npi")
            .with_columns(
                (pl.col(f"{dim1}_paid") - pl.col(f"{dim2}_paid")).abs().alias("abs_diff"),
                (pl.col(f"{dim1}_paid") - pl.col(f"{dim2}_paid")).alias("diff_value"),
                (
                    (pl.col(f"{dim1}_paid") - pl.col(f"{dim2}_paid"))
                    / pl.when(pl.col(f"{dim2}_paid") != 0)
                    .then(pl.col(f"{dim2}_paid"))
                    .otherwise(1)
                    * 100
                ).alias("pct_diff"),
            )
            .sort("abs_diff", descending=True)
            .with_columns(
                pl.concat_str(
                    [
                        pl.lit("$"),
                        pl.col("diff_value").cast(pl.Int64).cast(pl.Utf8),
                        pl.lit(" ("),
                        pl.col("pct_diff").round(1).cast(pl.Utf8),
                        pl.lit("%)"),
                    ]
                ).alias(f"Diff ({dim1} - {dim2})")
            )
            .drop([f"{dim1}_paid", f"{dim2}_paid", "abs_diff", "diff_value", "pct_diff"])
        )

    def _npi_multi_way(
        self, pivot_data: pl.DataFrame, combined: pl.DataFrame
    ) -> pl.DataFrame:
        metrics_pivoted = (
            pivot_data.with_columns(
                pl.concat_str(
                    [
                        pl.lit("Paid: $"),
                        pl.col("total_paid").cast(pl.Int64).cast(pl.Utf8),
                        pl.lit(" | Lines: "),
                        pl.col("claim_lines").cast(pl.Utf8),
                        pl.lit(" | Pts: "),
                        pl.col("unique_patients").cast(pl.Utf8),
                    ]
                ).alias("metrics")
            ).pivot(
                index="rendering_npi",
                on="dimension",
                values="metrics",
                aggregate_function="first",
            )
        )
        return (
            metrics_pivoted.join(
                combined.group_by("rendering_npi").agg(
                    pl.col("total_paid").max().alias("max_paid")
                ),
                on="rendering_npi",
            )
            .sort("max_paid", descending=True)
            .with_columns(
                pl.concat_str(
                    [pl.lit("$"), pl.col("max_paid").cast(pl.Int64).cast(pl.Utf8)]
                ).alias("Largest Value")
            )
            .drop("max_paid")
        )

    # ---- code-set analysis -------------------------------------------

    def code_set_relationship(
        self,
        wound_codes: list[str] | tuple[str, ...],
        skin_codes: list[str] | tuple[str, ...],
    ) -> dict[str, int]:
        ws = set(wound_codes)
        ss = set(skin_codes)
        return {
            "wound_total": len(ws),
            "skin_total": len(ss),
            "overlap": len(ws & ss),
            "wound_only": len(ws - ss),
            "skin_only": len(ss - ws),
        }
