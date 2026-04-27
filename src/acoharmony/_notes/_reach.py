# © 2025 HarmonyCares
# All rights reserved.

"""
ACO REACH analytics shared across the reach_* notebooks.

Year-over-year attribution analysis: pull BAR snapshots for two
windows (2025 and Jan 2026), diff the beneficiary sets, classify
term reasons against CRR death dates, and roll up temporal /
percentage breakdowns.

The schemas come from the silver tables ``bar.parquet`` and
``crr.parquet``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry


class ReachPlugins(PluginRegistry):
    """REACH attribution analytics for the y2y / cohort-loss dashboards."""

    # ---- BAR loading ---------------------------------------------------

    def load_bar(self, silver_path: Path) -> pl.LazyFrame:
        """Load ``bar.parquet`` and stamp a ``year_month`` column."""
        bar_path = Path(silver_path) / "bar.parquet"
        if not bar_path.exists():
            raise FileNotFoundError(f"BAR file not found: {bar_path}")
        return pl.scan_parquet(str(bar_path)).with_columns(
            (pl.col("start_date").dt.year() * 100 + pl.col("start_date").dt.month())
            .alias("year_month")
        )

    def benes_for_window(
        self,
        bar_lf: pl.LazyFrame,
        ym_min: int,
        ym_max: int | None = None,
    ) -> pl.DataFrame:
        """Distinct MBIs aligned within ``[ym_min, ym_max]`` (inclusive)."""
        upper = ym_max if ym_max is not None else ym_min
        return (
            bar_lf.filter(pl.col("year_month").is_between(ym_min, upper))
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("year_month"),
                pl.col("start_date"),
            )
            .unique(subset=["mbi"])
            .collect()
        )

    def benes_for_month(self, bar_lf: pl.LazyFrame, year_month: int) -> pl.DataFrame:
        """Distinct MBIs aligned during a specific ``YYYYMM``."""
        return (
            bar_lf.filter(pl.col("year_month") == year_month)
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("start_date"),
            )
            .collect()
        )

    # ---- y2y attribution diff ------------------------------------------

    def attribution_loss(
        self,
        benes_prev: pl.DataFrame,
        benes_next: pl.DataFrame,
    ) -> dict[str, Any]:
        """
        Set difference between two BAR snapshots.

        Returns ``{lost_mbis, lost_benes, total_lost, total_prev, total_next}``.
        ``lost_benes`` is the rows in ``benes_prev`` whose MBI no longer
        appears in ``benes_next``.
        """
        prev = set(benes_prev["mbi"].to_list())
        nxt = set(benes_next["mbi"].to_list())
        lost = prev - nxt
        return {
            "lost_mbis": lost,
            "lost_benes": benes_prev.filter(pl.col("mbi").is_in(list(lost))),
            "total_lost": len(lost),
            "total_prev": benes_prev.height,
            "total_next": benes_next.height,
        }

    # ---- term reason analysis -----------------------------------------

    def load_crr_for_lost(
        self,
        silver_path: Path,
        lost_mbis,
    ) -> pl.DataFrame | None:
        """CRR rows (mbi + bene_death_dt) for the lost cohort, or ``None``."""
        crr_path = Path(silver_path) / "crr.parquet"
        if not crr_path.exists():
            return None
        return (
            pl.scan_parquet(str(crr_path))
            .filter(pl.col("bene_mbi").is_in(list(lost_mbis)))
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("bene_death_dt"),
            )
            .collect()
        )

    def lost_bar_records(
        self,
        bar_lf: pl.LazyFrame,
        lost_mbis,
    ) -> pl.DataFrame:
        """Latest BAR row per lost MBI: last_alignment_month, end_date, death_date, voluntary_type."""
        return (
            bar_lf.filter(pl.col("bene_mbi").is_in(list(lost_mbis)))
            .sort("year_month", descending=True)
            .group_by("bene_mbi")
            .agg(
                pl.col("year_month").first().alias("last_alignment_month"),
                pl.col("end_date").first().alias("end_date"),
                pl.col("bene_date_of_death").first().alias("death_date"),
                pl.col("voluntary_alignment_type").first().alias("voluntary_type"),
            )
            .collect()
            .rename({"bene_mbi": "mbi"})
        )

    def categorize_term_reasons(
        self,
        lost_bar_records: pl.DataFrame,
        lost_crr: pl.DataFrame | None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Classify each lost MBI as Expired / Lost Provider / Other.

        Returns ``(per_mbi_categorized, term_summary_counts)``.
        """
        if lost_crr is not None:
            joined = lost_bar_records.join(lost_crr, on="mbi", how="left", suffix="_crr")
        else:
            joined = lost_bar_records.with_columns(pl.lit(None).alias("bene_death_dt"))

        categorized = joined.with_columns(
            pl.when(
                pl.col("death_date").is_not_null()
                | pl.col("bene_death_dt").is_not_null()
            )
            .then(pl.lit("Expired"))
            .when(pl.col("voluntary_type").is_not_null())
            .then(pl.lit("Lost Provider"))
            .otherwise(pl.lit("Other/Unknown"))
            .alias("term_category")
        )
        summary = (
            categorized.group_by("term_category")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )
        return categorized, summary

    def breakdown_stats(
        self,
        term_summary: pl.DataFrame,
        total_lost: int,
        has_end_date: int,
    ) -> dict[str, int]:
        """
        Flatten the term-summary into the dashboard's named buckets.

        ``Moved to MA`` / ``Moved to Hospice`` are zeros today — the BAR
        schema doesn't carry those flags. Kept in the shape so the
        dashboard layout doesn't have to special-case missing keys.
        """
        term_dict = {row["term_category"]: row["count"] for row in term_summary.to_dicts()}
        return {
            "Total Lost": total_lost,
            "With End Date": has_end_date,
            "Moved to MA": 0,
            "Moved to Hospice": 0,
            "Expired (SVA)": term_dict.get("Expired", 0),
            "Lost Provider": term_dict.get("Lost Provider", 0),
            "Other/Unknown Reason": term_dict.get("Other/Unknown", 0),
            "No End Date": total_lost - has_end_date,
        }

    # ---- temporal distribution -----------------------------------------

    def temporal_distribution(self, lost_bar_records: pl.DataFrame) -> pl.DataFrame:
        """Count of lost MBIs by their last_alignment_month (YYYYMM string)."""
        return (
            lost_bar_records.with_columns(
                pl.col("last_alignment_month").cast(pl.Utf8).alias("last_month_str")
            )
            .group_by("last_month_str")
            .agg(pl.len().alias("count"))
            .sort("last_month_str")
        )

    # ---- delivery provenance (calendar vs 4i inventory) ---------------

    @staticmethod
    def cadence_bucket(period: str | None) -> str:
        """Classify a period marker into a cadence bucket."""
        if period is None:
            return "unknown"
        if period.startswith("M"):
            return "monthly"
        if period.startswith("Q"):
            return "quarterly"
        if period.startswith("S"):
            return "semi_annual"
        if period == "A":
            return "annual"
        return "other"

    def delivery_status_pivot(
        self,
        df_prov: pl.DataFrame,
        statuses: tuple[str, ...] = ("on_time", "early", "late", "unscheduled"),
    ) -> pl.DataFrame:
        """One row per schema, columns are counts per status."""
        return (
            df_prov.filter(pl.col("schema_name").is_not_null())
            .group_by("schema_name")
            .agg(
                *[
                    (pl.col("delivery_status") == s).sum().alias(s)
                    for s in statuses
                ],
                pl.len().alias("total"),
            )
            .sort("total", descending=True)
        )

    def delivery_diff_stats(self, df_prov: pl.DataFrame) -> pl.DataFrame:
        """n / mean / median / quartiles / min / max of delivery_diff_days."""
        delivered = df_prov.filter(pl.col("delivery_diff_days").is_not_null())
        if delivered.height == 0:
            return pl.DataFrame()
        return delivered.select(
            pl.col("delivery_diff_days").count().alias("n"),
            pl.col("delivery_diff_days").mean().round(2).alias("mean_days"),
            pl.col("delivery_diff_days").median().alias("median_days"),
            pl.col("delivery_diff_days").quantile(0.25).alias("p25_days"),
            pl.col("delivery_diff_days").quantile(0.75).alias("p75_days"),
            pl.col("delivery_diff_days").min().alias("min_days"),
            pl.col("delivery_diff_days").max().alias("max_days"),
        )

    def delivery_outliers(
        self,
        df_prov: pl.DataFrame,
        kind: str,
        n: int = 20,
    ) -> pl.DataFrame:
        """Top-N late or early outlier rows."""
        descending = kind == "late"
        return (
            df_prov.filter(pl.col("delivery_status") == kind)
            .sort("delivery_diff_days", descending=descending)
            .select(
                "schema_name",
                "period",
                "py",
                "expected_date",
                "actual_delivery_date",
                "delivery_diff_days",
                "description",
            )
            .head(n)
        )

    def delivery_cadence(self, df_prov: pl.DataFrame) -> pl.DataFrame:
        """Dominant cadence + scheduled span per schema."""
        return (
            df_prov.filter(
                pl.col("schema_name").is_not_null()
                & pl.col("expected_date").is_not_null()
            )
            .with_columns(
                pl.col("period")
                .map_elements(self.cadence_bucket, return_dtype=pl.String)
                .alias("cadence_bucket")
            )
            .group_by("schema_name")
            .agg(
                pl.col("cadence_bucket").mode().first().alias("dominant_cadence"),
                pl.col("expected_date").min().alias("first_scheduled"),
                pl.col("expected_date").max().alias("last_scheduled"),
                pl.len().alias("scheduled_count"),
                pl.col("category").first().alias("category"),
            )
            .sort("scheduled_count", descending=True)
        )

    def delivery_trend(self, df_prov: pl.DataFrame) -> pl.DataFrame:
        """Mean lag days per (schema, year, quarter)."""
        delivered = df_prov.filter(pl.col("delivery_diff_days").is_not_null())
        if delivered.height == 0:
            return pl.DataFrame()
        return (
            delivered.with_columns(
                pl.col("expected_date").dt.year().alias("year"),
                ((pl.col("expected_date").dt.month() - 1) // 3 + 1).alias("quarter"),
            )
            .group_by("schema_name", "year", "quarter")
            .agg(
                pl.col("delivery_diff_days").mean().round(1).alias("mean_lag_days"),
                pl.len().alias("n_deliveries"),
            )
            .sort("schema_name", "year", "quarter")
        )

    def unexpected_deliveries(self, df_prov: pl.DataFrame) -> pl.DataFrame:
        """Rows tagged unscheduled — corrections / reissues / ad-hoc drops."""
        return (
            df_prov.filter(pl.col("delivery_status") == "unscheduled")
            .sort("actual_delivery_date", descending=True)
            .select(
                "schema_name",
                "period",
                "py",
                "actual_delivery_date",
                "actual_delivery_source",
                "delivered_file_count",
                "delivered_filenames",
            )
        )

    # ---- BNMR (Benchmark Report) analytics -----------------------------

    BNMR_TABLES = (
        "reach_bnmr_report_parameters",
        "reach_bnmr_financial_settlement",
        "reach_bnmr_claims",
        "reach_bnmr_risk",
        "reach_bnmr_county",
        "reach_bnmr_uspcc",
        "reach_bnmr_heba",
        "reach_bnmr_cap",
        "reach_bnmr_riskscore_ad",
        "reach_bnmr_riskscore_esrd",
        "reach_bnmr_stop_loss_charge",
        "reach_bnmr_stop_loss_payout",
        "reach_bnmr_stop_loss_county",
        "reach_bnmr_stop_loss_claims",
        "reach_bnmr_data_stop_loss_payout",
        "reach_bnmr_benchmark_historical_ad",
        "reach_bnmr_benchmark_historical_esrd",
    )

    BNMR_CLAIM_TYPE_LABELS = {
        "10": "10 – HHA",
        "20": "20 – SNF",
        "30": "30 – SNF Swing Beds",
        "40": "40 – Outpatient",
        "50": "50 – Hospice",
        "60": "60 – Inpatient",
        "71": "71 – Physician",
        "72": "72 – DME/Physician",
        "81": "81 – DMERC/non-DMEPOS",
        "82": "82 – DMERC/DMEPOS",
    }

    def bnmr_deliveries(self, report_parameters: pl.DataFrame) -> pl.DataFrame:
        """Distinct deliveries with parsed delivery_date from filename."""
        return (
            report_parameters.select("source_filename", "performance_year", "aco_id")
            .unique()
            .with_columns(
                pl.col("source_filename")
                .str.extract(r"\.D(\d{6})\.T", 1)
                .str.strptime(pl.Date, format="%y%m%d", strict=False)
                .alias("delivery_date")
            )
            .sort("delivery_date")
        )

    def bnmr_claims_spend(self, claims: pl.DataFrame) -> pl.DataFrame:
        """Gross claims spend by (delivery, performance_year, claim_type)."""
        return (
            claims.filter(pl.col("clm_pmt_amt_agg").is_not_null())
            .with_columns(
                pl.col("clm_type_cd")
                .cast(pl.Utf8)
                .replace(self.BNMR_CLAIM_TYPE_LABELS, default="Other")
                .alias("claim_type"),
            )
            .group_by("source_filename", "performance_year", "claim_type")
            .agg(
                pl.col("clm_pmt_amt_agg")
                .cast(pl.Float64, strict=False)
                .sum()
                .alias("total_spend")
            )
            .sort("source_filename")
        )

    def bnmr_county_rates(self, county: pl.DataFrame) -> pl.DataFrame:
        """Per-capita county benchmark rates with non-null cast cleanup."""
        return (
            county.select(
                "cty_accrl_cd",
                "bnmrk",
                "performance_year",
                pl.col("cty_rate").cast(pl.Float64, strict=False).alias("rate"),
                "source_filename",
            )
            .filter(pl.col("rate").is_not_null())
        )

    def bnmr_uspcc_trend(self, uspcc: pl.DataFrame) -> pl.DataFrame:
        return (
            uspcc.select(
                pl.col("clndr_yr").cast(pl.Int32, strict=False).alias("calendar_year"),
                "bnmrk",
                pl.col("uspcc").cast(pl.Float64, strict=False).alias("uspcc_value"),
                "performance_year",
            )
            .filter(pl.col("uspcc_value").is_not_null())
            .unique()
            .sort("calendar_year")
        )

    def bnmr_normalization_factor(self, rs_ad: pl.DataFrame) -> pl.DataFrame:
        return (
            rs_ad.filter(
                pl.col("line_description").is_not_null()
                & pl.col("line_description").str.contains("(?i)normalization factor")
                & pl.col("py_value").is_not_null()
            )
            .select(
                "source_filename",
                "performance_year",
                pl.col("py_value")
                .cast(pl.Float64, strict=False)
                .alias("normalization_factor"),
            )
            .unique()
            .sort("source_filename")
        )

    def bnmr_capitation_aggregate(self, cap: pl.DataFrame) -> pl.DataFrame:
        """Sum TCC per (pmt_mnth, py, segment), coalescing legacy/new column names."""
        has_old = "aco_tcc_amt_total" in cap.columns
        has_new = "aco_tcc_amt_post_seq_paid" in cap.columns
        tcc_expr = pl.coalesce(
            pl.col("aco_tcc_amt_total").cast(pl.Float64, strict=False)
            if has_old
            else pl.lit(None),
            pl.col("aco_tcc_amt_post_seq_paid").cast(pl.Float64, strict=False)
            if has_new
            else pl.lit(None),
        ).alias("tcc_amount")
        return (
            cap.with_columns(tcc_expr)
            .filter(pl.col("tcc_amount").is_not_null() & (pl.col("tcc_amount") != 0))
            .group_by("pmt_mnth", "performance_year", "bnmrk")
            .agg(pl.col("tcc_amount").sum().alias("total_tcc"))
            .sort("pmt_mnth")
        )

    def bnmr_stop_loss_county_payouts(self, slc: pl.DataFrame) -> pl.DataFrame:
        return (
            slc.select(
                "cty_accrl_cd",
                "bnmrk",
                "performance_year",
                pl.col("avg_payout_pct").cast(pl.Float64, strict=False).alias("payout_pct"),
            )
            .filter(pl.col("payout_pct").is_not_null())
        )

    def bnmr_risk_counts(self, risk: pl.DataFrame) -> pl.DataFrame:
        return (
            risk.filter(pl.col("bene_dcnt").is_not_null())
            .group_by("source_filename", "performance_year", "bnmrk")
            .agg(
                pl.col("bene_dcnt")
                .cast(pl.Int64, strict=False)
                .sum()
                .alias("total_bene_dcnt"),
                pl.col("elig_mnths")
                .cast(pl.Int64, strict=False)
                .sum()
                .alias("total_elig_mnths"),
            )
            .sort("source_filename")
        )

    def bnmr_silver_inventory(self, silver_path: Path) -> pl.DataFrame:
        """Per-table existence + row/column/delivery/perf-year counts."""
        rows = []
        for name in self.BNMR_TABLES:
            path = Path(silver_path) / f"{name}.parquet"
            if not path.exists():
                continue
            df = pl.read_parquet(str(path))
            n_deliveries = (
                df["source_filename"].n_unique()
                if "source_filename" in df.columns
                else 0
            )
            pys = (
                ", ".join(sorted(df["performance_year"].unique().to_list()))
                if "performance_year" in df.columns
                else ""
            )
            rows.append(
                {
                    "table": name.replace("reach_bnmr_", ""),
                    "rows": df.height,
                    "columns": df.width,
                    "deliveries": n_deliveries,
                    "performance_years": pys,
                }
            )
        return pl.DataFrame(rows).sort("table") if rows else pl.DataFrame()

    # ---- High-cost beneficiary by provider (reach_high_cost notebook) ---

    EXCLUDED_PROVIDER_NPIS = ("1770583577", "1184815383", "1285636043")

    def load_palmr_latest(self, silver_path: Path) -> pl.LazyFrame:
        """Load PALMR filtered to its most recent file_date."""
        path = Path(silver_path) / "palmr.parquet"
        if not path.exists():
            return pl.LazyFrame()
        lf = pl.scan_parquet(str(path))
        latest = lf.select(pl.col("file_date").max()).collect().item()
        return lf.filter(pl.col("file_date") == latest)

    def load_bar_latest_lazy(self, silver_path: Path) -> pl.LazyFrame:
        """Load BAR filtered to its most recent file_date (LazyFrame variant)."""
        path = Path(silver_path) / "bar.parquet"
        if not path.exists():
            return pl.LazyFrame()
        lf = pl.scan_parquet(str(path))
        latest = lf.select(pl.col("file_date").max()).collect().item()
        return lf.filter(pl.col("file_date") == latest)

    def reach_provider_npis(self, palmr_lf: pl.LazyFrame) -> pl.DataFrame:
        """Distinct ``(prvdr_npi, prvdr_tin)`` pairs from PALMR."""
        if palmr_lf.collect_schema() == {}:
            return pl.DataFrame(
                schema={"prvdr_npi": pl.Utf8, "prvdr_tin": pl.Utf8}
            )
        return (
            palmr_lf.select("prvdr_npi", "prvdr_tin")
            .unique()
            .filter(pl.col("prvdr_npi").is_not_null())
            .collect()
        )

    def provider_name_lookup(
        self,
        silver_path: Path,
        reach_providers_df: pl.DataFrame,
    ) -> pl.DataFrame:
        """Build ``rendering_npi → provider_name`` lookup from participant_list."""
        if reach_providers_df.is_empty():
            return pl.DataFrame(
                schema={"rendering_npi": pl.Utf8, "provider_name": pl.Utf8}
            )

        path = Path(silver_path) / "participant_list.parquet"
        if not path.exists():
            return pl.DataFrame(
                {
                    "rendering_npi": reach_providers_df["prvdr_npi"].to_list(),
                    "provider_name": [
                        f"Provider NPI {npi}"
                        for npi in reach_providers_df["prvdr_npi"].to_list()
                    ],
                }
            )
        return (
            pl.scan_parquet(str(path))
            .select(
                pl.col("individual_npi").alias("rendering_npi"),
                pl.when(
                    pl.col("individual_first_name").is_not_null()
                    & pl.col("individual_last_name").is_not_null()
                )
                .then(
                    pl.col("individual_first_name")
                    + pl.lit(" ")
                    + pl.col("individual_last_name")
                )
                .otherwise(pl.lit("Provider NPI ") + pl.col("individual_npi"))
                .alias("provider_name"),
            )
            .filter(pl.col("rendering_npi").is_not_null())
            .filter(~pl.col("rendering_npi").is_in(list(self.EXCLUDED_PROVIDER_NPIS)))
            .unique(subset=["rendering_npi"])
            .collect()
        )

    def join_beneficiary_data(
        self,
        beneficiary_metrics_lf: pl.LazyFrame,
        bar_lf: pl.LazyFrame,
        palmr_lf: pl.LazyFrame,
        reach_providers_df: pl.DataFrame,
        year: int = 2025,
    ) -> pl.DataFrame:
        """
        Beneficiary metrics joined to PALMR (for provider attribution) + BAR demographics.
        """
        if beneficiary_metrics_lf.collect_schema() == {} or reach_providers_df.is_empty():
            return pl.DataFrame()
        if palmr_lf.collect_schema() == {}:
            return pl.DataFrame()

        metrics = beneficiary_metrics_lf.filter(pl.col("year") == year)
        palmr_dedup = palmr_lf.select(
            "bene_mbi", "prvdr_npi", "prvdr_tin"
        ).unique(subset=["bene_mbi", "prvdr_npi"])
        metrics_with_provider = metrics.join(
            palmr_dedup, left_on="person_id", right_on="bene_mbi", how="inner"
        )
        if bar_lf.collect_schema() != {}:
            return metrics_with_provider.join(
                bar_lf.select(
                    "bene_mbi",
                    "bene_first_name",
                    "bene_last_name",
                    "bene_city",
                    "bene_state",
                    "bene_date_of_death",
                ),
                left_on="person_id",
                right_on="bene_mbi",
                how="left",
            ).collect()
        return metrics_with_provider.collect()

    def readmission_counts(
        self,
        readmissions_lf: pl.LazyFrame,
        year: int = 2025,
    ) -> pl.DataFrame:
        """Readmissions per patient_id for ``year``."""
        if readmissions_lf.collect_schema() == {}:
            return pl.DataFrame(
                schema={"patient_id": pl.Utf8, "readmission_count": pl.Int64}
            )
        return (
            readmissions_lf.filter(pl.col("index_admission_date").dt.year() == year)
            .group_by("patient_id")
            .agg(pl.len().alias("readmission_count"))
            .collect()
        )

    def rank_high_cost(
        self,
        enriched_df: pl.DataFrame,
        provider_npi: str,
        kind: str,
        readmission_counts: pl.DataFrame | None = None,
        n: int = 25,
    ) -> pl.DataFrame:
        """
        Top-N beneficiaries for one provider by ``kind`` ∈
        {total_spend, inpatient, er, readmissions, snf, home_health}.
        """
        if enriched_df.is_empty():
            return pl.DataFrame()
        scoped = enriched_df.filter(pl.col("prvdr_npi") == provider_npi)
        if kind == "total_spend":
            return scoped.sort("total_spend_ytd", descending=True).head(n)
        if kind == "inpatient":
            return (
                scoped.filter(pl.col("inpatient_admits_ytd") > 0)
                .sort("inpatient_admits_ytd", descending=True)
                .head(n)
            )
        if kind == "er":
            return (
                scoped.filter(pl.col("er_admits_ytd") > 0)
                .sort("er_admits_ytd", descending=True)
                .head(n)
            )
        if kind == "snf":
            return (
                scoped.filter(pl.col("snf_spend_ytd") > 0)
                .sort("snf_spend_ytd", descending=True)
                .head(n)
            )
        if kind == "home_health":
            return (
                scoped.filter(pl.col("home_health_spend_ytd") > 0)
                .sort("home_health_spend_ytd", descending=True)
                .head(n)
            )
        if kind == "readmissions":
            if readmission_counts is None or readmission_counts.is_empty():
                return pl.DataFrame()
            return (
                scoped.join(
                    readmission_counts,
                    left_on="person_id",
                    right_on="patient_id",
                    how="left",
                )
                .with_columns(pl.col("readmission_count").fill_null(0))
                .filter(pl.col("readmission_count") > 0)
                .sort("readmission_count", descending=True)
                .head(n)
            )
        raise ValueError(f"Unknown rank kind: {kind}")

    BNMR_REPORT_PARAM_COLS = (
        "performance_year", "source_filename",
        "discount", "shared_savings_rate", "quality_withhold", "quality_score",
        "blend_percentage", "blend_ceiling", "blend_floor",
        "ad_retrospective_trend", "esrd_retrospective_trend",
        "ad_completion_factor", "esrd_completion_factor",
        "aco_type", "risk_arrangement", "payment_mechanism",
        "stop_loss_elected", "stop_loss_type",
    )

    def bnmr_report_parameters_view(self, rp: pl.DataFrame) -> pl.DataFrame:
        present = [c for c in self.BNMR_REPORT_PARAM_COLS if c in rp.columns]
        return rp.select(present).unique().sort("source_filename")

    def schema_drilldown(self, df_prov: pl.DataFrame, schema: str) -> pl.DataFrame:
        """All scheduled and actual rows for a single schema, latest first."""
        return (
            df_prov.filter(pl.col("schema_name") == schema)
            .sort(
                pl.coalesce(["expected_date", "actual_delivery_date"]),
                descending=True,
                nulls_last=True,
            )
            .select(
                "period",
                "py",
                "expected_date",
                "actual_delivery_date",
                "delivery_diff_days",
                "delivery_status",
                "actual_delivery_source",
                "description",
                "delivered_file_count",
            )
        )
