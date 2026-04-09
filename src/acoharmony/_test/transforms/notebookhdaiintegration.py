# © 2025 HarmonyCares
# All rights reserved.

"""
Integration tests for HDAI REACH notebook.

Tests the full pipeline of loading, filtering, enriching, and displaying HDAI data.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date
from pathlib import Path

import polars as pl
import pytest
import acoharmony


class TestHdaiReachIntegration:
    """Integration tests for full HDAI REACH notebook pipeline."""

    @pytest.mark.unit
    def test_full_enrichment_pipeline(self, tmp_path: Path) -> None:
        """Test complete enrichment pipeline from raw data to enriched output."""
        # Create test data
        lf_hdai_raw = pl.DataFrame({
            "mbi": ["MBI001", "MBI002", "MBI003"],
            "patient_first_name": ["Alice", "Bob", "Charlie"],
            "patient_last_name": ["Smith", "Jones", "Brown"],
            "total_spend_ytd": [100000.0, 75000.0, 50000.0],
            "er_admits_ytd": [5, 3, 1],
            "er_admits_90_day_prior": [2, 1, 0],
            "any_inpatient_hospital_admits_ytd": [3, 2, 1],
            "any_inpatient_hospital_admits_90_day_prior": [1, 1, 0],
            "plurality_assigned_provider_name": ["Dr. A", "Dr. B", "Dr. A"],
            "hospice_admission": [False, False, False],
            "inpatient_spend_ytd": [50000.0, 35000.0, 20000.0],
            "outpatient_spend_ytd": [30000.0, 25000.0, 20000.0],
            "snf_cost_ytd": [10000.0, 10000.0, 5000.0],
            "home_health_spend_ytd": [5000.0, 3000.0, 3000.0],
            "em_visits_ytd": [10, 8, 6],
            "last_em_visit": [date(2024, 1, 15), date(2024, 1, 10), date(2024, 1, 5)],
            "aco_em_name": ["Dr. X", "Dr. Y", "Dr. Z"],
            "file_date": [date(2024, 2, 1), date(2024, 2, 1), date(2024, 2, 1)],
        }).lazy()

        lf_crosswalk = pl.DataFrame({
            "prvs_num": ["MBI001", "MBI002"],
            "crnt_num": ["MBI001_CURRENT", "MBI002_CURRENT"],
            "hcmpi": ["HCMPI_123", "HCMPI_456"],
            "mrn": ["MRN_001", "MRN_002"],
        }).lazy()

        lf_demographics = pl.DataFrame({
            "bene_mbi_id": ["MBI001_CURRENT", "MBI002_CURRENT", "MBI003"],
            "bene_city": ["Boston", "Seattle", "Portland"],
            "bene_state": ["MA", "WA", "OR"],
            "bene_zip": ["02101", "98101", "97201"],
            "bene_line_1_adr": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
            "bene_part_a_enrlmt_bgn_dt": [date(2020, 1, 1), date(2021, 1, 1), date(2022, 1, 1)],
            "bene_part_b_enrlmt_bgn_dt": [date(2020, 1, 1), date(2021, 1, 1), date(2022, 1, 1)],
            "bene_orgnl_entlmt_rsn_cd": ["1", "2", "1"],
            "bene_dual_stus_cd": ["00", "01", "00"],
        }).lazy()

        already_discussed_list = ["MBI001"]

        # Define the enrichment functions (copied from notebook logic)
        def get_most_recent_report_data(lf_raw):
            try:
                schema = lf_raw.collect_schema()
                if len(schema) == 0:
                    return lf_raw
            except Exception:
                return lf_raw

            if "file_date" not in lf_raw.collect_schema().names():
                return lf_raw

            most_recent_date = lf_raw.select(pl.col("file_date").max()).collect().item()
            return lf_raw.filter(pl.col("file_date") == most_recent_date)

        def enrich_with_enterprise_crosswalk(lf, lf_crosswalk):
            try:
                schema = lf.collect_schema()
                xwalk_schema = lf_crosswalk.collect_schema()
            except Exception:
                return lf.with_columns([
                    pl.lit(None).cast(pl.String).alias("current_mbi"),
                    pl.lit(None).cast(pl.String).alias("hcmpi"),
                    pl.lit(None).cast(pl.String).alias("mrn"),
                ])

            if len(schema) == 0 or len(xwalk_schema) == 0:
                return lf.with_columns([
                    pl.lit(None).cast(pl.String).alias("current_mbi"),
                    pl.lit(None).cast(pl.String).alias("hcmpi"),
                    pl.lit(None).cast(pl.String).alias("mrn"),
                ])

            lf_xwalk_dedup = (
                lf_crosswalk
                .select(["prvs_num", "crnt_num", "hcmpi", "mrn"])
                .unique(subset=["prvs_num"], maintain_order=True, keep="first")
            )

            enriched = lf.join(
                lf_xwalk_dedup,
                left_on="mbi",
                right_on="prvs_num",
                how="left"
            )

            enriched = enriched.with_columns([
                pl.coalesce([pl.col("crnt_num"), pl.col("mbi")]).alias("current_mbi"),
            ])

            cols_to_drop = []
            enriched_schema = enriched.collect_schema().names()
            if "prvs_num" in enriched_schema:
                cols_to_drop.append("prvs_num")
            if "crnt_num" in enriched_schema:
                cols_to_drop.append("crnt_num")

            if cols_to_drop:
                enriched = enriched.drop(cols_to_drop)

            return enriched

        def enrich_with_beneficiary_demographics(lf, lf_demographics):
            try:
                schema = lf.collect_schema()
                demo_schema = lf_demographics.collect_schema()
            except Exception:
                return lf.with_columns([
                    pl.lit(None).cast(pl.String).alias("bene_city"),
                    pl.lit(None).cast(pl.String).alias("bene_state"),
                    pl.lit(None).cast(pl.String).alias("bene_zip"),
                    pl.lit(None).cast(pl.Date).alias("bene_part_a_enrlmt_bgn_dt"),
                    pl.lit(None).cast(pl.Date).alias("bene_part_b_enrlmt_bgn_dt"),
                ])

            if len(schema) == 0 or len(demo_schema) == 0:
                return lf.with_columns([
                    pl.lit(None).cast(pl.String).alias("bene_city"),
                    pl.lit(None).cast(pl.String).alias("bene_state"),
                    pl.lit(None).cast(pl.String).alias("bene_zip"),
                    pl.lit(None).cast(pl.Date).alias("bene_part_a_enrlmt_bgn_dt"),
                    pl.lit(None).cast(pl.Date).alias("bene_part_b_enrlmt_bgn_dt"),
                ])

            available_demo_cols = demo_schema.names()
            demo_cols_to_select = ["bene_mbi_id"]
            optional_cols = [
                "bene_line_1_adr",
                "bene_city",
                "bene_state",
                "bene_zip",
                "bene_part_a_enrlmt_bgn_dt",
                "bene_part_b_enrlmt_bgn_dt",
                "bene_orgnl_entlmt_rsn_cd",
                "bene_dual_stus_cd",
            ]

            for col in optional_cols:
                if col in available_demo_cols:
                    demo_cols_to_select.append(col)

            lf_demo_subset = (
                lf_demographics
                .select(demo_cols_to_select)
                .unique(subset=["bene_mbi_id"], maintain_order=True, keep="first")
            )

            schema_names = schema.names()
            mbi_col = "current_mbi" if "current_mbi" in schema_names else "mbi"

            enriched = lf.join(
                lf_demo_subset,
                left_on=mbi_col,
                right_on="bene_mbi_id",
                how="left"
            )

            enriched_schema = enriched.collect_schema().names()
            if "bene_mbi_id" in enriched_schema:
                enriched = enriched.drop("bene_mbi_id")

            return enriched

        def flag_already_discussed_patients(lf, already_discussed):
            try:
                schema = lf.collect_schema()
                if len(schema) == 0:
                    return lf.with_columns([
                        pl.lit(False).alias("already_discussed"),
                    ])
            except Exception:
                return lf.with_columns([
                    pl.lit(False).alias("already_discussed"),
                ])

            return lf.with_columns([
                pl.col("mbi").is_in(already_discussed).alias("already_discussed")
            ])

        # Execute pipeline
        lf_filtered = get_most_recent_report_data(lf_hdai_raw)
        lf_xwalk = enrich_with_enterprise_crosswalk(lf_filtered, lf_crosswalk)
        lf_demo = enrich_with_beneficiary_demographics(lf_xwalk, lf_demographics)
        lf_flagged = flag_already_discussed_patients(lf_demo, already_discussed_list)

        # Collect result
        df_result = lf_flagged.collect()

        # Verify all enrichments applied
        assert "current_mbi" in df_result.columns
        assert "hcmpi" in df_result.columns
        assert "mrn" in df_result.columns
        assert "bene_city" in df_result.columns
        assert "bene_state" in df_result.columns
        assert "already_discussed" in df_result.columns

        # Verify data quality
        assert len(df_result) == 3  # All rows preserved

        # Verify crosswalk enrichment
        alice = df_result.filter(pl.col("mbi") == "MBI001").row(0, named=True)
        assert alice["current_mbi"] == "MBI001_CURRENT"
        assert alice["hcmpi"] == "HCMPI_123"
        assert alice["already_discussed"] is True

        # Verify demographics enrichment (using current_mbi)
        assert alice["bene_city"] == "Boston"
        assert alice["bene_state"] == "MA"

        # Verify MBI without crosswalk uses original MBI
        charlie = df_result.filter(pl.col("mbi") == "MBI003").row(0, named=True)
        assert charlie["current_mbi"] == "MBI003"  # Fallback to original
        assert charlie["bene_city"] == "Portland"  # Demographics joined on MBI003
        assert charlie["already_discussed"] is False

    @pytest.mark.unit
    def test_pipeline_is_idempotent(self) -> None:
        """Test that running the pipeline multiple times produces same result."""
        lf_hdai_raw = pl.DataFrame({
            "mbi": ["MBI001", "MBI002"],
            "patient_first_name": ["Alice", "Bob"],
            "patient_last_name": ["Smith", "Jones"],
            "total_spend_ytd": [100000.0, 75000.0],
            "er_admits_ytd": [5, 3],
            "file_date": [date(2024, 2, 1), date(2024, 2, 1)],
        }).lazy()

        lf_crosswalk = pl.DataFrame({
            "prvs_num": ["MBI001"],
            "crnt_num": ["MBI001_CURRENT"],
            "hcmpi": ["HCMPI_123"],
            "mrn": ["MRN_001"],
        }).lazy()

        lf_demographics = pl.DataFrame({
            "bene_mbi_id": ["MBI001_CURRENT", "MBI002"],
            "bene_city": ["Boston", "Seattle"],
            "bene_state": ["MA", "WA"],
            "bene_zip": ["02101", "98101"],
            "bene_part_a_enrlmt_bgn_dt": [date(2020, 1, 1), date(2021, 1, 1)],
            "bene_part_b_enrlmt_bgn_dt": [date(2020, 1, 1), date(2021, 1, 1)],
        }).lazy()

        already_discussed_list = ["MBI001"]

        # Define functions (same as above test)
        def get_most_recent_report_data(lf_raw):
            try:
                schema = lf_raw.collect_schema()
                if len(schema) == 0:
                    return lf_raw
            except Exception:
                return lf_raw
            if "file_date" not in lf_raw.collect_schema().names():
                return lf_raw
            most_recent_date = lf_raw.select(pl.col("file_date").max()).collect().item()
            return lf_raw.filter(pl.col("file_date") == most_recent_date)

        def enrich_with_enterprise_crosswalk(lf, lf_crosswalk):
            try:
                schema = lf.collect_schema()
                xwalk_schema = lf_crosswalk.collect_schema()
            except Exception:
                return lf.with_columns([
                    pl.lit(None).cast(pl.String).alias("current_mbi"),
                    pl.lit(None).cast(pl.String).alias("hcmpi"),
                    pl.lit(None).cast(pl.String).alias("mrn"),
                ])
            if len(schema) == 0 or len(xwalk_schema) == 0:
                return lf.with_columns([
                    pl.lit(None).cast(pl.String).alias("current_mbi"),
                    pl.lit(None).cast(pl.String).alias("hcmpi"),
                    pl.lit(None).cast(pl.String).alias("mrn"),
                ])
            lf_xwalk_dedup = (
                lf_crosswalk.select(["prvs_num", "crnt_num", "hcmpi", "mrn"])
                .unique(subset=["prvs_num"], maintain_order=True, keep="first")
            )
            enriched = lf.join(lf_xwalk_dedup, left_on="mbi", right_on="prvs_num", how="left")
            enriched = enriched.with_columns([
                pl.coalesce([pl.col("crnt_num"), pl.col("mbi")]).alias("current_mbi"),
            ])
            cols_to_drop = []
            enriched_schema = enriched.collect_schema().names()
            if "prvs_num" in enriched_schema:
                cols_to_drop.append("prvs_num")
            if "crnt_num" in enriched_schema:
                cols_to_drop.append("crnt_num")
            if cols_to_drop:
                enriched = enriched.drop(cols_to_drop)
            return enriched

        def enrich_with_beneficiary_demographics(lf, lf_demographics):
            try:
                schema = lf.collect_schema()
                demo_schema = lf_demographics.collect_schema()
            except Exception:
                return lf.with_columns([
                    pl.lit(None).cast(pl.String).alias("bene_city"),
                    pl.lit(None).cast(pl.String).alias("bene_state"),
                    pl.lit(None).cast(pl.String).alias("bene_zip"),
                    pl.lit(None).cast(pl.Date).alias("bene_part_a_enrlmt_bgn_dt"),
                    pl.lit(None).cast(pl.Date).alias("bene_part_b_enrlmt_bgn_dt"),
                ])
            if len(schema) == 0 or len(demo_schema) == 0:
                return lf.with_columns([
                    pl.lit(None).cast(pl.String).alias("bene_city"),
                    pl.lit(None).cast(pl.String).alias("bene_state"),
                    pl.lit(None).cast(pl.String).alias("bene_zip"),
                    pl.lit(None).cast(pl.Date).alias("bene_part_a_enrlmt_bgn_dt"),
                    pl.lit(None).cast(pl.Date).alias("bene_part_b_enrlmt_bgn_dt"),
                ])
            available_demo_cols = demo_schema.names()
            demo_cols_to_select = ["bene_mbi_id"]
            optional_cols = [
                "bene_line_1_adr", "bene_city", "bene_state", "bene_zip",
                "bene_part_a_enrlmt_bgn_dt", "bene_part_b_enrlmt_bgn_dt",
                "bene_orgnl_entlmt_rsn_cd", "bene_dual_stus_cd",
            ]
            for col in optional_cols:
                if col in available_demo_cols:
                    demo_cols_to_select.append(col)
            lf_demo_subset = (
                lf_demographics.select(demo_cols_to_select)
                .unique(subset=["bene_mbi_id"], maintain_order=True, keep="first")
            )
            schema_names = schema.names()
            mbi_col = "current_mbi" if "current_mbi" in schema_names else "mbi"
            enriched = lf.join(lf_demo_subset, left_on=mbi_col, right_on="bene_mbi_id", how="left")
            enriched_schema = enriched.collect_schema().names()
            if "bene_mbi_id" in enriched_schema:
                enriched = enriched.drop("bene_mbi_id")
            return enriched

        def flag_already_discussed_patients(lf, already_discussed):
            try:
                schema = lf.collect_schema()
                if len(schema) == 0:
                    return lf.with_columns([pl.lit(False).alias("already_discussed")])
            except Exception:
                return lf.with_columns([pl.lit(False).alias("already_discussed")])
            return lf.with_columns([pl.col("mbi").is_in(already_discussed).alias("already_discussed")])

        # Run pipeline twice
        def run_pipeline():
            lf_filtered = get_most_recent_report_data(lf_hdai_raw)
            lf_xwalk = enrich_with_enterprise_crosswalk(lf_filtered, lf_crosswalk)
            lf_demo = enrich_with_beneficiary_demographics(lf_xwalk, lf_demographics)
            lf_flagged = flag_already_discussed_patients(lf_demo, already_discussed_list)
            return lf_flagged.collect()

        result1 = run_pipeline()
        result2 = run_pipeline()

        # Results should be identical
        assert result1.equals(result2)
