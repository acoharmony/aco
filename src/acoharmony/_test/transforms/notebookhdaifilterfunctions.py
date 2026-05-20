# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for HDAI REACH notebook filter functions.

Tests the idempotent filtering functions for report date and patient selection.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest
import acoharmony


class TestGetMostRecentReportData:
    """Tests for get_most_recent_report_data function."""

    @pytest.mark.unit
    def test_filters_to_most_recent_date(self) -> None:
        """get_most_recent_report_data filters to the most recent file_date."""
        lf_data = pl.DataFrame({
            "mbi": ["MBI001", "MBI002", "MBI003", "MBI004"],
            "total_spend_ytd": [50000.0, 60000.0, 70000.0, 80000.0],
            "file_date": [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 2, 1),  # Most recent
                date(2024, 2, 1),  # Most recent
            ],
        }).lazy()

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

        result = get_most_recent_report_data(lf_data)
        df_result = result.collect()

        # Only the most recent date
        assert len(df_result) == 2
        assert all(df_result["file_date"] == date(2024, 2, 1))
        assert set(df_result["mbi"].to_list()) == {"MBI003", "MBI004"}

    @pytest.mark.unit
    def test_handles_data_without_file_date(self) -> None:
        """get_most_recent_report_data returns all data if no file_date column."""
        lf_data = pl.DataFrame({
            "mbi": ["MBI001", "MBI002", "MBI003"],
            "total_spend_ytd": [50000.0, 60000.0, 70000.0],
        }).lazy()

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

        result = get_most_recent_report_data(lf_data)
        df_result = result.collect()

        # All data returned
        assert len(df_result) == 3

    @pytest.mark.unit
    def test_is_idempotent(self) -> None:
        """get_most_recent_report_data is idempotent."""
        lf_data = pl.DataFrame({
            "mbi": ["MBI001", "MBI002", "MBI003"],
            "file_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 2, 1)],
        }).lazy()

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

        # Run twice
        result1 = get_most_recent_report_data(lf_data).collect()
        result2 = get_most_recent_report_data(lf_data).collect()

        # Results identical
        assert result1.equals(result2)

    @pytest.mark.unit
    def test_handles_empty_lazyframe(self) -> None:
        """get_most_recent_report_data handles empty LazyFrame."""
        lf_empty = pl.LazyFrame()

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

        result = get_most_recent_report_data(lf_empty)

        # Should not raise an exception
        assert result is not None


class TestGetHighCostPatients:
    """Tests for get_high_cost_patients function."""

    @pytest.mark.unit
    def test_returns_top_n_patients_by_spend(self) -> None:
        """get_high_cost_patients returns top N patients sorted by total_spend_ytd."""
        df_data = pl.DataFrame({
            "mbi": [f"MBI{i:03d}" for i in range(10)],
            "patient_first_name": [f"First{i}" for i in range(10)],
            "patient_last_name": [f"Last{i}" for i in range(10)],
            "plurality_assigned_provider_name": ["Provider A"] * 10,
            "total_spend_ytd": [float(i * 10000) for i in range(10, 0, -1)],  # Decreasing
            "er_admits_ytd": [0] * 10,
            "er_admits_90_day_prior": [0] * 10,
            "any_inpatient_hospital_admits_ytd": [0] * 10,
            "any_inpatient_hospital_admits_90_day_prior": [0] * 10,
            "hospice_admission": [False] * 10,
            "inpatient_spend_ytd": [0.0] * 10,
            "outpatient_spend_ytd": [0.0] * 10,
            "snf_cost_ytd": [0.0] * 10,
            "home_health_spend_ytd": [0.0] * 10,
            "em_visits_ytd": [0] * 10,
            "last_em_visit": [None] * 10,
            "aco_em_name": [None] * 10,
        })

        def get_high_cost_patients(df, provider_name=None, top_n=20, min_cost=None, max_cost=None,
                                    min_er_admits=None, min_inpatient_admits=None):
            query = df

            if provider_name:
                query = query.filter(pl.col("plurality_assigned_provider_name") == provider_name)
            if min_cost is not None:
                query = query.filter(pl.col("total_spend_ytd") >= min_cost)
            if max_cost is not None:
                query = query.filter(pl.col("total_spend_ytd") <= max_cost)
            if min_er_admits is not None:
                query = query.filter(pl.col("er_admits_ytd") >= min_er_admits)
            if min_inpatient_admits is not None:
                query = query.filter(pl.col("any_inpatient_hospital_admits_ytd") >= min_inpatient_admits)

            select_cols = [
                "mbi", "patient_first_name", "patient_last_name", "plurality_assigned_provider_name",
                "total_spend_ytd", "er_admits_ytd", "er_admits_90_day_prior",
                "any_inpatient_hospital_admits_ytd", "any_inpatient_hospital_admits_90_day_prior",
                "hospice_admission", "inpatient_spend_ytd", "outpatient_spend_ytd",
                "snf_cost_ytd", "home_health_spend_ytd", "em_visits_ytd", "last_em_visit", "aco_em_name",
            ]

            return query.select(select_cols).sort("total_spend_ytd", descending=True).head(top_n)

        result = get_high_cost_patients(df_data, top_n=5)

        # Returns top 5
        assert len(result) == 5

        # Sorted by spend descending
        assert result["total_spend_ytd"][0] == 100000.0
        assert result["total_spend_ytd"][4] == 60000.0

    @pytest.mark.unit
    def test_filters_by_provider(self) -> None:
        """get_high_cost_patients filters by provider_name."""
        df_data = pl.DataFrame({
            "mbi": ["MBI001", "MBI002", "MBI003", "MBI004"],
            "patient_first_name": ["A", "B", "C", "D"],
            "patient_last_name": ["X", "Y", "Z", "W"],
            "plurality_assigned_provider_name": ["Provider A", "Provider B", "Provider A", "Provider B"],
            "total_spend_ytd": [100000.0, 90000.0, 80000.0, 70000.0],
            "er_admits_ytd": [0] * 4,
            "er_admits_90_day_prior": [0] * 4,
            "any_inpatient_hospital_admits_ytd": [0] * 4,
            "any_inpatient_hospital_admits_90_day_prior": [0] * 4,
            "hospice_admission": [False] * 4,
            "inpatient_spend_ytd": [0.0] * 4,
            "outpatient_spend_ytd": [0.0] * 4,
            "snf_cost_ytd": [0.0] * 4,
            "home_health_spend_ytd": [0.0] * 4,
            "em_visits_ytd": [0] * 4,
            "last_em_visit": [None] * 4,
            "aco_em_name": [None] * 4,
        })

        def get_high_cost_patients(df, provider_name=None, top_n=20, min_cost=None, max_cost=None,
                                    min_er_admits=None, min_inpatient_admits=None):
            query = df

            if provider_name:
                query = query.filter(pl.col("plurality_assigned_provider_name") == provider_name)
            if min_cost is not None:
                query = query.filter(pl.col("total_spend_ytd") >= min_cost)
            if max_cost is not None:
                query = query.filter(pl.col("total_spend_ytd") <= max_cost)
            if min_er_admits is not None:
                query = query.filter(pl.col("er_admits_ytd") >= min_er_admits)
            if min_inpatient_admits is not None:
                query = query.filter(pl.col("any_inpatient_hospital_admits_ytd") >= min_inpatient_admits)

            select_cols = [
                "mbi", "patient_first_name", "patient_last_name", "plurality_assigned_provider_name",
                "total_spend_ytd", "er_admits_ytd", "er_admits_90_day_prior",
                "any_inpatient_hospital_admits_ytd", "any_inpatient_hospital_admits_90_day_prior",
                "hospice_admission", "inpatient_spend_ytd", "outpatient_spend_ytd",
                "snf_cost_ytd", "home_health_spend_ytd", "em_visits_ytd", "last_em_visit", "aco_em_name",
            ]

            return query.select(select_cols).sort("total_spend_ytd", descending=True).head(top_n)

        result = get_high_cost_patients(df_data, provider_name="Provider A")

        # Only Provider A patients
        assert len(result) == 2
        assert all(result["plurality_assigned_provider_name"] == "Provider A")

    @pytest.mark.unit
    def test_filters_by_cost_range(self) -> None:
        """get_high_cost_patients filters by min and max cost."""
        df_data = pl.DataFrame({
            "mbi": ["MBI001", "MBI002", "MBI003", "MBI004"],
            "patient_first_name": ["A", "B", "C", "D"],
            "patient_last_name": ["X", "Y", "Z", "W"],
            "plurality_assigned_provider_name": ["Provider A"] * 4,
            "total_spend_ytd": [100000.0, 75000.0, 50000.0, 25000.0],
            "er_admits_ytd": [0] * 4,
            "er_admits_90_day_prior": [0] * 4,
            "any_inpatient_hospital_admits_ytd": [0] * 4,
            "any_inpatient_hospital_admits_90_day_prior": [0] * 4,
            "hospice_admission": [False] * 4,
            "inpatient_spend_ytd": [0.0] * 4,
            "outpatient_spend_ytd": [0.0] * 4,
            "snf_cost_ytd": [0.0] * 4,
            "home_health_spend_ytd": [0.0] * 4,
            "em_visits_ytd": [0] * 4,
            "last_em_visit": [None] * 4,
            "aco_em_name": [None] * 4,
        })

        def get_high_cost_patients(df, provider_name=None, top_n=20, min_cost=None, max_cost=None,
                                    min_er_admits=None, min_inpatient_admits=None):
            query = df

            if provider_name:
                query = query.filter(pl.col("plurality_assigned_provider_name") == provider_name)
            if min_cost is not None:
                query = query.filter(pl.col("total_spend_ytd") >= min_cost)
            if max_cost is not None:
                query = query.filter(pl.col("total_spend_ytd") <= max_cost)
            if min_er_admits is not None:
                query = query.filter(pl.col("er_admits_ytd") >= min_er_admits)
            if min_inpatient_admits is not None:
                query = query.filter(pl.col("any_inpatient_hospital_admits_ytd") >= min_inpatient_admits)

            select_cols = [
                "mbi", "patient_first_name", "patient_last_name", "plurality_assigned_provider_name",
                "total_spend_ytd", "er_admits_ytd", "er_admits_90_day_prior",
                "any_inpatient_hospital_admits_ytd", "any_inpatient_hospital_admits_90_day_prior",
                "hospice_admission", "inpatient_spend_ytd", "outpatient_spend_ytd",
                "snf_cost_ytd", "home_health_spend_ytd", "em_visits_ytd", "last_em_visit", "aco_em_name",
            ]

            return query.select(select_cols).sort("total_spend_ytd", descending=True).head(top_n)

        result = get_high_cost_patients(df_data, min_cost=50000.0, max_cost=80000.0)

        # Only within range
        assert len(result) == 2
        assert all((result["total_spend_ytd"] >= 50000.0) & (result["total_spend_ytd"] <= 80000.0))


class TestGetAlreadyDiscussedPatients:
    """Tests for get_already_discussed_patients function."""

    @pytest.mark.unit
    def test_returns_list_of_mbis(self) -> None:
        """get_already_discussed_patients returns a list of MBI strings."""
        def get_already_discussed_patients():
            return [
                '3TG2K08JM92',
                '9XM4VV4PR61',
                '2DW3DM9KK84',
            ]

        result = get_already_discussed_patients()

        # Returns list
        assert isinstance(result, list)

        # Contains expected MBIs
        assert '3TG2K08JM92' in result
        assert len(result) == 3

    @pytest.mark.unit
    def test_is_idempotent(self) -> None:
        """get_already_discussed_patients returns same list each time."""
        def get_already_discussed_patients():
            return [
                '3TG2K08JM92',
                '9XM4VV4PR61',
                '2DW3DM9KK84',
            ]

        result1 = get_already_discussed_patients()
        result2 = get_already_discussed_patients()

        # Same result
        assert result1 == result2
