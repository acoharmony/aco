from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._financial_pmpm import FinancialPmpmExpression


class TestFinancialPmpmExpression:

    @pytest.mark.unit
    def test_import(self):
        assert FinancialPmpmExpression is not None

    @pytest.mark.unit
    def test_build_returns_expressions_and_config(self):
        """Cover lines 75-106: build() returns expressions dict with defaults."""
        result = FinancialPmpmExpression.build({})
        assert "expressions" in result
        assert "config" in result
        cfg = result["config"]
        assert cfg["time_period"] == "month"
        assert cfg["paid_amount_column"] == "paid_amount"
        assert cfg["patient_id_column"] == "patient_id"

    @pytest.mark.unit
    def test_build_with_custom_config(self):
        """build() respects custom config values."""
        result = FinancialPmpmExpression.build({
            "time_period": "quarter",
            "paid_amount_column": "total_paid",
        })
        assert result["config"]["time_period"] == "quarter"
        assert result["config"]["paid_amount_column"] == "total_paid"

    @pytest.mark.unit
    def test_build_expressions_keys(self):
        """build() returns expected expression keys."""
        result = FinancialPmpmExpression.build({})
        exprs = result["expressions"]
        assert "calculate_member_months" in exprs
        assert "categorize_spend" in exprs
        assert "aggregate_spend" in exprs
        assert "calculate_pmpm" in exprs


class TestTransformPatientSpendByCategory:
    """Cover lines 128-159: transform_patient_spend_by_category."""

    @pytest.mark.unit
    def test_basic_spend_aggregation(self):
        """Medical + pharmacy claims aggregated by patient/month/category."""
        medical = pl.DataFrame({
            "patient_id": ["P1", "P1", "P2"],
            "claim_end_date": [date(2024, 1, 15), date(2024, 1, 20), date(2024, 2, 10)],
            "paid_amount": [100.0, 200.0, 50.0],
        }).lazy()
        pharmacy = pl.DataFrame({
            "patient_id": ["P1"],
            "claim_end_date": [date(2024, 1, 5)],
            "paid_amount": [25.0],
        }).lazy()
        service_cats = pl.DataFrame({"id": [1]}).lazy()
        member_months = pl.DataFrame({"id": [1]}).lazy()

        result = FinancialPmpmExpression.transform_patient_spend_by_category(
            medical, pharmacy, member_months, service_cats, {}
        ).collect()

        assert "patient_id" in result.columns
        assert "year_month" in result.columns
        assert "service_category" in result.columns
        assert "paid_amount" in result.columns
        assert "claim_count" in result.columns
        # P1 medical in 2024-01 should have 2 claims
        p1_med = result.filter(
            (pl.col("patient_id") == "P1")
            & (pl.col("service_category") == "medical")
        )
        assert p1_med["claim_count"][0] == 2
        assert p1_med["paid_amount"][0] == 300.0


class TestTransformPmpmByPayer:
    """Cover lines 177-195: transform_pmpm_by_payer."""

    @pytest.mark.unit
    def test_pmpm_calculation(self):
        """PMPM = total_spend / total_member_months."""
        patient_spend = pl.DataFrame({
            "patient_id": ["P1", "P2"],
            "year_month": ["2024-01", "2024-01"],
            "service_category": ["medical", "medical"],
            "paid_amount": [1000.0, 500.0],
            "claim_count": [3, 1],
        }).lazy()
        member_months = pl.DataFrame({
            "patient_id": ["P1", "P2"],
            "year_month": ["2024-01", "2024-01"],
            "member_months": [1, 1],
        }).lazy()

        result = FinancialPmpmExpression.transform_pmpm_by_payer(
            patient_spend, member_months, {}
        ).collect()

        assert "pmpm" in result.columns
        assert "total_spend" in result.columns
        assert "total_member_months" in result.columns
        row = result.filter(pl.col("service_category") == "medical")
        assert row["total_spend"][0] == 1500.0
        assert row["total_member_months"][0] == 2
        assert row["pmpm"][0] == 750.0
