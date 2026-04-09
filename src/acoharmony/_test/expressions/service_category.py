# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._service_category module."""

import polars as pl
import pytest

from acoharmony._expressions._service_category import ServiceCategoryExpression


class TestServiceCategoryBuild:
    """Tests for ServiceCategoryExpression.build()."""

    @pytest.mark.unit
    def test_pharmacy_claim(self):
        """Pharmacy claims classified as 'pharmacy'."""
        df = pl.DataFrame({
            "claim_type": ["pharmacy"],
            "bill_type_code": [""],
            "revenue_code": [""],
            "place_of_service_code": [""],
        })
        expr = ServiceCategoryExpression.build({})
        result = df.select(expr)
        assert result["service_category_2"][0] == "pharmacy"

    @pytest.mark.unit
    def test_institutional_inpatient(self):
        """Institutional claim with bill type 11x → acute inpatient."""
        df = pl.DataFrame({
            "claim_type": ["institutional"],
            "bill_type_code": ["111"],
            "revenue_code": ["0100"],
            "place_of_service_code": [""],
        })
        result = df.select(ServiceCategoryExpression.build({}))
        assert result["service_category_2"][0] == "acute inpatient"

    @pytest.mark.unit
    def test_institutional_snf(self):
        """SNF bill type 21x → skilled nursing facility."""
        df = pl.DataFrame({
            "claim_type": ["institutional"],
            "bill_type_code": ["211"],
            "revenue_code": ["0100"],
            "place_of_service_code": [""],
        })
        result = df.select(ServiceCategoryExpression.build({}))
        assert result["service_category_2"][0] == "skilled nursing facility"

    @pytest.mark.unit
    def test_institutional_ed(self):
        """ED revenue code 0450 → emergency department."""
        df = pl.DataFrame({
            "claim_type": ["institutional"],
            "bill_type_code": ["131"],
            "revenue_code": ["0450"],
            "place_of_service_code": [""],
        })
        result = df.select(ServiceCategoryExpression.build({}))
        assert result["service_category_2"][0] == "emergency department"

    @pytest.mark.unit
    def test_professional_office(self):
        """Professional POS 11 → office-based."""
        df = pl.DataFrame({
            "claim_type": ["professional"],
            "bill_type_code": [""],
            "revenue_code": [""],
            "place_of_service_code": ["11"],
        })
        result = df.select(ServiceCategoryExpression.build({}))
        assert result["service_category_2"][0] == "office-based"

    @pytest.mark.unit
    def test_default_other(self):
        """Unknown claim type → other."""
        df = pl.DataFrame({
            "claim_type": ["unknown"],
            "bill_type_code": [""],
            "revenue_code": [""],
            "place_of_service_code": [""],
        })
        result = df.select(ServiceCategoryExpression.build({}))
        assert result["service_category_2"][0] == "other"


class TestCategorizeClaimsMethod:
    """Tests for ServiceCategoryExpression.categorize_claims()."""

    @pytest.mark.unit
    def test_categorize_adds_both_columns(self):
        """categorize_claims adds service_category_1 and service_category_2."""
        df = pl.DataFrame({
            "claim_type": ["pharmacy", "institutional", "professional"],
            "bill_type_code": ["", "111", ""],
            "revenue_code": ["", "0100", ""],
            "place_of_service_code": ["", "", "11"],
        }).lazy()

        result = ServiceCategoryExpression.categorize_claims(df, {}).collect()
        assert "service_category_1" in result.columns
        assert "service_category_2" in result.columns
        # Pharmacy → outpatient (per the classification)
        # Acute inpatient → inpatient
        # Office-based → office-based
        cats = result["service_category_1"].to_list()
        assert "inpatient" in cats
        assert "office-based" in cats
