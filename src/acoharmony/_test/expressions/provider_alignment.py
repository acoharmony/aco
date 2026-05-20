# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._provider_alignment module."""

import polars as pl
import pytest

from acoharmony._expressions._provider_alignment import ProviderAlignmentExpression


class TestCleanIndividualNpi:
    """Cover clean_individual_npi_expr line 91."""

    @pytest.mark.unit
    def test_strips_whitespace(self):
        df = pl.DataFrame({"individual_npi": ["  1234567890  ", "0987654321"]})
        result = df.select(ProviderAlignmentExpression.clean_individual_npi_expr())
        assert result["npi"][0] == "1234567890"
        assert result["npi"][1] == "0987654321"


class TestBuildProviderNameFromParts:
    """Cover build_provider_name_from_parts line 109."""

    @pytest.mark.unit
    def test_concat_name(self):
        df = pl.DataFrame({"first_name": ["John"], "last_name": ["Doe"]})
        result = df.select(ProviderAlignmentExpression.build_provider_name_from_parts())
        assert result["provider_name"][0] == "John Doe"


class TestStandardizeTin:
    """Cover standardize_tin line 212."""

    @pytest.mark.unit
    def test_tin_standardized(self):
        df = pl.DataFrame({"base_provider_tin": ["  123456789  "]})
        result = df.select(ProviderAlignmentExpression.standardize_tin())
        assert result["tin"][0] == "123456789"


class TestStandardizeNpi:
    """Cover standardize_npi line 230."""

    @pytest.mark.unit
    def test_npi_padded(self):
        df = pl.DataFrame({"npi": ["12345"]})
        result = df.select(ProviderAlignmentExpression.standardize_npi())
        assert result["npi"][0] == "0000012345"


class TestBuildProviderCategoryLabel:
    """Cover build_provider_category_label line 246."""

    @pytest.mark.unit
    def test_categories(self):
        df = pl.DataFrame({
            "individual_npi": ["1234567890", None, None],
            "organization_npi": [None, "9876543210", None],
        })
        result = df.select(ProviderAlignmentExpression.build_provider_category_label())
        cats = result["provider_category"].to_list()
        assert cats[0] == "Individual Participant"
        assert cats[1] == "Preferred Provider"
        assert cats[2] == "Unknown"
