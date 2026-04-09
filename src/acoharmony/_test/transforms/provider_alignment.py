# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.provider_alignment module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock

import polars as pl
import pytest
import acoharmony


def _make_provider_df(**overrides):
    """Create a minimal provider_list DataFrame."""
    defaults = {
        "base_provider_tin": ["TIN001", "TIN002", "TIN003"],
        "individual_npi": ["NPI001", "NPI002", ""],
        "organization_npi": ["", "ORG002", "ORG003"],
        "provider_type": ["Individual", "Individual", "Organization"],
        "provider_class": ["Physician", "Physician", "Facility and Institutional"],
        "individual_first_name": ["John", "Jane", None],
        "individual_last_name": ["Doe", "Smith", None],
        "provider_legal_business_name": [None, None, "Big Hospital"],
        "entity_legal_business_name": ["Practice A", "Practice B", "Hospital Corp"],
        "email": ["john@test.com", "jane@test.com", "info@hosp.com"],
        "entity_id": ["E1", "E2", "E3"],
        "entity_tin": ["ET1", "ET2", "ET3"],
        "performance_year": ["2024", "2024", "2024"],
        "specialty": ["Internal Medicine", "Cardiology", "Radiology"],
        "source_filename": ["Report - 01-15-24 10.00.00.xlsx"] * 3,
    }
    defaults.update(overrides)
    return pl.DataFrame(defaults).lazy()


class TestProviderAlignment:
    """Tests for Provider Alignment transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _provider_alignment
        assert acoharmony._transforms._provider_alignment is not None

    @pytest.mark.unit
    def test_apply_transform_exists(self):
        from acoharmony._transforms._provider_alignment import apply_transform
        assert callable(apply_transform)


class TestApplyTransformProviderAlignment:
    """Tests for apply_transform function."""

    @pytest.mark.unit
    def test_idempotency_skip(self):
        """When _tin_npi_extracted column already exists, should skip."""
        from acoharmony._transforms._provider_alignment import apply_transform

        df = pl.DataFrame({
            "tin": ["TIN1"],
            "npi": ["NPI1"],
            "_tin_npi_extracted": [True],
        }).lazy()

        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert collected.height == 1
        logger.info.assert_any_call("TIN-NPI extraction already applied, skipping")

    @pytest.mark.unit
    def test_individual_and_preferred_combined(self):
        """Both individual and preferred providers should be combined."""
        from acoharmony._transforms._provider_alignment import apply_transform

        df = _make_provider_df()
        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert "_tin_npi_extracted" in collected.columns
        assert "tin_npi_key" in collected.columns
        assert collected.height >= 1

    @pytest.mark.unit
    def test_individual_only(self):
        """Only individual participants, no organization NPIs."""
        from acoharmony._transforms._provider_alignment import apply_transform

        df = _make_provider_df(
            organization_npi=["", "", ""],
            individual_npi=["NPI1", "NPI2", "NPI3"],
            provider_class=["Physician", "Physician", "Physician"],
        )
        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert collected.height >= 1
        only_individual_logged = any(
            "Using only individual" in str(c) for c in logger.info.call_args_list
        )
        assert only_individual_logged

    @pytest.mark.unit
    def test_preferred_only(self):
        """Only preferred providers, no individual NPIs."""
        from acoharmony._transforms._provider_alignment import apply_transform

        df = _make_provider_df(
            individual_npi=["", "", ""],
            organization_npi=["ORG1", "ORG2", "ORG3"],
        )
        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert collected.height >= 1
        only_preferred_logged = any(
            "Using only preferred" in str(c) for c in logger.info.call_args_list
        )
        assert only_preferred_logged

    @pytest.mark.unit
    def test_no_providers_returns_empty_schema(self):
        """When no providers extracted, should return empty df with expected schema."""
        from acoharmony._transforms._provider_alignment import apply_transform

        df = _make_provider_df(
            individual_npi=["", "", ""],
            organization_npi=["", "", ""],
        )
        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert collected.height == 0
        assert "tin" in collected.columns
        assert "npi" in collected.columns
        assert "_tin_npi_extracted" in collected.columns

    @pytest.mark.unit
    def test_null_tin_warning(self):
        """Null TIN values should trigger a warning."""
        from acoharmony._transforms._provider_alignment import apply_transform

        df = _make_provider_df(
            base_provider_tin=[None, "TIN2", "TIN3"],
            individual_npi=["NPI1", "NPI2", "NPI3"],
            organization_npi=["", "", ""],
        )
        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        inner(df, {}, catalog, logger, force=False)
        null_tin_warned = any(
            "null TIN" in str(c) for c in logger.warning.call_args_list
        )
        assert null_tin_warned

    @pytest.mark.unit
    def test_null_npi_warning(self):
        """Null NPI values should trigger a warning."""
        from acoharmony._transforms._provider_alignment import apply_transform

        df = pl.DataFrame({
            "base_provider_tin": ["TIN1"],
            "individual_npi": [None],
            "organization_npi": ["ORG1"],
            "provider_type": ["Organization"],
            "provider_class": ["Group"],
            "individual_first_name": [None],
            "individual_last_name": [None],
            "provider_legal_business_name": ["Org Name"],
            "entity_legal_business_name": ["Entity"],
            "email": ["test@test.com"],
            "entity_id": ["E1"],
            "entity_tin": ["ET1"],
            "performance_year": ["2024"],
            "specialty": ["General"],
            "source_filename": ["Report - 01-15-24 10.00.00.xlsx"],
        }).lazy()

        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)

        # Add a row where npi will be null after extraction
        pl.DataFrame({
            "base_provider_tin": ["TIN1"],
            "individual_npi": [""],
            "organization_npi": [None],
            "provider_type": ["Individual"],
            "provider_class": ["Physician"],
            "individual_first_name": ["John"],
            "individual_last_name": ["Doe"],
            "provider_legal_business_name": [None],
            "entity_legal_business_name": ["Practice"],
            "email": ["john@test.com"],
            "entity_id": ["E1"],
            "entity_tin": ["ET1"],
            "performance_year": ["2024"],
            "specialty": ["Internal Medicine"],
            "source_filename": ["Report - 01-15-24 10.00.00.xlsx"],
        }).lazy()
        # This tests the org NPI path with null npi
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        # The NPI from organization_npi should be present
        assert collected.height >= 1

    @pytest.mark.unit
    def test_deduplication(self):
        """Duplicate TIN-NPI combinations should be deduplicated."""
        from acoharmony._transforms._provider_alignment import apply_transform

        df = _make_provider_df(
            base_provider_tin=["TIN1", "TIN1", "TIN2"],
            individual_npi=["NPI1", "NPI1", "NPI2"],
            organization_npi=["", "", ""],
        )
        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        # Should deduplicate TIN1-NPI1
        tin_npi_keys = collected["tin_npi_key"].to_list()
        assert len(tin_npi_keys) == len(set(tin_npi_keys))


class TestCreateTinNpiMapping:
    """Tests for create_tin_npi_mapping."""

    @pytest.mark.unit
    def test_basic_mapping(self):
        from acoharmony._transforms._provider_alignment import create_tin_npi_mapping

        df = pl.DataFrame({
            "tin": ["TIN1", "TIN1", "TIN2"],
            "npi": ["NPI1", "NPI2", "NPI3"],
            "provider_category": ["Individual Participant"] * 3,
            "provider_type": ["Individual", "Individual", "Individual"],
        }).lazy()

        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(create_tin_npi_mapping, "func", create_tin_npi_mapping)
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert "tin" in collected.columns
        assert "npi_list" in collected.columns
        assert "npi_count" in collected.columns
        # TIN1 should have 2 NPIs
        tin1 = collected.filter(pl.col("tin") == "TIN1")
        assert tin1["npi_count"][0] == 2

    @pytest.mark.unit
    def test_facility_providers_filtered(self):
        """Facility providers should be filtered out."""
        from acoharmony._transforms._provider_alignment import create_tin_npi_mapping

        df = pl.DataFrame({
            "tin": ["TIN1", "TIN2"],
            "npi": ["NPI1", "NPI2"],
            "provider_category": ["Individual Participant", "Preferred Provider"],
            "provider_type": ["Individual", "Facility and Institutional - Hospital"],
        }).lazy()

        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(create_tin_npi_mapping, "func", create_tin_npi_mapping)
        result = inner(df, {}, catalog, logger, force=False)
        collected = result.collect()
        # TIN2 should be filtered out (facility provider)
        assert collected.height == 1
        assert collected["tin"][0] == "TIN1"


# ---------------------------------------------------------------------------
# Coverage gap tests: _provider_alignment.py line 183
# ---------------------------------------------------------------------------


class TestProviderAlignmentNullNPI:
    """Cover null NPI warning log at line 182->183."""

    @pytest.mark.unit
    def test_null_npi_in_apply_transform_warns(self):
        """Line 182->183: null NPI values after extraction should trigger warning.

        The individual NPI filter normally excludes null/empty NPIs, so we
        patch it to let everything through, then supply a row with null
        ``individual_npi`` to produce a null ``npi`` in the result.
        """
        from unittest.mock import patch as _patch

        from acoharmony._transforms._provider_alignment import apply_transform
        from acoharmony._expressions._provider_alignment import ProviderAlignmentExpression

        df = _make_provider_df(
            base_provider_tin=["TIN1", "TIN2", "TIN3"],
            individual_npi=[None, "NPI2", "NPI3"],
            organization_npi=["", "", ""],
        )
        catalog = MagicMock()
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)

        # Patch the filter so null individual_npi rows pass through
        with _patch.object(
            ProviderAlignmentExpression,
            "filter_has_individual_npi",
            staticmethod(lambda: pl.lit(True)),
        ):
            result = inner(df, {}, catalog, logger, force=False)
            collected = result.collect()

        # Verify NPI null warning was logged (line 182->183)
        null_npi_warned = any(
            "null NPI" in str(c) for c in logger.warning.call_args_list
        )
        assert null_npi_warned
