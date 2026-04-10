# © 2025 HarmonyCares
# All rights reserved.

"""Tests for consolidated alignment pipeline registration and schema."""

import pytest

from acoharmony._pipes._registry import PipelineRegistry
from acoharmony._tables.consolidated_alignment import ConsolidatedAlignment


class TestConsolidatedAlignmentPipelineRegistration:
    """Test that the consolidated alignment pipeline is properly registered."""

    @pytest.mark.unit
    def test_alignment_pipeline_is_registered(self):
        """The alignment pipeline should be registered in PipelineRegistry."""
        # Importing the module triggers registration via @register_pipeline
        import acoharmony._pipes._alignment  # noqa: F401

        pipelines = PipelineRegistry.list_pipelines()
        assert "alignment" in pipelines

class TestConsolidatedAlignmentSchema:
    """Test that the ConsolidatedAlignment dataclass has correct structure."""

    @pytest.mark.unit
    def test_has_key_beneficiary_fields(self):
        """ConsolidatedAlignment should have core beneficiary identification fields."""
        import dataclasses as dc

        field_names = [f.name for f in dc.fields(ConsolidatedAlignment)]
        assert "bene_mbi" in field_names
        assert "bene_first_name" in field_names
        assert "bene_last_name" in field_names

    @pytest.mark.unit
    def test_has_enrollment_fields(self):
        """ConsolidatedAlignment should have enrollment tracking fields."""
        import dataclasses as dc

        field_names = [f.name for f in dc.fields(ConsolidatedAlignment)]
        assert "enrollment_blocks" in field_names
        assert "block_start" in field_names
        assert "block_end" in field_names
        assert "is_currently_aligned" in field_names

    @pytest.mark.unit
    def test_has_program_fields(self):
        """ConsolidatedAlignment should have program-specific fields."""
        import dataclasses as dc

        field_names = [f.name for f in dc.fields(ConsolidatedAlignment)]
        assert "current_program" in field_names
        assert "reach_tin" in field_names
        assert "mssp_tin" in field_names

    @pytest.mark.unit
    def test_schema_name_via_registry(self):
        """ConsolidatedAlignment should be registered with name 'consolidated_alignment'."""
        assert ConsolidatedAlignment.schema_name() == "consolidated_alignment"
