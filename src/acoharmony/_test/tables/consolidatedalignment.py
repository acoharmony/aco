# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for consolidated_alignment module."""

# TODO: Implement comprehensive tests for this module
# TODO: Add property-based tests using hypothesis where appropriate
# TODO: Ensure all edge cases are covered
# TODO: Add integration tests if needed

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

from acoharmony._tables.consolidated_alignment import ConsolidatedAlignment

if TYPE_CHECKING:
    pass


class TestConsolidatedAlignment:
    """Tests for ConsolidatedAlignment."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_consolidatedalignment_schema_fields(self) -> None:
        """ConsolidatedAlignment has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(ConsolidatedAlignment)
        field_names = [f.name for f in fields]
        expected = [
            "bene_mbi",
            "bene_first_name",
            "bene_last_name",
            "bene_middle_initial",
            "bene_state",
            "bene_zip_5",
            "bene_county",
            "office_location",
            "bene_death_date",
            "enrollment_blocks",
            "block_year",
            "block_month",
            "block_year_month",
            "block_start",
            "block_end",
            "enrollment_start",
            "enrollment_end",
            "current_program",
            "current_source_type",
            "file_temporal",
            "source_file_type",
            "source_period",
            "blocks_per_file",
            "current_alignment_source",
            "is_currently_aligned",
            "reach_attribution_type",
            "reach_tin",
            "reach_npi",
            "reach_provider_name",
            "mssp_tin",
            "mssp_npi",
            "mssp_provider_name",
            "has_voluntary_alignment",
            "has_valid_voluntary_alignment",
            "voluntary_alignment_date",
            "voluntary_alignment_type",
            "voluntary_provider_npi",
            "voluntary_provider_tin",
            "voluntary_provider_name",
            "first_valid_signature_date",
            "last_valid_signature_date",
            "last_signature_expiry_date",
            "signature_expiry_date",
            "signature_validity",
            "signature_valid_for_block",
            "first_sva_submission_date",
            "last_sva_submission_date",
            "last_sva_provider_name",
            "last_sva_provider_npi",
            "last_sva_provider_tin",
            "aligned_practitioner_name",
            "aligned_provider_tin",
            "aligned_provider_npi",
            "aligned_provider_org",
            "response_code_list",
            "latest_response_codes",
            "latest_response_detail",
            "error_category",
            "eligibility_issues",
            "precedence_issues",
            "previous_invalids",
            "previous_program",
            "enrollment_transition_date",
            "previous_source_type",
            "previous_alignment_source",
            "program_transitions",
            "transition_history",
            "days_in_current_program",
            "total_days_aligned",
            "alignment_days",
            "current_program_days",
            "previous_program_days",
            "reach_months",
            "mssp_months",
            "first_reach_date",
            "last_reach_date",
            "first_mssp_date",
            "last_mssp_date",
            "total_aligned_months",
            "latest_aco_id",
            "source_files",
            "num_source_files",
            "most_recent_file",
            "data_as_of_date",
            "last_updated",
            "lineage_source",
            "lineage_processed_at",
            "lineage_transform",
            "temporal_context",
            "transition_info",
            "gap_months",
            "is_transition",
            "continuous_enrollment",
            "enrollment_span_id",
            "reconciliation_info",
            "precedence_score",
            "max_observable_date",
            "max_recon_date",
            "max_current_date",
            "sva_tin_match",
            "sva_npi_match",
            "provider_on_current_list",
            "needs_provider_refresh",
            "invalid_provider_count",
            "prvs_num",
            "mapping_type",
            "hcmpi",
            "has_multiple_prvs_mbi",
            "sva_submitted_after_pbvar",
            "needs_sva_refresh_from_pbvar",
            "pbvar_report_date",
            "pbvar_response_codes",
            "signature_valid_for_current_py",
            "days_until_signature_expiry",
            "sva_outreach_priority",
            "signature_valid_for_pys",
            "mssp_sva_recruitment_target",
            "mssp_to_reach_status",
            "sva_action_needed",
            "has_ineligible_alignment",
            "sva_used_crosswalk",
            "crosswalked_sva_count",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_consolidatedalignment_data_types(self) -> None:
        """ConsolidatedAlignment field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(ConsolidatedAlignment)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "bene_mbi": "str",
        "bene_first_name": "str | None",
        "bene_last_name": "str | None",
        "bene_middle_initial": "str | None",
        "bene_state": "str | None",
        "bene_zip_5": "str | None",
        "bene_county": "str | None",
        "office_location": "str | None",
        "bene_death_date": "date | None",
        "enrollment_blocks": "str",
        "block_year": "str | None",
        "block_month": "str | None",
        "block_year_month": "str | None",
        "block_start": "date",
        "block_end": "date",
        "enrollment_start": "date | None",
        "enrollment_end": "date | None",
        "current_program": "str",
        "current_source_type": "str | None",
        "file_temporal": "str | None",
        "source_file_type": "str | None",
        "source_period": "str | None",
        "blocks_per_file": "str | None",
        "current_alignment_source": "str | None",
        "is_currently_aligned": "bool",
        "reach_attribution_type": "str | None",
        "reach_tin": "str | None",
        "reach_npi": "str | None",
        "reach_provider_name": "str | None",
        "mssp_tin": "str | None",
        "mssp_npi": "str | None",
        "mssp_provider_name": "str | None",
        "has_voluntary_alignment": "bool | None",
        "has_valid_voluntary_alignment": "bool | None",
        "voluntary_alignment_date": "date | None",
        "voluntary_alignment_type": "str | None",
        "voluntary_provider_npi": "str | None",
        "voluntary_provider_tin": "str | None",
        "voluntary_provider_name": "str | None",
        "first_valid_signature_date": "date | None",
        "last_valid_signature_date": "date | None",
        "last_signature_expiry_date": "date | None",
        "signature_expiry_date": "date | None",
        "signature_validity": "str | None",
        "signature_valid_for_block": "bool | None",
        "first_sva_submission_date": "date | None",
        "last_sva_submission_date": "date | None",
        "last_sva_provider_name": "str | None",
        "last_sva_provider_npi": "str | None",
        "last_sva_provider_tin": "str | None",
        "aligned_practitioner_name": "str | None",
        "aligned_provider_tin": "str | None",
        "aligned_provider_npi": "str | None",
        "aligned_provider_org": "str | None",
        "response_code_list": "str | None",
        "latest_response_codes": "str | None",
        "latest_response_detail": "str | None",
        "error_category": "str | None",
        "eligibility_issues": "str | None",
        "precedence_issues": "str | None",
        "previous_invalids": "str | None",
        "previous_program": "str | None",
        "enrollment_transition_date": "date | None",
        "previous_source_type": "str | None",
        "previous_alignment_source": "str | None",
        "program_transitions": "int | None",
        "transition_history": "str | None",
        "days_in_current_program": "int | None",
        "total_days_aligned": "int | None",
        "alignment_days": "str | None",
        "current_program_days": "str | None",
        "previous_program_days": "str | None",
        "reach_months": "str | None",
        "mssp_months": "str | None",
        "first_reach_date": "date | None",
        "last_reach_date": "date | None",
        "first_mssp_date": "date | None",
        "last_mssp_date": "date | None",
        "total_aligned_months": "str | None",
        "latest_aco_id": "str | None",
        "source_files": "str | None",
        "num_source_files": "str | None",
        "most_recent_file": "str | None",
        "data_as_of_date": "date | None",
        "last_updated": "str | None",
        "lineage_source": "str | None",
        "lineage_processed_at": "str | None",
        "lineage_transform": "str | None",
        "temporal_context": "str | None",
        "transition_info": "str | None",
        "gap_months": "str | None",
        "is_transition": "bool | None",
        "continuous_enrollment": "str | None",
        "enrollment_span_id": "str | None",
        "reconciliation_info": "str | None",
        "precedence_score": "str | None",
        "max_observable_date": "date | None",
        "max_recon_date": "date | None",
        "max_current_date": "date | None",
        "sva_tin_match": "bool | None",
        "sva_npi_match": "bool | None",
        "provider_on_current_list": "bool | None",
        "needs_provider_refresh": "bool | None",
        "invalid_provider_count": "int | None",
        "prvs_num": "str | None",
        "mapping_type": "str | None",
        "hcmpi": "str | None",
        "has_multiple_prvs_mbi": "bool | None",
        "sva_submitted_after_pbvar": "bool | None",
        "needs_sva_refresh_from_pbvar": "bool | None",
        "pbvar_report_date": "date | None",
        "pbvar_response_codes": "str | None",
        "signature_valid_for_current_py": "bool | None",
        "days_until_signature_expiry": "int | None",
        "sva_outreach_priority": "str | None",
        "signature_valid_for_pys": "str | None",
        "mssp_sva_recruitment_target": "bool | None",
        "mssp_to_reach_status": "str | None",
        "sva_action_needed": "str | None",
        "has_ineligible_alignment": "bool | None",
        "sva_used_crosswalk": "bool | None",
        "crosswalked_sva_count": "int | None",
        }
        for name, expected_type_str in expected.items():
            actual = type_map[name]
            if isinstance(actual, type):
                actual_str = actual.__name__
                if actual.__module__ not in ("builtins",):
                    actual_str = f"{actual.__module__}.{actual.__name__}"
                actual_str = actual_str.replace("datetime.", "").replace("decimal.", "")
            else:
                actual_str = str(actual).replace("datetime.", "").replace("decimal.", "")
            assert actual_str == expected_type_str, f"{name}: {actual_str} != {expected_type_str}"
