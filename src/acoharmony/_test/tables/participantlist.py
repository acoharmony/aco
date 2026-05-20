"""Tests for acoharmony._tables.participant_list module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._tables.participant_list import ParticipantList

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._tables.participant_list is not None


class TestParticipantList:
    """Tests for ParticipantList."""

    @pytest.mark.unit
    def test_participantlist_schema_fields(self) -> None:
        """ParticipantList has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(ParticipantList)
        field_names = [f.name for f in fields]
        expected = [
            "entity_id",
            "entity_tin",
            "entity_legal_business_name",
            "performance_year",
            "provider_type",
            "provider_class",
            "provider_legal_business_name",
            "individual_npi",
            "individual_first_name",
            "individual_last_name",
            "base_provider_tin",
            "organization_npi",
            "ccn",
            "sole_proprietor",
            "sole_proprietor_tin",
            "primary_care_services",
            "specialty",
            "base_provider_tin_status",
            "base_provider_tin_dropped_terminated_reason",
            "effective_start_date",
            "effective_end_date",
            "last_updated_date",
            "ad_hoc_provider_addition_reason",
            "pecos_check_results",
            "uses_cehrt",
            "cehrt_attestation",
            "cehrt_id",
            "low_volume_exception",
            "mips_exception",
            "mips_reweighting_exception",
            "other",
            "overlaps_deficiencies",
            "attestation_y_n",
            "total_care_capitation_pct_reduction",
            "primary_care_capitation_pct_reduction",
            "advanced_payment_pct_reduction",
            "cardiac_pulmonary_rehabilitation",
            "care_management_home_visit",
            "concurrent_care_for_hospice",
            "chronic_disease_management_reward",
            "cost_sharing_for_part_b",
            "diabetic_shoes",
            "home_health_homebound_waiver",
            "home_infusion_therapy",
            "hospice_care_certification",
            "medical_nutrition_therapy",
            "nurse_practitioner_services",
            "post_discharge_home_visit",
            "snf_3_day_stay_waiver",
            "telehealth",
            "email",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_participantlist_data_types(self) -> None:
        """ParticipantList field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(ParticipantList)
        type_map = {f.name: f.type for f in fields}
        expected = {
            "entity_id": "str | None",
            "entity_tin": "str | None",
            "entity_legal_business_name": "str | None",
            "performance_year": "str | None",
            "provider_type": "str | None",
            "provider_class": "str | None",
            "provider_legal_business_name": "str | None",
            "individual_npi": "str | None",
            "individual_first_name": "str | None",
            "individual_last_name": "str | None",
            "base_provider_tin": "str | None",
            "organization_npi": "str | None",
            "ccn": "str | None",
            "sole_proprietor": "str | None",
            "sole_proprietor_tin": "str | None",
            "primary_care_services": "str | None",
            "specialty": "str | None",
            "base_provider_tin_status": "str | None",
            "base_provider_tin_dropped_terminated_reason": "str | None",
            "effective_start_date": "date | None",
            "effective_end_date": "date | None",
            "last_updated_date": "date | None",
            "ad_hoc_provider_addition_reason": "str | None",
            "pecos_check_results": "str | None",
            "uses_cehrt": "str | None",
            "cehrt_attestation": "str | None",
            "cehrt_id": "str | None",
            "low_volume_exception": "str | None",
            "mips_exception": "str | None",
            "mips_reweighting_exception": "str | None",
            "other": "str | None",
            "overlaps_deficiencies": "str | None",
            "attestation_y_n": "str | None",
            "total_care_capitation_pct_reduction": "str | None",
            "primary_care_capitation_pct_reduction": "str | None",
            "advanced_payment_pct_reduction": "str | None",
            "cardiac_pulmonary_rehabilitation": "str | None",
            "care_management_home_visit": "str | None",
            "concurrent_care_for_hospice": "str | None",
            "chronic_disease_management_reward": "str | None",
            "cost_sharing_for_part_b": "str | None",
            "diabetic_shoes": "str | None",
            "home_health_homebound_waiver": "str | None",
            "home_infusion_therapy": "str | None",
            "hospice_care_certification": "str | None",
            "medical_nutrition_therapy": "str | None",
            "nurse_practitioner_services": "str | None",
            "post_discharge_home_visit": "str | None",
            "snf_3_day_stay_waiver": "str | None",
            "telehealth": "str | None",
            "email": "str | None",
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
