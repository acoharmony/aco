# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for hdai_reach module."""

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

from acoharmony._tables.hdai_reach import HdaiReach

if TYPE_CHECKING:
    pass


class TestHdaiReach:
    """Tests for HdaiReach."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_hdaireach_schema_fields(self) -> None:
        """HdaiReach has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(HdaiReach)
        field_names = [f.name for f in fields]
        expected = [
            "mbi",
            "patient_first_name",
            "patient_last_name",
            "patient_dob",
            "patient_dod",
            "patient_address",
            "patient_city",
            "patient_state",
            "patient_zip",
            "enrollment_status",
            "plurality_assigned_provider_npi",
            "plurality_assigned_provider_name",
            "b_carrier_cost",
            "dme_spend_ytd",
            "hospice_spend_ytd",
            "outpatient_spend_ytd",
            "snf_cost_ytd",
            "inpatient_spend_ytd",
            "home_health_spend_ytd",
            "total_spend_ytd",
            "wound_spend_ytd",
            "apcm_spend_ytd",
            "em_cost_ytd",
            "any_inpatient_hospital_admits_ytd",
            "any_inpatient_hospital_admits_90_day_prior",
            "er_admits_ytd",
            "er_admits_90_day_prior",
            "em_visits_ytd",
            "hospice_admission",
            "snf",
            "irf",
            "ltac",
            "home_health",
            "most_recent_awv_date",
            "awv_claim_id",
            "last_em_visit",
            "aco_em_npi",
            "aco_em_name",
            "flag_em_hcmg",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_hdaireach_data_types(self) -> None:
        """HdaiReach field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(HdaiReach)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "mbi": "str",
        "patient_first_name": "str | None",
        "patient_last_name": "str | None",
        "patient_dob": "date | None",
        "patient_dod": "date | None",
        "patient_address": "str | None",
        "patient_city": "str | None",
        "patient_state": "str | None",
        "patient_zip": "str | None",
        "enrollment_status": "str | None",
        "plurality_assigned_provider_npi": "str | None",
        "plurality_assigned_provider_name": "str | None",
        "b_carrier_cost": "Decimal | None",
        "dme_spend_ytd": "Decimal | None",
        "hospice_spend_ytd": "Decimal | None",
        "outpatient_spend_ytd": "Decimal | None",
        "snf_cost_ytd": "Decimal | None",
        "inpatient_spend_ytd": "Decimal | None",
        "home_health_spend_ytd": "Decimal | None",
        "total_spend_ytd": "Decimal | None",
        "wound_spend_ytd": "Decimal | None",
        "apcm_spend_ytd": "Decimal | None",
        "em_cost_ytd": "Decimal | None",
        "any_inpatient_hospital_admits_ytd": "int | None",
        "any_inpatient_hospital_admits_90_day_prior": "int | None",
        "er_admits_ytd": "int | None",
        "er_admits_90_day_prior": "int | None",
        "em_visits_ytd": "int | None",
        "hospice_admission": "bool | None",
        "snf": "str | None",
        "irf": "str | None",
        "ltac": "str | None",
        "home_health": "str | None",
        "most_recent_awv_date": "date | None",
        "awv_claim_id": "str | None",
        "last_em_visit": "date | None",
        "aco_em_npi": "str | None",
        "aco_em_name": "str | None",
        "flag_em_hcmg": "str | None",
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
