# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for enrollment module."""

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

from acoharmony._tables.enrollment import Enrollment

if TYPE_CHECKING:
    pass


class TestEnrollment:
    """Tests for Enrollment."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_enrollment_schema_fields(self) -> None:
        """Enrollment has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(Enrollment)
        field_names = [f.name for f in fields]
        expected = [
            "member_id",
            "person_id",
            "enrollment_start_date",
            "enrollment_end_date",
            "payer",
            "payer_type",
            "plan",
            "gender",
            "birth_date",
            "death_date",
            "race",
            "ethnicity",
            "state",
            "zip_code",
            "county",
            "dual_status_code",
            "medicare_status_code",
            "original_reason_entitlement_code",
            "program_type",
            "assignment_method",
            "data_source",
            "processed_date",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_enrollment_data_types(self) -> None:
        """Enrollment field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(Enrollment)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "member_id": "str",
        "person_id": "str",
        "enrollment_start_date": "date",
        "enrollment_end_date": "date",
        "payer": "str",
        "payer_type": "str | None",
        "plan": "str | None",
        "gender": "str | None",
        "birth_date": "date | None",
        "death_date": "date | None",
        "race": "str | None",
        "ethnicity": "str | None",
        "state": "str | None",
        "zip_code": "str | None",
        "county": "str | None",
        "dual_status_code": "str | None",
        "medicare_status_code": "str | None",
        "original_reason_entitlement_code": "str | None",
        "program_type": "str | None",
        "assignment_method": "str | None",
        "data_source": "str | None",
        "processed_date": "date | None",
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
