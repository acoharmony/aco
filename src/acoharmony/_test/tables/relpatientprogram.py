# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for rel_patient_program module."""

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

from acoharmony._tables.rel_patient_program import RelPatientProgram

if TYPE_CHECKING:
    pass


class TestRelPatientProgram:
    """Tests for RelPatientProgram."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_relpatientprogram_schema_fields(self) -> None:
        """RelPatientProgram has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(RelPatientProgram)
        field_names = [f.name for f in fields]
        expected = [
            "patient_id",
            "program_id",
            "program_name",
            "enrollment_date",
            "updated_date",
            "disenrollment_date",
            "status",
            "created_date",
            "mrn",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_relpatientprogram_data_types(self) -> None:
        """RelPatientProgram field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(RelPatientProgram)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "patient_id": "str",
        "program_id": "str",
        "program_name": "str | None",
        "enrollment_date": "date | None",
        "updated_date": "date | None",
        "disenrollment_date": "date | None",
        "status": "str | None",
        "created_date": "date | None",
        "mrn": "str | None",
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
