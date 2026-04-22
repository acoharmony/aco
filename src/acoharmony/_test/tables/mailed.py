# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for mailed module."""

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

from acoharmony._tables.mailed import Mailed

if TYPE_CHECKING:
    pass


class TestMailed:
    """Tests for Mailed."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_mailed_schema_fields(self) -> None:
        """Mailed has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(Mailed)
        field_names = [f.name for f in fields]
        expected = [
            "aco_id",
            "campaign_name",
            "letter_id",
            "mbi",
            "network_id",
            "network_name",
            "patient_id",
            "external_patient_id",
            "patient_name",
            "practice_name",
            "send_datetime",
            "send_date",
            "send_timestamp",
            "status",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_mailed_data_types(self) -> None:
        """Mailed field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(Mailed)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "aco_id": "str",
        "campaign_name": "str",
        "letter_id": "str",
        "mbi": "str",
        "network_id": "str",
        "network_name": "str",
        "patient_id": "str",
        "external_patient_id": "str | None",
        "patient_name": "str",
        "practice_name": "str",
        "send_datetime": "date | None",
        "send_date": "date",
        "send_timestamp": "datetime",
        "status": "str",
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
