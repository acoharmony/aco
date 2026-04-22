# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for emails module."""

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

from acoharmony._tables.emails import Emails

if TYPE_CHECKING:
    pass


class TestEmails:
    """Tests for Emails."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_emails_schema_fields(self) -> None:
        """Emails has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(Emails)
        field_names = [f.name for f in fields]
        expected = [
            "aco_id",
            "campaign",
            "email_id",
            "has_been_clicked",
            "has_been_opened",
            "mbi",
            "network_id",
            "network_name",
            "patient_id",
            "external_patient_id",
            "patient_name",
            "practice",
            "send_datetime",
            "status",
            "send_date",
            "send_timestamp",
            "opened_flag",
            "clicked_flag",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_emails_data_types(self) -> None:
        """Emails field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(Emails)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "aco_id": "str",
        "campaign": "str",
        "email_id": "str",
        "has_been_clicked": "str",
        "has_been_opened": "str",
        "mbi": "str | None",
        "network_id": "str",
        "network_name": "str",
        "patient_id": "str",
        "external_patient_id": "str | None",
        "patient_name": "str",
        "practice": "str",
        "send_datetime": "date | None",
        "status": "str",
        "send_date": "date",
        "send_timestamp": "datetime",
        "opened_flag": "bool",
        "clicked_flag": "bool",
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
