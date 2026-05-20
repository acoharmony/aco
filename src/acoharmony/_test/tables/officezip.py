# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for office_zip module."""

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

from acoharmony._tables.office_zip import OfficeZip

if TYPE_CHECKING:
    pass


class TestOfficeZip:
    """Tests for OfficeZip."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_officezip_schema_fields(self) -> None:
        """OfficeZip has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(OfficeZip)
        field_names = [f.name for f in fields]
        expected = [
            "zip_code",
            "state",
            "latitude",
            "longitude",
            "provider_code",
            "borderline_flag",
            "override_flag",
            "office_distance",
            "office_name",
            "market",
            "region_id",
            "region_name",
            "subdivision_name",
            "division_name",
            "rcmo_id",
            "rcmo_name",
            "meta_created_on",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_officezip_data_types(self) -> None:
        """OfficeZip field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(OfficeZip)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "zip_code": "str",
        "state": "str",
        "latitude": "float",
        "longitude": "float",
        "provider_code": "str | None",
        "borderline_flag": "str | None",
        "override_flag": "str | None",
        "office_distance": "float | None",
        "office_name": "str | None",
        "market": "str | None",
        "region_id": "str | None",
        "region_name": "str | None",
        "subdivision_name": "str | None",
        "division_name": "str | None",
        "rcmo_id": "str | None",
        "rcmo_name": "str | None",
        "meta_created_on": "str | None",
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
