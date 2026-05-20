# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for alr module."""

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

from acoharmony._tables.alr import Alr

if TYPE_CHECKING:
    pass


class TestAlr:
    """Tests for Alr."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_alr_schema_fields(self) -> None:
        """Alr has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(Alr)
        field_names = [f.name for f in fields]
        expected = [
            "bene_mbi",
            "bene_hic_num",
            "bene_first_name",
            "bene_last_name",
            "bene_sex_cd",
            "bene_birth_dt",
            "death_date",
            "master_id",
            "b_em_line_cnt_t",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_alr_data_types(self) -> None:
        """Alr field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(Alr)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "bene_mbi": "str",
        "bene_hic_num": "str | None",
        "bene_first_name": "str | None",
        "bene_last_name": "str | None",
        "bene_sex_cd": "str | None",
        "bene_birth_dt": "date | None",
        "death_date": "date | None",
        "master_id": "str | None",
        "b_em_line_cnt_t": "str | None",
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
