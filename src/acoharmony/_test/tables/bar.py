# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for bar module."""

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

from acoharmony._tables.bar import Bar

if TYPE_CHECKING:
    pass


class TestBar:
    """Tests for Bar."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_bar_schema_fields(self) -> None:
        """Bar has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(Bar)
        field_names = [f.name for f in fields]
        expected = [
            "bene_mbi",
            "start_date",
            "end_date",
            "bene_first_name",
            "bene_last_name",
            "bene_address_line_1",
            "bene_address_line_2",
            "bene_address_line_3",
            "bene_address_line_4",
            "bene_address_line_5",
            "bene_address_line_6",
            "bene_city",
            "bene_state",
            "bene_zip_5",
            "bene_zip_4",
            "bene_county_ssa",
            "bene_county_fips",
            "bene_gender",
            "bene_race_ethnicity",
            "bene_date_of_birth",
            "bene_age",
            "bene_date_of_death",
            "bene_eligibility_year_1",
            "bene_eligibility_year_2",
            "bene_part_d_year_1",
            "bene_part_d_year_2",
            "newly_aligned_flag",
            "prospective_plus_flag",
            "claims_based_flag",
            "voluntary_alignment_type",
            "mobility_impairment_flag",
            "frailty_flag",
            "medium_risk_unplanned_flag",
            "high_risk_flag",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_bar_data_types(self) -> None:
        """Bar field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(Bar)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "bene_mbi": "str",
        "start_date": "date | None",
        "end_date": "date | None",
        "bene_first_name": "str | None",
        "bene_last_name": "str | None",
        "bene_address_line_1": "str | None",
        "bene_address_line_2": "str | None",
        "bene_address_line_3": "str | None",
        "bene_address_line_4": "str | None",
        "bene_address_line_5": "str | None",
        "bene_address_line_6": "str | None",
        "bene_city": "str | None",
        "bene_state": "str | None",
        "bene_zip_5": "str | None",
        "bene_zip_4": "str | None",
        "bene_county_ssa": "str | None",
        "bene_county_fips": "str | None",
        "bene_gender": "str | None",
        "bene_race_ethnicity": "str | None",
        "bene_date_of_birth": "date | None",
        "bene_age": "int | None",
        "bene_date_of_death": "date | None",
        "bene_eligibility_year_1": "str | None",
        "bene_eligibility_year_2": "str | None",
        "bene_part_d_year_1": "str | None",
        "bene_part_d_year_2": "str | None",
        "newly_aligned_flag": "str | None",
        "prospective_plus_flag": "str | None",
        "claims_based_flag": "str | None",
        "voluntary_alignment_type": "str | None",
        "mobility_impairment_flag": "str | None",
        "frailty_flag": "str | None",
        "medium_risk_unplanned_flag": "str | None",
        "high_risk_flag": "str | None",
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
