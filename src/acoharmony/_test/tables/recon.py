# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for recon module."""

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

from acoharmony._tables.recon import Recon

if TYPE_CHECKING:
    pass


class TestRecon:
    """Tests for Recon."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_recon_schema_fields(self) -> None:
        """Recon has expected schema fields."""
        import dataclasses as dc

        fields = dc.fields(Recon)
        field_names = [f.name for f in fields]
        expected = [
            "aco_id",
            "performance_year",
            "report_quarter",
            "total_assigned_benes",
            "person_years",
            "avg_risk_score",
            "benchmark_expenditure",
            "actual_expenditure",
            "gross_savings",
            "msr_percent",
            "msr_amount",
            "mlr_percent",
            "mlr_amount",
            "quality_score",
            "quality_adjustment",
            "earned_savings",
            "owed_losses",
            "final_sharing_rate",
            "risk_arrangement",
            "benchmark_type",
            "trend_factor",
            "regional_adjustment",
            "prior_savings_adjustment",
            "sequestration_amount",
            "total_part_a_exp",
            "total_part_b_exp",
            "total_part_d_exp",
            "ip_admits_per_1000",
            "readmit_rate",
            "er_visits_per_1000",
            "awv_rate",
            "generic_rx_rate",
        ]
        assert field_names == expected

    @pytest.mark.unit
    def test_recon_data_types(self) -> None:
        """Recon field types match expectations."""
        import dataclasses as dc

        fields = dc.fields(Recon)
        type_map = {f.name: f.type for f in fields}
        expected = {
        "aco_id": "str",
        "performance_year": "int",
        "report_quarter": "str | None",
        "total_assigned_benes": "int | None",
        "person_years": "Decimal | None",
        "avg_risk_score": "Decimal | None",
        "benchmark_expenditure": "Decimal | None",
        "actual_expenditure": "Decimal | None",
        "gross_savings": "Decimal | None",
        "msr_percent": "Decimal | None",
        "msr_amount": "Decimal | None",
        "mlr_percent": "Decimal | None",
        "mlr_amount": "Decimal | None",
        "quality_score": "Decimal | None",
        "quality_adjustment": "Decimal | None",
        "earned_savings": "Decimal | None",
        "owed_losses": "Decimal | None",
        "final_sharing_rate": "Decimal | None",
        "risk_arrangement": "str | None",
        "benchmark_type": "str | None",
        "trend_factor": "Decimal | None",
        "regional_adjustment": "Decimal | None",
        "prior_savings_adjustment": "Decimal | None",
        "sequestration_amount": "Decimal | None",
        "total_part_a_exp": "Decimal | None",
        "total_part_b_exp": "Decimal | None",
        "total_part_d_exp": "Decimal | None",
        "ip_admits_per_1000": "Decimal | None",
        "readmit_rate": "Decimal | None",
        "er_visits_per_1000": "Decimal | None",
        "awv_rate": "Decimal | None",
        "generic_rx_rate": "Decimal | None",
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
