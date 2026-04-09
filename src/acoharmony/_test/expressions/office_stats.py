from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._expressions._office_stats import (
    build_office_alignment_type_aggregations,
    build_office_alignment_type_derived_metrics,
    build_office_enrollment_aggregations,
    build_office_enrollment_derived_metrics,
    build_office_program_distribution_aggregations,
    build_office_transition_aggregations,
    build_office_transition_derived_metrics,
)


class TestOfficeStats:

    @pytest.mark.unit
    def test_build_office_enrollment_aggregations_with_cols(self):
        schema = ['ym_202401_reach', 'ym_202401_mssp', 'ym_202401_ffs', 'has_valid_voluntary_alignment']
        exprs = build_office_enrollment_aggregations('202401', schema)
        assert len(exprs) >= 5

    @pytest.mark.unit
    def test_build_office_enrollment_aggregations_missing_cols(self):
        exprs = build_office_enrollment_aggregations('202401', ['id'])
        assert len(exprs) >= 5

    @pytest.mark.unit
    def test_build_office_enrollment_derived_metrics(self):
        exprs = build_office_enrollment_derived_metrics()
        assert len(exprs) == 5

    @pytest.mark.unit
    def test_build_office_alignment_type_aggregations_all(self):
        schema = ['ym_202401_reach', 'ym_202401_mssp', 'has_valid_voluntary_alignment']
        exprs = build_office_alignment_type_aggregations('202401', schema)
        assert len(exprs) >= 4

    @pytest.mark.unit
    def test_build_office_alignment_type_aggregations_reach_only(self):
        exprs = build_office_alignment_type_aggregations('202401', ['ym_202401_reach'])
        assert len(exprs) >= 3

    @pytest.mark.unit
    def test_build_office_alignment_type_aggregations_mssp_only(self):
        exprs = build_office_alignment_type_aggregations('202401', ['ym_202401_mssp'])
        assert len(exprs) >= 3

    @pytest.mark.unit
    def test_build_office_alignment_type_aggregations_none(self):
        exprs = build_office_alignment_type_aggregations('202401', ['id'])
        assert len(exprs) >= 3

    @pytest.mark.unit
    def test_build_office_alignment_type_derived_metrics(self):
        exprs = build_office_alignment_type_derived_metrics()
        assert len(exprs) == 2

    @pytest.mark.unit
    def test_build_office_program_distribution_all(self):
        schema = ['ym_202401_reach', 'ym_202401_mssp', 'ever_reach', 'ever_mssp']
        exprs = build_office_program_distribution_aggregations('202401', schema)
        assert len(exprs) == 5

    @pytest.mark.unit
    def test_build_office_program_distribution_no_ever(self):
        schema = ['ym_202401_reach', 'ym_202401_mssp']
        exprs = build_office_program_distribution_aggregations('202401', schema)
        assert len(exprs) == 5

    @pytest.mark.unit
    def test_build_office_program_distribution_reach_only(self):
        exprs = build_office_program_distribution_aggregations('202401', ['ym_202401_reach'])
        assert len(exprs) == 5

    @pytest.mark.unit
    def test_build_office_program_distribution_mssp_only(self):
        exprs = build_office_program_distribution_aggregations('202401', ['ym_202401_mssp'])
        assert len(exprs) == 5

    @pytest.mark.unit
    def test_build_office_program_distribution_none(self):
        exprs = build_office_program_distribution_aggregations('202401', ['id'])
        assert len(exprs) == 5

    @pytest.mark.unit
    def test_build_office_transition_aggregations_all(self):
        schema = ['has_program_transition', 'has_continuous_enrollment', 'months_in_reach', 'months_in_mssp', 'total_aligned_months']
        exprs = build_office_transition_aggregations(schema)
        assert len(exprs) == 6

    @pytest.mark.unit
    def test_build_office_transition_aggregations_none(self):
        exprs = build_office_transition_aggregations(['id'])
        assert len(exprs) == 6

    @pytest.mark.unit
    def test_build_office_transition_derived_metrics(self):
        exprs = build_office_transition_derived_metrics()
        assert len(exprs) == 2
