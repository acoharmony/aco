from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._aco_transition_inactive import (
    build_inactive_expr,
    build_inactivity_duration_expr,
    get_inactive_beneficiaries,
)


class TestAcoTransitionInactive:
    """Tests for _aco_transition_inactive expression builders."""

    @pytest.mark.unit
    def test_build_inactive_expr(self):
        df = pl.DataFrame({'ym_202412_reach': [True, True], 'ym_202501_reach': [False, False], 'ym_202501_first_claim': [False, True], 'death_date': pl.Series([None, None], dtype=pl.Date)})
        ref = date(2025, 6, 1)
        result = df.select(build_inactive_expr(2025, 2024, reference_date=ref, current_year_months=[1]))
        assert result['inactive_2024'][0] is True
        assert result['inactive_2024'][1] is False

    @pytest.mark.unit
    def test_build_inactivity_duration_expr(self):
        ref = date(2025, 6, 1)
        df = pl.DataFrame({'ffs_first_date': [date(2025, 1, 1)], 'last_reach_date': [date(2024, 12, 1)], 'last_mssp_date': pl.Series([None], dtype=pl.Date)})
        result = df.select(build_inactivity_duration_expr(reference_date=ref))
        days = result['days_since_last_activity'][0]
        assert days is not None
        assert days > 0

class TestACOTransitionInactive:
    """Test ACO transition inactive expression."""

    @pytest.mark.unit
    def test_build_inactive_expr_default_date(self):
        """build_inactive_expr with default reference_date (line 41)."""
        expr = build_inactive_expr(2023, 2024)
        assert expr is not None

    @pytest.mark.unit
    def test_build_inactive_expr_default_months(self):
        """build_inactive_expr with default months (line 48)."""
        expr = build_inactive_expr(2023, 2024, reference_date=date(2024, 6, 1))
        assert expr is not None

    @pytest.mark.unit
    def test_build_inactivity_duration_default_date(self):
        """build_inactivity_duration_expr with default date (line 90)."""
        expr = build_inactivity_duration_expr()
        assert expr is not None

    @pytest.mark.unit
    def test_get_inactive_beneficiaries(self):
        """get_inactive_beneficiaries filters by inactivity threshold (lines 124-145)."""
        df = pl.DataFrame({'current_mbi': ['A', 'B'], 'ffs_first_date': [date(2020, 1, 1), date(2024, 1, 1)], 'last_reach_date': [date(2020, 6, 1), date(2024, 6, 1)], 'last_mssp_date': [date(2020, 3, 1), None], 'last_outreach_date': [None, None], 'death_date': [None, None]}).lazy()
        result = get_inactive_beneficiaries(df, 2023, 2024, inactivity_threshold_days=365)
        collected = result.collect()
        assert 'current_mbi' in collected.columns
        assert 'days_since_last_activity' in collected.columns
