from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._aco_transition_expired_sva import build_expired_sva_expr


class TestAcoTransitionExpiredSva:
    """Tests for _aco_transition_expired_sva expression builders."""

    @pytest.mark.unit
    def test_build_expired_sva_expr(self):
        df = pl.DataFrame({'has_voluntary_alignment': [True, True, False], 'last_valid_signature_date': [date(2024, 6, 1), date(2025, 3, 1), None], 'ym_202412_reach': [True, True, True], 'ym_202501_reach': [False, False, False]})
        result = df.select(build_expired_sva_expr(2025, 2024, current_year_months=[1]))
        assert result['expired_sva_2024'][0] is True
        assert result['expired_sva_2024'][1] is False
        assert result['expired_sva_2024'][2] is False

    @pytest.mark.unit
    def test_build_expired_sva_expr_none_months_defaults_to_all(self):
        """Line 41->42: when current_year_months is None, it defaults to all
        12 months (range 1..12).  We verify that passing None produces the
        same result as passing list(range(1,13)) explicitly.
        """
        # Build a DataFrame that has reach columns for all 12 months of the
        # current year (2025) so that the expression can be evaluated.
        data: dict[str, list] = {
            "has_voluntary_alignment": [True, False],
            "last_valid_signature_date": [date(2024, 6, 1), None],
            "ym_202412_reach": [True, True],
        }
        for m in range(1, 13):
            data[f"ym_2025{m:02d}_reach"] = [False, False]

        df = pl.DataFrame(data)

        result_none = df.select(
            build_expired_sva_expr(2025, 2024, current_year_months=None)
        )
        result_explicit = df.select(
            build_expired_sva_expr(2025, 2024, current_year_months=list(range(1, 13)))
        )

        assert result_none["expired_sva_2024"].to_list() == result_explicit["expired_sva_2024"].to_list()
        # First row: had voluntary alignment, signature expired, was in REACH
        # in Dec prev, not in REACH in any current month -> True
        assert result_none["expired_sva_2024"][0] is True
        # Second row: no voluntary alignment -> False
        assert result_none["expired_sva_2024"][1] is False

    @pytest.mark.unit
    def test_get_sva_expirations(self):
        sva_df = pl.LazyFrame({'normalized_mbi': ['MBI1', 'MBI2'], 'signature_date': [date(2024, 3, 1), date(2025, 2, 1)], 'most_recent_sva_date': [date(2024, 3, 1), date(2025, 2, 1)]})
        voluntary_df = pl.LazyFrame({'x': [1]})
        result = get_sva_expirations(sva_df, voluntary_df, 2024, 2025).collect()
        assert result.height == 1
        assert result['current_mbi'][0] == 'MBI1'
