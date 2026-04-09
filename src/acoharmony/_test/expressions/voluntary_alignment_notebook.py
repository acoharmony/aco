from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._voluntary_alignment_notebook import (
    build_email_engaged_expr,
    build_has_any_sva_expr,
    build_has_outreach_expr,
    build_has_valid_sva_expr,
    build_needs_outreach_expr,
    build_needs_sva_renewal_expr,
    build_voluntary_alignment_aggregations,
)


class TestVoluntaryAlignmentNotebook:
    """Tests for _voluntary_alignment_notebook expression builders."""

    @pytest.mark.unit
    def test_build_has_valid_sva_expr_with_col(self):
        df = pl.DataFrame({'has_valid_voluntary_alignment': [True, False]})
        result = df.select(build_has_valid_sva_expr(df.columns))
        assert result['has_valid_voluntary_alignment'].to_list() == [True, False]

    @pytest.mark.unit
    def test_build_has_valid_sva_expr_without_col(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_has_valid_sva_expr(df.columns))
        assert result['literal'][0] is False

    @pytest.mark.unit
    def test_build_has_any_sva_expr(self):
        df = pl.DataFrame({'has_voluntary_alignment': [True, False]})
        result = df.select(build_has_any_sva_expr(df.columns))
        assert result['has_voluntary_alignment'].to_list() == [True, False]

    @pytest.mark.unit
    def test_build_has_any_sva_expr_without_col(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_has_any_sva_expr(df.columns))
        assert result['literal'][0] is False

    @pytest.mark.unit
    def test_build_needs_sva_renewal_expr(self):
        df = pl.DataFrame({'has_voluntary_alignment': [True, True, False], 'has_valid_voluntary_alignment': [False, True, False]})
        result = df.select(build_needs_sva_renewal_expr(df.columns))
        assert result[0, 0] is True
        assert result[1, 0] is False
        assert result[2, 0] is False

    @pytest.mark.unit
    def test_build_has_outreach_expr(self):
        df = pl.DataFrame({'has_voluntary_outreach': [True, False]})
        result = df.select(build_has_outreach_expr(df.columns))
        assert result['has_voluntary_outreach'].to_list() == [True, False]

    @pytest.mark.unit
    def test_build_has_outreach_expr_without_col(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_has_outreach_expr(df.columns))
        assert result['literal'][0] is False

    @pytest.mark.unit
    def test_build_email_engaged_expr(self):
        df = pl.DataFrame({'voluntary_emails_opened': [5, 0]})
        result = df.select(build_email_engaged_expr(df.columns))
        assert result[0, 0] is True
        assert result[1, 0] is False

    @pytest.mark.unit
    def test_build_email_engaged_expr_without_col(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_email_engaged_expr(df.columns))
        assert result['literal'][0] is False

    @pytest.mark.unit
    def test_build_needs_outreach_expr(self):
        df = pl.DataFrame({'has_valid_voluntary_alignment': [False, True, False], 'has_voluntary_outreach': [False, False, True]})
        result = df.select(build_needs_outreach_expr(df.columns))
        assert result[0, 0] is True
        assert result[1, 0] is False
        assert result[2, 0] is False

    @pytest.mark.unit
    def test_build_voluntary_alignment_aggregations(self):
        df = pl.DataFrame({'has_valid_voluntary_alignment': [True, False, False], 'has_voluntary_alignment': [True, True, False], 'has_voluntary_outreach': [True, False, False], 'voluntary_emails_opened': [3, 0, 0]})
        aggs = build_voluntary_alignment_aggregations(df.columns)
        assert 'has_valid_sva_count' in aggs
        assert 'total_count' in aggs
        result = df.select(**aggs)
        assert result['has_valid_sva_count'][0] == 1
        assert result['has_any_sva_count'][0] == 2
        assert result['total_count'][0] == 3
