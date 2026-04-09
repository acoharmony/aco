from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._voluntary_alignment_emails import (
    build_email_aggregation_exprs,
    build_email_derived_exprs,
)


class TestVoluntaryAlignmentEmails:
    """Tests for _voluntary_alignment_emails expression builders."""

    @pytest.mark.unit
    def test_build_email_aggregation_exprs(self):
        exprs = build_email_aggregation_exprs()
        assert len(exprs) == 5

    @pytest.mark.unit
    def test_build_email_derived_exprs(self):
        df = pl.DataFrame({'emails_opened': [3], 'emails_clicked': [1], 'email_campaigns_sent': [10]})
        result = df.select(build_email_derived_exprs())
        assert abs(result['email_open_rate'][0] - 30.0) < 0.01
        assert abs(result['email_click_rate'][0] - 10.0) < 0.01
