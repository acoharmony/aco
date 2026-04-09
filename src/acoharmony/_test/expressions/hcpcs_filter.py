from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._hcpcs_filter import HCPCSFilterExpression


class TestHCPCSFilterExpression:

    @pytest.mark.unit
    def test_filter_home_visit_hcpcs(self):
        df = pl.DataFrame({'hcpcs_code': ['99341', '99213', 'G2211']})
        result = df.filter(HCPCSFilterExpression.filter_home_visit_hcpcs())
        assert len(result) == 2

    @pytest.mark.unit
    def test_filter_office_visit_hcpcs(self):
        df = pl.DataFrame({'hcpcs_code': ['99213', '99341', '99202']})
        result = df.filter(HCPCSFilterExpression.filter_office_visit_hcpcs())
        assert len(result) == 2

    @pytest.mark.unit
    def test_wound_care(self):
        df = pl.DataFrame({'hcpcs_code': ['97597', '99213']})
        result = df.filter(HCPCSFilterExpression.wound_care())
        assert len(result) == 1

    @pytest.mark.unit
    def test_skin_substitutes(self):
        df = pl.DataFrame({'hcpcs_code': ['Q4101', '99213']})
        result = df.filter(HCPCSFilterExpression.skin_substitutes())
        assert len(result) == 1

    @pytest.mark.unit
    def test_code_lists_are_populated(self):
        assert len(HCPCSFilterExpression.home_visit_codes) > 0
        assert len(HCPCSFilterExpression.office_visit_codes) > 0
        assert len(HCPCSFilterExpression.wound_care_codes) > 0
        assert len(HCPCSFilterExpression.skin_substitute_codes) > 0
