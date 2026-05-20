from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._claim_id_match import ClaimIdMatchExpression


class TestClaimIdMatchExpression:

    @pytest.mark.unit
    def test_claim_id_match_flag(self):
        df = pl.DataFrame({'hdai_claim_id': ['C1', None, 'C3'], 'cclf_claim_id': ['C1', 'C2', None]})
        expr = ClaimIdMatchExpression.claim_id_match_flag()
        result = df.select(expr.alias('flag'))
        vals = result['flag'].to_list()
        assert vals[0] == 'yes'
        assert vals[1] == 'missing_hdai'
        assert vals[2] == 'missing_cclf'

    @pytest.mark.unit
    def test_has_claim_id_in_source(self):
        df = pl.DataFrame({'my_col': ['A', None]})
        expr = ClaimIdMatchExpression.has_claim_id_in_source('my_col')
        result = df.select(expr.alias('present'))
        assert result['present'].to_list() == [True, False]

    @pytest.mark.unit
    def test_claim_ids_match(self):
        df = pl.DataFrame({'hdai_claim_id': ['C1', 'C2', None], 'cclf_claim_id': ['C1', 'C3', 'C4']})
        expr = ClaimIdMatchExpression.claim_ids_match()
        result = df.select(expr.alias('match'))
        assert result['match'].to_list() == [True, False, False]
