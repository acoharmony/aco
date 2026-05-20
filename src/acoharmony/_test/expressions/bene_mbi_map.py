from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._bene_mbi_map import BeneficiaryMbiMappingExpression


class TestBeneficiaryMbiMappingExpression:

    @pytest.mark.unit
    def test_build_validation_expressions(self):
        df = pl.DataFrame({'prvs_num': ['12345678901'], 'crnt_num': ['12345678902'], 'prvs_id_efctv_dt': [date(2024, 1, 1)], 'chain_depth': [2]})
        exprs = BeneficiaryMbiMappingExpression.build_validation_expressions()
        result = df.select(exprs)
        assert result['is_valid_format'][0] is True
        assert result['is_crosswalk'][0] is True
        assert result['has_effective_date'][0] is True
        assert result['is_transitive'][0] is True

    @pytest.mark.unit
    def test_build_metadata_expressions(self):
        df = pl.DataFrame({'dummy': [1]})
        exprs = BeneficiaryMbiMappingExpression.build_metadata_expressions()
        result = df.select(exprs)
        assert result['created_by'][0] == 'ACOHarmony'
        assert result['source_system'][0] == 'CCLF9'
