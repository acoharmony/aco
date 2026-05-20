from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._ccsr import CcsrExpression


class TestCcsrExpression:

    @pytest.mark.unit
    def test_map_diagnoses_to_ccsr(self):
        claims = pl.DataFrame({'diagnosis_code_1': ['A01', 'B02']}).lazy()
        mapping = pl.DataFrame({'icd_10_cm_code': ['A01'], 'default_ccsr_category_ip': ['INF001'], 'default_ccsr_category_description_ip': ['Infection'], 'default_ccsr_category_op': ['INF002'], 'default_ccsr_category_description_op': ['Infection OP'], 'ccsr_category_1': ['INF001'], 'ccsr_category_1_description': ['Cat1'], 'ccsr_category_2': [None], 'ccsr_category_2_description': [None], 'ccsr_category_3': [None], 'ccsr_category_3_description': [None], 'ccsr_category_4': [None], 'ccsr_category_4_description': [None], 'ccsr_category_5': [None], 'ccsr_category_5_description': [None], 'ccsr_category_6': [None], 'ccsr_category_6_description': [None]}).lazy()
        result = CcsrExpression.map_diagnoses_to_ccsr(claims, mapping, {}).collect()
        assert 'ccsr_body_system' in result.columns

    @pytest.mark.unit
    def test_map_procedures_to_ccsr(self):
        claims = pl.DataFrame({'procedure_code_1': ['0001']}).lazy()
        mapping = pl.DataFrame({'icd_10_pcs': ['0001'], 'prccsr': ['SUR001'], 'prccsr_description': ['Surgery'], 'clinical_domain': ['CNS']}).lazy()
        result = CcsrExpression.map_procedures_to_ccsr(claims, mapping, {}).collect()
        assert 'prccsr' in result.columns
