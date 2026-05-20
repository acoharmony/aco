from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._response_code_parser import (
    RESPONSE_CODE_MAP,
    ResponseCodeParserExpression,
)


class TestResponseCodeParserExpression:

    @pytest.mark.unit
    def test_parse_response_codes(self):
        df = pl.DataFrame({'response_codes': ['A0,V1,E2', 'P0,P1', None]})
        exprs = ResponseCodeParserExpression.parse_response_codes()
        result = df.select(exprs)
        assert result['has_acceptance'][0] is True
        assert result['has_validation_error'][0] is True
        assert result['has_eligibility_issue'][0] is True
        assert result['has_precedence_issue'][1] is True
        assert result['error_category'][0] == 'eligibility_issues'
        assert result['error_category'][1] == 'precedence_issues'
        assert result['response_code_list'][2] is None

    @pytest.mark.unit
    def test_parse_response_codes_custom_col(self):
        df = pl.DataFrame({'my_codes': ['A0']})
        exprs = ResponseCodeParserExpression.parse_response_codes('my_codes')
        result = df.select(exprs)
        assert result['has_acceptance'][0] is True

    @pytest.mark.unit
    def test_response_code_map(self):
        assert 'A0' in RESPONSE_CODE_MAP
        assert RESPONSE_CODE_MAP['A0'][0] == 'acceptance'
        assert 'E5' in RESPONSE_CODE_MAP

    @pytest.mark.unit
    def test_a2_ineligible(self):
        df = pl.DataFrame({'response_codes': ['A2']})
        exprs = ResponseCodeParserExpression.parse_response_codes()
        result = df.select(exprs)
        assert result['has_ineligible_alignment'][0] is True
        assert result['error_category'][0] == 'accepted_ineligible'
