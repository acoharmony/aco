from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._aco_demographics_join import (
    build_county_expr,
    build_demographics_select_expr,
    build_zip5_expr,
)


class TestAcoDemographicsJoin:
    """Tests for _aco_demographics_join expression builders."""

    @pytest.mark.unit
    def test_build_demographics_select_expr(self):
        exprs = build_demographics_select_expr()
        assert isinstance(exprs, list)
        assert len(exprs) == 8
        df = pl.DataFrame({'current_bene_mbi_id': ['MBI1'], 'bene_fst_name': ['John'], 'bene_lst_name': ['Doe'], 'bene_mdl_name': ['A'], 'bene_line_1_adr': ['123 Main St'], 'geo_zip_plc_name': ['Springfield'], 'geo_usps_state_cd': ['IL'], 'geo_zip5_cd': ['62701']})
        result = df.select(exprs)
        assert result.columns == ['current_mbi', 'bene_first_name', 'bene_last_name', 'bene_middle_initial', 'bene_address_line_1', 'bene_city', 'bene_state', 'bene_zip']
        assert result['current_mbi'][0] == 'MBI1'

    @pytest.mark.unit
    def test_build_county_expr(self):
        df = pl.DataFrame({'bene_fips_cnty_cd': ['17167']})
        result = df.select(build_county_expr())
        assert result.columns == ['bene_county']
        assert result['bene_county'][0] == '17167'

    @pytest.mark.unit
    def test_build_county_expr_custom_col(self):
        df = pl.DataFrame({'my_county': ['99999']})
        result = df.select(build_county_expr('my_county'))
        assert result['bene_county'][0] == '99999'

    @pytest.mark.unit
    def test_build_zip5_expr(self):
        df = pl.DataFrame({'bene_zip': ['627011234']})
        result = df.select(build_zip5_expr())
        assert result['bene_zip_5'][0] == '62701'

    @pytest.mark.unit
    def test_build_zip5_expr_short_zip(self):
        df = pl.DataFrame({'bene_zip': ['123']})
        result = df.select(build_zip5_expr())
        assert result['bene_zip_5'][0] == '123'
