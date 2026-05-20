from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from unittest.mock import MagicMock

import polars as pl
import pytest

import acoharmony

# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._transforms._bnex module."""





class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._bnex is not None



# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.bnex module."""





def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestBnexTransform:
    """Tests for BNEX transform."""

    @pytest.mark.unit
    def test_bnex_dob_parsing(self):

        df = pl.DataFrame({
            "DOB": ["19500315", "19800101"],
            "source_filename": [
                "P.A2671.BNEX.Y25.D251203.T1136030.xml",
                "P.A2671.BNEX.Y25.D240601.T0800000.xml",
            ],
        }).lazy()
        logger = MagicMock()
        result = apply_transform(df, schema={}, catalog=None, logger=logger).collect()
        assert result["DOB"].dtype == pl.Date
        assert result["DOB"][0] == datetime.date(1950, 3, 15)
        assert result["DOB"][1] == datetime.date(1980, 1, 1)

    @pytest.mark.unit
    def test_bnex_file_date_extraction(self):

        df = pl.DataFrame({
            "DOB": ["19500315"],
            "source_filename": ["P.A2671.BNEX.Y25.D251203.T1136030.xml"],
        }).lazy()
        logger = MagicMock()
        result = apply_transform(df, schema={}, catalog=None, logger=logger).collect()
        assert "file_date" in result.columns
        assert result["file_date"][0] == datetime.date(2025, 12, 3)

    @pytest.mark.unit
    def test_bnex_file_date_no_pattern(self):

        df = pl.DataFrame({
            "DOB": ["19500315"],
            "source_filename": ["no_date_pattern.xml"],
        }).lazy()
        logger = MagicMock()
        result = apply_transform(df, schema={}, catalog=None, logger=logger).collect()
        assert result["file_date"][0] is None


class TestBnexTransformExtended:
    """Tests for BNEX transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._bnex is not None

    @pytest.mark.unit
    def test_apply_transform_exists(self):
        assert hasattr(_bnex, "apply_transform")
