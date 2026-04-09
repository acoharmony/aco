"""Tests for acoharmony._transforms._aco_alignment_office module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._aco_alignment_office is not None


class TestApplyTransformOffice:
    """Cover apply_transform lines 49-148."""

    @pytest.mark.unit
    def test_null_office_zip_adds_flag(self):
        """Cover lines 62-64: office_zip not found → skip with flag."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_office import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1"],
            "bene_zip_5": ["60601"],
        }).lazy()

        catalog = MagicMock()
        catalog.scan_table.return_value = None

        result = apply_transform(df, {}, catalog, MagicMock(), force=True).collect()
        assert "_office_matched" in result.columns
        assert result["_office_matched"][0] is True

    @pytest.mark.unit
    def test_idempotency_skip(self):
        """Cover lines 50-52: already processed → skip."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_office import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1"],
            "_office_matched": [True],
        }).lazy()

        result = apply_transform(df, {}, MagicMock(), MagicMock(), force=False)
        assert result.collect().height == 1

    @pytest.mark.unit
    def test_direct_match_all_matched(self):
        """Cover lines 76-134: direct match with all records matched."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_office import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2"],
            "bene_zip_5": ["60601", "60602"],
        }).lazy()

        office_zip = pl.DataFrame({
            "zip_code": ["60601", "60602"],
            "office_name": ["Office A", "Office B"],
            "market": ["Chicago", "Chicago"],
            "office_distance": [None, None],
        }).lazy()

        catalog = MagicMock()
        catalog.scan_table.return_value = office_zip

        result = apply_transform(df, {}, catalog, MagicMock(), force=True).collect()

        assert "office_name" in result.columns
        assert "office_location" in result.columns
        assert "_office_matched" in result.columns
        assert result.height == 2

    @pytest.mark.unit
    def test_fuzzy_match_for_unmatched(self):
        """Cover lines 97-131: direct match misses, fuzzy match applied."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_office import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2"],
            "bene_zip_5": ["60601", "99999"],
        }).lazy()

        office_zip = pl.DataFrame({
            "zip_code": ["60601", "99998"],
            "office_name": ["Office A", None],
            "market": ["Chicago", "NY"],
            "office_distance": [None, 5.2],
        }).lazy()

        catalog = MagicMock()
        catalog.scan_table.return_value = office_zip

        result = apply_transform(df, {}, catalog, MagicMock(), force=True).collect()

        assert "_office_matched" in result.columns
        assert result.height == 2

    @pytest.mark.unit
    def test_fuzzy_match_exception_fallback(self):
        """Cover lines 136-139: exception in fuzzy path → fallback concat."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_office import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2"],
            "bene_zip_5": ["60601", "99999"],
        }).lazy()

        # Office zip: M1 matches directly, M2 has no direct match
        # Fuzzy entries have bad data that will cause an error when selecting columns
        office_zip = pl.DataFrame({
            "zip_code": ["60601"],
            "office_name": ["Office A"],
            "market": ["Chicago"],
            "office_distance": [None],
        }).lazy()

        catalog = MagicMock()
        catalog.scan_table.return_value = office_zip

        # This will have unmatched records (M2) but fuzzy_office is empty
        # The sort/unique on empty works fine normally, so let's use a patch
        # to make the "unique" call inside the try block raise
        from unittest.mock import patch as _patch
        original_unique = pl.LazyFrame.unique
        patched = [False]
        def bad_unique(self, *args, **kwargs):
            if patched[0]:
                raise RuntimeError("forced fuzzy error")
            return original_unique(self, *args, **kwargs)

        # Monkey-patch temporarily — the exception gets caught at line 136
        with _patch.object(pl.LazyFrame, "unique", bad_unique):
            patched[0] = True
            result = apply_transform(df, {}, catalog, MagicMock(), force=True).collect()

        assert "_office_matched" in result.columns
