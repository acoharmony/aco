from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._sva_log import SvaLogExpression


class TestSvaLogExpression:

    @pytest.mark.unit
    def test_is_upload(self):
        df = pl.DataFrame({'event_type': ['upload', 'connection', 'upload']})
        result = df.filter(SvaLogExpression.is_upload())
        assert len(result) == 2

    @pytest.mark.unit
    def test_is_connection_event(self):
        df = pl.DataFrame({'event_type': ['connection', 'disconnect', 'upload']})
        result = df.filter(SvaLogExpression.is_connection_event())
        assert len(result) == 2

    @pytest.mark.unit
    def test_auth_succeeded(self):
        df = pl.DataFrame({'message': ['Authentication succeeded', 'Failed']})
        result = df.filter(SvaLogExpression.auth_succeeded())
        assert len(result) == 1

    @pytest.mark.unit
    def test_disconnected_cleanly(self):
        df = pl.DataFrame({'message': ['SFTP connection closed', 'Error']})
        result = df.filter(SvaLogExpression.disconnected_cleanly())
        assert len(result) == 1

    @pytest.mark.unit
    def test_is_sva_form(self):
        df = pl.DataFrame({'filename': ['John SVA 03.12.2026.pdf', 'other.pdf', None]})
        result = df.filter(SvaLogExpression.is_sva_form())
        assert len(result) == 1

    @pytest.mark.unit
    def test_patient_name(self):
        df = pl.DataFrame({'filename': ['Andrew Weigert Jr SVA 02.182026.pdf', 'Cabb.pdf', None]})
        result = df.select(SvaLogExpression.patient_name())
        assert result['patient_name'][0] is not None
        assert result['patient_name'][2] is None


class TestCleanFilenameToName:
    """Pinned cases for filename → name extraction (regression-locked).

    All names below are synthetic placeholders chosen to exercise each
    structural variant we see in real Mabel uploads — never use real
    beneficiary names in tests.
    """

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "filename, expected",
        [
            # Canonical SVA filename with space-separated date
            ("Foo Bar Jr SVA 02.182026.pdf", "Foo Bar Jr"),
            # Underscore-separated, uppercase SVA marker
            ("Foo_Bar_SVA_05.11.2026.pdf", "Foo Bar"),
            # Underscore-separated, lowercase sva marker
            ("Foo_Bar_sva_05.11.2026.pdf", "Foo Bar"),
            # Leading numeric chart ID, no SVA marker at all
            ("6128324 Foo Bar.pdf", "Foo Bar"),
            # Typo'd marker: "SA" instead of "SVA"
            ("Foo_Bar_SA_5.11.2026.pdf", "Foo Bar"),
            # No SVA marker, just date
            ("Foo Bar 4.15.2026.pdf", "Foo Bar"),
            ("Foo Bar 04.14.2026.pdf", "Foo Bar"),
            # Double whitespace inside the name
            ("Foo D  Bar SVA 05.112026.pdf", "Foo D Bar"),
            ("Foo Bar  SVA 1-8-2026.pdf", "Foo Bar"),
            # Underscore in last name + uppercase
            ("FOO BAR_BAZ SVA 04 01 26.pdf", "Foo Bar Baz"),
            # Period instead of space between first/last
            ("Foo.Bar_SVA_05.11.2026.pdf", "Foo Bar"),
            # Extra punctuation between tokens
            ("Foo_.Bar_sva_05.11.2026.pdf", "Foo Bar"),
            # Hyphenated last name preserved
            ("Foo Bar-Baz SVA 04.30.2026.pdf", "Foo Bar-Baz"),
            # Spurious space inside date (just before the period)
            ("Foo K Bar 05 .112026.pdf", "Foo K Bar"),
            # Bare last name, no date, no SVA marker
            ("Bar.pdf", "Bar"),
            # All uppercase, space-separated date
            ("FOO BAR 04 09 26.pdf", "Foo Bar"),
            # Already lowercase suffix
            ("Foo_Bar_sva_05.11.2026.pdf", "Foo Bar"),
            # No SVA marker, joined date (MM.DDYYYY)
            ("Foo T Bar 05.112026.pdf", "Foo T Bar"),
            # Single-token fallback
            ("Bar.pdf", "Bar"),
            # None passthrough
            (None, None),
        ],
    )
    def test_clean_cases(self, filename, expected):
        from acoharmony._expressions._sva_log import clean_filename_to_name

        assert clean_filename_to_name(filename) == expected

    @pytest.mark.unit
    def test_patient_name_key(self):
        df = pl.DataFrame({'patient_name': ['John Smith Jr', 'Jane Doe']})
        result = df.select(SvaLogExpression.patient_name_key())
        assert 'patient_name_key' in result.columns

    @pytest.mark.unit
    def test_submission_date_str(self):
        df = pl.DataFrame({'filename': ['test SVA 02.182026.pdf', 'other.txt']})
        result = df.select(SvaLogExpression.submission_date_str())
        assert 'submission_date_str' in result.columns

    @pytest.mark.unit
    def test_submission_date(self):
        df = pl.DataFrame({'submission_date': [date(2026, 3, 12)]})
        result = df.select(SvaLogExpression.submission_date())
        assert result['submission_date'][0] == date(2026, 3, 12)

class TestTokenSimilarity:

    @pytest.mark.unit
    def test_identical(self):
        assert _token_similarity('hello', 'hello') == 1.0

    @pytest.mark.unit
    def test_empty(self):
        assert _token_similarity('', 'hello') == 0.0
        assert _token_similarity('hello', '') == 0.0

    @pytest.mark.unit
    def test_single_char(self):
        assert _token_similarity('a', 'a') == 1.0
        assert _token_similarity('a', 'b') == 0.0

    @pytest.mark.unit
    def test_different(self):
        sim = _token_similarity('john', 'jane')
        assert 0.0 <= sim <= 1.0

class TestFindDuplicatePatients:

    @pytest.mark.unit
    def test_no_duplicates(self):
        df = pl.DataFrame({'patient_name': ['Alice Smith', 'Bob Jones']})
        result = find_duplicate_patients(df)
        assert len(result) == 0

    @pytest.mark.unit
    def test_swap_detection(self):
        df = pl.DataFrame({'patient_name': ['Isabel Trevino', 'Trevino Isabel', 'Bob Jones']})
        result = find_duplicate_patients(df)
        assert len(result) >= 1
        assert result['match_type'][0] == 'exact_tokens'

    @pytest.mark.unit
    def test_fuzzy_match(self):
        df = pl.DataFrame({'patient_name': ['John Smith', 'Jon Smith', 'Alice Wonderland']})
        result = find_duplicate_patients(df, similarity_threshold=0.5)
        assert len(result) >= 1

    @pytest.mark.unit
    def test_lazyframe_input(self):
        df = pl.DataFrame({'patient_name': ['Alice Smith', 'Bob Jones']}).lazy()
        result = find_duplicate_patients(df)
        assert isinstance(result, pl.DataFrame)

    @pytest.mark.unit
    def test_empty_names(self):
        df = pl.DataFrame({'patient_name': [None, None]}, schema={'patient_name': pl.Utf8})
        result = find_duplicate_patients(df)
        assert len(result) == 0

class TestSVALogTokenSimilarity:
    """Cover edge case in _token_similarity."""

    @pytest.mark.unit
    def test_single_char_bigrams(self):
        """Line 52: single-char strings produce singleton set."""
        result = _token_similarity('a', 'a')
        assert result == 1.0

    @pytest.mark.unit
    def test_empty_bigrams_return_zero(self):
        """Line 52: empty string returns 0.0."""
        result = _token_similarity('', 'abc')
        assert result == 0.0


class TestTokenSimilarityBranches:
    """Cover branches 47->48 (empty/None) and 47->49 (both non-empty)."""

    @pytest.mark.unit
    def test_both_empty(self):
        """Branch 47->48: a is empty."""
        assert _token_similarity('', '') == 0.0

    @pytest.mark.unit
    def test_a_none(self):
        """Branch 47->48: a is None (falsy)."""
        assert _token_similarity(None, 'abc') == 0.0

    @pytest.mark.unit
    def test_b_none(self):
        """Branch 47->48: b is None (falsy)."""
        assert _token_similarity('abc', None) == 0.0

    @pytest.mark.unit
    def test_both_nonempty(self):
        """Branch 47->49: both are non-empty, compute similarity."""
        result = _token_similarity('hello', 'hello')
        assert result == 1.0


class TestFindDuplicatePatientsBranches:
    """Cover branches 219->220/222, 222->224/246, 224->225/235, 236->222/237, 246->247/256."""

    @pytest.mark.unit
    def test_exact_token_match(self):
        """Branch 222->224, 224->225: exact sorted-token match (name swap)."""
        from acoharmony._expressions._sva_log import find_duplicate_patients

        df = pl.DataFrame({'patient_name': ['John Smith', 'Smith John']})
        result = find_duplicate_patients(df, similarity_threshold=0.5)
        assert len(result) >= 1
        match_types = result['match_type'].to_list()
        assert 'exact_tokens' in match_types

    @pytest.mark.unit
    def test_fuzzy_match_above_threshold(self):
        """Branch 224->235, 236->237: fuzzy match above threshold."""
        from acoharmony._expressions._sva_log import find_duplicate_patients

        df = pl.DataFrame({'patient_name': ['Jonathan Smith', 'Jonathen Smyth']})
        result = find_duplicate_patients(df, similarity_threshold=0.3)
        # Should find fuzzy match
        assert len(result) >= 1

    @pytest.mark.unit
    def test_no_matches_returns_empty(self):
        """Branch 246->247: no pairs found, returns empty DataFrame with schema."""
        from acoharmony._expressions._sva_log import find_duplicate_patients

        df = pl.DataFrame({'patient_name': ['Aaaa', 'Zzzz']})
        result = find_duplicate_patients(df, similarity_threshold=0.99)
        assert result.height == 0
        assert 'name_a' in result.columns

    @pytest.mark.unit
    def test_matches_returns_sorted(self):
        """Branch 246->256: pairs found, returns sorted DataFrame."""
        from acoharmony._expressions._sva_log import find_duplicate_patients

        df = pl.DataFrame({'patient_name': ['Alice Smith', 'Smith Alice', 'Bob Jones']})
        result = find_duplicate_patients(df, similarity_threshold=0.3)
        assert result.height >= 1
        assert result.columns == ['name_a', 'name_b', 'similarity', 'match_type']
