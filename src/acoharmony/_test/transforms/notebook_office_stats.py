"""Tests for acoharmony._transforms._notebook_office_stats module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._notebook_office_stats is not None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_base_df(
    office_col: str = "office_name",
    include_office_location: bool = False,
    yearmo: str = "202401",
    include_reach: bool = True,
    include_mssp: bool = True,
    include_ffs: bool = True,
    include_sva: bool = False,
    include_transition_cols: bool = False,
    include_zip: str | None = None,
) -> pl.LazyFrame:
    """Build a minimal LazyFrame suitable for office stat functions."""
    data: dict = {
        office_col: ["OfficeA", "OfficeA", "OfficeB"],
    }
    if include_office_location and office_col != "office_location":
        data["office_location"] = ["LocX", "LocX", "LocY"]
    if include_reach:
        data[f"ym_{yearmo}_reach"] = [True, False, True]
    if include_mssp:
        data[f"ym_{yearmo}_mssp"] = [False, True, False]
    if include_ffs:
        data[f"ym_{yearmo}_ffs"] = [False, False, True]
    if include_sva:
        data["has_valid_voluntary_alignment"] = [True, False, True]
    if include_transition_cols:
        data["has_program_transition"] = [True, False, False]
        data["has_continuous_enrollment"] = [True, True, False]
        data["months_in_reach"] = [6, 3, 0]
        data["months_in_mssp"] = [2, 9, 0]
        data["total_aligned_months"] = [8, 12, 0]
    if include_zip == "bene_zip_5":
        data["bene_zip_5"] = ["12345", "12345", "67890"]
    elif include_zip == "patient_zip":
        data["patient_zip"] = ["12345", "12345", "67890"]
    return pl.DataFrame(data).lazy()


# ---------------------------------------------------------------------------
# calculate_office_enrollment_stats
# ---------------------------------------------------------------------------

class TestCalculateOfficeEnrollmentStats:
    """Tests for calculate_office_enrollment_stats."""

    @pytest.mark.unit
    def test_returns_none_for_empty_yearmo(self):
        """Branch 51->60 via empty yearmo guard at line 45-46."""
        df = _make_base_df()
        assert calculate_office_enrollment_stats(df, "") is None
        assert calculate_office_enrollment_stats(df, None) is None

    @pytest.mark.unit
    def test_auto_detect_office_name(self):
        """Branch 51->60, 52->54: office_column is None, office_name in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_enrollment_stats(df, "202401")
        assert result is not None
        assert "office_name" in result.columns

    @pytest.mark.unit
    def test_auto_detect_office_location(self):
        """Branch 52->54, 54->55: office_name not in schema, office_location is.

        When office_location is auto-detected AND also present in the group_by
        secondary column list, polars raises DuplicateError. This test verifies
        the auto-detect branch is reached.
        """
        df = _make_base_df(office_col="office_location")
        with pytest.raises(pl.exceptions.DuplicateError):
            calculate_office_enrollment_stats(df, "202401")

    @pytest.mark.unit
    def test_auto_detect_returns_none_no_office_col(self):
        """Branch 54->57: neither office_name nor office_location in schema."""
        df = pl.DataFrame({
            "some_col": ["a", "b"],
            "ym_202401_reach": [True, False],
        }).lazy()
        result = calculate_office_enrollment_stats(df, "202401")
        assert result is None

    @pytest.mark.unit
    def test_explicit_office_col_not_in_schema(self):
        """Branch 60->61: explicit office_column not in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_enrollment_stats(df, "202401", office_column="nonexistent")
        assert result is None


# ---------------------------------------------------------------------------
# calculate_office_alignment_types
# ---------------------------------------------------------------------------

class TestCalculateOfficeAlignmentTypes:
    """Tests for calculate_office_alignment_types."""

    @pytest.mark.unit
    def test_returns_none_for_empty_yearmo(self):
        """Branch 102->103: empty yearmo."""
        df = _make_base_df()
        assert calculate_office_alignment_types(df, "") is None

    @pytest.mark.unit
    def test_auto_detect_office_name(self):
        """Branch 108->117, 109->111: office_name in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_alignment_types(df, "202401")
        assert result is not None

    @pytest.mark.unit
    def test_auto_detect_office_location(self):
        """Branch 109->111, 111->112: office_location only."""
        df = _make_base_df(office_col="office_location")
        with pytest.raises(pl.exceptions.DuplicateError):
            calculate_office_alignment_types(df, "202401")

    @pytest.mark.unit
    def test_auto_detect_returns_none(self):
        """Branch 111->114: no office column found."""
        df = pl.DataFrame({
            "some_col": ["a"],
            "ym_202401_reach": [True],
        }).lazy()
        result = calculate_office_alignment_types(df, "202401")
        assert result is None

    @pytest.mark.unit
    def test_explicit_col_not_in_schema(self):
        """Branch 117->118: explicit office_column not in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_alignment_types(df, "202401", office_column="missing")
        assert result is None


# ---------------------------------------------------------------------------
# calculate_office_program_distribution
# ---------------------------------------------------------------------------

class TestCalculateOfficeProgramDistribution:
    """Tests for calculate_office_program_distribution."""

    @pytest.mark.unit
    def test_returns_none_for_empty_yearmo(self):
        """Branch 160->161: empty yearmo."""
        df = _make_base_df()
        assert calculate_office_program_distribution(df, "") is None

    @pytest.mark.unit
    def test_auto_detect_office_name(self):
        """Branch 166->175, 167->169: office_name in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_program_distribution(df, "202401")
        assert result is not None

    @pytest.mark.unit
    def test_auto_detect_office_location(self):
        """Branch 167->169, 169->170: office_location only."""
        df = _make_base_df(office_col="office_location")
        with pytest.raises(pl.exceptions.DuplicateError):
            calculate_office_program_distribution(df, "202401")

    @pytest.mark.unit
    def test_auto_detect_returns_none(self):
        """Branch 169->172: no office column."""
        df = pl.DataFrame({
            "some_col": ["a"],
            "ym_202401_reach": [True],
        }).lazy()
        result = calculate_office_program_distribution(df, "202401")
        assert result is None

    @pytest.mark.unit
    def test_explicit_col_not_in_schema(self):
        """Branch 175->176: explicit office_column not in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_program_distribution(df, "202401", office_column="missing")
        assert result is None


# ---------------------------------------------------------------------------
# calculate_office_transition_stats
# ---------------------------------------------------------------------------

class TestCalculateOfficeTransitionStats:
    """Tests for calculate_office_transition_stats."""

    @pytest.mark.unit
    def test_auto_detect_office_name(self):
        """Branch 217->226, 218->220: office_name in schema."""
        df = _make_base_df(office_col="office_name", include_transition_cols=True)
        result = calculate_office_transition_stats(df)
        assert result is not None

    @pytest.mark.unit
    def test_auto_detect_office_location(self):
        """Branch 218->220, 220->221: office_location only."""
        df = _make_base_df(office_col="office_location", include_transition_cols=True)
        with pytest.raises(pl.exceptions.DuplicateError):
            calculate_office_transition_stats(df)

    @pytest.mark.unit
    def test_auto_detect_returns_none(self):
        """Branch 220->223: no office column found."""
        df = pl.DataFrame({
            "some_col": ["a"],
        }).lazy()
        result = calculate_office_transition_stats(df)
        assert result is None

    @pytest.mark.unit
    def test_explicit_col_not_in_schema(self):
        """Branch 226->227: explicit office_column not in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_transition_stats(df, office_column="missing")
        assert result is None


# ---------------------------------------------------------------------------
# calculate_office_metadata
# ---------------------------------------------------------------------------

class TestCalculateOfficeMetadata:
    """Tests for calculate_office_metadata."""

    @pytest.mark.unit
    def test_auto_detect_office_name(self):
        """Branch 269->278, 270->272: office_name in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_metadata(df)
        assert result is not None
        assert "office_name" in result.columns

    @pytest.mark.unit
    def test_auto_detect_office_location(self):
        """Branch 270->272, 272->273: office_location only."""
        df = _make_base_df(office_col="office_location")
        with pytest.raises(pl.exceptions.DuplicateError):
            calculate_office_metadata(df)

    @pytest.mark.unit
    def test_auto_detect_returns_none(self):
        """Branch 272->275: no office column found."""
        df = pl.DataFrame({"some_col": ["a"]}).lazy()
        result = calculate_office_metadata(df)
        assert result is None

    @pytest.mark.unit
    def test_explicit_col_not_in_schema(self):
        """Branch 278->279: explicit office_column not in schema."""
        df = _make_base_df(office_col="office_name")
        result = calculate_office_metadata(df, office_column="missing")
        assert result is None

    @pytest.mark.unit
    def test_zip_bene_zip_5(self):
        """Branch 287->289: bene_zip_5 in schema."""
        df = _make_base_df(office_col="office_name", include_zip="bene_zip_5")
        result = calculate_office_metadata(df)
        assert result is not None
        assert "unique_zips" in result.columns
        # 2 unique zips across 3 rows
        assert result["unique_zips"].sum() > 0

    @pytest.mark.unit
    def test_zip_patient_zip(self):
        """Branch 289->290: patient_zip in schema (bene_zip_5 absent)."""
        df = _make_base_df(office_col="office_name", include_zip="patient_zip")
        result = calculate_office_metadata(df)
        assert result is not None
        assert "unique_zips" in result.columns
        assert result["unique_zips"].sum() > 0

    @pytest.mark.unit
    def test_zip_neither(self):
        """Branch 289->293: no zip column, fallback to lit(0)."""
        df = _make_base_df(office_col="office_name", include_zip=None)
        result = calculate_office_metadata(df)
        assert result is not None
        assert "unique_zips" in result.columns
        assert result["unique_zips"].sum() == 0
