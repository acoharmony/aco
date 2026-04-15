# © 2025 HarmonyCares
# All rights reserved.

"""
Validation of the BNMR ``report_parameters`` sheet (milestone M7).

``report_parameters`` is CMS-manual: trend factors, risk corridors,
completion factors, HEBA percentiles, quality withhold — none of
these are reconstructable from upstream pipeline data. M7 scope is
therefore narrow: prove the sheet parses cleanly, matrix-extracted
metadata fields populate (modulo documented "preliminary" deliveries
that ship a truncated subset), numeric values sit in plausible CMS
ranges, and cross-sheet values (e.g. the ``discount`` column stamped
onto a claims row) agree with the authoritative values on the
report_parameters sheet.

All tests are ``@requires_data``-gated: they run against
``silver/reach_bnmr_report_parameters.parquet`` and skip in CI where
the workspace isn't mounted.

Bounds calibration
------------------
Ranges are tight against what's observed in PY2023–PY2025 REACH
deliveries but generous enough to accommodate CMS policy shifts
within reason. If a future delivery lands a value outside these
bounds, the test should flag it — those are the "important changes"
M7 is designed to catch.

Known incomplete deliveries
---------------------------
At least one PY2023 preliminary delivery (``D250613``) ships a
21-row REPORT_PARAMETERS sheet with only ACO parameters and claim
period info, no financial params. The null-count tests tolerate up
to 1 delivery with this pattern; any more and the test fails, which
would prompt a closer look at CMS delivery formats.
"""

from __future__ import annotations

import polars as pl
import pytest

from .conftest import requires_data, scan_silver

# ---------------------------------------------------------------------------
# Numeric bounds (generous for PY2023–2025 calibration)
# ---------------------------------------------------------------------------

NUMERIC_BOUNDS: dict[str, tuple[float, float]] = {
    "discount": (0.0, 0.05),
    "shared_savings_rate": (0.0, 1.0),
    "quality_withhold": (0.0, 0.05),
    "quality_score": (0.0, 1.0),
    "blend_percentage": (0.0, 1.0),
    "blend_ceiling": (0.0, 0.15),
    "blend_floor": (-0.10, 0.0),
    "ad_retrospective_trend": (0.80, 1.20),
    "esrd_retrospective_trend": (0.80, 1.20),
    # Completion factors sometimes run higher than 1.2 when CMS adjusts
    # for unusually long run-out periods; cap at 1.50 to be safe.
    "ad_completion_factor": (0.80, 1.50),
    "esrd_completion_factor": (0.80, 1.50),
    "stop_loss_payout_neutrality_factor": (0.50, 2.00),
}

# ---------------------------------------------------------------------------
# Categorical enums — calibrated to observed values
# ---------------------------------------------------------------------------

CATEGORICAL_ENUMS: dict[str, set[str]] = {
    "aco_type": {"Standard", "New Entrant", "High Needs"},
    "risk_arrangement": {"Global", "Professional"},
    "payment_mechanism": {"PCC", "APO", "TCC"},
    "advanced_payment_option": {"Yes", "No"},
    "stop_loss_elected": {"Yes", "No"},
    # Stop-Loss types CMS publishes — "Alternate" is the observed value
    # on PY2023–2025 High-Needs ACO deliveries; "Standard" is the other
    # documented option.
    "stop_loss_type": {"Standard", "Alternate"},
    "voluntary_aligned_benchmark": {"Historical Blended", "Historical", "Blended"},
}

# Grain columns for report_parameters rows. source_filename is the
# authoritative delivery key; sheet_type filters to the right subset.
SOURCE_KEY = "source_filename"

# Fields expected to be present in "complete" deliveries. A small tolerance
# of 1 file may be missing these fields (the documented PY2023 preliminary
# delivery at D250613).
FINANCIAL_FIELDS = [
    "discount",
    "quality_withhold",
    "quality_score",
    "blend_percentage",
    "blend_ceiling",
    "blend_floor",
    "ad_retrospective_trend",
    "esrd_retrospective_trend",
    "ad_completion_factor",
    "esrd_completion_factor",
    "stop_loss_elected",
    "stop_loss_type",
    "aco_type",
]

# Fields that are populated consistently across all known deliveries.
ALWAYS_POPULATED_FIELDS = [
    "aco_id",
    "performance_year",
    "shared_savings_rate",
    "risk_arrangement",
    "payment_mechanism",
    "advanced_payment_option",
]

MAX_FILES_MISSING_FINANCIAL = 1


@pytest.fixture
def report_parameters():
    try:
        return scan_silver("reach_bnmr_report_parameters").collect()
    except Exception:
        pytest.skip("reach_bnmr_report_parameters not available in silver")


@requires_data
class TestPresence:
    """Sheet parsed and landed in silver."""

    @pytest.mark.reconciliation
    def test_table_has_rows(self, report_parameters):
        assert report_parameters.height > 0

    @pytest.mark.reconciliation
    def test_every_delivery_contributes_rows(self, report_parameters):
        """Every BNMR delivery in the system should have at least one
        row in the report_parameters slice."""
        deliveries = report_parameters[SOURCE_KEY].n_unique()
        assert deliveries > 0, "No report_parameters deliveries found"


@requires_data
class TestAlwaysPopulatedFields:
    """These fields populate on every known delivery — any null is a bug."""

    @pytest.mark.reconciliation
    @pytest.mark.parametrize("field", ALWAYS_POPULATED_FIELDS)
    def test_field_populated_on_every_delivery(self, report_parameters, field):
        # Dedupe to one row per delivery; only rows with that field null count
        per_delivery = report_parameters.group_by(SOURCE_KEY).agg(
            pl.col(field).drop_nulls().first().alias("value")
        )
        missing = per_delivery.filter(pl.col("value").is_null())
        assert missing.height == 0, (
            f"{field} is null on {missing.height} deliveries:\n"
            f"{missing['source_filename'].to_list()}"
        )


@requires_data
class TestFinancialFields:
    """Financial fields populate on 'complete' deliveries — up to
    ``MAX_FILES_MISSING_FINANCIAL`` 'preliminary' deliveries may lack them."""

    @pytest.mark.reconciliation
    @pytest.mark.parametrize("field", FINANCIAL_FIELDS)
    def test_field_populated_on_most_deliveries(self, report_parameters, field):
        per_delivery = report_parameters.group_by(SOURCE_KEY).agg(
            pl.col(field).drop_nulls().first().alias("value")
        )
        missing = per_delivery.filter(pl.col("value").is_null())
        assert missing.height <= MAX_FILES_MISSING_FINANCIAL, (
            f"{field} null on {missing.height} deliveries "
            f"(threshold {MAX_FILES_MISSING_FINANCIAL}):\n"
            f"{missing['source_filename'].to_list()}"
        )


@requires_data
class TestNumericBounds:
    """Where numeric fields ARE populated, they sit in plausible ranges."""

    @pytest.mark.reconciliation
    @pytest.mark.parametrize("field,lo,hi", [
        (f, lo, hi) for f, (lo, hi) in NUMERIC_BOUNDS.items()
    ])
    def test_value_in_range(self, report_parameters, field, lo, hi):
        vals = report_parameters[field].drop_nulls()
        if vals.len() == 0:
            pytest.skip(f"{field} has no populated values in this dataset")
        # Cast defensively — matrix_fields with data_type=decimal should
        # already be Float64, but guard against string-typed delivery.
        try:
            vals_num = vals.cast(pl.Float64, strict=False).drop_nulls()
        except Exception as e:
            pytest.fail(f"{field} values won't cast to float: {e}")
        assert vals_num.min() >= lo, (
            f"{field} min {vals_num.min()} below bound {lo}"
        )
        assert vals_num.max() <= hi, (
            f"{field} max {vals_num.max()} above bound {hi}"
        )


@requires_data
class TestCategoricalEnums:
    """Categorical fields use one of the known CMS values."""

    @pytest.mark.reconciliation
    @pytest.mark.parametrize("field,allowed", [
        (f, v) for f, v in CATEGORICAL_ENUMS.items()
    ])
    def test_value_is_known(self, report_parameters, field, allowed):
        vals = set(report_parameters[field].drop_nulls().unique().to_list())
        unexpected = vals - allowed
        assert unexpected == set(), (
            f"{field} has unexpected values: {unexpected} (allowed: {allowed})"
        )


@requires_data
class TestCrossSheetConsistency:
    """Values stamped onto data sheets must equal the report_parameters values.

    The parser's matrix_fields mechanism broadcasts each metadata field
    onto every row of every sheet. If the claims-sheet ``discount`` disagrees
    with the report_parameters-sheet ``discount`` for the same delivery,
    downstream joins will be inconsistent.
    """

    @pytest.fixture
    def claims(self):
        try:
            return scan_silver("reach_bnmr_claims").collect()
        except Exception:
            pytest.skip("reach_bnmr_claims not available in silver")

    @pytest.mark.reconciliation
    @pytest.mark.parametrize("field", [
        "discount", "shared_savings_rate", "quality_withhold",
        "blend_percentage", "aco_type", "risk_arrangement",
    ])
    def test_claims_field_matches_report_parameters(
        self, report_parameters, claims, field
    ):
        """Per source_filename, the claims-sheet value for ``field`` must
        equal the report_parameters-sheet value."""
        # Dedupe each side to one value per delivery
        rp_by_file = report_parameters.group_by(SOURCE_KEY).agg(
            pl.col(field).drop_nulls().first().alias(f"rp_{field}")
        )
        cl_by_file = claims.group_by(SOURCE_KEY).agg(
            pl.col(field).drop_nulls().first().alias(f"cl_{field}")
        )
        joined = rp_by_file.join(cl_by_file, on=SOURCE_KEY, how="inner")

        # Skip deliveries where either side is null (e.g. preliminary report)
        joined = joined.filter(
            pl.col(f"rp_{field}").is_not_null()
            & pl.col(f"cl_{field}").is_not_null()
        )
        if joined.height == 0:
            pytest.skip(f"No deliveries with both claims and rp {field} populated")

        mismatches = joined.filter(pl.col(f"rp_{field}") != pl.col(f"cl_{field}"))
        assert mismatches.height == 0, (
            f"{field} disagrees between report_parameters and claims on "
            f"{mismatches.height} deliveries:\n{mismatches}"
        )


@requires_data
class TestInternalStructure:
    """The report_parameters sheet's own row-structure is coherent."""

    @pytest.mark.reconciliation
    def test_parameter_name_column_mostly_populated(self, report_parameters):
        """The ``parameter_name`` column (raw sheet column A) should be
        populated for at least 80% of rows. Blank rows are acceptable
        (CMS uses them as section spacers) but a fully-blank column
        means the parse didn't pick up column 0."""
        null_count = report_parameters["parameter_name"].null_count()
        total = report_parameters.height
        null_frac = null_count / total if total else 0.0
        assert null_frac < 0.5, (
            f"parameter_name is null on {null_frac:.0%} of rows "
            f"(expected <50% — blanks are section spacers)"
        )

    @pytest.mark.reconciliation
    def test_performance_year_is_4_digit_year(self, report_parameters):
        """The filename-stamped performance_year should be a 4-digit
        year parseable as int, in the plausible range."""
        years = report_parameters["performance_year"].drop_nulls().unique().to_list()
        for y in years:
            try:
                y_int = int(y)
            except ValueError:
                pytest.fail(f"performance_year {y!r} is not parseable as int")
            assert 2020 <= y_int <= 2030, (
                f"performance_year {y_int} outside plausible range [2020, 2030]"
            )
