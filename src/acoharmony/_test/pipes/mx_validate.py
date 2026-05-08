# © 2025 HarmonyCares — tests for acoharmony._pipes._mx_validate
"""Unit tests for the mx_validate pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from acoharmony._pipes import _mx_validate as mxv


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_blqqr_ref(
    silver: Path,
    measure: str,
    rows: list[dict],
    source_filenames: list[str] | None = None,
) -> Path:
    """
    Write a synthetic silver/blqqr_<measure>.parquet that mimics the real
    schema closely enough for the scope enumerator and tieout to work.

    BLQQR carries both bene_id (4i internal numeric ID) and mbi (Medicare
    Beneficiary Identifier). The tieout join uses mbi. If a fixture row
    omits mbi, we synthesize one from bene_id so the existing tests don't
    need rewriting — the value is irrelevant when the fixture doesn't
    care about a particular bene matching computed output.
    """
    silver.mkdir(parents=True, exist_ok=True)
    if source_filenames is None:
        source_filenames = [f"REACH.D0259.BLQQR.Q1.PY2024.{measure.upper()}.csv"] * len(rows)
    keys = list(rows[0])
    if "mbi" not in keys and "bene_id" in keys:
        keys.append("mbi")
        for r in rows:
            r.setdefault("mbi", f"MBI_{r['bene_id']}")
    df = pl.DataFrame(
        {
            **{k: [r[k] for r in rows] for k in keys},
            "source_filename": source_filenames,
        }
    )
    out = silver / f"blqqr_{measure}.parquet"
    df.write_parquet(out)
    return out


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHashColumns:
    def test_stable_independent_of_order(self):
        a = mxv._hash_columns(["b", "a", "c"])
        b = mxv._hash_columns(["a", "c", "b"])
        assert a == b
        assert len(a) == 12

    def test_changes_with_columns(self):
        assert mxv._hash_columns(["a"]) != mxv._hash_columns(["a", "b"])


@pytest.mark.unit
class TestScopeOutputName:
    def test_format(self):
        assert (
            mxv._scope_output_name("uamcc", 2025, 3)
            == "mx_validate_uamcc_PY2025_Q3.parquet"
        )


# ---------------------------------------------------------------------------
# discover_scopes
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDiscoverScopes:
    def test_greedy_full_matrix_for_one_measure(self, tmp_path):
        silver = tmp_path / "silver"
        # Two PYs × two quarters = 4 scopes for ACR
        rows = []
        fns = []
        for py in (2024, 2025):
            for q in (1, 2):
                rows.append(
                    {"aco_id": "D0259", "bene_id": f"B{py}{q}", "radm30_flag": 0}
                )
                fns.append(f"REACH.D0259.BLQQR.Q{q}.PY{py}.ACR.csv")
        _write_blqqr_ref(silver, "acr", rows, source_filenames=fns)

        scopes = mxv.discover_scopes(silver)
        assert {(s.py, s.quarter) for s in scopes} == {
            (2024, 1), (2024, 2), (2025, 1), (2025, 2)
        }
        assert all(s.measure == "acr" for s in scopes)
        assert all(s.status == "ready" for s in scopes)
        assert all(s.transform_class == "NQF1789" for s in scopes)

    def test_skips_when_transform_unregistered(self, tmp_path, monkeypatch):
        silver = tmp_path / "silver"
        _write_blqqr_ref(
            silver,
            "acr",
            [{"aco_id": "D0259", "bene_id": "B1", "radm30_flag": 0}],
        )

        # Pretend nothing is registered
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        monkeypatch.setattr(MeasureFactory, "list_measures", classmethod(lambda cls: []))

        scopes = mxv.discover_scopes(silver)
        assert len(scopes) == 1
        assert scopes[0].status == "skip:no_transform"
        assert scopes[0].transform_class is None
        assert "NQF1789" in scopes[0].skip_reason

    def test_missing_ref_file_yields_no_scope(self, tmp_path):
        # Empty silver dir → nothing discovered
        scopes = mxv.discover_scopes(tmp_path / "silver")
        assert scopes == []

    def test_unparseable_filename_skipped(self, tmp_path):
        silver = tmp_path / "silver"
        _write_blqqr_ref(
            silver,
            "acr",
            [{"aco_id": "D0259", "bene_id": "B1", "radm30_flag": 0}],
            source_filenames=["totally-not-a-blqqr-name.csv"],
        )
        scopes = mxv.discover_scopes(silver)
        assert scopes == []

    def test_zero_rows_per_scope_marked_skip(self, tmp_path, monkeypatch):
        """Force the skip:no_ref_rows branch by stubbing the row-count query."""
        silver = tmp_path / "silver"
        _write_blqqr_ref(
            silver,
            "acr",
            [{"aco_id": "D0259", "bene_id": "B1", "radm30_flag": 0}],
            source_filenames=["REACH.D0259.BLQQR.Q1.PY2024.ACR.csv"],
        )

        # Patch the count step inside discover_scopes to return 0
        # to simulate "filename present in unique() but no data rows".
        original_collect = pl.LazyFrame.collect

        def stub_collect(self, *args, **kwargs):
            # Hijack only the .select(pl.len()) call
            df = original_collect(self, *args, **kwargs)
            if df.shape == (1, 1) and df.columns == ["len"]:
                return pl.DataFrame({"len": [0]})
            return df

        monkeypatch.setattr(pl.LazyFrame, "collect", stub_collect)
        scopes = mxv.discover_scopes(silver)
        assert any(s.status == "skip:no_ref_rows" for s in scopes)
        skip = next(s for s in scopes if s.status == "skip:no_ref_rows")
        assert "empty" in skip.skip_reason.lower()


# ---------------------------------------------------------------------------
# _scope_rows_to_frame
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestScopeRowsToFrame:
    def test_round_trip(self):
        rows = [
            mxv.ScopeRow(
                measure="acr",
                py=2024,
                quarter=1,
                source_filename="REACH.D0259.BLQQR.Q1.PY2024.ACR.csv",
                ref_row_count=42,
                ref_columns_hash="abc123",
                transform_class="NQF1789",
                status="ready",
                skip_reason=None,
            )
        ]
        df = mxv._scope_rows_to_frame(rows)
        assert df.shape == (1, 9)
        assert df["measure"].to_list() == ["acr"]
        assert df["status"].to_list() == ["ready"]


# ---------------------------------------------------------------------------
# tieout_scope: agreement math
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTieoutScope:
    def test_perfect_agreement(self):
        scope = mxv.ScopeRow(
            measure="dah",
            py=2024,
            quarter=1,
            source_filename="REACH.D0259.BLQQR.Q1.PY2024.DAH.csv",
            ref_row_count=3,
            ref_columns_hash="x",
            transform_class="REACH_DAH",
            status="ready",
            skip_reason=None,
        )
        ref = pl.LazyFrame(
            {"person_id": ["A", "B", "C"], "ref_value": [365, 200, 0]}
        )
        computed = pl.LazyFrame(
            {
                "person_id": ["A", "B", "C"],
                "denom_flag": [True, True, True],
                "num_flag": [True, True, True],
                "observed_dah": [365, 200, 0],
            }
        )
        row = mxv.tieout_scope(scope, computed, ref)
        assert row["bene_count_ref"] == 3
        assert row["bene_count_matched"] == 3
        assert row["agreement_pct"] == 1.0
        assert row["mean_abs_diff"] == 0.0
        assert json.loads(row["worst_mismatches_json"]) == []

    def test_partial_agreement_with_mismatches(self):
        scope = mxv.ScopeRow(
            measure="dah",
            py=2024,
            quarter=1,
            source_filename="f",
            ref_row_count=4,
            ref_columns_hash="x",
            transform_class="REACH_DAH",
            status="ready",
            skip_reason=None,
        )
        ref = pl.LazyFrame(
            {"person_id": ["A", "B", "C", "D"], "ref_value": [365, 200, 100, 50]}
        )
        computed = pl.LazyFrame(
            {
                "person_id": ["A", "B", "C", "D"],
                "denom_flag": [True] * 4,
                "num_flag": [True] * 4,
                "observed_dah": [365, 195, 100, 0],  # B off by 5, D off by 50
            }
        )
        row = mxv.tieout_scope(scope, computed, ref)
        assert row["bene_count_matched"] == 2
        assert row["agreement_pct"] == 0.5
        assert row["mean_abs_diff"] == pytest.approx((0 + 5 + 0 + 50) / 4)
        worst = json.loads(row["worst_mismatches_json"])
        assert worst[0]["person_id"] == "D"  # largest abs_diff comes first

    def test_acr_uses_numerator_flag_as_int(self):
        scope = mxv.ScopeRow(
            measure="acr",
            py=2025,
            quarter=2,
            source_filename="f",
            ref_row_count=2,
            ref_columns_hash="x",
            transform_class="NQF1789",
            status="ready",
            skip_reason=None,
        )
        ref = pl.LazyFrame({"person_id": ["A", "B"], "ref_value": [1, 0]})
        computed = pl.LazyFrame(
            {
                "person_id": ["A", "B"],
                "denom_flag": [True, True],
                "num_flag": [True, False],
                "numerator_flag": [1, 0],
            }
        )
        row = mxv.tieout_scope(scope, computed, ref)
        assert row["agreement_pct"] == 1.0

    def test_missing_computed_treated_as_zero(self):
        scope = mxv.ScopeRow(
            measure="dah",
            py=2024,
            quarter=1,
            source_filename="f",
            ref_row_count=2,
            ref_columns_hash="x",
            transform_class="REACH_DAH",
            status="ready",
            skip_reason=None,
        )
        ref = pl.LazyFrame({"person_id": ["A", "B"], "ref_value": [365, 200]})
        # Computed only knows about A
        computed = pl.LazyFrame(
            {
                "person_id": ["A"],
                "denom_flag": [True],
                "num_flag": [True],
                "observed_dah": [365],
            }
        )
        row = mxv.tieout_scope(scope, computed, ref)
        # B is missing in computed → treated as 0 → off by 200
        assert row["bene_count_matched"] == 1
        assert row["mean_abs_diff"] == 100.0


# ---------------------------------------------------------------------------
# _ref_value_for_scope
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLoadValueSets:
    def test_dah_returns_empty_dict(self, tmp_path):
        """DAH carries no codeset value-sets — _VALUE_SET_FILES['dah'] = {}."""
        out = mxv._load_value_sets(tmp_path, "dah")
        assert out == {}

    def test_skips_missing_files(self, tmp_path):
        """For uamcc, with no parquets present, all keys silently absent."""
        out = mxv._load_value_sets(tmp_path, "uamcc")
        assert out == {}

    def test_loads_present_files_only(self, tmp_path):
        """Loads the parquets that exist, skips the rest."""
        # Create one of the expected uamcc files
        f = tmp_path / "value_sets_uamcc_value_set_paa1.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(f)
        out = mxv._load_value_sets(tmp_path, "uamcc")
        assert set(out.keys()) == {"paa1"}


@pytest.mark.unit
class TestReachAlignedPersonsForPy:
    """_reach_aligned_persons_for_py implements §3 p11 alignment-eligible-month rule."""

    def test_includes_bene_aligned_in_any_month_of_py(self):
        # Two benes, two PYs of monthly REACH flags
        cols = {
            "current_mbi": ["A", "B"],
        }
        for m in range(1, 13):
            cols[f"ym_2024{m:02d}_reach"] = [m == 7, False]   # A aligned in July only
            cols[f"ym_2025{m:02d}_reach"] = [False, m >= 6]    # B aligned Jun-Dec
        df = pl.LazyFrame(cols)
        assert mxv._reach_aligned_persons_for_py(df, 2024).collect()["person_id"].to_list() == ["A"]
        assert mxv._reach_aligned_persons_for_py(df, 2025).collect()["person_id"].to_list() == ["B"]

    def test_excludes_bene_with_no_alignment_in_py(self):
        cols = {"current_mbi": ["X"]}
        for m in range(1, 13):
            cols[f"ym_2024{m:02d}_reach"] = [False]
        df = pl.LazyFrame(cols)
        assert mxv._reach_aligned_persons_for_py(df, 2024).collect().height == 0


@pytest.mark.unit
class TestRefValueForScope:
    def test_loads_correct_column_per_measure(self, tmp_path):
        silver = tmp_path / "silver"
        _write_blqqr_ref(
            silver,
            "uamcc",
            [
                {"aco_id": "D0259", "bene_id": "B1", "count_unplanned_adm": 3},
                {"aco_id": "D0259", "bene_id": "B2", "count_unplanned_adm": 0},
            ],
        )
        scope = mxv.ScopeRow(
            measure="uamcc",
            py=2024,
            quarter=1,
            source_filename="REACH.D0259.BLQQR.Q1.PY2024.UAMCC.csv",
            ref_row_count=2,
            ref_columns_hash="x",
            transform_class="NQF2888",
            status="ready",
            skip_reason=None,
        )
        out = mxv._ref_value_for_scope(silver, scope).collect()
        assert set(out.columns) == {"person_id", "ref_value"}
        # _write_blqqr_ref synthesizes mbi as f"MBI_{bene_id}" when not given,
        # and _ref_value_for_scope aliases mbi → person_id for the join.
        assert sorted(out["person_id"].to_list()) == ["MBI_B1", "MBI_B2"]
        assert sorted(out["ref_value"].to_list()) == [0, 3]


# ---------------------------------------------------------------------------
# end-to-end pipeline (synthetic data)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPipelineEndToEnd:
    def test_runs_and_emits_three_tables(self, tmp_path):
        # Re-import to re-register the pipeline (autouse conftest clears it).
        import importlib
        from acoharmony._pipes import _mx_validate as m

        importlib.reload(m)

        bronze = tmp_path / "bronze"
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        for p in (bronze, silver, gold):
            p.mkdir()

        # One DAH scope only — keeps the test deterministic and fast.
        # 2024 is a leap year → eligible_days = 366.
        # mbi must match the eligibility person_id so the tieout join
        # produces a row (mbi-keyed join, see _ref_value_for_scope).
        _write_blqqr_ref(
            silver,
            "dah",
            [
                {
                    "aco_id": "D0259",
                    "bene_id": "B1",
                    "mbi": "B1",
                    "survival_days": "366",
                    "observed_dah": 366,
                    "observed_dic": 0,
                }
            ],
            source_filenames=["REACH.D0259.BLQQR.Q1.PY2024.DAH.csv"],
        )

        # Gold inputs satisfying the spec-correct DAH denominator
        # (CMS PY2025 QMMR §3.3.2 p15): adult ≥18, alive on PY start,
        # ≥12-month prior FFS lookback, continuous through PY end.
        pl.DataFrame(
            {
                "person_id": ["B1"],
                "birth_date": ["1950-01-01"],
                "death_date": [None],
                "enrollment_start_date": ["2022-01-01"],  # ≥12-mo prior to PY2024
                "enrollment_end_date": ["2024-12-31"],
            }
        ).with_columns(
            [
                pl.col("birth_date").str.to_date(),
                pl.col("death_date").cast(pl.Date),
                pl.col("enrollment_start_date").str.to_date(),
                pl.col("enrollment_end_date").str.to_date(),
            ]
        ).write_parquet(gold / "eligibility.parquet")

        # HCC scores for year-before-PY (criterion 4: avg ≥ 2.0).
        pl.DataFrame(
            {
                "mbi": ["B1"],
                "performance_year": [2023],
                "model_version": ["v28"],
                "total_risk_score": [3.5],
            },
            schema={
                "mbi": pl.Utf8,
                "performance_year": pl.Int64,
                "model_version": pl.Utf8,
                "total_risk_score": pl.Float64,
            },
        ).write_parquet(gold / "hcc_risk_scores.parquet")

        # Empty medical_claim with the full DAH-relevant schema.
        pl.DataFrame(
            {c: [] for c in (
                "claim_id", "person_id", "bill_type_code",
                "admission_date", "discharge_date",
                "claim_start_date", "claim_end_date",
                "claim_line_start_date",
                "revenue_center_code", "hcpcs_code", "diagnosis_code_1",
            )},
            schema={
                "claim_id": pl.Utf8,
                "person_id": pl.Utf8,
                "bill_type_code": pl.Utf8,
                "admission_date": pl.Date,
                "discharge_date": pl.Date,
                "claim_start_date": pl.Date,
                "claim_end_date": pl.Date,
                "claim_line_start_date": pl.Date,
                "revenue_center_code": pl.Utf8,
                "hcpcs_code": pl.Utf8,
                "diagnosis_code_1": pl.Utf8,
            },
        ).write_parquet(gold / "medical_claim.parquet")

        executor = MagicMock()
        from acoharmony.medallion import MedallionLayer

        executor.storage_config.get_path.side_effect = lambda layer: {
            MedallionLayer.BRONZE: bronze,
            MedallionLayer.SILVER: silver,
            MedallionLayer.GOLD: gold,
        }[layer]
        logger = MagicMock()

        result = m.apply_mx_validate_pipeline(executor, logger, force=True)

        # All three artifacts written
        assert (bronze / "mx_validate_scope.parquet").exists()
        scope_df = pl.read_parquet(bronze / "mx_validate_scope.parquet")
        assert scope_df.height == 1
        assert scope_df["status"].to_list() == ["ready"]

        compute_file = silver / "mx_validate_dah_PY2024_Q1.parquet"
        assert compute_file.exists()
        compute_df = pl.read_parquet(compute_file)
        assert "observed_dah" in compute_df.columns

        tieout_file = gold / "mx_validate_tieout.parquet"
        assert tieout_file.exists()
        tie_df = pl.read_parquet(tieout_file)
        assert tie_df.height == 1
        # B1: ref=365, computed=365 → perfect match
        assert tie_df["agreement_pct"].to_list() == [1.0]

        assert result["scope"] == bronze / "mx_validate_scope.parquet"
        assert compute_file in result["compute"]

    def test_skip_existing_when_force_false(self, tmp_path):
        import importlib
        from acoharmony._pipes import _mx_validate as m

        importlib.reload(m)

        bronze = tmp_path / "bronze"
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        for p in (bronze, silver, gold):
            p.mkdir()

        _write_blqqr_ref(
            silver,
            "dah",
            [
                {
                    "aco_id": "D0259",
                    "bene_id": "B1",
                    "survival_days": "365",
                    "observed_dah": 365,
                    "observed_dic": 0,
                }
            ],
            source_filenames=["REACH.D0259.BLQQR.Q1.PY2024.DAH.csv"],
        )
        # Pre-create the compute file so the pipeline takes the skip branch
        existing = silver / "mx_validate_dah_PY2024_Q1.parquet"
        pl.DataFrame(
            {
                "person_id": ["B1"],
                "denom_flag": [True],
                "num_flag": [True],
                "observed_dah": [365],
            }
        ).write_parquet(existing)

        # Empty gold inputs would normally fail compute, but skip avoids it
        pl.DataFrame(
            {
                "person_id": [],
                "birth_date": [],
                "death_date": [],
                "enrollment_start_date": [],
                "enrollment_end_date": [],
            },
            schema={
                "person_id": pl.Utf8,
                "birth_date": pl.Date,
                "death_date": pl.Date,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
            },
        ).write_parquet(gold / "eligibility.parquet")
        pl.DataFrame(
            {"claim_id": [], "person_id": [], "bill_type_code": [],
             "admission_date": [], "discharge_date": []},
            schema={"claim_id": pl.Utf8, "person_id": pl.Utf8,
                    "bill_type_code": pl.Utf8, "admission_date": pl.Date,
                    "discharge_date": pl.Date},
        ).write_parquet(gold / "medical_claim.parquet")

        executor = MagicMock()
        from acoharmony.medallion import MedallionLayer
        executor.storage_config.get_path.side_effect = lambda layer: {
            MedallionLayer.BRONZE: bronze,
            MedallionLayer.SILVER: silver,
            MedallionLayer.GOLD: gold,
        }[layer]
        logger = MagicMock()

        m.apply_mx_validate_pipeline(executor, logger, force=False)
        # File should still match what we pre-wrote (not re-computed)
        df = pl.read_parquet(existing)
        assert df["observed_dah"].to_list() == [365]


# ---------------------------------------------------------------------------
# compute_scope: ACR branch (uses numerator_flag → int)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestComputeScopeAcr:
    def test_acr_branch_returns_int_numerator(self):
        """compute_scope's ACR branch maps numerator_flag → Int64 column."""
        from datetime import date as _date
        from unittest.mock import patch

        scope = mxv.ScopeRow(
            measure="acr",
            py=2024,
            quarter=1,
            source_filename="f",
            ref_row_count=1,
            ref_columns_hash="x",
            transform_class="NQF1789",
            status="ready",
            skip_reason=None,
        )

        # Stub MeasureFactory.create to return an object whose
        # calculate_denominator/numerator emit the shapes compute_scope expects.
        class _StubMeasure:
            def calculate_denominator(self, claims, elig, vs):
                return pl.LazyFrame(
                    {"person_id": ["A", "B"], "denominator_flag": [True, True]}
                )

            def calculate_numerator(self, denom, claims, vs):
                return pl.LazyFrame(
                    {"person_id": ["A"], "numerator_flag": [True]}
                )

        from acoharmony._transforms._quality_measure_base import MeasureFactory

        with patch.object(MeasureFactory, "create", return_value=_StubMeasure()):
            out = mxv.compute_scope(
                scope, pl.LazyFrame(), pl.LazyFrame()
            ).collect()

        assert set(out.columns) == {"person_id", "denom_flag", "num_flag", "numerator_flag"}
        assert sorted(out["person_id"].to_list()) == ["A", "B"]
        # A has numerator_flag True → 1; B has it filled to False → 0
        by_person = dict(zip(out["person_id"].to_list(), out["numerator_flag"].to_list()))
        assert by_person["A"] == 1
        assert by_person["B"] == 0


# ---------------------------------------------------------------------------
# Pipeline branch coverage: skipped scope log + missing compute file
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPipelineWithAlignmentFile:
    """Cover the branch where consolidated_alignment.parquet IS present."""

    def test_alignment_file_loaded_and_passed_through(self, tmp_path):
        import importlib
        from acoharmony._pipes import _mx_validate as m

        importlib.reload(m)

        bronze = tmp_path / "bronze"
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        for p in (bronze, silver, gold):
            p.mkdir()

        _write_blqqr_ref(
            silver,
            "dah",
            [
                {
                    "aco_id": "D0259",
                    "bene_id": "B1",
                    "survival_days": "366",
                    "observed_dah": 366,
                    "observed_dic": 0,
                }
            ],
            source_filenames=["REACH.D0259.BLQQR.Q1.PY2024.DAH.csv"],
        )

        # Eligibility passes all DAH criteria.
        pl.DataFrame(
            {
                "person_id": ["B1"],
                "birth_date": ["1950-01-01"],
                "death_date": [None],
                "enrollment_start_date": ["2022-01-01"],
                "enrollment_end_date": ["2024-12-31"],
            }
        ).with_columns(
            [
                pl.col("birth_date").str.to_date(),
                pl.col("death_date").cast(pl.Date),
                pl.col("enrollment_start_date").str.to_date(),
                pl.col("enrollment_end_date").str.to_date(),
            ]
        ).write_parquet(gold / "eligibility.parquet")

        # HCC ≥ 2.0
        pl.DataFrame(
            {
                "mbi": ["B1"],
                "performance_year": [2023],
                "model_version": ["v28"],
                "total_risk_score": [3.5],
            },
            schema={
                "mbi": pl.Utf8, "performance_year": pl.Int64,
                "model_version": pl.Utf8, "total_risk_score": pl.Float64,
            },
        ).write_parquet(gold / "hcc_risk_scores.parquet")

        # consolidated_alignment with B1 REACH-aligned in 2024.
        align_cols = {"current_mbi": ["B1"]}
        for mo in range(1, 13):
            align_cols[f"ym_2024{mo:02d}_reach"] = [True]
        pl.DataFrame(align_cols).write_parquet(gold / "consolidated_alignment.parquet")

        pl.DataFrame(
            {c: [] for c in (
                "claim_id", "person_id", "bill_type_code",
                "admission_date", "discharge_date",
                "claim_start_date", "claim_end_date",
                "claim_line_start_date",
                "revenue_center_code", "hcpcs_code", "diagnosis_code_1",
            )},
            schema={
                "claim_id": pl.Utf8, "person_id": pl.Utf8, "bill_type_code": pl.Utf8,
                "admission_date": pl.Date, "discharge_date": pl.Date,
                "claim_start_date": pl.Date, "claim_end_date": pl.Date,
                "claim_line_start_date": pl.Date,
                "revenue_center_code": pl.Utf8, "hcpcs_code": pl.Utf8,
                "diagnosis_code_1": pl.Utf8,
            },
        ).write_parquet(gold / "medical_claim.parquet")

        executor = MagicMock()
        from acoharmony.medallion import MedallionLayer
        executor.storage_config.get_path.side_effect = lambda layer: {
            MedallionLayer.BRONZE: bronze,
            MedallionLayer.SILVER: silver,
            MedallionLayer.GOLD: gold,
        }[layer]
        logger = MagicMock()
        m.apply_mx_validate_pipeline(executor, logger, force=True)

        # No "consolidated_alignment.parquet not found" warning was emitted
        warn_calls = [
            str(c) for c in logger.warning.call_args_list
            if "consolidated_alignment" in str(c) and "not found" in str(c)
        ]
        assert warn_calls == []
        # Compute file exists with the aligned bene
        compute_df = pl.read_parquet(silver / "mx_validate_dah_PY2024_Q1.parquet")
        assert "B1" in compute_df["person_id"].to_list()


@pytest.mark.unit
class TestPipelineBranchCoverage:
    def test_skipped_scope_logged_and_compute_missing_skipped_in_tieout(
        self, tmp_path, monkeypatch
    ):
        """Cover (a) the per-skip log line, (b) tieout's missing-file skip."""
        import importlib
        from acoharmony._pipes import _mx_validate as m

        importlib.reload(m)

        bronze = tmp_path / "bronze"
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        for p in (bronze, silver, gold):
            p.mkdir()

        # One scope marked skip:no_transform via unregistered measure
        _write_blqqr_ref(
            silver,
            "acr",
            [{"aco_id": "D0259", "bene_id": "B1", "radm30_flag": 0}],
            source_filenames=["REACH.D0259.BLQQR.Q1.PY2024.ACR.csv"],
        )
        from acoharmony._transforms._quality_measure_base import MeasureFactory
        monkeypatch.setattr(
            MeasureFactory, "list_measures", classmethod(lambda cls: [])
        )

        # Empty gold inputs (won't be touched — no ready scopes)
        for name in ("eligibility", "medical_claim"):
            pl.DataFrame(
                {"person_id": []}, schema={"person_id": pl.Utf8}
            ).write_parquet(gold / f"{name}.parquet")

        executor = MagicMock()
        from acoharmony.medallion import MedallionLayer
        executor.storage_config.get_path.side_effect = lambda layer: {
            MedallionLayer.BRONZE: bronze,
            MedallionLayer.SILVER: silver,
            MedallionLayer.GOLD: gold,
        }[layer]
        logger = MagicMock()

        m.apply_mx_validate_pipeline(executor, logger, force=True)

        # The skip-log path was hit
        skip_calls = [
            str(c) for c in logger.info.call_args_list if "skip acr" in str(c)
        ]
        assert skip_calls, "expected skip log line for unregistered acr scope"

        # No tieout file written (zero ready scopes)
        assert not (gold / "mx_validate_tieout.parquet").exists()

    def test_tieout_skips_when_compute_file_missing(self, tmp_path, monkeypatch):
        """Cover the `if not out_file.exists(): continue` branch in tieout."""
        import importlib
        from acoharmony._pipes import _mx_validate as m

        importlib.reload(m)

        bronze = tmp_path / "bronze"
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        for p in (bronze, silver, gold):
            p.mkdir()

        _write_blqqr_ref(
            silver,
            "dah",
            [
                {
                    "aco_id": "D0259",
                    "bene_id": "B1",
                    "survival_days": "365",
                    "observed_dah": 365,
                    "observed_dic": 0,
                }
            ],
            source_filenames=["REACH.D0259.BLQQR.Q1.PY2024.DAH.csv"],
        )
        for name in ("eligibility", "medical_claim"):
            pl.DataFrame(
                {"person_id": []}, schema={"person_id": pl.Utf8}
            ).write_parquet(gold / f"{name}.parquet")

        # Force compute_scope to raise so no compute file is written,
        # but the scope is still in `ready` → tieout enters the loop and
        # hits the `not out_file.exists()` branch.
        monkeypatch.setattr(
            m, "compute_scope", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom"))
        )

        executor = MagicMock()
        from acoharmony.medallion import MedallionLayer
        executor.storage_config.get_path.side_effect = lambda layer: {
            MedallionLayer.BRONZE: bronze,
            MedallionLayer.SILVER: silver,
            MedallionLayer.GOLD: gold,
        }[layer]
        logger = MagicMock()

        m.apply_mx_validate_pipeline(executor, logger, force=True)

        # compute_scope failure was logged
        err_calls = [
            str(c) for c in logger.error.call_args_list if "FAILED" in str(c)
        ]
        assert err_calls
        # No compute file → tieout loop skipped → no tieout file
        assert not (silver / "mx_validate_dah_PY2024_Q1.parquet").exists()
        assert not (gold / "mx_validate_tieout.parquet").exists()
