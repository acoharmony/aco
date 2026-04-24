# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms._identity_timeline_metrics — per-file_date
churn and quality metrics computed over the silver identity_timeline."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from acoharmony._transforms import _identity_timeline_metrics
from acoharmony._test.transforms.identity_timeline import (
    _MockStorage,
    _seed_cclf8,
    _seed_cclf9,
    _seed_empty_hcmpi,
)
from acoharmony._transforms._identity_timeline import execute as silver_execute


@pytest.fixture
def workspace_with_metrics(tmp_path):
    """Two CCLF9 file_dates so we can exercise the per-file_date metrics
    grouping and the new-vs-repeat edge logic."""
    root = tmp_path / "workspace"
    storage = _MockStorage(root)
    silver = storage.get_path("silver")
    # File_date 2025-01-15: A->B (new edge)
    # File_date 2025-06-15: A->B (repeat) and C->D (new edge)
    _seed_cclf9(
        silver,
        [
            ("A", "B", "2025-01-15"),
            ("A", "B", "2025-06-15"),  # repeat
            ("C", "D", "2025-06-15"),  # new on this file_date
        ],
    )
    _seed_cclf8(
        silver,
        [
            ("B", date(2025, 1, 10)),
            ("B", date(2025, 6, 10)),
            ("D", date(2025, 6, 10)),
            ("LONELY", date(2025, 6, 10)),
        ],
    )
    _seed_empty_hcmpi(silver)
    silver_execute(SimpleNamespace(storage_config=storage)).collect().write_parquet(
        silver / "identity_timeline.parquet"
    )
    return SimpleNamespace(storage_config=storage)


class TestExecute:
    def test_emits_long_format_metric_rows(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        assert {"metric_name", "file_date", "value", "computed_at"}.issubset(df.columns)

    def test_remaps_total_per_file_date(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        rt = df.filter(pl.col("metric_name") == "remaps_total").sort("file_date")
        # Jan 15: 1 remap; Jun 15: 2 remaps
        as_dict = {row["file_date"]: row["value"] for row in rt.iter_rows(named=True)}
        assert as_dict[date(2025, 1, 15)] == 1
        assert as_dict[date(2025, 6, 15)] == 2

    def test_remaps_new_excludes_repeat_edges(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        rn = df.filter(pl.col("metric_name") == "remaps_new").sort("file_date")
        as_dict = {row["file_date"]: row["value"] for row in rn.iter_rows(named=True)}
        # Jan 15: A->B is brand new → 1
        # Jun 15: A->B is repeat (first seen Jan 15), C->D is new → 1
        assert as_dict[date(2025, 1, 15)] == 1
        assert as_dict[date(2025, 6, 15)] == 1

    def test_self_obs_total_emitted(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        assert df.filter(pl.col("metric_name") == "self_obs_total").height >= 1

    def test_chains_touched_emitted(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        ct = df.filter(pl.col("metric_name") == "chains_touched")
        assert ct.height >= 1
        # All values must be positive
        assert (ct["value"] > 0).all()

    def test_multi_mbi_chains_counts_chains_with_more_than_one_mbi(self, tmp_path):
        # Need both mbis of a chain to share a file_date — build a fixture
        # where A->B remap (which inserts A into the timeline on its own
        # file_date) lands on the same date as a CCLF8 row for B.
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(silver, [("A", "B", "2025-06-15")])
        # B has a CCLF8 row on the same date as the remap → the chain
        # has both A (from remap) and B (from cclf8) at file_date 2025-06-15
        _seed_cclf8(silver, [("B", date(2025, 6, 15))])
        _seed_empty_hcmpi(silver)
        silver_execute(SimpleNamespace(storage_config=storage)).collect().write_parquet(
            silver / "identity_timeline.parquet"
        )
        df = _identity_timeline_metrics.execute(SimpleNamespace(storage_config=storage)).collect()
        mm = df.filter(pl.col("metric_name") == "multi_mbi_chains")
        assert mm["value"].sum() > 0

    def test_singleton_chains_counts_lonely_mbis(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        sg = df.filter(pl.col("metric_name") == "singleton_chains")
        # LONELY produces a singleton chain on its file_date
        assert sg["value"].sum() > 0

    def test_chain_len_p50_and_max_emitted(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        assert df.filter(pl.col("metric_name") == "chain_len_p50").height >= 1
        assert df.filter(pl.col("metric_name") == "chain_len_max").height >= 1

    def test_hcmpi_coverage_pct_zero_when_no_hcmpi(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        cov = df.filter(pl.col("metric_name") == "hcmpi_coverage_pct")
        assert (cov["value"] == 0).all()

    def test_circular_refs_zero_with_no_cycles(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        # Fixture has no A->B + B->A cycle, so circular_refs should be empty
        # (no group_by output rows with value>0). The metric won't appear at
        # all if zero — verify by checking that if it does appear, value=0.
        cr = df.filter(pl.col("metric_name") == "circular_refs")
        if cr.height:
            assert (cr["value"] == 0).all() or cr.height == 0

    def test_circular_refs_detects_cycle(self, tmp_path):
        # A->B and B->A on the same file_date should register a cycle.
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(
            silver,
            [("A", "B", "2025-06-15"), ("B", "A", "2025-06-15")],
        )
        _seed_cclf8(silver, [("A", date(2025, 6, 10)), ("B", date(2025, 6, 10))])
        _seed_empty_hcmpi(silver)
        silver_execute(SimpleNamespace(storage_config=storage)).collect().write_parquet(
            silver / "identity_timeline.parquet"
        )
        df = _identity_timeline_metrics.execute(SimpleNamespace(storage_config=storage)).collect()
        cr = df.filter(pl.col("metric_name") == "circular_refs")
        assert cr.height >= 1
        assert cr["value"].sum() > 0

    def test_computed_at_iso_timestamp(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        # computed_at is set on every row; just verify the format
        assert df["computed_at"].null_count() == 0
        sample = df["computed_at"][0]
        # Bare-minimum ISO format check: contains 'T'
        assert "T" in sample

    def test_output_sorted_by_file_date_then_metric_name(self, workspace_with_metrics):
        df = _identity_timeline_metrics.execute(workspace_with_metrics).collect()
        # Sort spec: ["file_date", "metric_name"] — verify by re-sorting
        # and asserting equality
        re_sorted = df.sort(["file_date", "metric_name"])
        assert df.equals(re_sorted)

    def test_hcmpi_coverage_pct_nonzero_when_hcmpi_present(self, tmp_path):
        # Re-build with a non-empty hcmpi_master so the coverage pct branch
        # produces a positive value.
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(silver, [("A", "B", "2025-06-15")])
        _seed_cclf8(silver, [("A", date(2025, 6, 10)), ("B", date(2025, 6, 10))])
        pl.DataFrame(
            [
                {"identifier": "A", "identifier_src_field": "member_mbi", "hcmpi": "H1"},
                {"identifier": "B", "identifier_src_field": "member_mbi", "hcmpi": "H1"},
            ]
        ).write_parquet(silver / "hcmpi_master.parquet")
        silver_execute(SimpleNamespace(storage_config=storage)).collect().write_parquet(
            silver / "identity_timeline.parquet"
        )
        df = _identity_timeline_metrics.execute(SimpleNamespace(storage_config=storage)).collect()
        cov = df.filter(pl.col("metric_name") == "hcmpi_coverage_pct")
        assert (cov["value"] > 0).any()
