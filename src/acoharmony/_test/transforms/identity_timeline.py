# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms._identity_timeline module — silver-tier MBI
chain construction from CCLF8/CCLF9, plus the per-chain canonical
lookups consumed by downstream dedup transforms."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from acoharmony._transforms._identity_timeline import (
    _compute_chain_ids,
    current_mbi_lookup_lazy,
    current_mbi_with_hcmpi_lookup_lazy,
    execute,
)


class _MockStorage:
    def __init__(self, root: Path):
        self._root = root

    def get_path(self, tier):
        from acoharmony.medallion import MedallionLayer
        tier_name = tier.data_tier if isinstance(tier, MedallionLayer) else str(tier).lower()
        p = self._root / tier_name
        p.mkdir(parents=True, exist_ok=True)
        return p


def _seed_cclf9(silver: Path, edges: list[tuple[str, str, str]]) -> None:
    """Write a CCLF9 fixture from (prvs_num, crnt_num, file_date) tuples."""
    rows = [
        {
            "prvs_num": p,
            "crnt_num": c,
            "prvs_id_efctv_dt": None,
            "prvs_id_obslt_dt": None,
            "file_date": fd,
            "source_filename": f"P.D0259.ZC9R26.D{fd.replace('-', '')[2:]}.txt",
        }
        for p, c, fd in edges
    ]
    pl.DataFrame(
        rows,
        schema={
            "prvs_num": pl.String,
            "crnt_num": pl.String,
            "prvs_id_efctv_dt": pl.Date,
            "prvs_id_obslt_dt": pl.Date,
            "file_date": pl.String,
            "source_filename": pl.String,
        },
    ).write_parquet(silver / "cclf9.parquet")


def _seed_cclf8(silver: Path, observations: list[tuple[str, date]]) -> None:
    """Write a CCLF8 fixture from (mbi, file_date) tuples."""
    rows = [
        {
            "bene_mbi_id": m,
            "source_filename": f"P.D0259.ZC8Y26.D{fd.isoformat().replace('-', '')[2:]}.txt",
            "file_date": fd,
        }
        for m, fd in observations
    ]
    pl.DataFrame(rows).write_parquet(silver / "cclf8.parquet")


def _seed_hcmpi(silver: Path, mappings: list[tuple[str, str]]) -> None:
    """(mbi, hcmpi) pairs — only `member_mbi` rows count."""
    rows = [
        {"identifier": m, "identifier_src_field": "member_mbi", "hcmpi": h}
        for m, h in mappings
    ]
    pl.DataFrame(rows).write_parquet(silver / "hcmpi_master.parquet")


def _seed_empty_hcmpi(silver: Path) -> None:
    """The transform tries to scan hcmpi_master.parquet; an empty fixture
    keeps the join valid without enriching anything."""
    pl.DataFrame(
        schema={"identifier": pl.String, "identifier_src_field": pl.String, "hcmpi": pl.String}
    ).write_parquet(silver / "hcmpi_master.parquet")


@pytest.fixture
def workspace(tmp_path):
    root = tmp_path / "workspace"
    storage = _MockStorage(root)
    silver = storage.get_path("silver")
    _seed_cclf9(silver, [("OLD1", "MID1", "2025-01-15"), ("MID1", "NEW1", "2025-06-15")])
    _seed_cclf8(silver, [("NEW1", date(2025, 1, 10)), ("NEW1", date(2025, 7, 10)), ("LONELY", date(2025, 5, 10))])
    _seed_empty_hcmpi(silver)
    return SimpleNamespace(storage_config=storage, silver=silver)


class TestComputeChainIds:
    def test_empty_edges_returns_empty_frame(self):
        empty = pl.DataFrame(schema={"prvs_num": pl.String, "crnt_num": pl.String})
        result = _compute_chain_ids(empty)
        assert result.height == 0
        assert set(result.columns) == {"mbi", "chain_id"}

    def test_single_edge_groups_two_mbis_into_one_chain(self):
        edges = pl.DataFrame({"prvs_num": ["A"], "crnt_num": ["B"]})
        result = _compute_chain_ids(edges)
        assert result.height == 2
        assert result["chain_id"].n_unique() == 1

    def test_disconnected_edges_produce_distinct_chains(self):
        edges = pl.DataFrame({"prvs_num": ["A", "C"], "crnt_num": ["B", "D"]})
        result = _compute_chain_ids(edges)
        assert result["chain_id"].n_unique() == 2

    def test_transitive_chain_collapses_into_one(self):
        # A->B and B->C -> {A,B,C} in one chain
        edges = pl.DataFrame({"prvs_num": ["A", "B"], "crnt_num": ["B", "C"]})
        result = _compute_chain_ids(edges)
        assert result["chain_id"].n_unique() == 1
        assert result.height == 3

    def test_null_edges_skipped(self):
        edges = pl.DataFrame({"prvs_num": ["A", None], "crnt_num": ["B", "C"]})
        # Null edge skipped; the {A,B} edge still produces a 2-mbi chain;
        # C still appears as a singleton node.
        result = _compute_chain_ids(edges)
        assert "C" in result["mbi"].to_list()

    def test_chain_id_deterministic_across_runs(self):
        edges = pl.DataFrame({"prvs_num": ["A", "B"], "crnt_num": ["B", "C"]})
        first = _compute_chain_ids(edges)["chain_id"][0]
        second = _compute_chain_ids(edges)["chain_id"][0]
        assert first == second

    def test_union_picks_lex_smallest_root(self):
        # Both unions converge to root "A" (smallest)
        edges = pl.DataFrame(
            {"prvs_num": ["B", "C", "D"], "crnt_num": ["A", "A", "B"]}
        )
        result = _compute_chain_ids(edges)
        assert result["chain_id"].n_unique() == 1
        assert result["mbi"].n_unique() == 4

    def test_redundant_edge_no_op_union(self):
        # A->B and A->B (duplicate) → second union sees same root and
        # short-circuits via the `if ra == rb: return` branch.
        edges = pl.DataFrame({"prvs_num": ["A", "A"], "crnt_num": ["B", "B"]})
        result = _compute_chain_ids(edges)
        assert result["chain_id"].n_unique() == 1
        assert sorted(result["mbi"].to_list()) == ["A", "B"]

    def test_cycle_edge_short_circuits_union(self):
        # A->B then B->A: by the second union both are already in the
        # same component, so the early return fires.
        edges = pl.DataFrame({"prvs_num": ["A", "B"], "crnt_num": ["B", "A"]})
        result = _compute_chain_ids(edges)
        assert result["chain_id"].n_unique() == 1


class TestExecute:
    def test_emits_one_row_per_observation_with_chain_columns(self, workspace):
        df = execute(workspace).collect()
        cols = set(df.columns)
        assert {"mbi", "maps_to_mbi", "chain_id", "hop_index", "observation_type"}.issubset(cols)

    def test_remap_rows_carry_observation_type_cclf9_remap(self, workspace):
        df = execute(workspace).collect()
        remaps = df.filter(pl.col("observation_type") == "cclf9_remap")
        assert remaps.height == 2
        assert remaps["maps_to_mbi"].drop_nulls().len() == 2

    def test_self_rows_carry_observation_type_cclf8_self_with_null_target(self, workspace):
        df = execute(workspace).collect()
        selfs = df.filter(pl.col("observation_type") == "cclf8_self")
        assert selfs.height >= 2
        assert selfs["maps_to_mbi"].is_null().all()

    def test_canonical_at_hop_index_zero(self, workspace):
        df = execute(workspace).collect()
        new1 = df.filter(pl.col("mbi") == "NEW1").select("hop_index").unique()
        # NEW1 never appears as prvs_num → it's the leaf, hop_index = 0
        assert new1["hop_index"].to_list() == [0]
        old1 = df.filter(pl.col("mbi") == "OLD1").select("hop_index").unique()
        # OLD1 is the deepest historical, gets a non-zero hop
        assert old1["hop_index"][0] > 0

    def test_singleton_mbi_gets_own_chain(self, workspace):
        df = execute(workspace).collect()
        lonely = df.filter(pl.col("mbi") == "LONELY")
        # No CCLF9 edges for LONELY → singleton chain, only their own mbi in it
        chain_id = lonely["chain_id"][0]
        chain_members = df.filter(pl.col("chain_id") == chain_id)["mbi"].unique()
        assert chain_members.to_list() == ["LONELY"]

    def test_is_current_as_of_file_date_set_for_latest_cclf9_remap(self, workspace):
        df = execute(workspace).collect()
        # Most recent CCLF9 file_date is 2025-06-15
        latest_remaps = df.filter(
            (pl.col("observation_type") == "cclf9_remap")
            & (pl.col("file_date") == date(2025, 6, 15))
        )
        assert latest_remaps["is_current_as_of_file_date"].all()

    def test_is_current_as_of_file_date_false_for_older_cclf9(self, workspace):
        df = execute(workspace).collect()
        older = df.filter(
            (pl.col("observation_type") == "cclf9_remap")
            & (pl.col("file_date") == date(2025, 1, 15))
        )
        assert not older["is_current_as_of_file_date"].any()

    def test_hcmpi_backfill_when_master_present(self, tmp_path):
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(silver, [("OLD1", "NEW1", "2025-06-15")])
        _seed_cclf8(silver, [("NEW1", date(2025, 6, 10))])
        _seed_hcmpi(silver, [("NEW1", "HCMPI-001"), ("OLD1", "HCMPI-001")])

        df = execute(SimpleNamespace(storage_config=storage)).collect()
        assert "hcmpi" in df.columns
        new1_hcmpi = df.filter(pl.col("mbi") == "NEW1")["hcmpi"].unique().to_list()
        assert "HCMPI-001" in new1_hcmpi

    def test_hcmpi_column_null_when_master_empty(self, workspace):
        # workspace fixture has an EMPTY hcmpi_master.parquet (no rows). The
        # left join yields all-null hcmpi, exercising the present-but-no-
        # match branch.
        df = execute(workspace).collect()
        assert df["hcmpi"].is_null().all()

    def test_hcmpi_column_null_when_master_file_absent(self, tmp_path):
        # No hcmpi_master.parquet on disk at all. The transform must
        # short-circuit via path.exists() and emit a literal-null column.
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(silver, [("OLD1", "NEW1", "2025-06-15")])
        _seed_cclf8(silver, [("NEW1", date(2025, 6, 10))])
        # NB: deliberately no _seed_empty_hcmpi here
        df = execute(SimpleNamespace(storage_config=storage)).collect()
        assert "hcmpi" in df.columns
        assert df["hcmpi"].is_null().all()

    def test_self_loops_in_cclf9_filtered(self, tmp_path):
        # Self-loop (A->A) must not appear as a remap row
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(silver, [("A", "A", "2025-06-15"), ("A", "B", "2025-06-15")])
        _seed_cclf8(silver, [("A", date(2025, 6, 10)), ("B", date(2025, 6, 10))])
        _seed_empty_hcmpi(silver)
        df = execute(SimpleNamespace(storage_config=storage)).collect()
        remaps = df.filter(pl.col("observation_type") == "cclf9_remap")
        # Only the A->B edge should appear; A->A self-loop is dropped
        assert remaps.height == 1
        assert remaps["mbi"][0] == "A"
        assert remaps["maps_to_mbi"][0] == "B"

    def test_cclf8_string_file_date_coerced_to_date(self, tmp_path):
        # Some CCLF8 deliveries land as String. Make sure execute handles it.
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(silver, [("A", "B", "2025-06-15")])
        # Build CCLF8 with string file_date (not Date)
        pl.DataFrame(
            [
                {"bene_mbi_id": "A", "source_filename": "f1", "file_date": "2025-01-10"},
                {"bene_mbi_id": "B", "source_filename": "f2", "file_date": "2025-06-10"},
            ]
        ).write_parquet(silver / "cclf8.parquet")
        _seed_empty_hcmpi(silver)
        df = execute(SimpleNamespace(storage_config=storage)).collect()
        assert df.filter(pl.col("observation_type") == "cclf8_self").height >= 2


class TestCurrentMbiLookupLazy:
    def test_returns_prvs_to_crnt_pairs(self, workspace):
        # Have to materialize the silver/identity_timeline.parquet first
        df = execute(workspace).collect()
        df.write_parquet(workspace.silver / "identity_timeline.parquet")

        lookup = current_mbi_lookup_lazy(workspace.silver).collect()
        assert "prvs_num" in lookup.columns
        assert "crnt_num" in lookup.columns
        assert lookup.height > 0

    def test_canonical_rows_present_for_every_chain_member(self, workspace):
        execute(workspace).collect().write_parquet(workspace.silver / "identity_timeline.parquet")
        lookup = current_mbi_lookup_lazy(workspace.silver).collect()
        # Every historical mbi in the chain should have a row mapping to NEW1
        for mbi in ["OLD1", "MID1", "NEW1"]:
            assert mbi in lookup["prvs_num"].to_list()
        new1_rows = lookup.filter(pl.col("prvs_num") == "OLD1")
        assert new1_rows["crnt_num"].to_list() == ["NEW1"]


class TestCurrentMbiWithHcmpiLookupLazy:
    def test_includes_hcmpi_column_from_chain(self, tmp_path):
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        _seed_cclf9(silver, [("OLD1", "NEW1", "2025-06-15")])
        _seed_cclf8(silver, [("NEW1", date(2025, 6, 10))])
        _seed_hcmpi(silver, [("NEW1", "HCMPI-001"), ("OLD1", "HCMPI-001")])

        execute(SimpleNamespace(storage_config=storage)).collect().write_parquet(
            silver / "identity_timeline.parquet"
        )
        lookup = current_mbi_with_hcmpi_lookup_lazy(silver).collect()
        assert "hcmpi" in lookup.columns
        old1_row = lookup.filter(pl.col("prvs_num") == "OLD1")
        assert old1_row["hcmpi"][0] == "HCMPI-001"
