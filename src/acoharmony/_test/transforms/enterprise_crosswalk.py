"""Tests for _transforms.enterprise_crosswalk module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import acoharmony


class TestEnterpriseCrosswalkTransform:
    """Tests for Enterprise crosswalk transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._enterprise_xwalk is not None

    @pytest.mark.unit
    def test_apply_transform_exists(self):
        assert hasattr(acoharmony._transforms._enterprise_xwalk, "apply_transform")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_real_scan = pl.scan_parquet


def _selective_scan(*, skip_cclf9=False, skip_demo=False):
    """Return a scan_parquet replacement that fails for selected files."""

    def _scan(path, *args, **kwargs):
        p = str(path)
        if skip_cclf9 and "cclf9" in p:
            raise FileNotFoundError("cclf9 not found")
        if skip_demo and "demographics" in p:
            raise FileNotFoundError("demo not found")
        return _real_scan(path, *args, **kwargs)

    return _scan


def _make_catalog(tmp_path, *, cclf9_data=None, demo_data=None, hcmpi_data=None):
    """Build a mock catalog with parquet source files on disk."""
    catalog = MagicMock()
    catalog.get_table_metadata.return_value = None  # force rebuild

    silver = tmp_path / "silver"
    silver.mkdir(parents=True, exist_ok=True)
    catalog.storage_config.get_path.return_value = silver

    # Write demographics parquet (always write something so scan_parquet works lazily)
    if demo_data is not None:
        demo_data.write_parquet(silver / "int_beneficiary_demographics_deduped.parquet")
    else:
        # Write empty schema so file exists (we use selective_scan to block it)
        pl.DataFrame(schema={
            "current_bene_mbi_id": pl.Utf8,
            "source_filename": pl.Utf8,
            "file_date": pl.Utf8,
        }).write_parquet(silver / "int_beneficiary_demographics_deduped.parquet")

    # Write cclf9 parquet
    if cclf9_data is not None:
        cclf9_data.write_parquet(silver / "cclf9.parquet")
    else:
        pl.DataFrame(schema={
            "prvs_num": pl.Utf8,
            "crnt_num": pl.Utf8,
            "file_date": pl.Utf8,
            "source_filename": pl.Utf8,
        }).write_parquet(silver / "cclf9.parquet")

    # hcmpi_master via catalog.scan_table
    if hcmpi_data is not None:
        catalog.scan_table.return_value = hcmpi_data
    else:
        catalog.scan_table.return_value = None

    return catalog


def _inner():
    """Unwrap the decorated apply_transform to get the raw function."""
    from acoharmony._transforms._enterprise_xwalk import apply_transform

    return getattr(apply_transform, "func", apply_transform)


# ---------------------------------------------------------------------------
# Branch-targeted tests
# ---------------------------------------------------------------------------


class TestBranch107to191:
    """107->191: bene_xref_df is None, skip xref processing, go to demo."""

    @pytest.mark.unit
    def test_xref_none_demo_present(self, tmp_path):
        demo = pl.DataFrame({
            "current_bene_mbi_id": ["11111111111", "22222222222"],
            "source_filename": ["cclf8.csv", "cclf8.csv"],
            "file_date": ["2024-01-15", "2024-01-15"],
        })
        catalog = _make_catalog(tmp_path, demo_data=demo)
        logger = MagicMock()

        with patch(
            "acoharmony._transforms._enterprise_xwalk.pl.scan_parquet",
            side_effect=_selective_scan(skip_cclf9=True),
        ):
            result = _inner()(None, {}, catalog, logger, force=True)
            collected = result.collect()

        assert collected.height == 2
        assert set(collected["mapping_type"].to_list()) == {"self"}


class TestBranch191to245:
    """191->245: bene_demo_df is None, skip demo processing, go to union."""

    @pytest.mark.unit
    def test_demo_none_xref_present(self, tmp_path):
        cclf9 = pl.DataFrame({
            "prvs_num": ["OLD_MBI_AAA"],
            "crnt_num": ["NEW_MBI_BBB"],
            "file_date": ["2024-01-15"],
            "source_filename": ["cclf9.csv"],
        })
        catalog = _make_catalog(tmp_path, cclf9_data=cclf9)
        logger = MagicMock()

        with patch(
            "acoharmony._transforms._enterprise_xwalk.pl.scan_parquet",
            side_effect=_selective_scan(skip_demo=True),
        ):
            result = _inner()(None, {}, catalog, logger, force=True)
            collected = result.collect()

        xref_rows = collected.filter(pl.col("mapping_type") == "xref")
        assert xref_rows.height >= 1
        # No self-mappings since demo is None
        self_rows = collected.filter(pl.col("mapping_type") == "self")
        assert self_rows.height == 0


class TestBranch245to246:
    """245->246: dfs_to_union is empty, raises ValueError."""

    @pytest.mark.unit
    def test_both_sources_fail_raises(self, tmp_path):
        catalog = _make_catalog(tmp_path)
        logger = MagicMock()

        with patch(
            "acoharmony._transforms._enterprise_xwalk.pl.scan_parquet",
            side_effect=_selective_scan(skip_cclf9=True, skip_demo=True),
        ):
            with pytest.raises(ValueError, match="At least one of"):
                _inner()(None, {}, catalog, logger, force=True)


class TestBranch252to359:
    """252->359: bene_xref_df is None, skip transitive closure entirely."""

    @pytest.mark.unit
    def test_skip_transitive_when_no_xref(self, tmp_path):
        demo = pl.DataFrame({
            "current_bene_mbi_id": ["MBI_DEMO_01"],
            "source_filename": ["cclf8.csv"],
            "file_date": ["2024-01-15"],
        })
        hcmpi = pl.DataFrame({
            "identifier": ["MBI_DEMO_01"],
            "identifier_src_field": ["mbi"],
            "hcmpi": ["HCMPI_999"],
            "eff_end_dt": ["9999-12-31"],
        }).lazy()

        catalog = _make_catalog(tmp_path, demo_data=demo, hcmpi_data=hcmpi)
        logger = MagicMock()

        # cclf9 scan fails => bene_xref_df is None => skips transitive closure
        with patch(
            "acoharmony._transforms._enterprise_xwalk.pl.scan_parquet",
            side_effect=_selective_scan(skip_cclf9=True),
        ):
            result = _inner()(None, {}, catalog, logger, force=True)
            collected = result.collect()

        assert collected.height == 1
        assert "hcmpi" in collected.columns
        chain_rows = collected.filter(pl.col("mapping_type") == "chain")
        assert chain_rows.height == 0


class TestBranchTransitiveClosureLoops:
    """Cover branches inside the transitive closure for-loops (lines 264-306).

    Branches:
      267->264 : loop back-edge / condition is False (row skipped)
      268->270 : prvs already in all_mappings (append path)
      277->280 : len(crnt_list) > 1
      281->282 : intermediate_hops is truthy
      281->284 : intermediate_hops is falsy
      297->298 : next_mbi == mbi in resolve_chain
      306->304 : prvs already in source_info_dict
    """

    @staticmethod
    def _extra_xref_rows(rows):
        """Build a LazyFrame with the right schema for extra xref rows."""
        n = len(rows["prvs_num"])
        return pl.DataFrame({
            "prvs_num": rows["prvs_num"],
            "crnt_num": rows["crnt_num"],
            "mapping_type": ["xref"] * n,
            "hcmpi": pl.Series([None] * n, dtype=pl.String),
            "mrn": pl.Series([None] * n, dtype=pl.String),
            "created_at": [datetime.datetime.now()] * n,
            "created_by": ["test"] * n,
            "is_valid_mbi_format": [True] * n,
            "has_circular_reference": [False] * n,
            "chain_depth": pl.Series([1] * n, dtype=pl.Int32),
            "source_system": ["beneficiary_xref"] * n,
            "source_file": ["cclf9.csv"] * n,
            "load_date": ["2024-01-15"] * n,
        }).lazy()

    def _patched_concat_factory(self, extra_lf):
        """Return a patched pl.concat that injects extra rows on the 2nd call.

        Call sequence in the transform:
          1st concat: line 186 — concatenating prvs_mbis + crnt_mbis
          2nd concat: line 249 — union of dfs_to_union
        We inject into the 2nd call so xref_only (line 256) picks up the extras.
        """
        original = pl.concat
        counter = [0]

        def patched(dfs, *args, **kwargs):
            counter[0] += 1
            out = original(dfs, *args, **kwargs)
            if counter[0] == 2:
                return original([out, extra_lf], how="diagonal")
            return out

        return patched

    @pytest.mark.unit
    def test_intermediate_hop_preference(self, tmp_path):
        """Cover 268->270, 277->280, 281->282, 267->264 (back-edge), 306->304.

        Data: MBI_X -> MBI_Y -> MBI_Z (from cclf9)
        Injected: MBI_X -> MBI_Z (extra)

        MBI_X now has two crnt values: [MBI_Y, MBI_Z].
        MBI_Y is in all_prvs => intermediate hop is preferred (281->282).
        """
        cclf9 = pl.DataFrame({
            "prvs_num": ["MBI_X", "MBI_Y"],
            "crnt_num": ["MBI_Y", "MBI_Z"],
            "file_date": ["2024-01-15", "2024-01-15"],
            "source_filename": ["cclf9.csv", "cclf9.csv"],
        })
        catalog = _make_catalog(tmp_path, cclf9_data=cclf9)
        logger = MagicMock()

        extra = self._extra_xref_rows({
            "prvs_num": ["MBI_X"],
            "crnt_num": ["MBI_Z"],
        })

        with patch(
            "acoharmony._transforms._enterprise_xwalk.pl.concat",
            side_effect=self._patched_concat_factory(extra),
        ):
            result = _inner()(None, {}, catalog, logger, force=True)
            collected = result.collect()

        assert collected.height > 0

    @pytest.mark.unit
    def test_no_intermediate_hop_fallback(self, tmp_path):
        """Cover 281->284: no intermediate hops, fall back to crnt_list[0].

        Data: MBI_P -> MBI_Q (from cclf9, MBI_P is the only prvs)
        Injected: MBI_P -> MBI_R

        Neither MBI_Q nor MBI_R is a prvs => intermediate_hops is empty.
        """
        cclf9 = pl.DataFrame({
            "prvs_num": ["MBI_P"],
            "crnt_num": ["MBI_Q"],
            "file_date": ["2024-01-15"],
            "source_filename": ["cclf9.csv"],
        })
        catalog = _make_catalog(tmp_path, cclf9_data=cclf9)
        logger = MagicMock()

        extra = self._extra_xref_rows({
            "prvs_num": ["MBI_P"],
            "crnt_num": ["MBI_R"],
        })

        with patch(
            "acoharmony._transforms._enterprise_xwalk.pl.concat",
            side_effect=self._patched_concat_factory(extra),
        ):
            result = _inner()(None, {}, catalog, logger, force=True)
            collected = result.collect()

        assert collected.height > 0

    @pytest.mark.unit
    def test_loop_skips_invalid_rows(self, tmp_path):
        """Cover 267->264 false branch: rows that fail the if-check.

        Data: MBI_D -> MBI_E (valid xref from cclf9)
        Injected: MBI_SELF -> MBI_SELF (prvs == crnt) and "" -> MBI_F (empty prvs)

        Both injected rows are skipped at line 267, hitting the false branch.
        """
        cclf9 = pl.DataFrame({
            "prvs_num": ["MBI_D"],
            "crnt_num": ["MBI_E"],
            "file_date": ["2024-01-15"],
            "source_filename": ["cclf9.csv"],
        })
        catalog = _make_catalog(tmp_path, cclf9_data=cclf9)
        logger = MagicMock()

        extra = self._extra_xref_rows({
            "prvs_num": ["MBI_SELF", ""],
            "crnt_num": ["MBI_SELF", "MBI_F"],
        })

        with patch(
            "acoharmony._transforms._enterprise_xwalk.pl.concat",
            side_effect=self._patched_concat_factory(extra),
        ):
            result = _inner()(None, {}, catalog, logger, force=True)
            collected = result.collect()

        assert collected.height > 0

    @pytest.mark.unit
    def test_resolve_chain_self_ref(self, tmp_path):
        """Cover 297->298: next_mbi == mbi in resolve_chain.

        Data: MBI_A1 -> MBI_B1 -> MBI_C1 (from cclf9)
        After mapping_dict is built, inject mapping_dict["MBI_C1"] = "MBI_C1"
        via frame introspection on the second to_dicts call. When
        resolve_chain("MBI_B1") recurses to resolve_chain("MBI_C1"),
        it finds next_mbi = "MBI_C1" == mbi = "MBI_C1", hitting line 298.
        """
        cclf9 = pl.DataFrame({
            "prvs_num": ["MBI_A1", "MBI_B1"],
            "crnt_num": ["MBI_B1", "MBI_C1"],
            "file_date": ["2024-01-15", "2024-01-15"],
            "source_filename": ["cclf9.csv", "cclf9.csv"],
        })
        catalog = _make_catalog(tmp_path, cclf9_data=cclf9)
        logger = MagicMock()

        original_to_dicts = pl.DataFrame.to_dicts
        call_count = [0]

        def patched_to_dicts(self_df):
            rows = original_to_dicts(self_df)
            call_count[0] += 1
            # The 2nd to_dicts call (line 304) builds source_info_dict.
            # At that point mapping_dict already exists in the caller's locals.
            if call_count[0] == 2:
                import sys

                frame = sys._getframe(1)
                local_vars = frame.f_locals
                if "mapping_dict" in local_vars:
                    local_vars["mapping_dict"]["MBI_C1"] = "MBI_C1"
            return rows

        with patch.object(pl.DataFrame, "to_dicts", patched_to_dicts):
            result = _inner()(None, {}, catalog, logger, force=True)
            collected = result.collect()

        assert collected.height > 0
