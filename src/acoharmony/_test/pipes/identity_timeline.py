# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _pipes._identity_timeline — orchestrates the identity
timeline pipeline (silver → gold + bnex → metrics) with checkpoint
support. Stage 1 writes to silver; stages 2 and 3 write to gold."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import logging
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from acoharmony._pipes._identity_timeline import apply_identity_timeline_pipeline


class _MockStorage:
    def __init__(self, root: Path):
        self._root = root

    def get_path(self, tier):
        from acoharmony.medallion import MedallionLayer
        tier_name = tier.data_tier if isinstance(tier, MedallionLayer) else str(tier).lower()
        p = self._root / tier_name
        p.mkdir(parents=True, exist_ok=True)
        return p


def _fake_transform(rows: list[dict]):
    def _execute(executor):
        return pl.DataFrame(rows).lazy()
    return SimpleNamespace(execute=_execute)


@pytest.fixture
def isolated_workspace(tmp_path, monkeypatch):
    storage = _MockStorage(tmp_path / "workspace")
    executor = SimpleNamespace(storage_config=storage)

    checkpoint_dir = tmp_path / "checkpoints"
    checkpoint_dir.mkdir()
    import acoharmony._pipes._checkpoint as cp_module
    monkeypatch.setattr(cp_module, "Path", lambda *a, **kw: checkpoint_dir if a == ("/opt/s3/data/workspace/logs/tracking",) else Path(*a, **kw))
    return executor, storage


@pytest.fixture
def fake_modules(monkeypatch):
    silver_tl = _fake_transform([{"mbi": "A", "chain_id": "c1", "hop_index": 0}])
    gold_tl = _fake_transform([{"mbi": "A", "observation_type": "bnex_optout"}])
    metrics = _fake_transform([{"metric_name": "remaps_total", "value": 0}])

    import acoharmony._transforms as transforms_pkg
    monkeypatch.setattr(transforms_pkg, "_identity_timeline", silver_tl, raising=False)
    monkeypatch.setattr(transforms_pkg, "_identity_timeline_gold", gold_tl, raising=False)
    monkeypatch.setattr(transforms_pkg, "_identity_timeline_metrics", metrics, raising=False)
    return SimpleNamespace(silver=silver_tl, gold=gold_tl, metrics=metrics)


class TestApplyIdentityTimelinePipeline:
    def test_runs_three_stages(self, isolated_workspace, fake_modules):
        executor, storage = isolated_workspace
        result = apply_identity_timeline_pipeline(executor, logging.getLogger("test"))
        assert set(result.keys()) == {
            "identity_timeline",
            "identity_timeline_gold",
            "identity_timeline_metrics",
        }

    def test_stage1_writes_to_silver_stages_2_3_to_gold(
        self, isolated_workspace, fake_modules
    ):
        executor, storage = isolated_workspace
        apply_identity_timeline_pipeline(executor, logging.getLogger("test"))
        silver = storage.get_path("silver")
        gold = storage.get_path("gold")
        assert (silver / "identity_timeline.parquet").exists()
        # Gold stage 2 emits to gold/identity_timeline.parquet (renamed
        # from the stage's `identity_timeline_gold` name)
        assert (gold / "identity_timeline.parquet").exists()
        assert (gold / "identity_timeline_metrics.parquet").exists()

    def test_force_reruns_all_stages(self, isolated_workspace, fake_modules):
        executor, storage = isolated_workspace
        logger = logging.getLogger("test")
        apply_identity_timeline_pipeline(executor, logger)
        silver = storage.get_path("silver")
        first_mtime = (silver / "identity_timeline.parquet").stat().st_mtime
        import time
        time.sleep(0.01)
        apply_identity_timeline_pipeline(executor, logger, force=True)
        assert (silver / "identity_timeline.parquet").stat().st_mtime >= first_mtime

    def test_resume_skips_completed_stages(self, isolated_workspace, monkeypatch):
        executor, storage = isolated_workspace
        ok = _fake_transform([{"mbi": "A", "chain_id": "c1", "hop_index": 0}])

        call_counts = {"gold": 0}

        def _flaky(executor):
            call_counts["gold"] += 1
            if call_counts["gold"] == 1:
                raise RuntimeError("boom")
            return pl.DataFrame([{"mbi": "A"}]).lazy()

        flaky = SimpleNamespace(execute=_flaky)
        metrics = _fake_transform([{"m": "v"}])

        import acoharmony._transforms as transforms_pkg
        monkeypatch.setattr(transforms_pkg, "_identity_timeline", ok, raising=False)
        monkeypatch.setattr(transforms_pkg, "_identity_timeline_gold", flaky, raising=False)
        monkeypatch.setattr(transforms_pkg, "_identity_timeline_metrics", metrics, raising=False)

        with pytest.raises(RuntimeError, match="boom"):
            apply_identity_timeline_pipeline(executor, logging.getLogger("test"))
        assert call_counts["gold"] == 1

        # Resume — silver stage 1 should be skipped, gold retried, metrics runs
        apply_identity_timeline_pipeline(executor, logging.getLogger("test"))
        assert call_counts["gold"] == 2
        gold = storage.get_path("gold")
        assert (gold / "identity_timeline_metrics.parquet").exists()

    def test_logs_per_stage_row_counts(self, isolated_workspace, fake_modules, caplog):
        executor, _ = isolated_workspace
        with caplog.at_level(logging.INFO):
            apply_identity_timeline_pipeline(executor, logging.getLogger("test"))
        text = caplog.text
        assert "identity_timeline:" in text
        assert "rows" in text

    def test_summary_skips_stage_not_in_completed_list_via_checkpoint_state(
        self, isolated_workspace, fake_modules, monkeypatch
    ):
        # The summary loop's `if stage.name in checkpoint.completed_stages`
        # only hits the False branch when an in-flight pipeline lands with
        # a partial completed_stages set on the checkpoint object. The
        # natural "stage raised" path doesn't reach this loop (it raises
        # first), so synthesise the state by patching PipelineCheckpoint
        # to expose only stage 1 as completed.
        executor, _ = isolated_workspace

        from acoharmony._pipes import _checkpoint as cp_module
        original_init = cp_module.PipelineCheckpoint.__init__

        def _truncated_init(self, name, force=False):
            original_init(self, name, force)
            # After normal init, mutate so the for-loop in
            # apply_identity_timeline_pipeline sees completed_stages
            # missing one of the three stage names. We append the silver
            # stage in mark_stage_complete; suppress that for stages 2/3.
            self._mark_orig = self.mark_stage_complete

            def _selective_mark(stage_name):
                if stage_name == "identity_timeline":
                    self._mark_orig(stage_name)
                # else: don't track stages 2/3 in completed_stages

            self.mark_stage_complete = _selective_mark

        monkeypatch.setattr(cp_module.PipelineCheckpoint, "__init__", _truncated_init)
        apply_identity_timeline_pipeline(executor, logging.getLogger("test"))

    def test_summary_skips_missing_output_file(
        self, isolated_workspace, fake_modules, monkeypatch
    ):
        # `if Path(file_path).exists(): ... ` — the False branch fires
        # when the stage was marked complete but its parquet was deleted
        # between the stage finishing and the summary block. Use the
        # checkpoint's mark_stage_complete to insert all three names and
        # delete one file just before the summary loop runs by hooking
        # `mark_pipeline_complete`.
        executor, storage = isolated_workspace
        gold = storage.get_path("gold")

        from acoharmony._pipes import _checkpoint as cp_module

        # Replace mark_pipeline_complete so it deletes one of the gold
        # outputs immediately before the summary scan would read them.
        # (The summary loop iterates BEFORE mark_pipeline_complete, so we
        # need to hook earlier — patch checkpoint.mark_stage_complete to
        # delete files after the third stage marks complete.)
        original_mark = cp_module.PipelineCheckpoint.mark_stage_complete

        def _delete_after_metrics(self, stage_name):
            original_mark(self, stage_name)
            if stage_name == "identity_timeline_metrics":
                # Now that all 3 are in completed_stages, delete one
                # output so the summary loop hits the False branch.
                target = gold / "identity_timeline_metrics.parquet"
                if target.exists():
                    target.unlink()

        monkeypatch.setattr(
            cp_module.PipelineCheckpoint, "mark_stage_complete", _delete_after_metrics
        )
        apply_identity_timeline_pipeline(executor, logging.getLogger("test"))
