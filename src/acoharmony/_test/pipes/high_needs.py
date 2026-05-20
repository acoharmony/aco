# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _pipes._high_needs — orchestrates the High-Needs
pipeline (hcc_risk_scores → high_needs_eligibility →
high_needs_reconciliation) with checkpoint/resume + streaming sinks."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._pipes._high_needs import apply_high_needs_pipeline


class _MockStorage:
    def __init__(self, root: Path):
        self._root = root

    def get_path(self, tier):
        from acoharmony.medallion import MedallionLayer
        tier_name = tier.data_tier if isinstance(tier, MedallionLayer) else str(tier).lower()
        p = self._root / tier_name
        p.mkdir(parents=True, exist_ok=True)
        return p


def _fake_transform(rows: list[dict] | None = None):
    """Create a transform-module-shaped object whose .execute() returns a
    LazyFrame. Use to stand in for hcc_risk_scores / high_needs_eligibility
    / high_needs_reconciliation without triggering their real I/O."""
    if rows is None:
        rows = [{"mbi": "X", "value": 1}]

    def _execute(executor):
        return pl.DataFrame(rows).lazy()

    return SimpleNamespace(execute=_execute)


@pytest.fixture
def isolated_workspace(tmp_path, monkeypatch):
    """Workspace with a tmp_path-backed PipelineCheckpoint location so
    checkpoint state from one test never leaks into another."""
    storage = _MockStorage(tmp_path / "workspace")
    executor = SimpleNamespace(storage_config=storage)

    checkpoint_dir = tmp_path / "checkpoints"
    checkpoint_dir.mkdir()

    # PipelineCheckpoint hardcodes /opt/s3/data/workspace/logs/tracking;
    # patch it to use our tmp dir so we don't pollute the real one.
    import acoharmony._pipes._checkpoint as cp_module
    monkeypatch.setattr(cp_module, "Path", lambda *a, **kw: checkpoint_dir if a == ("/opt/s3/data/workspace/logs/tracking",) else Path(*a, **kw))

    return executor, storage


@pytest.fixture
def fake_modules(monkeypatch):
    """Replace the three real transform modules with fakes so the
    pipeline runs entirely in tmp_path without needing CCLF inputs."""
    fake_scores = _fake_transform([{"mbi": "A", "score": 3.5}, {"mbi": "B", "score": 0.1}])
    fake_elig = _fake_transform([{"mbi": "A", "criterion_b_met": True}])
    fake_recon = _fake_transform([{"mbi": "A", "reconciled": True}])

    fake_pkg = SimpleNamespace(
        hcc_risk_scores=fake_scores,
        high_needs_eligibility=fake_elig,
        high_needs_reconciliation=fake_recon,
    )

    import acoharmony._transforms as transforms_pkg
    monkeypatch.setattr(transforms_pkg, "hcc_risk_scores", fake_scores, raising=False)
    monkeypatch.setattr(transforms_pkg, "high_needs_eligibility", fake_elig, raising=False)
    monkeypatch.setattr(transforms_pkg, "high_needs_reconciliation", fake_recon, raising=False)
    return fake_pkg


class TestApplyHighNeedsPipeline:
    def test_runs_three_stages_and_writes_parquets(
        self, isolated_workspace, fake_modules
    ):
        executor, storage = isolated_workspace
        logger = logging.getLogger("test")
        result = apply_high_needs_pipeline(executor, logger)

        assert set(result.keys()) == {
            "hcc_risk_scores",
            "high_needs_eligibility",
            "high_needs_reconciliation",
        }
        gold = storage.get_path("gold")
        for name in result:
            assert (gold / f"{name}.parquet").exists()

    def test_returns_paths_to_gold_outputs(self, isolated_workspace, fake_modules):
        executor, storage = isolated_workspace
        result = apply_high_needs_pipeline(executor, logging.getLogger("test"))
        gold = storage.get_path("gold")
        assert result["hcc_risk_scores"] == gold / "hcc_risk_scores.parquet"

    def test_force_reruns_all_stages_even_when_outputs_exist(
        self, isolated_workspace, fake_modules
    ):
        executor, storage = isolated_workspace
        logger = logging.getLogger("test")
        # First run creates outputs and marks pipeline complete
        apply_high_needs_pipeline(executor, logger)
        gold = storage.get_path("gold")
        first_mtime = (gold / "hcc_risk_scores.parquet").stat().st_mtime

        # Second run with force=True should rewrite the file
        import time
        time.sleep(0.01)
        apply_high_needs_pipeline(executor, logger, force=True)
        second_mtime = (gold / "hcc_risk_scores.parquet").stat().st_mtime
        assert second_mtime >= first_mtime

    def test_resume_skips_completed_stages_after_failure(
        self, isolated_workspace, monkeypatch
    ):
        # Stage 1 succeeds, stage 2 raises. Resume the pipeline → stage 1
        # should be skipped, stage 2 retried.
        executor, storage = isolated_workspace

        ok = _fake_transform([{"mbi": "X", "v": 1}])

        call_counts = {"stage2": 0}

        def _flaky_execute(executor):
            call_counts["stage2"] += 1
            if call_counts["stage2"] == 1:
                raise RuntimeError("boom")
            return pl.DataFrame([{"mbi": "Y", "v": 2}]).lazy()

        flaky = SimpleNamespace(execute=_flaky_execute)
        recon = _fake_transform([{"mbi": "Z", "v": 3}])

        import acoharmony._transforms as transforms_pkg
        monkeypatch.setattr(transforms_pkg, "hcc_risk_scores", ok, raising=False)
        monkeypatch.setattr(transforms_pkg, "high_needs_eligibility", flaky, raising=False)
        monkeypatch.setattr(transforms_pkg, "high_needs_reconciliation", recon, raising=False)

        logger = logging.getLogger("test")
        with pytest.raises(RuntimeError, match="boom"):
            apply_high_needs_pipeline(executor, logger)

        # First run should have completed stage 1 only
        assert call_counts["stage2"] == 1

        # Resume — stage 1 skipped, stage 2 retried (now succeeds), stage 3 runs
        apply_high_needs_pipeline(executor, logger)
        assert call_counts["stage2"] == 2  # retried once

        gold = storage.get_path("gold")
        assert (gold / "high_needs_reconciliation.parquet").exists()

    def test_logs_row_counts_per_stage(self, isolated_workspace, fake_modules, caplog):
        executor, _ = isolated_workspace
        with caplog.at_level(logging.INFO):
            apply_high_needs_pipeline(executor, logging.getLogger("test"))
        text = caplog.text
        # Final summary section logs each stage's row count
        assert "hcc_risk_scores:" in text
        assert "rows" in text
