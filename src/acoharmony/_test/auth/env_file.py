"""Tests for auth env-file helpers."""

from __future__ import annotations

import pytest

from acoharmony._auth.env_file import find_deploy_dir


@pytest.mark.unit
def test_find_deploy_dir_skips_package_internal_deploy(tmp_path) -> None:
    repo = tmp_path / "repo"
    (repo / "src" / "deploy" / "compose").mkdir(parents=True)
    real_deploy = repo / "deploy"
    real_deploy.mkdir(parents=True)
    (real_deploy / "docker-compose.yml").write_text("name: test\n")
    start = repo / "src" / "acoharmony" / "_auth" / "env_file.py"
    start.parent.mkdir(parents=True)
    start.write_text("")

    assert find_deploy_dir(start=start) == real_deploy
