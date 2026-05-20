# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_images.py."""


from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from acoharmony._deploy._images import service_images


class TestServiceImages:
    @pytest.mark.unit
    def test_keeps_acoharmony_strips_tag(self) -> None:
        config = {
            "services": {
                "4icli": {"image": "ghcr.io/acoharmony/4icli:latest"},
                "marimo": {"image": "ghcr.io/acoharmony/marimo:v0.0.20"},
                "postgres": {"image": "docker.io/postgres:16"},
                "no_image_svc": {},
            }
        }
        with patch(
            "acoharmony._deploy._images.subprocess.run",
            return_value=MagicMock(returncode=0, stdout=json.dumps(config)),
        ):
            result = service_images(Path("/dev/null/compose.yml"))
        assert result == {
            "4icli": "ghcr.io/acoharmony/4icli",
            "marimo": "ghcr.io/acoharmony/marimo",
        }

    @pytest.mark.unit
    def test_handles_missing_services_key(self) -> None:
        with patch(
            "acoharmony._deploy._images.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="{}"),
        ):
            assert service_images(Path("/dev/null/compose.yml")) == {}

    @pytest.mark.unit
    def test_skips_non_string_image(self) -> None:
        config = {"services": {"weird": {"image": 12345}}}
        with patch(
            "acoharmony._deploy._images.subprocess.run",
            return_value=MagicMock(returncode=0, stdout=json.dumps(config)),
        ):
            assert service_images(Path("/dev/null/compose.yml")) == {}
