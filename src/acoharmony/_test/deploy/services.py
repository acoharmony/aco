# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_services.py."""



# Magic auto-import: brings in ALL exports from module under test
from dataclasses import dataclass
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._deploy._services import (  # noqa: E402
    SERVICES,
    ServiceDefinition,
    get_service_definition,
    get_service_dependencies,
    list_services_by_category,
)


class TestServiceDefinition:
    """Tests for the ServiceDefinition dataclass."""

    @pytest.mark.unit
    def test_basic_construction(self) -> None:
        svc = ServiceDefinition(name="test", description="A test service", ports=["8080"])
        assert svc.name == "test"
        assert svc.description == "A test service"
        assert svc.ports == ["8080"]
        assert svc.dependencies == []
        assert svc.healthcheck_url is None
        assert svc.required_env_vars == []
        assert svc.category == "other"

    @pytest.mark.unit
    def test_full_construction(self) -> None:
        svc = ServiceDefinition(
            name="full",
            description="Full service",
            ports=["9090", "9091"],
            dependencies=["db"],
            healthcheck_url="http://localhost:9090/health",
            required_env_vars=["TOKEN"],
            category="monitoring",
        )
        assert svc.dependencies == ["db"]
        assert svc.healthcheck_url == "http://localhost:9090/health"
        assert svc.required_env_vars == ["TOKEN"]
        assert svc.category == "monitoring"


class TestServicesDict:
    """Tests for the SERVICES catalog."""

    @pytest.mark.unit
    def test_postgres_defined(self) -> None:
        svc = SERVICES["postgres"]
        assert svc.name == "postgres"
        assert "5432" in svc.ports
        assert svc.category == "core"
        assert "POSTGRES_USER" in svc.required_env_vars

    @pytest.mark.unit
    def test_s3api_defined(self) -> None:
        svc = SERVICES["s3api"]
        assert svc.category == "core"
        assert "10001" in svc.ports

    @pytest.mark.unit
    def test_catalog_depends_on_postgres_and_s3api(self) -> None:
        svc = SERVICES["catalog"]
        assert "postgres" in svc.dependencies
        assert "s3api" in svc.dependencies

    @pytest.mark.unit
    def test_dev_tools_services_exist(self) -> None:
        for name in ["marimo", "docs", "gitea"]:
            assert name in SERVICES
            assert SERVICES[name].category == "dev-tools"

    @pytest.mark.unit
    def test_app_services_exist(self) -> None:
        for name in ["4icli", "aco"]:
            assert name in SERVICES
            assert SERVICES[name].category == "app"

class TestGetServiceDefinition:
    """Tests for get_service_definition()."""

    @pytest.mark.unit
    def test_valid_service(self) -> None:
        svc = get_service_definition("postgres")
        assert svc.name == "postgres"

    @pytest.mark.unit
    def test_unknown_service_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown service: 'nonexistent'"):
            get_service_definition("nonexistent")

    @pytest.mark.unit
    def test_error_lists_available(self) -> None:
        with pytest.raises(ValueError, match="Available services:"):
            get_service_definition("missing")


class TestListServicesByCategory:
    """Tests for list_services_by_category()."""

    @pytest.mark.unit
    def test_core_category(self) -> None:
        result = list_services_by_category("core")
        assert "postgres" in result
        assert "s3api" in result
        assert "catalog" in result
        assert result == sorted(result)

    @pytest.mark.unit
    def test_empty_category(self) -> None:
        result = list_services_by_category("nonexistent_category")
        assert result == []


class TestGetServiceDependencies:
    """Tests for get_service_dependencies()."""

    @pytest.mark.unit
    def test_no_dependencies(self) -> None:
        deps = get_service_dependencies("marimo")
        assert deps == ["postgres", "s3api"]

    @pytest.mark.unit
    def test_direct_dependencies(self) -> None:
        deps = get_service_dependencies("gitea")
        assert "postgres" in deps

    @pytest.mark.unit
    def test_transitive_dependencies(self) -> None:
        deps = get_service_dependencies("catalog")
        assert "postgres" in deps
        assert "s3api" in deps

    @pytest.mark.unit
    def test_deep_transitive_dependencies(self) -> None:
        # aco depends on catalog which depends on postgres and s3api
        deps = get_service_dependencies("aco")
        assert "catalog" in deps
        assert "postgres" in deps
        assert "s3api" in deps

    @pytest.mark.unit
    def test_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match=r".*"):
            get_service_dependencies("nonexistent")

    @pytest.mark.unit
    def test_result_is_sorted(self) -> None:
        deps = get_service_dependencies("aco")
        assert deps == sorted(deps)

