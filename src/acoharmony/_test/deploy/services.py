# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_services.py."""


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
    @pytest.mark.unit
    def test_dev_tools_services_exist(self) -> None:
        for name in ("marimo", "docs"):
            assert name in SERVICES
            assert SERVICES[name].category == "dev-tools"

    @pytest.mark.unit
    def test_app_services_exist(self) -> None:
        for name in ("4icli", "aco"):
            assert name in SERVICES
            assert SERVICES[name].category == "app"

    @pytest.mark.unit
    def test_catalog_matches_compose(self) -> None:
        # SERVICES must mirror deploy/docker-compose.yml — drift breaks
        # `aco deploy start/restart`.
        assert set(SERVICES.keys()) == {"4icli", "aco", "docs", "marimo"}


class TestGetServiceDefinition:
    @pytest.mark.unit
    def test_valid_service(self) -> None:
        assert get_service_definition("4icli").name == "4icli"

    @pytest.mark.unit
    def test_unknown_service_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown service: 'nonexistent'"):
            get_service_definition("nonexistent")

    @pytest.mark.unit
    def test_error_lists_available(self) -> None:
        with pytest.raises(ValueError, match="Available services:"):
            get_service_definition("missing")


class TestListServicesByCategory:
    @pytest.mark.unit
    def test_dev_tools_category(self) -> None:
        result = list_services_by_category("dev-tools")
        assert result == ["docs", "marimo"]

    @pytest.mark.unit
    def test_app_category(self) -> None:
        result = list_services_by_category("app")
        assert result == ["4icli", "aco"]

    @pytest.mark.unit
    def test_empty_category(self) -> None:
        assert list_services_by_category("nonexistent_category") == []


class TestGetServiceDependencies:
    @pytest.mark.unit
    def test_no_dependencies(self) -> None:
        # All real services are top-level after the cleanup.
        assert get_service_dependencies("4icli") == []

    @pytest.mark.unit
    def test_unknown_raises(self) -> None:
        with pytest.raises(ValueError):
            get_service_dependencies("nonexistent")

    @pytest.mark.unit
    def test_result_is_sorted(self) -> None:
        deps = get_service_dependencies("aco")
        assert deps == sorted(deps)
