# © 2025 HarmonyCares
# All rights reserved.

"""
Service metadata and definitions.

This module defines metadata for all deployable services, including
ports, dependencies, health checks, and required environment variables.
"""

from dataclasses import dataclass, field


@dataclass
class ServiceDefinition:
    """
    Metadata for a deployable service.

        Attributes

        name : str
            Service name (matches docker-compose service name)
        description : str
            Human-readable description
        ports : list[str]
            Exposed ports
        dependencies : list[str]
            Services this service depends on
        healthcheck_url : str, optional
            URL to check service health
        required_env_vars : list[str], optional
            Required environment variables
        category : str, optional
            Service category (core, monitoring, data, etc.)
    """

    name: str
    description: str
    ports: list[str]
    dependencies: list[str] = field(default_factory=list)
    healthcheck_url: str | None = None
    required_env_vars: list[str] = field(default_factory=list)
    category: str = "other"


# Service catalog - metadata for all deployable services
SERVICES: dict[str, ServiceDefinition] = {
    # Core infrastructure services
    "postgres": ServiceDefinition(
        name="postgres",
        description="PostgreSQL database",
        ports=["5432"],
        dependencies=[],
        healthcheck_url="http://localhost:5432",
        required_env_vars=["POSTGRES_USER"],
        category="core",
    ),
    "s3api": ServiceDefinition(
        name="s3api",
        description="MinIO S3-compatible object storage",
        ports=["9000", "10001"],
        dependencies=[],
        healthcheck_url="http://localhost:9000",
        category="core",
    ),
    # Data services
    "catalog": ServiceDefinition(
        name="catalog",
        description="Catalog service",
        ports=["8080"],
        dependencies=["postgres", "s3api"],
        healthcheck_url="http://localhost:8080/health",
        category="core",
    ),
    # Development tools
    "marimo": ServiceDefinition(
        name="marimo",
        description="Marimo interactive notebooks",
        ports=["7777"],
        dependencies=["postgres", "s3api"],
        healthcheck_url="http://localhost:7777",
        category="dev-tools",
    ),
    "docs": ServiceDefinition(
        name="docs",
        description="Docusaurus documentation server",
        ports=["8000"],
        dependencies=[],
        healthcheck_url="http://localhost:8000",
        category="dev-tools",
    ),
    "gitea": ServiceDefinition(
        name="gitea",
        description="Gitea Git service",
        ports=["3000"],
        dependencies=["postgres"],
        healthcheck_url="http://localhost:3000",
        category="dev-tools",
    ),
    # Application services
    "4icli": ServiceDefinition(
        name="4icli",
        description="4Innovation CLI service",
        ports=[],
        dependencies=[],
        category="app",
    ),
    "aco": ServiceDefinition(
        name="aco",
        description="ACO Harmony service",
        ports=[],
        dependencies=["catalog"],
        category="app",
    ),
}


def get_service_definition(name: str) -> ServiceDefinition:
    """
    Get service definition by name.

        Parameters

        name : str
            Service name

        Returns

        ServiceDefinition
            The service metadata

        Raises

        ValueError
            If service is not defined
    """
    if name not in SERVICES:
        available = ", ".join(sorted(SERVICES.keys()))
        raise ValueError(f"Unknown service: '{name}'. Available services: {available}")
    return SERVICES[name]


def list_services_by_category(category: str) -> list[str]:
    """
    List all services in a category.

        Parameters

        category : str
            The category name (core, monitoring, data, etc.)

        Returns

        list[str]
            Service names in the category
    """
    return sorted([name for name, svc in SERVICES.items() if svc.category == category])


def get_service_dependencies(name: str) -> list[str]:
    """
    Get all dependencies for a service (recursive).

        Parameters

        name : str
            Service name

        Returns

        list[str]
            All dependencies (direct and transitive)
    """
    service = get_service_definition(name)
    deps = set(service.dependencies)

    # Recursively get dependencies
    for dep in service.dependencies:
        deps.update(get_service_dependencies(dep))

    return sorted(deps)
