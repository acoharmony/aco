# © 2025 HarmonyCares
# All rights reserved.

"""
ACO Harmony - Healthcare Data Processing Framework

A comprehensive data processing framework for Accountable Care Organizations (ACOs).
Built on Polars for high-performance data transformations.

"""

try:
    from ._version import __version__
except ImportError:
    __version__ = "0.0.0.dev0"

# Initialize Polars environment variables BEFORE any imports
# This ensures optimal thread pool size is set when Polars is first imported
import os
from pathlib import Path


def _init_polars_env() -> None:
    """Set Polars environment variables from profile before import."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore
        except ImportError:
            return  # Skip if tomli not available

    try:
        # Find pyproject.toml
        package_root = Path(__file__).parent.parent.parent
        pyproject_path = package_root / "pyproject.toml"

        if not pyproject_path.exists():
            return

        with open(pyproject_path, "rb") as f:
            pyproject_data = tomllib.load(f)

        # Get active profile
        acoharmony_config = pyproject_data.get("tool", {}).get("acoharmony", {})
        active_profile = os.getenv("ACO_PROFILE") or acoharmony_config.get("default_profile", "dev")

        # Get Polars config from profile
        polars_config = (
            pyproject_data.get("tool", {})
            .get("acoharmony", {})
            .get("profiles", {})
            .get(active_profile, {})
            .get("polars", {})
        )

        # Set environment variable for thread pool BEFORE polars import
        if "max_threads" in polars_config and "POLARS_MAX_THREADS" not in os.environ:
            os.environ["POLARS_MAX_THREADS"] = str(polars_config["max_threads"])

    except Exception:
        pass  # Graceful degradation if anything fails


# Call early initialization
_init_polars_env()

# Always available (skinny base)
from ._exceptions import (  # noqa: E402
    ACOHarmonyException,
    ExceptionRegistry,
    StorageAccessError,
    StorageBackendError,
    StorageConfigurationError,
    explain,
    log_errors,
    trace_errors,
)
from ._log import setup_logging  # noqa: E402

# Skinny exports (always available)
__all__ = [
    # Version
    "__version__",
    # Exceptions
    "ACOHarmonyException",
    "StorageBackendError",
    "StorageAccessError",
    "StorageConfigurationError",
    "explain",
    "trace_errors",
    "log_errors",
    "ExceptionRegistry",
]

# Initialize logging
setup_logging()

# Full package imports — only available with `uv pip install acoharmony[full]`
try:
    from ._catalog import (  # noqa: E402
        Catalog,
        TableMetadata,
    )
    from ._runner import TransformRunner  # noqa: E402
    from ._store import StorageBackend  # noqa: E402
    from ._trace import setup_tracing  # noqa: E402
    from .config import (  # noqa: E402
        AppConfig,
        ProcessingConfig,
        TransformConfig,
        apply_polars_config,
        get_config,
    )
    from .medallion import MedallionLayer, UnityCatalogNamespace  # noqa: E402
    from .result import (  # noqa: E402
        PipelineResult,
        Result,
        ResultStatus,
        TransformResult,
    )
    from .tracking import TransformTracker  # noqa: E402
    from .transforms import TransformRegistry  # noqa: E402

    # Initialize tracing and Polars optimization (full package only)
    setup_tracing()
    apply_polars_config()

    __all__ += [
        # Core types
        "Result",
        "TransformResult",
        "PipelineResult",
        "ResultStatus",
        # Configuration
        "AppConfig",
        "ProcessingConfig",
        "TransformConfig",
        "get_config",
        # Storage
        "StorageBackend",
        # Medallion Architecture
        "MedallionLayer",
        "UnityCatalogNamespace",
        # Catalog
        "Catalog",
        "TableMetadata",
        # Runner
        "TransformRunner",
        # Transforms
        "TransformRegistry",
        # Tracking
        "TransformTracker",
    ]
except ImportError:
    pass
