# © 2025 HarmonyCares
# All rights reserved.
# ruff: noqa: F821, E722
"""
Storage-related exceptions.

Exceptions for storage backend errors, configuration issues,
and access problems.
"""

from __future__ import annotations

from ._base import ACOHarmonyException
from ._registry import register_exception


@register_exception(
    error_code="STORAGE_001",
    category="storage",
    why_template="StorageBackend could not be initialized with the specified profile",
    how_template="Verify profile configuration exists and is valid",
    default_causes=[
        "Missing or invalid profile configuration",
        "Invalid storage backend configuration in profile YAML",
        "Storage paths don't exist or aren't accessible",
        "Missing dependencies for storage backend type (rustfs, boto3, etc.)",
    ],
    default_remediation=[
        "Verify profile configuration exists: ls -la src/acoharmony/_config/profiles/",
        "Check your active profile: echo $ACO_PROFILE",
        "Validate profile YAML syntax",
        "Test storage backend manually",
        "Run storage setup for local development",
    ],
)
class StorageBackendError(ACOHarmonyException):
    """
    Raised when StorageBackend cannot be initialized.

        This is a critical error that must be fixed immediately - the application
        cannot function without a properly configured storage backend.
    """

    @classmethod
    def from_initialization_error(
        cls,
        original_error: Exception,
        profile: str | None = None,
    ) -> StorageBackendError:
        """
        Create error from StorageBackend initialization failure.

                Parameters

                original_error : Exception
                    The original exception that caused the failure
                profile : str, optional
                    The profile that was attempted to be loaded

                Returns

                StorageBackendError
                    Exception with comprehensive error message and remediation steps
        """

        profile_info = f" with profile '{profile}'" if profile else ""

        message = f"""
        ╔════════════════════════════════════════════════════════════════════════════╗
        ║                     STORAGE BACKEND INITIALIZATION FAILED                   ║
        ╚════════════════════════════════════════════════════════════════════════════╝

        The StorageBackend could not be initialized{profile_info}.
        This is a critical error - the application cannot function without storage.
        """

        causes = [
            "Missing or invalid profile configuration\n     → Check: src/acoharmony/_config/profiles/*.yml",
            f"Invalid storage backend configuration\n     → Check: Environment variable ACO_PROFILE (current: {profile or 'not set'})\n     → Check: Profile YAML has valid 'storage' section",
            "Storage paths don't exist or aren't accessible\n     → Check: base_path in profile configuration\n     → Verify: Permissions on storage directories",
            "Missing dependencies for storage backend type\n     → Local: Requires filesystem access\n     → RustFS: Requires rustfs installation\n     → S3: Requires boto3 and valid credentials",
        ]

        remediation = [
            "Verify profile configuration exists:\n     ls -la src/acoharmony/_config/profiles/",
            "Check your active profile:\n     echo $ACO_PROFILE",
            "Validate profile YAML syntax:\n     cat src/acoharmony/_config/profiles/$ACO_PROFILE.yml",
            'Test storage backend manually:\n     python -c "from acoharmony._store import StorageBackend; StorageBackend()"',
            "Run storage setup (for local development):\n     uv run python -m acoharmony._dev.store_setup",
        ]

        return cls(
            message=message.strip(),
            original_error=original_error,
            why=f"Profile '{profile}' could not be loaded or initialized",
            how="Follow the remediation steps below to diagnose and fix the issue",
            causes=causes,
            remediation_steps=remediation,
            metadata={
                "profile": profile or "not_set",
                "original_error_type": type(original_error).__name__,
            },
        )


@register_exception(
    error_code="STORAGE_002",
    category="storage",
    why_template="Storage configuration file is missing or invalid",
    how_template="Create or fix the profile configuration YAML file",
)
class StorageConfigurationError(ACOHarmonyException):
    """
    Raised when storage configuration is invalid or missing.

        This indicates a problem with the configuration file itself, not with
        the ability to access storage.
    """

    @classmethod
    def missing_profile(
        cls, profile: str, available_profiles: list[str]
    ) -> StorageConfigurationError:
        """Create error for missing profile."""
        available = "\n  - ".join(available_profiles) if available_profiles else "(none found)"

        message = f"""
        Storage profile '{profile}' not found.

        AVAILABLE PROFILES:
          - {available}

        LOCATION:
          src/acoharmony/_config/profiles/

        CREATE NEW PROFILE:
          1. Copy an existing profile:
             cp src/acoharmony/_config/profiles/local.yml src/acoharmony/_config/profiles/{profile}.yml

          2. Edit the configuration for your environment

          3. Set the environment variable:
             export ACO_PROFILE={profile}
        """

        return cls(
            message=message.strip(),
            why=f"Profile '{profile}' does not exist in the configuration directory",
            how="Create the profile YAML file or use an existing profile",
            metadata={
                "requested_profile": profile,
                "available_profiles": available_profiles,
            },
        )


@register_exception(
    error_code="STORAGE_003",
    category="storage",
    why_template="Storage is configured correctly but cannot be accessed at runtime",
    how_template="Check permissions, network connectivity, and service availability",
)
class StorageAccessError(ACOHarmonyException):
    """
    Raised when storage is configured correctly but cannot be accessed.

        This indicates a runtime problem (permissions, network, etc.) not a
        configuration problem.
    """

    pass


@register_exception(
    error_code="STORAGE_004",
    category="storage",
    why_template="Storage path does not exist or is not accessible",
    how_template="Verify path exists and has correct permissions",
)
class StoragePathError(ACOHarmonyException):
    """
    Raised when a storage path does not exist or cannot be accessed.
    """

    @classmethod
    def path_not_found(cls, path: str, storage_type: str = "local") -> StoragePathError:
        """Create error for path not found."""
        return cls(
            f"Storage path not found: {path}",
            why=f"The specified {storage_type} storage path does not exist",
            how="Ensure the path exists and is accessible:\n"
            f"  - For local: mkdir -p {path}\n"
            f"  - For S3: verify bucket exists\n"
            f"  - For RustFS: ensure RustFS is mounted",
            metadata={
                "path": path,
                "storage_type": storage_type,
            },
        )


@register_exception(
    error_code="STORAGE_005",
    category="storage",
    why_template="Medallion tier is invalid or not supported",
    how_template="Use valid tier: raw, bronze, silver, gold, or platinum",
)
class InvalidTierError(ACOHarmonyException):
    """
    Raised when an invalid medallion tier is specified.
    """

    @classmethod
    def invalid_tier(cls, tier: str) -> InvalidTierError:
        """Create error for invalid tier."""
        valid_tiers = ["raw", "bronze", "silver", "gold", "platinum"]

        return cls(
            f"Invalid medallion tier: {tier}",
            why=f"Tier '{tier}' is not a recognized medallion architecture tier",
            how=f"Use one of the valid tiers: {', '.join(valid_tiers)}",
            causes=[
                "Typo in tier name",
                "Using old/deprecated tier name",
                "Custom tier not configured",
            ],
            remediation_steps=[
                f"Change tier to one of: {', '.join(valid_tiers)}",
                "Check schema YAML for correct tier specification",
            ],
            metadata={
                "invalid_tier": tier,
                "valid_tiers": valid_tiers,
            },
        )
