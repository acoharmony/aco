# © 2025 HarmonyCares
# All rights reserved.

"""
Profile-aware service selection and mapping.

This module maps deployment profiles to their appropriate service groups,
ensuring that only relevant services are started for each environment.
"""

# Profile-to-service-group mapping
# Each profile defines logical groups of services
PROFILE_SERVICE_GROUPS: dict[str, dict[str, list[str]]] = {
    "local": {
        "infrastructure": ["postgres", "s3api"],
        "analytics": ["marimo", "docs"],
    },
    "dev": {
        "infrastructure": ["postgres", "s3api", "4icli", "aco"],
        "analytics": ["marimo", "docs"],
    },
    "staging": {
        "infrastructure": ["postgres", "s3api", "4icli", "aco"],
        "analytics": ["marimo", "docs"],
    },
    "prod": {
        # Production doesn't use local Docker services
        # Uses external managed services (Databricks, AWS RDS, etc.)
    },
}


class ProfileServiceMapper:
    """
    Maps deployment profiles to available services and groups.

         profile-aware service selection, ensuring that
        only appropriate services are deployed for each environment.

        Parameters

        profile : str
            The deployment profile (local, dev, staging, prod)

        Attributes

        profile : str
            The active deployment profile
        groups : dict
            Service groups available for this profile
    """

    def __init__(self, profile: str):
        """
        Initialize the service mapper for a profile.

                Parameters

                profile : str
                    The deployment profile
        """
        self.profile = profile
        self.groups = PROFILE_SERVICE_GROUPS.get(profile, {})

        if profile not in PROFILE_SERVICE_GROUPS:
            raise ValueError(
                f"Unknown profile: '{profile}'. "
                f"Available profiles: {', '.join(PROFILE_SERVICE_GROUPS.keys())}"
            )

    def get_group_services(self, group: str) -> list[str]:
        """
        Get services for a named group.

                Parameters

                group : str
                    The service group name (e.g., 'core', 'monitoring')

                Returns

                list[str]
                    List of service names in the group

                Raises

                ValueError
                    If the group doesn't exist for this profile
        """
        if group not in self.groups:
            available = ", ".join(self.list_groups())
            raise ValueError(
                f"Group '{group}' not found in profile '{self.profile}'. "
                f"Available groups: {available}"
            )
        return self.groups[group]

    def get_all_services(self) -> list[str]:
        """
        Get all services across all groups for this profile.

                Returns

                list[str]
                    Sorted list of all service names
        """
        all_services: set[str] = set()
        for services in self.groups.values():
            if isinstance(services, list):
                all_services.update(services)
        return sorted(all_services)

    def list_groups(self) -> list[str]:
        """
        List available service groups for this profile.

                Returns

                list[str]
                    Sorted list of group names
        """
        return sorted([k for k, v in self.groups.items() if isinstance(v, list)])

    def is_production(self) -> bool:
        """
        Check if the profile is production (no local services).

                Returns

                bool
                    True if production profile, False otherwise
        """
        return self.profile == "prod"

    def validate_services(self, services: list[str]) -> tuple[list[str], list[str]]:
        """
        Validate that services are available in this profile.

                Parameters

                services : list[str]
                    Services to validate

                Returns

                tuple[list[str], list[str]]
                    (valid_services, invalid_services)
        """
        available = set(self.get_all_services())
        valid = [s for s in services if s in available]
        invalid = [s for s in services if s not in available]
        return valid, invalid
