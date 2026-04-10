# © 2025 HarmonyCares
# All rights reserved.

"""
Core deployment management.

 the main DeploymentManager class that coordinates
Docker Compose operations, profile-aware service selection, and command execution.
"""

import os
from pathlib import Path
from typing import Any

from ._docker import DockerComposeManager
from ._profiles import ProfileServiceMapper
from ._registry import get_deploy_command


class DeploymentManager:
    """
    Manages Docker Compose services with profile awareness.

        This class coordinates deployment operations, ensuring that only
        appropriate services are managed based on the active profile.

        Parameters

        profile : str, optional
            Deployment profile (local, dev, staging, prod).
            Defaults to ACO_PROFILE environment variable or 'dev' from pyproject.toml.

        Attributes

        profile : str
            The active deployment profile
        compose_path : Path
            Path to docker-compose.yml file
        docker : DockerComposeManager
            Docker Compose operations manager
        service_mapper : ProfileServiceMapper
            Profile-aware service mapper
    """

    def __init__(self, profile: str | None = None):
        """
        Initialize the deployment manager.

                Parameters

                profile : str, optional
                    The deployment profile to use
        """
        self.profile = profile or self._get_profile()
        self.compose_path = self._get_compose_path()
        self.docker = DockerComposeManager(self.compose_path)
        self.service_mapper = ProfileServiceMapper(self.profile)

    def _get_profile(self) -> str:
        """
        Get the active profile from the environment or the packaged aco.toml.

                Returns

                str
                    The profile name (defaults to 'dev' if not specified)
        """
        # Check environment variable first
        env_profile = os.getenv("ACO_PROFILE")
        if env_profile:
            return env_profile

        # Read default from the packaged aco.toml
        try:
            from .._config_loader import load_aco_config

            return load_aco_config().get("default_profile", "dev")
        except Exception:
            pass

        # Final fallback
        return "dev"

    def _get_compose_path(self) -> Path:
        """
        Get path to docker-compose.yml file.

                Returns

                Path
                    Absolute path to docker-compose.yml

                Raises

                FileNotFoundError
                    If docker-compose.yml cannot be found
        """
        # Try to locate deploy/compose/docker-compose.yml
        # Start from package root and go up
        current = Path(__file__).parent
        while current != current.parent:
            compose_file = current / "deploy" / "compose" / "docker-compose.yml"
            if compose_file.exists():
                return compose_file
            current = current.parent

        # Fallback: check if we're in the project already
        fallback = Path.cwd() / "deploy" / "compose" / "docker-compose.yml"
        if fallback.exists():
            return fallback

        raise FileNotFoundError(
            "Could not locate deploy/compose/docker-compose.yml. "
            "Make sure you're running from the project root or the file exists."
        )

    def execute_command(self, command: str, **kwargs: Any) -> Any:
        """
        Execute a registered deployment command.

                Parameters

                command : str
                    The command name (start, stop, restart, etc.)
                **kwargs
                    Command-specific arguments

                Returns

                Any
                    Command-specific return value
        """
        cmd_class = get_deploy_command(command)
        cmd_instance = cmd_class(self)
        return cmd_instance.execute(**kwargs)

    def get_services_for_group(self, group: str) -> list[str]:
        """
        Get services for a named group in current profile.

                Parameters

                group : str
                    The service group name

                Returns

                list[str]
                    Services in the group
        """
        return self.service_mapper.get_group_services(group)

    def get_all_services(self) -> list[str]:
        """
        Get all services for current profile.

                Returns

                list[str]
                    All available services
        """
        return self.service_mapper.get_all_services()

    def validate_services(self, services: list[str]) -> tuple[list[str], list[str]]:
        """
        Validate that services are available in current profile.

                Parameters

                services : list[str]
                    Services to validate

                Returns

                tuple[list[str], list[str]]
                    (valid_services, invalid_services)
        """
        return self.service_mapper.validate_services(services)
