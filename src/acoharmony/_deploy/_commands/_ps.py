# © 2025 HarmonyCares
# All rights reserved.

"""List running deployment services."""

from .._registry import register_deploy_command


@register_deploy_command("ps")
class PsCommand:
    """
    List running deployment services.

        This command lists all running Docker Compose services.
    """

    def __init__(self, manager):
        """
        Initialize ps command.

                Parameters

                manager : DeploymentManager
                    The deployment manager instance
        """
        self.manager = manager

    def execute(
        self,
        services: list[str] | None = None,
        group: str | None = None,
        **kwargs,
    ) -> int:
        """
        Execute the ps command.

                Parameters

                services : list[str], optional
                    Filter to specific services
                group : str, optional
                    Filter to service group
                **kwargs
                    Additional arguments (ignored)

                Returns

                int
                    Exit code (0 for success, non-zero for failure)
        """
        # Check if production profile
        if self.manager.service_mapper.is_production():
            print("⚠  Production profile uses external managed services")
            print("   No local Docker services to list")
            return 0

        # Determine which services to list
        if group:
            try:
                services = self.manager.get_services_for_group(group)
            except (
                ValueError
            ) as e:  # ALLOWED: CLI command handler, prints error and returns exit code
                print(f"[ERROR] Error: {e}")
                return 1
        elif services:
            # Validate provided services
            valid, invalid = self.manager.validate_services(services)
            if invalid:
                print(f"⚠  Warning: Unknown services will be skipped: {', '.join(invalid)}")
            services = valid

        # List running services
        try:
            result = self.manager.docker.ps(services)

            if result.stdout:
                print(result.stdout)
            else:
                print("No services currently running")

            return result.returncode

        except Exception as e:  # ALLOWED: CLI command handler, prints error and returns exit code
            print(f"[ERROR] Exception occurred: {e}")
            return 1
