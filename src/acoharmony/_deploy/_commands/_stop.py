# © 2025 HarmonyCares
# All rights reserved.

"""Stop deployment services."""

from .._registry import register_deploy_command


@register_deploy_command("stop")
class StopCommand:
    """
    Stop deployment services.

        This command stops running Docker Compose services.
    """

    def __init__(self, manager):
        """
        Initialize stop command.

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
        Execute the stop command.

                Parameters

                services : list[str], optional
                    Specific services to stop
                group : str, optional
                    Service group to stop
                **kwargs
                    Additional arguments (ignored)

                Returns

                int
                    Exit code (0 for success, non-zero for failure)
        """
        # Check if production profile
        if self.manager.service_mapper.is_production():
            print("⚠  Production profile uses external managed services")
            print("   No local Docker services to stop")
            return 0

        # Determine which services to stop
        if group:
            try:
                services = self.manager.get_services_for_group(group)
                print(f"Stopping service group '{group}' ({len(services)} services)...")
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
        else:
            # Stop all services
            services = None
            print(f"Stopping all services for profile '{self.manager.profile}'...")

        if services is not None and not services:
            print("[ERROR] No valid services to stop")
            return 1

        if services:
            print(f"Services: {', '.join(services)}")

        # Stop services
        try:
            if services:
                result = self.manager.docker.stop(services)
            else:
                result = self.manager.docker.down()

            if result.returncode == 0:
                print("[OK] Services stopped successfully")
                if result.stdout:
                    print(result.stdout)
                return 0
            else:
                print("[ERROR] Error stopping services:")
                print(result.stderr)
                return result.returncode

        except Exception as e:  # ALLOWED: CLI command handler, prints error and returns exit code
            print(f"[ERROR] Exception occurred: {e}")
            return 1
