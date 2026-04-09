# © 2025 HarmonyCares
# All rights reserved.

"""Restart deployment services."""

from .._registry import register_deploy_command


@register_deploy_command("restart")
class RestartCommand:
    """
    Restart deployment services.

        This command restarts running Docker Compose services.
    """

    def __init__(self, manager):
        """
        Initialize restart command.

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
        Execute the restart command.

                Parameters

                services : list[str], optional
                    Specific services to restart
                group : str, optional
                    Service group to restart
                **kwargs
                    Additional arguments (ignored)

                Returns

                int
                    Exit code (0 for success, non-zero for failure)
        """
        # Check if production profile
        if self.manager.service_mapper.is_production():
            print("⚠  Production profile uses external managed services")
            print("   No local Docker services to restart")
            return 0

        # Determine which services to restart
        if group:
            try:
                services = self.manager.get_services_for_group(group)
                print(f"Restarting service group '{group}' ({len(services)} services)...")
            except (
                ValueError
            ) as e:  # ALLOWED: CLI command handler, prints error and returns exit code
                print(f"[ERROR] Error: {e}")
                return 1
        elif not services:
            # Restart all services for profile
            services = self.manager.get_all_services()
            print(
                f"Restarting all services for profile '{self.manager.profile}' ({len(services)} services)..."
            )

        # Validate services
        valid, invalid = self.manager.validate_services(services)
        if invalid:
            print(f"⚠  Warning: Unknown services will be skipped: {', '.join(invalid)}")
        services = valid

        if not services:
            print("[ERROR] No valid services to restart")
            return 1

        print(f"Services: {', '.join(services)}")

        # Restart services
        try:
            result = self.manager.docker.restart(services)

            if result.returncode == 0:
                print("[OK] Services restarted successfully")
                if result.stdout:
                    print(result.stdout)
                return 0
            else:
                print("[ERROR] Error restarting services:")
                print(result.stderr)
                return result.returncode

        except Exception as e:  # ALLOWED: CLI command handler, prints error and returns exit code
            print(f"[ERROR] Exception occurred: {e}")
            return 1
