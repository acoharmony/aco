# © 2025 HarmonyCares
# All rights reserved.

"""View deployment service logs."""

from .._registry import register_deploy_command


@register_deploy_command("logs")
class LogsCommand:
    """
    View deployment service logs.

        This command displays logs from Docker Compose services.
    """

    def __init__(self, manager):
        """
        Initialize logs command.

                Parameters

                manager : DeploymentManager
                    The deployment manager instance
        """
        self.manager = manager

    def execute(
        self,
        services: list[str] | None = None,
        group: str | None = None,
        follow: bool = False,
        tail: int | None = None,
        **kwargs,
    ) -> int:
        """
        Execute the logs command.

                Parameters

                services : list[str], optional
                    Specific services to show logs for
                group : str, optional
                    Service group to show logs for
                follow : bool, optional
                    Follow log output (default: False)
                tail : int, optional
                    Number of lines to show from end
                **kwargs
                    Additional arguments (ignored)

                Returns

                int
                    Exit code (0 for success, non-zero for failure)
        """
        # Check if production profile
        if self.manager.service_mapper.is_production():
            print("⚠  Production profile uses external managed services")
            print("   No local Docker services to show logs for")
            return 0

        # Determine which services to show logs for
        if group:
            try:
                services = self.manager.get_services_for_group(group)
                print(f"Showing logs for service group '{group}' ({len(services)} services)...")
            except (
                ValueError
            ) as e:  # ALLOWED: CLI command handler, prints error and returns exit code
                print(f"[ERROR] Error: {e}")
                return 1
        elif not services:
            # Show logs for all services
            services = self.manager.get_all_services()
            print(f"Showing logs for all services ({len(services)} services)...")

        # Validate services
        valid, invalid = self.manager.validate_services(services)
        if invalid:
            print(f"⚠  Warning: Unknown services will be skipped: {', '.join(invalid)}")
        services = valid

        if not services:
            print("[ERROR] No valid services to show logs for")
            return 1

        # Show logs
        try:
            result = self.manager.docker.logs(services, follow=follow, tail=tail)

            # Print the logs
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)

            return result.returncode

        except KeyboardInterrupt:  # ALLOWED: Graceful shutdown on keyboard interrupt
            # User interrupted follow mode
            print("\n[OK] Log viewing stopped")
            return 0
        except Exception as e:  # ALLOWED: CLI command handler, prints error and returns exit code
            print(f"[ERROR] Exception occurred: {e}")
            return 1
