# © 2025 HarmonyCares
# All rights reserved.

"""Show deployment status."""

from .._registry import register_deploy_command


@register_deploy_command("status")
class StatusCommand:
    """
    Show deployment status.

        This command displays the current status of deployment services,
        including which profile is active and which services are available.
    """

    def __init__(self, manager):
        """
        Initialize status command.

                Parameters

                manager : DeploymentManager
                    The deployment manager instance
        """
        self.manager = manager

    def execute(self, **kwargs) -> int:
        """
        Execute the status command.

                Parameters

                **kwargs
                    Additional arguments (ignored)

                Returns

                int
                    Exit code (0 for success)
        """
        print(f"\n{'=' * 60}")
        print("ACO Harmony Deployment Status")
        print(f"{'=' * 60}\n")

        # Profile information
        print(f"Active Profile: {self.manager.profile}")
        print(f"Compose File:   {self.manager.compose_path}")

        # Check if production
        if self.manager.service_mapper.is_production():
            print("\n⚠  Production profile - no local Docker services")
            print("   Using external managed services (Databricks, AWS RDS, etc.)")
            return 0

        # Service groups
        groups = self.manager.service_mapper.list_groups()
        print("\nAvailable Service Groups:")
        for group in groups:
            services = self.manager.get_services_for_group(group)
            print(f"  • {group:15s} ({len(services):2d} services): {', '.join(services)}")

        # Total services
        total_services = len(self.manager.get_all_services())
        print(f"\nTotal Services: {total_services}")

        # Running services
        print(f"\n{'-' * 60}")
        print("Running Services:")
        print(f"{'-' * 60}")

        try:
            result = self.manager.docker.ps()
            if result.stdout:
                print(result.stdout)
            else:
                print("No services currently running")

        except Exception as e:  # ALLOWED: CLI command handler, prints error and returns exit code
            print(f"[ERROR] Error checking service status: {e}")
            return 1

        print(f"\n{'=' * 60}\n")
        return 0
