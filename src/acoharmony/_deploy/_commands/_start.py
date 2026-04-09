# © 2025 HarmonyCares
# All rights reserved.

"""Start deployment services."""

from .._registry import register_deploy_command


@register_deploy_command("start")
class StartCommand:
    """
    Start deployment services.

        This command starts Docker Compose services based on the current profile.
        Services can be started individually, by group, or all at once.
    """

    def __init__(self, manager):
        """
        Initialize start command.

                Parameters

                manager : DeploymentManager
                    The deployment manager instance
        """
        self.manager = manager

    def execute(
        self,
        services: list[str] | None = None,
        group: str | None = None,
        build: bool = False,
        **kwargs,
    ) -> int:
        """
        Execute the start command.

                Parameters

                services : list[str], optional
                    Specific services to start
                group : str, optional
                    Service group to start
                build : bool, optional
                    Build images before starting (default: False - use existing images)
                **kwargs
                    Additional arguments (ignored)

                Returns

                int
                    Exit code (0 for success, non-zero for failure)
        """
        # Check if production profile
        if self.manager.service_mapper.is_production():
            print("⚠  Production profile uses external managed services")
            print("   No local Docker services to start")
            return 0

        # Determine which services to start
        if group:
            try:
                services = self.manager.get_services_for_group(group)
                print(f"Starting service group '{group}' ({len(services)} services)...")
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
            if not services:
                print("[ERROR] No valid services to start")
                return 1
        else:
            # Start all services for profile
            services = self.manager.get_all_services()
            print(
                f"Starting all services for profile '{self.manager.profile}' ({len(services)} services)..."
            )

        if not services:
            print("[ERROR] No services to start")
            return 1

        print(f"Services: {', '.join(services)}")

        # Start services
        try:
            # Only build if explicitly requested (--build flag)
            # By default, use existing images (cold start)
            result = self.manager.docker.up(services, detach=True, no_build=not build)

            if result.returncode == 0:
                print("[OK] Services started successfully")
                if result.stdout:
                    print(result.stdout)
                return 0
            else:
                print("[ERROR] Error starting services:")
                print(result.stderr)
                return result.returncode

        except Exception as e:  # ALLOWED: CLI command handler, prints error and returns exit code
            print(f"[ERROR] Exception occurred: {e}")
            return 1
