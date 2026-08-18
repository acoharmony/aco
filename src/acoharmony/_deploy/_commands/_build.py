# © 2025 HarmonyCares
# All rights reserved.

"""Build deployment service images."""

from .._freshness import deploy_state_tracker
from .._images import service_images
from .._registry import register_deploy_command
from .._version import latest_release_tag


@register_deploy_command("build")
class BuildCommand:
    """
    Build deployment service images.

        This command rebuilds Docker images for services that have build
        configurations defined in docker-compose.yml. Use this when you've
        made changes to Dockerfiles or build contexts.
    """

    def __init__(self, manager):
        """
        Initialize build command.

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
        Execute the build command.

                Parameters

                services : list[str], optional
                    Specific services to build
                group : str, optional
                    Service group to build
                **kwargs
                    Additional arguments (ignored)

                Returns

                int
                    Exit code (0 for success, non-zero for failure)
        """
        # Check if production profile
        if self.manager.service_mapper.is_production():
            print("⚠  Production profile uses external managed services")
            print("   No local Docker images to build")
            return 0

        # Determine which services to build
        if group:
            try:
                services = self.manager.get_services_for_group(group)
                print(f"Building service group '{group}' ({len(services)} services)...")
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
                print("[ERROR] No valid services to build")
                return 1
            print(f"Building {len(services)} services...")
        else:
            # Build all services for profile
            services = None  # None = build all
            print(f"Building all images for profile '{self.manager.profile}'...")

        print(f"Services: {', '.join(services) if services else 'all'}")

        # Build images
        try:
            result = self.manager.docker.build(services)

            if result.returncode == 0:
                self._record_built_images(services)
                print("[OK] Images built successfully")
                if result.stdout:
                    print(result.stdout)
                return 0
            else:
                print("[ERROR] Error building images:")
                print(result.stderr)
                return result.returncode

        except Exception as e:  # ALLOWED: CLI command handler, prints error and returns exit code
            print(f"[ERROR] Exception occurred: {e}")
            return 1

    def _record_built_images(self, services: list[str] | None) -> None:
        """Mark locally built ACOHarmony images current for this checkout.

        ``start`` and ``restart`` use deploy state to avoid needless pulls. A
        local build should be treated as intentional, especially for services
        like ACOMS whose vendor artifact is local until the next release image
        is published.
        """
        tag = latest_release_tag()
        if tag is None:
            return

        try:
            image_map = service_images(self.manager.docker.compose_file)
            target_services = services if services is not None else sorted(image_map)
            tracker = deploy_state_tracker()
            for service in target_services:
                repo = image_map.get(service)
                if repo is not None:
                    tracker.record(repo, tag)
        except Exception:
            # State recording is an optimization for the next start; a
            # successful build should not become a failure because the local
            # checkout cannot resolve git tags or write tracking state.
            return
