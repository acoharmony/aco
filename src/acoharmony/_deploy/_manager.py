"""
Docker Compose deployment manager for ACO Harmony services.
"""

import subprocess
from pathlib import Path


class DeploymentManager:
    """Manages Docker Compose deployment of ACO Harmony services."""

    # Service group definitions
    SERVICE_GROUPS = {
        "root": [
            "4icli",
        ],
        "infrastructure": [
            "4icli",
        ],
        "analytics": [
            "marimo",
            "aco",
        ],
        "development": [
            "docs",
        ],
    }

    def __init__(self):
        """Initialize deployment manager."""
        # Find compose file
        self.project_root = Path.cwd()
        self.compose_file = self.project_root / "deploy" / "compose" / "docker-compose.yml"

        if not self.compose_file.exists():
            raise FileNotFoundError(
                f"Docker Compose file not found: {self.compose_file}\n"
                "Make sure you're running from the project root."
            )

    def _run_compose(
        self, args: list[str], check: bool = True, capture_output: bool = False
    ) -> subprocess.CompletedProcess:
        """Run docker compose command."""
        cmd = ["docker", "compose", "-f", str(self.compose_file)] + args
        return subprocess.run(
            cmd,
            check=check,
            capture_output=capture_output,
            text=True,
        )

    def _get_service_list(self, services: list[str] | None, group: str | None) -> list[str]:
        """Get list of services to operate on."""
        # If services list contains a single item that's a known group, treat it as a group
        if services and len(services) == 1 and services[0] in self.SERVICE_GROUPS:
            print(f"Detected group shorthand: '{services[0]}' -> group '{services[0]}'")
            return self.SERVICE_GROUPS[services[0]]
        elif services:
            return services
        elif group and group in self.SERVICE_GROUPS:
            return self.SERVICE_GROUPS[group]
        elif group:
            raise ValueError(
                f"Unknown service group: {group}\n"
                f"Available groups: {', '.join(self.SERVICE_GROUPS.keys())}"
            )
        else:
            # No services or group specified - operate on all
            return []

    def execute_command(
        self,
        action: str,
        services: list[str] | None = None,
        group: str | None = None,
        follow: bool = False,
        tail: int | None = None,
    ) -> int:
        """
        Execute a deployment command.

        Args:
            action: Command to execute (start, stop, restart, status, logs, ps)
            services: Specific services to act on
            group: Service group to act on
            follow: Follow log output (for logs command)
            tail: Number of log lines to show

        Returns:
            Exit code (0 for success, non-zero for error)
        """
        service_list = self._get_service_list(services, group)

        if action == "start":
            print(f"Starting services: {', '.join(service_list) if service_list else 'all'}")
            self._run_compose(["up", "-d"] + service_list)
            print("[OK] Services started")
            return 0

        elif action == "stop":
            print(f"Stopping services: {', '.join(service_list) if service_list else 'all'}")
            self._run_compose(["stop"] + service_list)
            print("[OK] Services stopped")
            return 0

        elif action == "restart":
            print(f"Restarting services: {', '.join(service_list) if service_list else 'all'}")
            self._run_compose(["restart"] + service_list)
            print("[OK] Services restarted")
            return 0

        elif action == "status":
            self._run_compose(["ps"] + service_list, check=False)
            return 0

        elif action == "logs":
            args = ["logs"]
            if follow:
                args.append("-f")
            if tail:
                args.extend(["--tail", str(tail)])
            args.extend(service_list)
            self._run_compose(args, check=False)
            return 0

        elif action == "ps":
            self._run_compose(["ps"] + service_list, check=False)
            return 0

        else:
            raise ValueError(f"Unknown action: {action}")
