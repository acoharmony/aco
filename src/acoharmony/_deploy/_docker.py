# © 2025 HarmonyCares
# All rights reserved.

"""
Docker Compose operations wrapper.

 a Python wrapper around the docker compose CLI,
handling command execution, error handling, and output formatting.
"""

import subprocess
from pathlib import Path


class DockerComposeManager:
    """
    Wrapper for docker compose CLI operations.

        This class abstracts docker compose commands, providing a clean Python
        interface for managing containerized services.

        Parameters

        compose_file : Path
            Path to the docker-compose.yml file

        Attributes

        compose_file : Path
            The docker-compose.yml file path
        compose_dir : Path
            Directory containing the compose file
    """

    def __init__(self, compose_file: Path):
        """
        Initialize Docker Compose manager.

                Parameters

                compose_file : Path
                    Path to docker-compose.yml
        """
        self.compose_file = compose_file
        self.compose_dir = compose_file.parent

        if not self.compose_file.exists():
            raise FileNotFoundError(f"Docker Compose file not found: {self.compose_file}")

    def _run_compose(self, args: list[str], check: bool = True) -> subprocess.CompletedProcess:
        """
        Run a docker compose command.

                Parameters

                args : list[str]
                    Arguments to pass to docker compose
                check : bool, optional
                    Whether to raise exception on non-zero exit code

                Returns

                subprocess.CompletedProcess
                    The completed process result
        """
        cmd = ["docker", "compose", "-f", str(self.compose_file)] + args
        return subprocess.run(
            cmd, cwd=self.compose_dir, capture_output=True, text=True, check=check
        )

    def up(
        self,
        services: list[str],
        detach: bool = True,
        no_build: bool = True,
    ) -> subprocess.CompletedProcess:
        """
        Start services.

                Parameters

                services : list[str]
                    Services to start
                detach : bool, optional
                    Run in detached mode (default: True)
                no_build : bool, optional
                    Do not build images before starting (default: True).
                    Set to False to rebuild images if Dockerfile/context changed.

                Returns

                subprocess.CompletedProcess
                    The command result
        """
        args = ["up"]
        if detach:
            args.append("-d")
        if no_build:
            args.append("--no-build")
        args.extend(services)
        return self._run_compose(args)

    def down(self, services: list[str] | None = None) -> subprocess.CompletedProcess:
        """
        Stop and remove services.

                Parameters

                services : list[str], optional
                    Specific services to stop. If None, stops all services.

                Returns

                subprocess.CompletedProcess
                    The command result
        """
        args = ["down"]
        if services:
            args.extend(services)
        return self._run_compose(args)

    def stop(self, services: list[str]) -> subprocess.CompletedProcess:
        """
        Stop services without removing them.

                Parameters

                services : list[str]
                    Services to stop

                Returns

                subprocess.CompletedProcess
                    The command result
        """
        args = ["stop"] + services
        return self._run_compose(args)

    def restart(self, services: list[str]) -> subprocess.CompletedProcess:
        """
        Restart services.

                Parameters

                services : list[str]
                    Services to restart

                Returns

                subprocess.CompletedProcess
                    The command result
        """
        args = ["restart"] + services
        return self._run_compose(args)

    def ps(self, services: list[str] | None = None) -> subprocess.CompletedProcess:
        """
        List running services.

                Parameters

                services : list[str], optional
                    Filter to specific services

                Returns

                subprocess.CompletedProcess
                    The command result with service status
        """
        args = ["ps"]
        if services:
            args.extend(services)
        return self._run_compose(args, check=False)

    def logs(
        self,
        services: list[str],
        follow: bool = False,
        tail: int | None = None,
    ) -> subprocess.CompletedProcess:
        """
        View service logs.

                Parameters

                services : list[str]
                    Services to show logs for
                follow : bool, optional
                    Follow log output (default: False)
                tail : int, optional
                    Number of lines to show from the end

                Returns

                subprocess.CompletedProcess
                    The command result with logs
        """
        args = ["logs"]
        if follow:
            args.append("-f")
        if tail:
            args.extend(["--tail", str(tail)])
        args.extend(services)
        return self._run_compose(args, check=False)

    def exec(self, service: str, command: list[str]) -> subprocess.CompletedProcess:
        """
        Execute command in a running service.

                Parameters

                service : str
                    The service name
                command : list[str]
                    Command and arguments to execute

                Returns

                subprocess.CompletedProcess
                    The command result
        """
        args = ["exec", service] + command
        return self._run_compose(args)

    def pull(self, services: list[str] | None = None) -> subprocess.CompletedProcess:
        """
        Pull service images.

                Parameters

                services : list[str], optional
                    Services to pull images for. If None, pulls all.

                Returns

                subprocess.CompletedProcess
                    The command result
        """
        args = ["pull"]
        if services:
            args.extend(services)
        return self._run_compose(args)

    def build(self, services: list[str] | None = None) -> subprocess.CompletedProcess:
        """
        Build service images.

                Parameters

                services : list[str], optional
                    Services to build. If None, builds all.

                Returns

                subprocess.CompletedProcess
                    The command result
        """
        args = ["build"]
        if services:
            args.extend(services)
        return self._run_compose(args)
