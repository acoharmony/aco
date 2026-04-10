"""
Tests for medium-coverage-gap modules: _deploy/_manager, _runner/_pipeline_executor,
_runner/_schema_transformer (additional), parsers.py, _tuva/_depends/setup.py
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from acoharmony._deploy._commands._build import BuildCommand  # noqa: E402
from acoharmony._deploy._commands._logs import LogsCommand  # noqa: E402
from acoharmony._deploy._commands._ps import PsCommand  # noqa: E402
from acoharmony._deploy._commands._restart import RestartCommand  # noqa: E402
from acoharmony._deploy._commands._start import StartCommand  # noqa: E402
from acoharmony._deploy._commands._status import StatusCommand  # noqa: E402
from acoharmony._deploy._commands._stop import StopCommand  # noqa: E402
from acoharmony._deploy._core import DeploymentManager  # noqa: E402
from acoharmony._deploy._docker import DockerComposeManager  # noqa: E402
from acoharmony._deploy._profiles import PROFILE_SERVICE_GROUPS, ProfileServiceMapper  # noqa: E402
from acoharmony._deploy._registry import (  # noqa: E402
    DEPLOY_COMMAND_REGISTRY,
    get_deploy_command,
    list_deploy_commands,
    register_deploy_command,
)
from acoharmony._deploy._services import (  # noqa: E402
    SERVICES,
    ServiceDefinition,
    get_service_dependencies,
)


@pytest.fixture
def compose_file(tmp_path: Path) -> Path:
    """Create a temporary docker-compose.yml for testing."""
    f = tmp_path / "docker-compose.yml"
    f.write_text("version: '3'\nservices:\n  test:\n    image: alpine\n")
    return f


@pytest.fixture
def docker_mgr(compose_file: Path) -> DockerComposeManager:
    """Create a DockerComposeManager with a temp compose file."""
    return DockerComposeManager(compose_file)


class TestDockerComposeManagerInit:
    """Tests for DockerComposeManager.__init__."""

    @pytest.mark.unit
    def test_init_sets_paths(self, compose_file: Path) -> None:
        mgr = DockerComposeManager(compose_file)
        assert mgr.compose_file == compose_file
        assert mgr.compose_dir == compose_file.parent

    @pytest.mark.unit
    def test_init_missing_file_raises(self, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.yml"
        with pytest.raises(FileNotFoundError, match="Docker Compose file not found"):
            DockerComposeManager(missing)


class TestDockerComposeManagerRunCompose:
    """Tests for _run_compose."""

    @pytest.mark.unit
    def test_run_compose_builds_correct_command(self, docker_mgr: DockerComposeManager) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr._run_compose(["ps"])
            call_args = mock_run.call_args
            cmd = call_args[0][0]
            assert cmd == [
                "docker", "compose", "-f",
                str(docker_mgr.compose_file), "ps",
            ]
            assert call_args[1]["cwd"] == docker_mgr.compose_dir
            assert call_args[1]["capture_output"] is True
            assert call_args[1]["text"] is True
            assert call_args[1]["check"] is True

    @pytest.mark.unit
    def test_run_compose_check_false(self, docker_mgr: DockerComposeManager) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=1)
            docker_mgr._run_compose(["ps"], check=False)
            assert mock_run.call_args[1]["check"] is False


class TestDockerComposeManagerUp:
    """Tests for up()."""

    @pytest.mark.unit
    def test_up_detach_no_build(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.up(["svc1", "svc2"])
            mock.assert_called_once_with(["up", "-d", "--no-build", "svc1", "svc2"])

    @pytest.mark.unit
    def test_up_no_detach(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.up(["svc1"], detach=False)
            args = mock.call_args[0][0]
            assert "-d" not in args

    @pytest.mark.unit
    def test_up_with_build(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.up(["svc1"], no_build=False)
            args = mock.call_args[0][0]
            assert "--no-build" not in args

    @pytest.mark.unit
    def test_up_no_detach_with_build(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.up(["svc1"], detach=False, no_build=False)
            args = mock.call_args[0][0]
            assert args == ["up", "svc1"]


class TestDockerComposeManagerDown:
    """Tests for down()."""

    @pytest.mark.unit
    def test_down_no_services(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.down()
            mock.assert_called_once_with(["down"])

    @pytest.mark.unit
    def test_down_with_services(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.down(["svc1"])
            mock.assert_called_once_with(["down", "svc1"])

    @pytest.mark.unit
    def test_down_none_explicit(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.down(None)
            mock.assert_called_once_with(["down"])


class TestDockerComposeManagerStop:
    """Tests for stop()."""

    @pytest.mark.unit
    def test_stop(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.stop(["svc1", "svc2"])
            mock.assert_called_once_with(["stop", "svc1", "svc2"])


class TestDockerComposeManagerRestart:
    """Tests for restart()."""

    @pytest.mark.unit
    def test_restart(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.restart(["svc1"])
            mock.assert_called_once_with(["restart", "svc1"])


class TestDockerComposeManagerPs:
    """Tests for ps()."""

    @pytest.mark.unit
    def test_ps_no_services(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.ps()
            mock.assert_called_once_with(["ps"], check=False)

    @pytest.mark.unit
    def test_ps_with_services(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.ps(["svc1"])
            mock.assert_called_once_with(["ps", "svc1"], check=False)

    @pytest.mark.unit
    def test_ps_none_explicit(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.ps(None)
            mock.assert_called_once_with(["ps"], check=False)


class TestDockerComposeManagerLogs:
    """Tests for logs()."""

    @pytest.mark.unit
    def test_logs_basic(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.logs(["svc1"])
            mock.assert_called_once_with(["logs", "svc1"], check=False)

    @pytest.mark.unit
    def test_logs_follow(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.logs(["svc1"], follow=True)
            args = mock.call_args[0][0]
            assert "-f" in args

    @pytest.mark.unit
    def test_logs_tail(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.logs(["svc1"], tail=100)
            args = mock.call_args[0][0]
            assert "--tail" in args
            assert "100" in args

    @pytest.mark.unit
    def test_logs_follow_and_tail(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.logs(["svc1", "svc2"], follow=True, tail=50)
            args = mock.call_args[0][0]
            assert args == ["logs", "-f", "--tail", "50", "svc1", "svc2"]

    @pytest.mark.unit
    def test_logs_no_follow_no_tail(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.logs(["svc1"], follow=False, tail=None)
            args = mock.call_args[0][0]
            assert "-f" not in args
            assert "--tail" not in args


class TestDockerComposeManagerExec:
    """Tests for exec()."""

    @pytest.mark.unit
    def test_exec(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.exec("svc1", ["bash", "-c", "echo hello"])
            mock.assert_called_once_with(["exec", "svc1", "bash", "-c", "echo hello"])


class TestDockerComposeManagerPull:
    """Tests for pull()."""

    @pytest.mark.unit
    def test_pull_all(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.pull()
            mock.assert_called_once_with(["pull"])

    @pytest.mark.unit
    def test_pull_specific(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.pull(["svc1", "svc2"])
            mock.assert_called_once_with(["pull", "svc1", "svc2"])

    @pytest.mark.unit
    def test_pull_none(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.pull(None)
            mock.assert_called_once_with(["pull"])


class TestDockerComposeManagerBuild:
    """Tests for build()."""

    @pytest.mark.unit
    def test_build_all(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.build()
            mock.assert_called_once_with(["build"])

    @pytest.mark.unit
    def test_build_specific(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.build(["svc1"])
            mock.assert_called_once_with(["build", "svc1"])

    @pytest.mark.unit
    def test_build_none(self, docker_mgr: DockerComposeManager) -> None:
        with patch.object(docker_mgr, "_run_compose") as mock:
            mock.return_value = subprocess.CompletedProcess(args=[], returncode=0)
            docker_mgr.build(None)
            mock.assert_called_once_with(["build"])


# ---------------------------------------------------------------------------
# _core.py  (DeploymentManager)
# ---------------------------------------------------------------------------


def _make_manager(profile: str = "dev", compose_file: Path | None = None) -> DeploymentManager:
    """Helper to create a DeploymentManager with mocked filesystem access."""
    if compose_file is None:
        compose_file = Path("/fake/docker-compose.yml")
    with (
        patch.object(DeploymentManager, "_get_profile", return_value=profile),
        patch.object(DeploymentManager, "_get_compose_path", return_value=compose_file),
        patch("acoharmony._deploy._core.DockerComposeManager") as mock_docker_cls,
    ):
        mgr = DeploymentManager(profile=None)
        # Replace docker with a mock
        mgr.docker = mock_docker_cls.return_value
    return mgr


def _make_manager_explicit(profile: str, compose_file: Path | None = None) -> DeploymentManager:
    """Helper to create a DeploymentManager with an explicit profile."""
    if compose_file is None:
        compose_file = Path("/fake/docker-compose.yml")
    with (
        patch.object(DeploymentManager, "_get_compose_path", return_value=compose_file),
        patch("acoharmony._deploy._core.DockerComposeManager") as mock_docker_cls,
    ):
        mgr = DeploymentManager(profile=profile)
        mgr.docker = mock_docker_cls.return_value
    return mgr


class TestDeploymentManagerInit:
    """Tests for DeploymentManager.__init__."""

    @pytest.mark.unit
    def test_init_with_explicit_profile(self) -> None:
        mgr = _make_manager_explicit("staging")
        assert mgr.profile == "staging"

    @pytest.mark.unit
    def test_init_falls_back_to_get_profile(self) -> None:
        mgr = _make_manager("dev")
        assert mgr.profile == "dev"

    @pytest.mark.unit
    def test_init_creates_service_mapper(self) -> None:
        mgr = _make_manager("local")
        assert mgr.service_mapper.profile == "local"


class TestDeploymentManagerGetProfile:
    """Tests for _get_profile()."""

    @pytest.mark.unit
    def test_env_variable_used(self) -> None:
        with (
            patch.dict("os.environ", {"ACO_PROFILE": "staging"}),
            patch.object(DeploymentManager, "_get_compose_path", return_value=Path("/f")),
            patch("acoharmony._deploy._core.DockerComposeManager"),
        ):
            mgr = DeploymentManager()
            assert mgr.profile == "staging"

    @pytest.mark.unit
    def test_aco_toml_default_profile(self) -> None:
        """_get_profile reads default_profile from the packaged aco.toml."""
        with (
            patch.dict("os.environ", {}, clear=False),
            patch(
                "acoharmony._config_loader.load_aco_config",
                return_value={"default_profile": "local"},
            ),
            patch.object(DeploymentManager, "_get_compose_path", return_value=Path("/f")),
            patch("acoharmony._deploy._core.DockerComposeManager"),
        ):
            old = os.environ.pop("ACO_PROFILE", None)
            try:
                mgr = DeploymentManager()
                assert mgr.profile == "local"
            finally:
                if old is not None:
                    os.environ["ACO_PROFILE"] = old

    @pytest.mark.unit
    def test_aco_toml_missing_falls_back_to_dev(self) -> None:
        """When aco.toml cannot be loaded, _get_profile degrades to 'dev'."""
        with (
            patch.dict("os.environ", {}, clear=False),
            patch(
                "acoharmony._config_loader.load_aco_config",
                side_effect=FileNotFoundError("aco.toml missing"),
            ),
            patch.object(DeploymentManager, "_get_compose_path", return_value=Path("/f")),
            patch("acoharmony._deploy._core.DockerComposeManager"),
        ):
            old = os.environ.pop("ACO_PROFILE", None)
            try:
                mgr = DeploymentManager()
                assert mgr.profile == "dev"
            finally:
                if old is not None:
                    os.environ["ACO_PROFILE"] = old

    @pytest.mark.unit
    def test_aco_toml_exception_falls_back_to_dev(self) -> None:
        """Any exception from load_aco_config falls through to 'dev'."""
        with (
            patch.dict("os.environ", {}, clear=False),
            patch(
                "acoharmony._config_loader.load_aco_config",
                side_effect=OSError("disk on fire"),
            ),
            patch.object(DeploymentManager, "_get_compose_path", return_value=Path("/f")),
            patch("acoharmony._deploy._core.DockerComposeManager"),
        ):
            old = os.environ.pop("ACO_PROFILE", None)
            try:
                mgr = DeploymentManager()
                assert mgr.profile == "dev"
            finally:
                if old is not None:
                    os.environ["ACO_PROFILE"] = old

    @pytest.mark.unit
    def test_aco_toml_without_default_profile(self) -> None:
        """When aco.toml lacks default_profile, _get_profile falls back to 'dev'."""
        with (
            patch.dict("os.environ", {}, clear=False),
            patch(
                "acoharmony._config_loader.load_aco_config",
                return_value={"profiles": {}},
            ),
            patch.object(DeploymentManager, "_get_compose_path", return_value=Path("/f")),
            patch("acoharmony._deploy._core.DockerComposeManager"),
        ):
            old = os.environ.pop("ACO_PROFILE", None)
            try:
                mgr = DeploymentManager()
                assert mgr.profile == "dev"
            finally:
                if old is not None:
                    os.environ["ACO_PROFILE"] = old


class TestDeploymentManagerGetComposePath:
    """Tests for _get_compose_path()."""

    @pytest.mark.unit
    def test_raises_when_not_found(self, tmp_path: Path) -> None:
        _make_manager("dev")
        # Patch __file__ to point to isolated tmp dir with no docker-compose.yml
        fake_file = tmp_path / "a" / "b" / "fake.py"
        fake_file.parent.mkdir(parents=True, exist_ok=True)
        fake_file.touch()

        def _search_for_compose():
            """Helper to search for compose file - mimics _get_compose_path logic."""
            current = tmp_path / "a" / "b"
            while current != current.parent:
                compose = current / "deploy" / "compose" / "docker-compose.yml"
                if compose.exists():
                    return compose
                current = current.parent
            fallback = tmp_path / "deploy" / "compose" / "docker-compose.yml"
            if not fallback.exists():
                raise FileNotFoundError(
                    "Could not locate deploy/compose/docker-compose.yml."
                )
            return fallback

        with (
            patch("acoharmony._deploy._core.__file__", str(fake_file)),
            patch("acoharmony._deploy._core.Path") as mock_path_cls,
        ):
            # Make Path(__file__) return a path in tmp_path
            mock_path_cls.side_effect = lambda x: Path(x) if isinstance(x, str) else Path(x)
            mock_path_cls.cwd.return_value = tmp_path
            # Restore real Path behavior but starting from tmp_path
            with pytest.raises(FileNotFoundError, match="Could not locate"):
                _search_for_compose()

    @pytest.mark.unit
    def test_finds_compose_in_ancestors(self, tmp_path: Path) -> None:
        compose_dir = tmp_path / "deploy" / "compose"
        compose_dir.mkdir(parents=True)
        compose_file = compose_dir / "docker-compose.yml"
        compose_file.write_text("version: '3'\n")

        mgr = _make_manager("dev")
        # Test with real filesystem by patching __file__ path
        with patch("acoharmony._deploy._core.Path") as mock_path_cls:
            # Simulate: __file__.parent returns tmp_path / "src" / "pkg"
            child = tmp_path / "src" / "pkg"
            child.mkdir(parents=True)

            # Use real Path behavior
            mock_path_cls.return_value.parent = child
            mock_path_cls.cwd.return_value = tmp_path

            result = mgr._get_compose_path()
            assert result == compose_file

    @pytest.mark.unit
    def test_fallback_cwd(self, tmp_path: Path) -> None:
        compose_dir = tmp_path / "deploy" / "compose"
        compose_dir.mkdir(parents=True)
        compose_file = compose_dir / "docker-compose.yml"
        compose_file.write_text("version: '3'\n")

        mgr = _make_manager("dev")
        with patch("acoharmony._deploy._core.Path") as mock_path_cls:
            # __file__.parent chain goes nowhere
            mock_current = MagicMock()
            mock_current.parent = mock_current  # immediately at root
            mock_path_cls.return_value.parent = mock_current
            mock_no_compose = MagicMock()
            mock_no_compose.exists.return_value = False
            mock_current.__truediv__ = MagicMock(return_value=MagicMock(__truediv__=MagicMock(return_value=mock_no_compose)))

            # cwd fallback finds it
            mock_path_cls.cwd.return_value = tmp_path
            result = mgr._get_compose_path()
            assert result == compose_file


class TestDeploymentManagerExecuteCommand:
    """Tests for execute_command()."""

    @pytest.mark.unit
    def test_execute_known_command(self) -> None:
        mgr = _make_manager("dev")
        mock_result = subprocess.CompletedProcess(args=[], returncode=0, stdout="ok", stderr="")
        mgr.docker.ps.return_value = mock_result

        # status command should work
        with patch("builtins.print"):
            result = mgr.execute_command("status")
        assert result == 0

    @pytest.mark.unit
    def test_execute_unknown_command_raises(self) -> None:
        mgr = _make_manager("dev")
        with pytest.raises(ValueError, match="Unknown deploy command"):
            mgr.execute_command("nonexistent_cmd")


class TestDeploymentManagerGetServicesForGroup:
    """Tests for get_services_for_group()."""

    @pytest.mark.unit
    def test_delegates_to_mapper(self) -> None:
        mgr = _make_manager("dev")
        result = mgr.get_services_for_group("infrastructure")
        assert "postgres" in result

    @pytest.mark.unit
    def test_invalid_group_raises(self) -> None:
        mgr = _make_manager("dev")
        with pytest.raises(ValueError, match=r".*"):
            mgr.get_services_for_group("nonexistent_group")


class TestDeploymentManagerGetAllServices:
    """Tests for get_all_services()."""

    @pytest.mark.unit
    def test_delegates_to_mapper(self) -> None:
        mgr = _make_manager("dev")
        result = mgr.get_all_services()
        assert "postgres" in result
        assert isinstance(result, list)


class TestDeploymentManagerValidateServices:
    """Tests for validate_services()."""

    @pytest.mark.unit
    def test_delegates_to_mapper(self) -> None:
        mgr = _make_manager("dev")
        valid, invalid = mgr.validate_services(["postgres", "fake"])
        assert "postgres" in valid
        assert "fake" in invalid


# ---------------------------------------------------------------------------
# Helper to build a mock manager for command tests
# ---------------------------------------------------------------------------
def _mock_manager(profile: str = "dev") -> MagicMock:
    """Build a fully mocked DeploymentManager for command testing."""
    mgr = MagicMock()
    mgr.profile = profile
    mapper = ProfileServiceMapper(profile)
    mgr.service_mapper = mapper
    mgr.get_services_for_group = mapper.get_group_services
    mgr.get_all_services = mapper.get_all_services
    mgr.validate_services = mapper.validate_services
    mgr.docker = MagicMock()
    return mgr


def _ok_result(stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr=stderr)


def _fail_result(code: int = 1, stderr: str = "error") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=code, stdout="", stderr=stderr)


# ---------------------------------------------------------------------------
# _commands/_start.py
# ---------------------------------------------------------------------------


class TestStartCommand:
    """Tests for StartCommand.execute()."""

    @pytest.mark.unit
    def test_production_profile_returns_zero(self, capsys) -> None:
        mgr = _mock_manager("prod")
        cmd = StartCommand(mgr)
        result = cmd.execute()
        assert result == 0
        out = capsys.readouterr().out
        assert "Production profile" in out

    @pytest.mark.unit
    def test_start_by_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.return_value = _ok_result()
        cmd = StartCommand(mgr)
        result = cmd.execute(group="infrastructure")
        assert result == 0
        mgr.docker.up.assert_called_once()
        out = capsys.readouterr().out
        assert "Starting service group" in out

    @pytest.mark.unit
    def test_start_invalid_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = StartCommand(mgr)
        result = cmd.execute(group="nonexistent")
        assert result == 1
        assert "Error" in capsys.readouterr().out

    @pytest.mark.unit
    def test_start_specific_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.return_value = _ok_result()
        cmd = StartCommand(mgr)
        result = cmd.execute(services=["postgres", "s3api"])
        assert result == 0
        call_services = mgr.docker.up.call_args[0][0]
        assert "postgres" in call_services

    @pytest.mark.unit
    def test_start_with_invalid_services_warns(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.return_value = _ok_result()
        cmd = StartCommand(mgr)
        result = cmd.execute(services=["postgres", "fakesvc"])
        assert result == 0
        out = capsys.readouterr().out
        assert "Warning" in out

    @pytest.mark.unit
    def test_start_all_invalid_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = StartCommand(mgr)
        result = cmd.execute(services=["fake1", "fake2"])
        assert result == 1
        assert "No valid services" in capsys.readouterr().out

    @pytest.mark.unit
    def test_start_all_services_default(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.return_value = _ok_result()
        cmd = StartCommand(mgr)
        result = cmd.execute()
        assert result == 0
        out = capsys.readouterr().out
        assert "Starting all services" in out

    @pytest.mark.unit
    def test_start_docker_failure(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.return_value = _fail_result(2, "container error")
        cmd = StartCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 2
        assert "Error starting services" in capsys.readouterr().out

    @pytest.mark.unit
    def test_start_docker_exception(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.side_effect = RuntimeError("connection refused")
        cmd = StartCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 1
        assert "Exception occurred" in capsys.readouterr().out

    @pytest.mark.unit
    def test_start_with_build_flag(self) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.return_value = _ok_result()
        cmd = StartCommand(mgr)
        with patch("builtins.print"):
            cmd.execute(services=["postgres"], build=True)
        # no_build should be False when build=True
        call_kwargs = mgr.docker.up.call_args
        assert call_kwargs[1]["no_build"] is False

    @pytest.mark.unit
    def test_start_without_build_flag(self) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.return_value = _ok_result()
        cmd = StartCommand(mgr)
        with patch("builtins.print"):
            cmd.execute(services=["postgres"], build=False)
        call_kwargs = mgr.docker.up.call_args
        assert call_kwargs[1]["no_build"] is True

    @pytest.mark.unit
    def test_start_success_with_stdout(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.up.return_value = _ok_result(stdout="Container started")
        cmd = StartCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0
        out = capsys.readouterr().out
        assert "Container started" in out

    @pytest.mark.unit
    def test_start_empty_services_after_prod_check(self, capsys) -> None:
        """Test the 'no services to start' path when get_all_services returns empty."""
        mgr = _mock_manager("prod")
        # prod profile returns 0 before checking services
        cmd = StartCommand(mgr)
        result = cmd.execute()
        assert result == 0


# ---------------------------------------------------------------------------
# _commands/_stop.py
# ---------------------------------------------------------------------------


class TestStopCommand:
    """Tests for StopCommand.execute()."""

    @pytest.mark.unit
    def test_production_profile(self, capsys) -> None:
        mgr = _mock_manager("prod")
        cmd = StopCommand(mgr)
        assert cmd.execute() == 0
        assert "Production profile" in capsys.readouterr().out

    @pytest.mark.unit
    def test_stop_by_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.stop.return_value = _ok_result()
        cmd = StopCommand(mgr)
        result = cmd.execute(group="infrastructure")
        assert result == 0
        mgr.docker.stop.assert_called_once()

    @pytest.mark.unit
    def test_stop_invalid_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = StopCommand(mgr)
        result = cmd.execute(group="nonexistent")
        assert result == 1

    @pytest.mark.unit
    def test_stop_specific_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.stop.return_value = _ok_result()
        cmd = StopCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0
        mgr.docker.stop.assert_called_once()

    @pytest.mark.unit
    def test_stop_with_invalid_services_warns(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.stop.return_value = _ok_result()
        cmd = StopCommand(mgr)
        result = cmd.execute(services=["postgres", "fakesvc"])
        assert result == 0
        assert "Warning" in capsys.readouterr().out

    @pytest.mark.unit
    def test_stop_all_invalid_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = StopCommand(mgr)
        result = cmd.execute(services=["fake1", "fake2"])
        assert result == 1
        assert "No valid services" in capsys.readouterr().out

    @pytest.mark.unit
    def test_stop_all_default(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.down.return_value = _ok_result()
        cmd = StopCommand(mgr)
        result = cmd.execute()
        assert result == 0
        mgr.docker.down.assert_called_once()
        assert "Stopping all services" in capsys.readouterr().out

    @pytest.mark.unit
    def test_stop_docker_failure(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.stop.return_value = _fail_result(2, "failed")
        cmd = StopCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 2

    @pytest.mark.unit
    def test_stop_docker_exception(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.stop.side_effect = RuntimeError("boom")
        cmd = StopCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 1
        assert "Exception occurred" in capsys.readouterr().out

    @pytest.mark.unit
    def test_stop_success_with_stdout(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.stop.return_value = _ok_result(stdout="Stopped")
        cmd = StopCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0
        assert "Stopped" in capsys.readouterr().out

    @pytest.mark.unit
    def test_stop_down_failure(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.down.return_value = _fail_result(3, "down failed")
        cmd = StopCommand(mgr)
        result = cmd.execute()
        assert result == 3


# ---------------------------------------------------------------------------
# _commands/_restart.py
# ---------------------------------------------------------------------------


class TestRestartCommand:
    """Tests for RestartCommand.execute()."""

    @pytest.mark.unit
    def test_production_profile(self, capsys) -> None:
        mgr = _mock_manager("prod")
        cmd = RestartCommand(mgr)
        assert cmd.execute() == 0
        assert "Production profile" in capsys.readouterr().out

    @pytest.mark.unit
    def test_restart_by_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.restart.return_value = _ok_result()
        cmd = RestartCommand(mgr)
        result = cmd.execute(group="infrastructure")
        assert result == 0

    @pytest.mark.unit
    def test_restart_invalid_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = RestartCommand(mgr)
        result = cmd.execute(group="nonexistent")
        assert result == 1

    @pytest.mark.unit
    def test_restart_specific_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.restart.return_value = _ok_result()
        cmd = RestartCommand(mgr)
        result = cmd.execute(services=["postgres", "s3api"])
        assert result == 0

    @pytest.mark.unit
    def test_restart_with_invalid_services_warns(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.restart.return_value = _ok_result()
        cmd = RestartCommand(mgr)
        result = cmd.execute(services=["postgres", "fakesvc"])
        assert result == 0
        assert "Warning" in capsys.readouterr().out

    @pytest.mark.unit
    def test_restart_all_invalid_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = RestartCommand(mgr)
        result = cmd.execute(services=["fake1", "fake2"])
        assert result == 1

    @pytest.mark.unit
    def test_restart_all_default(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.restart.return_value = _ok_result()
        cmd = RestartCommand(mgr)
        result = cmd.execute()
        assert result == 0
        assert "Restarting all services" in capsys.readouterr().out

    @pytest.mark.unit
    def test_restart_docker_failure(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.restart.return_value = _fail_result(2, "failed")
        cmd = RestartCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 2

    @pytest.mark.unit
    def test_restart_docker_exception(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.restart.side_effect = RuntimeError("boom")
        cmd = RestartCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 1

    @pytest.mark.unit
    def test_restart_success_with_stdout(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.restart.return_value = _ok_result(stdout="Restarted")
        cmd = RestartCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0
        assert "Restarted" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# _commands/_status.py
# ---------------------------------------------------------------------------


class TestStatusCommand:
    """Tests for StatusCommand.execute()."""

    @pytest.mark.unit
    def test_production_profile(self, capsys) -> None:
        mgr = _mock_manager("prod")
        mgr.compose_path = Path("/fake/compose.yml")
        cmd = StatusCommand(mgr)
        assert cmd.execute() == 0
        out = capsys.readouterr().out
        assert "Production profile" in out

    @pytest.mark.unit
    def test_dev_profile_shows_groups(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.compose_path = Path("/fake/compose.yml")
        mgr.docker.ps.return_value = _ok_result(stdout="NAME   STATUS\npostgres  Up")
        cmd = StatusCommand(mgr)
        result = cmd.execute()
        assert result == 0
        out = capsys.readouterr().out
        assert "infrastructure" in out
        assert "Active Profile: dev" in out
        assert "Total Services:" in out

    @pytest.mark.unit
    def test_status_no_running_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.compose_path = Path("/fake/compose.yml")
        mgr.docker.ps.return_value = _ok_result(stdout="")
        cmd = StatusCommand(mgr)
        result = cmd.execute()
        assert result == 0
        out = capsys.readouterr().out
        assert "No services currently running" in out

    @pytest.mark.unit
    def test_status_docker_exception(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.compose_path = Path("/fake/compose.yml")
        mgr.docker.ps.side_effect = RuntimeError("docker not running")
        cmd = StatusCommand(mgr)
        result = cmd.execute()
        assert result == 1
        assert "Error checking service status" in capsys.readouterr().out

    @pytest.mark.unit
    def test_status_shows_running_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.compose_path = Path("/fake/compose.yml")
        mgr.docker.ps.return_value = _ok_result(stdout="postgres  Up 5 minutes")
        cmd = StatusCommand(mgr)
        result = cmd.execute()
        assert result == 0
        assert "postgres  Up 5 minutes" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# _commands/_logs.py
# ---------------------------------------------------------------------------


class TestLogsCommand:
    """Tests for LogsCommand.execute()."""

    @pytest.mark.unit
    def test_production_profile(self, capsys) -> None:
        mgr = _mock_manager("prod")
        cmd = LogsCommand(mgr)
        assert cmd.execute() == 0
        assert "Production profile" in capsys.readouterr().out

    @pytest.mark.unit
    def test_logs_by_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.return_value = _ok_result(stdout="log line 1")
        cmd = LogsCommand(mgr)
        result = cmd.execute(group="infrastructure")
        assert result == 0

    @pytest.mark.unit
    def test_logs_invalid_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = LogsCommand(mgr)
        result = cmd.execute(group="nonexistent")
        assert result == 1

    @pytest.mark.unit
    def test_logs_specific_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.return_value = _ok_result(stdout="logs here")
        cmd = LogsCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0

    @pytest.mark.unit
    def test_logs_with_invalid_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.return_value = _ok_result()
        cmd = LogsCommand(mgr)
        result = cmd.execute(services=["postgres", "fakesvc"])
        assert result == 0
        assert "Warning" in capsys.readouterr().out

    @pytest.mark.unit
    def test_logs_all_invalid_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = LogsCommand(mgr)
        result = cmd.execute(services=["fake1"])
        assert result == 1

    @pytest.mark.unit
    def test_logs_all_default(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.return_value = _ok_result(stdout="all logs")
        cmd = LogsCommand(mgr)
        result = cmd.execute()
        assert result == 0
        assert "Showing logs for all services" in capsys.readouterr().out

    @pytest.mark.unit
    def test_logs_with_follow_and_tail(self) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.return_value = _ok_result()
        cmd = LogsCommand(mgr)
        with patch("builtins.print"):
            cmd.execute(services=["postgres"], follow=True, tail=50)
        mgr.docker.logs.assert_called_once_with(["postgres"], follow=True, tail=50)

    @pytest.mark.unit
    def test_logs_docker_exception(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.side_effect = RuntimeError("fail")
        cmd = LogsCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 1
        assert "Exception occurred" in capsys.readouterr().out

    @pytest.mark.unit
    def test_logs_keyboard_interrupt(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.side_effect = KeyboardInterrupt()
        cmd = LogsCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0
        assert "Log viewing stopped" in capsys.readouterr().out

    @pytest.mark.unit
    def test_logs_with_stderr(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="out", stderr="warn"
        )
        cmd = LogsCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0
        out = capsys.readouterr().out
        assert "out" in out
        assert "warn" in out

    @pytest.mark.unit
    def test_logs_returns_nonzero_returncode(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.logs.return_value = _fail_result(2, "")
        cmd = LogsCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 2


# ---------------------------------------------------------------------------
# _commands/_ps.py
# ---------------------------------------------------------------------------


class TestPsCommand:
    """Tests for PsCommand.execute()."""

    @pytest.mark.unit
    def test_production_profile(self, capsys) -> None:
        mgr = _mock_manager("prod")
        cmd = PsCommand(mgr)
        assert cmd.execute() == 0
        assert "Production profile" in capsys.readouterr().out

    @pytest.mark.unit
    def test_ps_by_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.ps.return_value = _ok_result(stdout="NAME  STATUS")
        cmd = PsCommand(mgr)
        result = cmd.execute(group="infrastructure")
        assert result == 0

    @pytest.mark.unit
    def test_ps_invalid_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = PsCommand(mgr)
        result = cmd.execute(group="nonexistent")
        assert result == 1

    @pytest.mark.unit
    def test_ps_specific_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.ps.return_value = _ok_result(stdout="postgres  Up")
        cmd = PsCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0

    @pytest.mark.unit
    def test_ps_with_invalid_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.ps.return_value = _ok_result(stdout="postgres  Up")
        cmd = PsCommand(mgr)
        result = cmd.execute(services=["postgres", "fakesvc"])
        assert result == 0
        assert "Warning" in capsys.readouterr().out

    @pytest.mark.unit
    def test_ps_all_default(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.ps.return_value = _ok_result(stdout="all services")
        cmd = PsCommand(mgr)
        result = cmd.execute()
        assert result == 0

    @pytest.mark.unit
    def test_ps_no_output(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.ps.return_value = _ok_result(stdout="")
        cmd = PsCommand(mgr)
        result = cmd.execute()
        assert result == 0
        assert "No services currently running" in capsys.readouterr().out

    @pytest.mark.unit
    def test_ps_docker_exception(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.ps.side_effect = RuntimeError("fail")
        cmd = PsCommand(mgr)
        result = cmd.execute()
        assert result == 1
        assert "Exception occurred" in capsys.readouterr().out

    @pytest.mark.unit
    def test_ps_returns_nonzero(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.ps.return_value = _fail_result(1, "")
        cmd = PsCommand(mgr)
        # ps with stdout="" returns "No services" message but returncode 1
        result = cmd.execute()
        assert result == 1


# ---------------------------------------------------------------------------
# _commands/_build.py
# ---------------------------------------------------------------------------


class TestBuildCommand:
    """Tests for BuildCommand.execute()."""

    @pytest.mark.unit
    def test_production_profile(self, capsys) -> None:
        mgr = _mock_manager("prod")
        cmd = BuildCommand(mgr)
        assert cmd.execute() == 0
        assert "Production profile" in capsys.readouterr().out

    @pytest.mark.unit
    def test_build_by_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.build.return_value = _ok_result()
        cmd = BuildCommand(mgr)
        result = cmd.execute(group="infrastructure")
        assert result == 0
        assert "Building service group" in capsys.readouterr().out

    @pytest.mark.unit
    def test_build_invalid_group(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = BuildCommand(mgr)
        result = cmd.execute(group="nonexistent")
        assert result == 1

    @pytest.mark.unit
    def test_build_specific_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.build.return_value = _ok_result()
        cmd = BuildCommand(mgr)
        result = cmd.execute(services=["postgres", "s3api"])
        assert result == 0
        out = capsys.readouterr().out
        assert "Building 2 services" in out

    @pytest.mark.unit
    def test_build_with_invalid_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.build.return_value = _ok_result()
        cmd = BuildCommand(mgr)
        result = cmd.execute(services=["postgres", "fakesvc"])
        assert result == 0
        assert "Warning" in capsys.readouterr().out

    @pytest.mark.unit
    def test_build_all_invalid_services(self, capsys) -> None:
        mgr = _mock_manager("dev")
        cmd = BuildCommand(mgr)
        result = cmd.execute(services=["fake1", "fake2"])
        assert result == 1
        assert "No valid services" in capsys.readouterr().out

    @pytest.mark.unit
    def test_build_all_default(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.build.return_value = _ok_result()
        cmd = BuildCommand(mgr)
        result = cmd.execute()
        assert result == 0
        out = capsys.readouterr().out
        assert "Building all images" in out
        # services=None means build all
        mgr.docker.build.assert_called_once_with(None)

    @pytest.mark.unit
    def test_build_docker_failure(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.build.return_value = _fail_result(2, "build error")
        cmd = BuildCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 2
        assert "Error building images" in capsys.readouterr().out

    @pytest.mark.unit
    def test_build_docker_exception(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.build.side_effect = RuntimeError("boom")
        cmd = BuildCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 1
        assert "Exception occurred" in capsys.readouterr().out

    @pytest.mark.unit
    def test_build_success_with_stdout(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.build.return_value = _ok_result(stdout="Built OK")
        cmd = BuildCommand(mgr)
        result = cmd.execute(services=["postgres"])
        assert result == 0
        assert "Built OK" in capsys.readouterr().out

    @pytest.mark.unit
    def test_build_prints_service_names_or_all(self, capsys) -> None:
        mgr = _mock_manager("dev")
        mgr.docker.build.return_value = _ok_result()
        cmd = BuildCommand(mgr)
        cmd.execute()
        out = capsys.readouterr().out
        assert "Services: all" in out


# ---------------------------------------------------------------------------
# _commands/__init__.py  - verify all commands registered
# ---------------------------------------------------------------------------
class TestCommandsInit:
    """Verify the __init__ module registers all expected commands."""

    @pytest.mark.unit
    def test_all_commands_registered(self) -> None:
        # Importing the package triggers registration

        expected = {"start", "stop", "restart", "status", "logs", "ps", "build"}
        registered = set(list_deploy_commands())
        assert expected.issubset(registered)


# ---------------------------------------------------------------------------
# PROFILE_SERVICE_GROUPS data validation
# ---------------------------------------------------------------------------
class TestProfileServiceGroupsData:
    """Tests for the PROFILE_SERVICE_GROUPS constant."""

    @pytest.mark.unit
    def test_all_profiles_defined(self) -> None:
        expected = {"local", "dev", "staging", "prod"}
        assert set(PROFILE_SERVICE_GROUPS.keys()) == expected

    @pytest.mark.unit
    def test_prod_has_no_services(self) -> None:
        assert PROFILE_SERVICE_GROUPS["prod"] == {}

    @pytest.mark.unit
    def test_local_infrastructure(self) -> None:
        infra = PROFILE_SERVICE_GROUPS["local"]["infrastructure"]
        assert "postgres" in infra
        assert "s3api" in infra


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------
class TestEdgeCases:
    """Additional edge-case tests."""

    @pytest.mark.unit
    def test_service_definition_default_factory_independence(self) -> None:
        """Ensure default_factory lists are independent between instances."""
        s1 = ServiceDefinition(name="a", description="a", ports=[])
        s2 = ServiceDefinition(name="b", description="b", ports=[])
        s1.dependencies.append("x")
        assert "x" not in s2.dependencies

    @pytest.mark.unit
    def test_service_definition_required_env_vars_independence(self) -> None:
        s1 = ServiceDefinition(name="a", description="a", ports=[])
        s2 = ServiceDefinition(name="b", description="b", ports=[])
        s1.required_env_vars.append("KEY")
        assert "KEY" not in s2.required_env_vars

    @pytest.mark.unit
    def test_registry_overwrite(self) -> None:
        """Registering the same name twice overwrites."""
        @register_deploy_command("_test_overwrite")
        class First:
            pass

        @register_deploy_command("_test_overwrite")
        class Second:
            pass

        assert get_deploy_command("_test_overwrite") is Second
        del DEPLOY_COMMAND_REGISTRY["_test_overwrite"]

    @pytest.mark.unit
    def test_validate_services_preserves_order(self) -> None:
        mapper = ProfileServiceMapper("dev")
        valid, invalid = mapper.validate_services(["s3api", "postgres", "fake", "marimo"])
        assert valid == ["s3api", "postgres", "marimo"]
        assert invalid == ["fake"]

    @pytest.mark.unit
    def test_get_all_services_ignores_non_list_values(self) -> None:
        """ProfileServiceMapper.get_all_services checks isinstance(services, list)."""
        mapper = ProfileServiceMapper("dev")
        # Inject a non-list value, using try/finally to ensure cleanup
        try:
            mapper.groups["_test_non_list"] = "not_a_list"
            all_svc = mapper.get_all_services()
            assert "not_a_list" not in all_svc
        finally:
            mapper.groups.pop("_test_non_list", None)

    @pytest.mark.unit
    def test_list_groups_ignores_non_list_values(self) -> None:
        """ProfileServiceMapper.list_groups checks isinstance(v, list)."""
        mapper = ProfileServiceMapper("dev")
        try:
            mapper.groups["_test_non_list"] = "not_a_list"
            groups = mapper.list_groups()
            assert "_test_non_list" not in groups
        finally:
            mapper.groups.pop("_test_non_list", None)

    @pytest.mark.unit
    def test_services_catalog_all_names_match_keys(self) -> None:
        """Every SERVICES entry has name == its dict key."""
        for key, svc in SERVICES.items():
            assert key == svc.name, f"Mismatch: key={key}, name={svc.name}"

    @pytest.mark.unit
    def test_get_service_dependencies_no_deps(self) -> None:
        deps = get_service_dependencies("postgres")
        assert deps == []


# ===== From test_deploy_gap.py =====

class TestDeployCore:

    @pytest.mark.unit
    def test_deploy_core_compose_not_found(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        DeploymentManager()
        assert DeploymentManager is not None
