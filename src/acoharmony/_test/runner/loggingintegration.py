"""
Functional tests for the logging system.

Tests that logging works correctly across all components.
"""

import logging
import threading
import time

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._log import setup_logging
from acoharmony._log.writer import LogWriter
from acoharmony._runner import TransformRunner


class TestLoggingIntegration:
    """Integration tests for logging system."""

    @pytest.mark.unit
    def test_setup_logging(self):
        """Test the setup_logging function."""
        # Should be able to call multiple times without error
        setup_logging()
        setup_logging()

        # After setup, should be able to get loggers

        logger = logging.getLogger("acoharmony")
        assert logger is not None

    @pytest.mark.unit
    def test_logging_in_transform_context(self):
        """Test logging during transformation."""

        # Create a runner which uses logging internally
        runner = TransformRunner()

        # Runner should have access to logging
        # This is implicitly tested - if logging wasn't set up, this would fail
        assert runner.logger is not None

    @pytest.mark.unit
    def test_logging_in_runner_context(self):
        """Test logging during runner operations."""

        # Create a runner
        runner = TransformRunner()

        # Runner should have a logger
        assert runner.logger is not None

        # Should be able to log
        runner.logger.info("Test message from runner")

        # List pipelines (this logs internally)
        pipelines = runner.list_pipelines()
        # Pipeline list may be empty if a prior test cleared the registry
        assert isinstance(pipelines, list)

    @pytest.mark.unit
    def test_logging_with_errors(self):
        """Test that errors are properly logged."""
        writer = LogWriter("error_test")

        try:
            # Simulate an error
            raise ValueError("Test error")
        except ValueError as e:
            # Log the error
            writer.logger.error(f"Caught error: {e}", exc_info=True)

        # Should complete without raising

    @pytest.mark.unit
    def test_concurrent_logging(self):
        """Test that multiple components can log concurrently."""

        def log_messages(component_name: str, count: int):
            writer = LogWriter(component_name)
            for i in range(count):
                writer.logger.info(f"Message {i} from {component_name}")
                time.sleep(0.001)  # Small delay

        # Create multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=log_messages, args=(f"component_{i}", 10))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should complete without deadlocks or errors
