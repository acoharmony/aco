# © 2025 HarmonyCares
# All rights reserved.

"""Main entry point for ACO Harmony."""

from ._runner import TransformRunner


def run():
    """Main entry point - run the transform runner."""
    runner = TransformRunner()
    # This is the public interface
    # Users can list and run transforms
    return runner


if __name__ == "__main__":
    # When run as a module, just initialize the runner
    runner = run()
    print(f"ACO Harmony initialized. Available transforms: {runner.list_transforms()}")
