# © 2025 HarmonyCares
# All rights reserved.

"""
Registry for deployment commands.

 a decorator-based registry pattern for deployment commands,
allowing commands to self-register and be discovered automatically.
"""

# Global registry for deployment commands
DEPLOY_COMMAND_REGISTRY: dict[str, type] = {}


def register_deploy_command(name: str):
    """
    Decorator to register a deployment command.

        Parameters

        name : str
            The command name (e.g., 'start', 'stop', 'restart')

        Returns

        Callable
            The decorator function
    """

    def decorator(cls: type) -> type:
        DEPLOY_COMMAND_REGISTRY[name] = cls
        return cls

    return decorator


def get_deploy_command(name: str) -> type:
    """
    Get a deployment command class by name.

        Parameters

        name : str
            The command name

        Returns

        Type
            The command class

        Raises

        ValueError
            If the command is not registered
    """
    if name not in DEPLOY_COMMAND_REGISTRY:
        available = ", ".join(sorted(DEPLOY_COMMAND_REGISTRY.keys()))
        raise ValueError(f"Unknown deploy command: '{name}'. Available commands: {available}")
    return DEPLOY_COMMAND_REGISTRY[name]


def list_deploy_commands() -> list[str]:
    """
    List all registered deployment commands.

        Returns

        list[str]
            Sorted list of command names
    """
    return sorted(DEPLOY_COMMAND_REGISTRY.keys())


def command_exists(name: str) -> bool:
    """
    Check if a deployment command is registered.

        Parameters

        name : str
            The command name

        Returns

        bool
            True if the command exists, False otherwise
    """
    return name in DEPLOY_COMMAND_REGISTRY
