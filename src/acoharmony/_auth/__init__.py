"""Authentication diagnostics for operator-managed DataHub CLIs."""

from .paths import AuthPaths, get_auth_paths
from .registry import AuthRegistry

__all__ = ["AuthPaths", "AuthRegistry", "get_auth_paths"]
