"""
Magic decorator for test files to auto-import all exports from the module under test.

Usage:
    from acoharmony._test._import_magic import auto_import

    @auto_import
    class _: pass  # noqa: E701  # Trigger at module level

    # Now all functions/classes from the module are available
"""

import importlib
import inspect
import re
from pathlib import Path


def _extract_module_from_docstring(test_module) -> str | None:
    """
    Extract the module path from the test file's docstring.

    Example:
        'Tests for _transforms.admissions_analysis module.' -> acoharmony._transforms.admissions_analysis
        'Tests for expressions.registry module.' -> acoharmony._expressions._registry
    """
    if not test_module or not hasattr(test_module, '__doc__') or not test_module.__doc__:
        return None

    docstring = test_module.__doc__.strip()

    # Pattern: "Tests for <module_path> module."
    # Examples:
    #   "Tests for _transforms.admissions_analysis module."
    #   "Tests for expressions.registry module."
    match = re.search(r'Tests for ([a-z_][a-z0-9_.]*) module', docstring, re.IGNORECASE)
    if not match:
        return None

    module_path_fragment = match.group(1)

    # If it starts with "_", it's already prefixed (e.g., _transforms.admissions_analysis)
    # Otherwise, we need to add prefixes (e.g., expressions.registry -> _expressions._registry)
    if module_path_fragment.startswith('_'):
        # Already has prefix, just prepend acoharmony
        return f'acoharmony.{module_path_fragment}'
    else:
        # Need to add prefixes to each part
        parts = module_path_fragment.split('.')
        # Special handling for known submodules
        prefixed_parts = []
        for i, part in enumerate(parts):
            if part == "foureye":
                prefixed_parts.append("_4icli")
            elif part == "decor8":
                prefixed_parts.append("_decor8")
            elif i == 0:
                # First part gets underscore prefix (submodule directory)
                prefixed_parts.append("_" + part)
            else:
                # Subsequent parts might be module names - prefix if they don't start with _
                if part.startswith('_'):
                    prefixed_parts.append(part)
                else:
                    prefixed_parts.append("_" + part)
        return "acoharmony." + ".".join(prefixed_parts)


def auto_import(cls):
    """
    Decorator that auto-imports all public exports from the module under test.

    Call this at module level to inject all classes and functions into the test namespace.

    Example:
        # In src/acoharmony/_test/expressions/registry.py
        from acoharmony._test._import_magic import auto_import

        @auto_import
        class _: pass

        # Now ExpressionRegistry, build_*_expr, etc. are all available
    """
    # Get the calling module
    frame = inspect.currentframe().f_back
    test_module = inspect.getmodule(frame)

    if not test_module or not hasattr(test_module, '__file__'):
        return cls

    test_file = Path(test_module.__file__)

    # First, try to extract the module name from the test file's docstring
    # Example: """Tests for _transforms.admissions_analysis module.""" -> acoharmony._transforms.admissions_analysis
    module_path = _extract_module_from_docstring(test_module)

    # If that fails, derive source module path from test file path
    # Example: src/acoharmony/_test/expressions/registry.py -> acoharmony._expressions._registry
    if not module_path:
        module_path = _get_module_under_test(str(test_file))

    if not module_path:
        return cls

    try:
        # Import the target module
        target_module = importlib.import_module(module_path)

        # Get all exports: public names + single underscore (private) names, but not dunder
        # This allows tests to access both public APIs and private helpers
        exports = [name for name in dir(target_module)
                   if not name.startswith('__')]

        # Inject into test module's namespace
        for name in exports:
            setattr(test_module, name, getattr(target_module, name))

        # Also inject the module itself with its name (e.g., _aco_alignment_demographics)
        module_name = module_path.split('.')[-1]
        setattr(test_module, module_name, target_module)

    except ImportError:
        # If import fails with underscore prefix, try without it
        # Some transform modules don't have underscore prefixes (e.g., admissions_analysis.py)
        if module_path and '._' in module_path:
            try:
                # Remove underscore prefix from the last component
                parts = module_path.split('.')
                if parts[-1].startswith('_'):
                    parts[-1] = parts[-1][1:]  # Remove leading underscore
                    alternate_path = '.'.join(parts)

                    target_module = importlib.import_module(alternate_path)

                    # Get all exports
                    exports = [name for name in dir(target_module)
                               if not name.startswith('__')]

                    # Inject into test module's namespace
                    for name in exports:
                        setattr(test_module, name, getattr(target_module, name))

                    # Also inject the module itself (with the unprefixed name)
                    module_name = parts[-1]
                    setattr(test_module, module_name, target_module)
            except ImportError:
                # Module doesn't exist - let test fail naturally
                pass

    return cls


def _get_module_under_test(test_file_path: str) -> str | None:
    """
    Derive the module path from the test file path.

    Examples:
        src/acoharmony/_test/expressions/registry.py -> acoharmony._expressions._registry
        src/acoharmony/_test/transforms/base.py -> acoharmony._transforms._base
        src/acoharmony/_test/foureye/config.py -> acoharmony._4icli._config
    """
    path = Path(test_file_path)

    if "_test" not in path.parts:
        return None

    try:
        test_idx = path.parts.index("_test")
        submodule_parts = path.parts[test_idx + 1 : -1]
        module_name = path.stem

        # Special case: If there are no submodule parts, this is a test for a top-level module
        # E.g., _test/config.py -> acoharmony.config
        if len(submodule_parts) == 0:
            if module_name != "init":
                return f"acoharmony.{module_name}"
            else:
                return "acoharmony"

        # Special case: If the submodule part matches the module name, it's testing a top-level module
        # E.g., _test/config/config.py -> acoharmony.config
        if len(submodule_parts) == 1 and submodule_parts[0] == module_name:
            return f"acoharmony.{module_name}"

        # Convert: expressions/registry -> _expressions._registry
        # Special case: foureye -> _4icli
        # Special case: decor8 modules don't use underscore prefixes
        prefixed_parts = []
        for part in submodule_parts:
            if part == "foureye":
                prefixed_parts.append("_4icli")
            elif part == "decor8":
                prefixed_parts.append("_decor8")
            else:
                prefixed_parts.append("_" + part)

        # Special handling for decor8: module names don't get prefixed with underscore
        if submodule_parts and submodule_parts[0] == "decor8" and module_name != "init":
            prefixed_module = module_name  # No underscore prefix for decor8 modules
        else:
            prefixed_module = "_" + module_name if module_name != "init" else ""

        module_path = "acoharmony." + ".".join(prefixed_parts)
        if prefixed_module:
            module_path += "." + prefixed_module

        return module_path
    except (ValueError, IndexError):
        return None
