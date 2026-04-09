# © 2025 HarmonyCares
# All rights reserved.

"""Test coverage tools for auto-generating and managing test files."""

import ast
from pathlib import Path


class TestCoverageManager:
    """Manage test file structure and coverage."""

    def generate_missing_test_files(self) -> None:
        """Generate test stubs for modules without tests."""
        print("=" * 80)
        print("GENERATING TEST STUBS")
        print("=" * 80)
        print()

        src_modules = list(Path('src/acoharmony').rglob('*.py'))
        created_count = 0
        skipped_count = 0

        for module_path in src_modules:
            # Skip __init__.py, _dev, _rewind during stub generation
            if module_path.name == '__init__.py':
                continue
            if '_dev' in module_path.parts or '_rewind' in module_path.parts:
                continue

            # Compute test path
            rel_path = module_path.relative_to('src/acoharmony')
            test_path = Path('tests') / rel_path.parent / f'test_{rel_path.name}'

            # Create if missing
            if not test_path.exists():
                self._create_test_stub(module_path, test_path)
                created_count += 1
                print(f"[OK] Created: {test_path}")
            else:
                skipped_count += 1

        print()
        print(f"Created: {created_count}")
        print(f"Skipped (already exist): {skipped_count}")
        print()

    def _create_test_stub(self, module_path: Path, test_path: Path) -> None:
        """Create a test file stub with auto-generated test functions."""
        try:
            tree = ast.parse(module_path.read_text())
        except SyntaxError:
            # Skip files with syntax errors
            return

        # Find public functions and classes
        functions = [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and not node.name.startswith('_')
        ]
        classes = [node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]

        # Generate stub
        module_name = module_path.stem
        class_name = module_name.title().replace('_', '')

        stub = f'''# © 2025 HarmonyCares
"""Tests for {module_path.relative_to('src')}."""

import pytest


class Test{class_name}:
    """Test suite for {module_name}."""

'''

        for func in functions[:5]:  # Limit to first 5 functions
            stub += f'''    def test_{func}(self) -> None:
        """Test {func} function."""
        pytest.skip("Test stub - not yet implemented")

'''

        for cls in classes[:3]:  # Limit to first 3 classes
            stub += f'''    def test_{cls.lower()}_init(self) -> None:
        """Test {cls} initialization."""
        pytest.skip("Test stub - not yet implemented")

'''

        if not functions and not classes:
            stub += '''    def test_module_loads(self) -> None:
        """Test module can be imported."""
        pytest.skip("Test stub - not yet implemented")
'''

        test_path.parent.mkdir(parents=True, exist_ok=True)
        test_path.write_text(stub)

    def cleanup_orphaned_tests(self) -> None:
        """Remove test files for deleted modules."""
        print("=" * 80)
        print("CLEANING UP ORPHANED TESTS")
        print("=" * 80)
        print()

        test_files = list(Path('tests').rglob('test_*.py'))
        removed_count = 0

        for test_file in test_files:
            # Find corresponding source module
            rel_path = test_file.relative_to('tests')
            module_name = test_file.name.replace('test_', '')
            module_path = Path('src/acoharmony') / rel_path.parent / module_name

            if not module_path.exists():
                test_file.unlink()
                removed_count += 1
                print(f"[ERROR] Removed orphaned: {test_file}")

        if removed_count == 0:
            print("[OK] No orphaned test files found")

        print()
        print(f"Removed: {removed_count}")
        print()
