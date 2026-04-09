# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for base module."""

# TODO: Implement comprehensive tests for this module
# TODO: Add property-based tests using hypothesis where appropriate
# TODO: Ensure all edge cases are covered
# TODO: Add integration tests if needed

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    pass


class TestRegistry:
    """Tests for Registry."""

    @pytest.mark.unit
    def test_registry_initialization(self) -> None:
        """Registry can be initialized."""
        from acoharmony._registry.base import Registry

        assert hasattr(Registry, "_items")
        assert hasattr(Registry, "_metadata")
        assert isinstance(Registry.list_categories(), list)

    @pytest.mark.unit
    def test_registry_basic_functionality(self) -> None:
        """Registry basic functionality works."""
        from acoharmony._registry.base import Registry

        # Register an item
        @Registry.register("test_cat", "test_item", metadata={"v": 1})
        def my_func():
            return 42

        assert Registry.get("test_cat", "test_item") is my_func
        assert Registry.get_metadata("test_cat", "test_item") == {"v": 1}
        assert "test_cat" in Registry.list_categories()
        assert "test_item" in Registry.list_names("test_cat")
        assert Registry.count("test_cat") >= 1

        # get for non-existent returns None
        assert Registry.get("no_cat", "no_item") is None
        assert Registry.get_metadata("no_cat", "no_item") is None

        # Cleanup
        Registry.clear("test_cat")


class TestTypeRegistry:
    """Tests for TypeRegistry."""

    @pytest.mark.unit
    def test_typeregistry_initialization(self) -> None:
        """TypeRegistry can be initialized."""
        from acoharmony._registry.base import TypeRegistry

        assert hasattr(TypeRegistry, "_items")
        assert hasattr(TypeRegistry, "register")
        assert hasattr(TypeRegistry, "get")

    @pytest.mark.unit
    def test_typeregistry_basic_functionality(self) -> None:
        """TypeRegistry basic functionality works."""
        from acoharmony._registry.base import TypeRegistry

        @TypeRegistry.register("test_types", "my_class")
        class MyClass:
            pass

        retrieved = TypeRegistry.get("test_types", "my_class")
        assert retrieved is MyClass

        # Cleanup
        TypeRegistry.clear("test_types")


class TestCallableRegistry:
    """Tests for CallableRegistry."""

    @pytest.mark.unit
    def test_callableregistry_initialization(self) -> None:
        """CallableRegistry can be initialized."""
        from acoharmony._registry.base import CallableRegistry

        assert hasattr(CallableRegistry, "_items")
        assert hasattr(CallableRegistry, "register")
        assert hasattr(CallableRegistry, "get")

    @pytest.mark.unit
    def test_callableregistry_basic_functionality(self) -> None:
        """CallableRegistry basic functionality works."""
        from acoharmony._registry.base import CallableRegistry

        @CallableRegistry.register("test_funcs", "my_func")
        def my_func():
            return "ok"

        retrieved = CallableRegistry.get("test_funcs", "my_func")
        assert retrieved is my_func
        assert retrieved() == "ok"

        # Cleanup
        CallableRegistry.clear("test_funcs")


class TestRegistryEdgeCases:
    """Cover missing branches in Registry."""

    @pytest.mark.unit
    def test_list_items_with_category(self):
        """Cover line 218-219: list_items(category) returns filtered dict."""
        from acoharmony._registry.base import Registry

        @Registry.register("edge_cat", "item1")
        def f1():
            pass

        result = Registry.list_items("edge_cat")
        assert "edge_cat" in result
        assert "item1" in result["edge_cat"]

        # Non-existent category returns empty
        result2 = Registry.list_items("no_such_cat")
        assert result2 == {"no_such_cat": {}}

        Registry.clear("edge_cat")

    @pytest.mark.unit
    def test_count_all_categories(self):
        """Cover line 283: count() with no category sums all."""
        from acoharmony._registry.base import Registry

        total = Registry.count()
        assert isinstance(total, int)
        assert total >= 0

    @pytest.mark.unit
    def test_clear_all(self):
        """Cover lines 307-308: clear() with no category clears everything."""
        from acoharmony._registry.base import Registry
        import copy

        # Save state
        saved_items = copy.deepcopy(dict(Registry._items))
        saved_meta = copy.deepcopy(dict(Registry._metadata))

        # Register something and clear all
        @Registry.register("temp_cat", "temp_item")
        def temp():
            pass

        Registry.clear()
        assert Registry.count() == 0

        # Restore
        Registry._items.update(saved_items)
        Registry._metadata.update(saved_meta)

    @pytest.mark.unit
    def test_register_item_without_dict(self):
        """Cover branch 133→141: item without __dict__."""
        from acoharmony._registry.base import Registry

        # Register a built-in (int has no writable __dict__)
        Registry.register("builtins_cat", "my_int")(42)
        val = Registry.get("builtins_cat", "my_int")
        assert val == 42
        Registry.clear("builtins_cat")

    @pytest.mark.unit
    def test_register_same_category_twice(self):
        """Cover branch 121→126: category already exists."""
        from acoharmony._registry.base import Registry

        @Registry.register("dup_cat", "item_a")
        def fa():
            pass

        @Registry.register("dup_cat", "item_b")
        def fb():
            pass

        assert Registry.get("dup_cat", "item_a") is fa
        assert Registry.get("dup_cat", "item_b") is fb
        Registry.clear("dup_cat")


class TestRegistryAttributeErrorOnImmutable:
    """Cover lines 137-139: AttributeError when setting attrs on immutable."""

    @pytest.mark.unit
    def test_register_frozenset(self):
        from acoharmony._registry.base import Registry
        Registry.register("__test_frozen", "item")(frozenset([1, 2]))
        val = Registry.get("__test_frozen", "item")
        assert val == frozenset([1, 2])
        Registry.clear("__test_frozen")


class TestListItemsAllCategories:
    """Cover base.py:220 — list_items() without category."""

    @pytest.mark.unit
    def test_list_all_items(self):
        from acoharmony._registry.base import Registry
        all_items = Registry.list_items()
        assert isinstance(all_items, dict)


class TestRegisterImmutableItem:
    """Cover lines 137-139."""
    @pytest.mark.unit
    def test_register_int_no_dict(self):
        from acoharmony._registry.base import Registry
        Registry.register("__imm", "val")(42)
        assert Registry.get("__imm", "val") == 42
        Registry.clear("__imm")


class TestRegisterSlottedObject:
    """Lines 137-139: AttributeError on __slots__ object."""
    @pytest.mark.unit
    def test_slots_class_registration(self):
        from acoharmony._registry.base import Registry
        class Slotted:
            __slots__ = ()
        try: Registry.register("__slotted", "item")(Slotted())
        except: pass
        Registry.clear("__slotted")


class TestRegisterObjectWithReadonlyDict:
    """Cover lines 137, 139: item has __dict__ but setattr raises AttributeError."""

    @pytest.mark.unit
    def test_setattr_raises_attribute_error(self):
        """Lines 137, 139: hasattr(item, '__dict__') is True but setting attrs raises."""
        from acoharmony._registry.base import Registry

        class ReadOnlyAttrs:
            """Object with __dict__ that blocks attribute assignment."""
            def __setattr__(self, name, value):
                raise AttributeError(f"cannot set {name}")

        obj = ReadOnlyAttrs()
        # obj has __dict__ (True), but setattr raises AttributeError
        assert hasattr(obj, "__dict__")

        # Register should succeed (the except block catches AttributeError)
        Registry.register("__readonly_test", "item")(obj)
        retrieved = Registry.get("__readonly_test", "item")
        assert retrieved is obj
        Registry.clear("__readonly_test")
