"""Tests for acoharmony._pipes._builder module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._builder is not None


class TestPipelineStageBuilder:
    @pytest.mark.unit
    def test_creation_defaults(self):
        from acoharmony._pipes._builder import PipelineStage

        s = PipelineStage(name="x", module=None, group="g", order=1)
        assert s.name == "x"
        assert s.module is None
        assert s.group == "g"
        assert s.order == 1
        assert s.depends_on == []

    @pytest.mark.unit
    def test_creation_with_deps(self):
        from acoharmony._pipes._builder import PipelineStage

        s = PipelineStage("x", None, "g", 2, depends_on=["a", "b"])
        assert s.depends_on == ["a", "b"]

    @pytest.mark.unit
    def test_repr_no_deps(self):
        from acoharmony._pipes._builder import PipelineStage

        s = PipelineStage("out", None, "claims", 3)
        r = repr(s)
        assert "Stage(3: out [claims])" == r

    @pytest.mark.unit
    def test_repr_with_deps(self):
        from acoharmony._pipes._builder import PipelineStage

        s = PipelineStage("out", None, "claims", 3, depends_on=["dep1"])
        r = repr(s)
        assert "dep1" in r
        assert "depends on" in r


class TestStageOrdering:
    @pytest.mark.unit
    def test_stages_sorted_by_order(self):
        from acoharmony._pipes._builder import PipelineStage

        stages = [
            PipelineStage("c", None, "g", 3),
            PipelineStage("a", None, "g", 1),
            PipelineStage("b", None, "g", 2),
        ]
        sorted_stages = sorted(stages, key=lambda s: s.order)
        assert [s.name for s in sorted_stages] == ["a", "b", "c"]

    @pytest.mark.unit
    def test_bronze_stages_sorted_by_order(self):
        from acoharmony._pipes._builder import BronzeStage

        stages = [
            BronzeStage("z", "g", 10),
            BronzeStage("a", "g", 1),
            BronzeStage("m", "g", 5),
        ]
        sorted_stages = sorted(stages, key=lambda s: s.order)
        assert [s.name for s in sorted_stages] == ["a", "m", "z"]
