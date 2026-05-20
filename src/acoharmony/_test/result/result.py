"""Tests for result.py — ResultStatus, Result[T], TransformResult, PipelineResult."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock

import pytest


class TestResultStatus:
    """Tests for ResultStatus enum."""

    @pytest.mark.unit
    def test_enum_values(self):

        assert ResultStatus.SUCCESS.value == "success"
        assert ResultStatus.FAILURE.value == "failure"
        assert ResultStatus.PARTIAL.value == "partial"
        assert ResultStatus.SKIPPED.value == "skipped"

    @pytest.mark.unit
    def test_all_members(self):

        members = list(ResultStatus)
        assert len(members) == 4


class TestResult:
    """Tests for Result[T] generic class."""

    @pytest.mark.unit
    def test_ok_factory(self):

        r = Result.ok(data=42, message="success", extra="info")
        assert r.status == ResultStatus.SUCCESS
        assert r.data == 42
        assert r.message == "success"
        assert r.metadata == {"extra": "info"}
        assert r.success is True
        assert r.failed is False

    @pytest.mark.unit
    def test_ok_no_data(self):

        r = Result.ok()
        assert r.status == ResultStatus.SUCCESS
        assert r.data is None
        assert r.message == ""

    @pytest.mark.unit
    def test_error_factory(self):

        r = Result.error("something broke", errors=["err1", "err2"], ctx="debug")
        assert r.status == ResultStatus.FAILURE
        assert r.data is None
        assert r.message == "something broke"
        assert r.errors == ["err1", "err2"]
        assert r.metadata == {"ctx": "debug"}
        assert r.success is False
        assert r.failed is True

    @pytest.mark.unit
    def test_error_default_errors_list(self):

        r = Result.error("broken")
        assert r.errors == ["broken"]

    @pytest.mark.unit
    def test_partial_factory(self):

        r = Result.partial(data=[1, 2], message="some ok", warnings=["warn1"], key="val")
        assert r.status == ResultStatus.PARTIAL
        assert r.data == [1, 2]
        assert r.warnings == ["warn1"]
        assert r.metadata == {"key": "val"}
        assert r.success is False
        assert r.failed is False

    @pytest.mark.unit
    def test_partial_no_warnings(self):

        r = Result.partial(data=None, message="partial")
        assert r.warnings == []

    @pytest.mark.unit
    def test_skipped_factory(self):

        r = Result.skipped("already done", reason="cached")
        assert r.status == ResultStatus.SKIPPED
        assert r.data is None
        assert r.message == "already done"
        assert r.metadata == {"reason": "cached"}
        assert r.success is False
        assert r.failed is False

    @pytest.mark.unit
    def test_unwrap_success(self):

        r = Result.ok(data="hello")
        assert r.unwrap() == "hello"

    @pytest.mark.unit
    def test_unwrap_failure_raises(self):

        r = Result.error("bad")
        with pytest.raises(ValueError, match="Cannot unwrap failed result"):
            r.unwrap()

    @pytest.mark.unit
    def test_unwrap_or_success(self):

        r = Result.ok(data=10)
        assert r.unwrap_or(99) == 10

    @pytest.mark.unit
    def test_unwrap_or_failure(self):

        r = Result.error("bad")
        assert r.unwrap_or(99) == 99

    @pytest.mark.unit
    def test_unwrap_or_skipped(self):

        r = Result.skipped("skip")
        assert r.unwrap_or("default") == "default"

    @pytest.mark.unit
    def test_map_success(self):

        r = Result.ok(data=5, message="original")
        mapped = r.map(lambda x: x * 2)
        assert mapped.status == ResultStatus.SUCCESS
        assert mapped.data == 10

    @pytest.mark.unit
    def test_map_failure_passthrough(self):

        r = Result.error("bad")
        mapped = r.map(lambda x: x * 2)
        assert mapped.status == ResultStatus.FAILURE
        assert mapped is r

    @pytest.mark.unit
    def test_map_none_data_passthrough(self):

        r = Result.ok(data=None, message="empty")
        mapped = r.map(lambda x: x * 2)
        assert mapped is r

    @pytest.mark.unit
    def test_map_exception_returns_error(self):

        r = Result.ok(data="not_a_number")
        mapped = r.map(lambda x: int(x))
        assert mapped.status == ResultStatus.FAILURE
        assert "invalid literal" in mapped.message

    @pytest.mark.unit
    def test_and_then_success(self):

        r = Result.ok(data=10)
        chained = r.and_then(lambda x: Result.ok(data=x + 5))
        assert chained.status == ResultStatus.SUCCESS
        assert chained.data == 15

    @pytest.mark.unit
    def test_and_then_failure_passthrough(self):

        r = Result.error("bad")
        chained = r.and_then(lambda x: Result.ok(data=x + 5))
        assert chained.status == ResultStatus.FAILURE
        assert chained is r

    @pytest.mark.unit
    def test_and_then_chain_fails_midway(self):

        r = Result.ok(data=10)
        chained = r.and_then(lambda x: Result.error("fail mid"))
        assert chained.status == ResultStatus.FAILURE

    @pytest.mark.unit
    def test_bool_success(self):

        r = Result.ok(data=1)
        assert bool(r) is True

    @pytest.mark.unit
    def test_bool_failure(self):

        r = Result.error("no")
        assert bool(r) is False

    @pytest.mark.unit
    def test_bool_skipped(self):

        r = Result.skipped("skip")
        assert bool(r) is False

    @pytest.mark.unit
    def test_str_success(self):

        r = Result.ok(message="all good")
        s = str(r)
        assert "[OK]" in s
        assert "all good" in s

    @pytest.mark.unit
    def test_str_failure_with_errors(self):

        r = Result.error("failed", errors=["e1", "e2"])
        s = str(r)
        assert "[ERROR]" in s
        assert "Errors:" in s
        assert "e1" in s

    @pytest.mark.unit
    def test_str_partial_with_warnings(self):

        r = Result.partial(data=None, message="partial", warnings=["w1"])
        s = str(r)
        assert "Warnings:" in s
        assert "w1" in s

    @pytest.mark.unit
    def test_str_skipped(self):

        r = Result.skipped("not needed")
        s = str(r)
        assert "not needed" in s


class TestResultDeeper:
    """Cover Result paths not yet exercised."""

    @pytest.mark.unit
    def test_map_on_failure(self):

        r = Result.error("fail")
        mapped = r.map(lambda x: x * 2)
        assert mapped.failed

    @pytest.mark.unit
    def test_map_with_none_data(self):

        r = Result.ok(data=None, message="empty success")
        mapped = r.map(lambda x: x * 2)
        assert mapped.success  # passes through unchanged

    @pytest.mark.unit
    def test_map_function_raises(self):

        r = Result.ok(data=42)
        mapped = r.map(lambda x: 1 / 0)
        assert mapped.failed

    @pytest.mark.unit
    def test_and_then_on_failure(self):

        r = Result.error("fail")
        chained = r.and_then(lambda x: Result.ok(x * 2))
        assert chained.failed

    @pytest.mark.unit
    def test_and_then_success(self):

        r = Result.ok(data=5)
        chained = r.and_then(lambda x: Result.ok(x * 2))
        assert chained.success
        assert chained.data == 10

    @pytest.mark.unit
    def test_str_with_errors(self):

        r = Result.error("fail", errors=["err1", "err2"])
        s = str(r)
        assert "err1" in s
        assert "err2" in s

    @pytest.mark.unit
    def test_str_with_warnings(self):

        r = Result.partial(data=None, message="partial", warnings=["w1"])
        s = str(r)
        assert "w1" in s

    @pytest.mark.unit
    def test_bool_true(self):

        assert bool(Result.ok()) is True

    @pytest.mark.unit
    def test_bool_false(self):

        assert bool(Result.error("x")) is False

    @pytest.mark.unit
    def test_unwrap_failure(self):

        r = Result.error("nope")
        with pytest.raises(ValueError, match="Cannot unwrap"):
            r.unwrap()


class TestTransformResult:
    """Tests for TransformResult."""

    @pytest.mark.unit
    def test_transform_ok(self):

        r = TransformResult.transform_ok(records=1000, files=3, output="/out/data.parquet")
        assert r.status == ResultStatus.SUCCESS
        assert r.records_processed == 1000
        assert r.files_processed == 3
        assert r.output_path == "/out/data.parquet"
        assert r.data == {"records": 1000, "files": 3, "output": "/out/data.parquet"}
        assert "1,000" in r.message

    @pytest.mark.unit
    def test_transform_ok_custom_message(self):

        r = TransformResult.transform_ok(records=5, files=1, output="/x", message="custom msg")
        assert r.message == "custom msg"

    @pytest.mark.unit
    def test_transform_error(self):

        r = TransformResult.transform_error("disk full", errors=["no space"])
        assert r.status == ResultStatus.FAILURE
        assert r.errors == ["no space"]
        assert r.records_processed == 0
        assert r.files_processed == 0
        assert r.output_path is None

    @pytest.mark.unit
    def test_transform_error_default_errors(self):

        r = TransformResult.transform_error("crash")
        assert r.errors == ["crash"]


class TestPipelineResult:
    """Tests for PipelineResult."""

    @pytest.mark.unit
    def test_completion_rate_zero_stages(self):

        r = PipelineResult(
            status=None, stages_completed=0, stages_total=0
        )
        assert r.completion_rate == 0.0

    @pytest.mark.unit
    def test_completion_rate_all_done(self):

        r = PipelineResult(
            status=ResultStatus.SUCCESS, stages_completed=3, stages_total=3
        )
        assert r.completion_rate == 100.0

    @pytest.mark.unit
    def test_completion_rate_partial(self):

        r = PipelineResult(
            status=ResultStatus.PARTIAL, stages_completed=1, stages_total=4
        )
        assert r.completion_rate == 25.0

    @pytest.mark.unit
    def test_pipeline_ok_all_success(self):

        results = [
            TransformResult.transform_ok(100, 1, "/a"),
            TransformResult.transform_ok(200, 2, "/b"),
        ]
        pr = PipelineResult.pipeline_ok(results)
        assert pr.status == ResultStatus.SUCCESS
        assert pr.stages_completed == 2
        assert pr.stages_total == 2
        assert "2/2" in pr.message

    @pytest.mark.unit
    def test_pipeline_ok_with_failure(self):

        results = [
            TransformResult.transform_ok(100, 1, "/a"),
            TransformResult.transform_error("failed stage"),
        ]
        pr = PipelineResult.pipeline_ok(results)
        assert pr.status == ResultStatus.PARTIAL
        assert pr.stages_completed == 1
        assert pr.stages_total == 2

    @pytest.mark.unit
    def test_pipeline_ok_with_skipped(self):

        ok_result = TransformResult.transform_ok(100, 1, "/a")
        skipped_result = TransformResult(status=ResultStatus.SKIPPED, message="skip")
        results = [ok_result, skipped_result]
        pr = PipelineResult.pipeline_ok(results)
        assert pr.status == ResultStatus.SUCCESS
        assert pr.stages_completed == 2

    @pytest.mark.unit
    def test_pipeline_ok_empty(self):

        pr = PipelineResult.pipeline_ok([])
        assert pr.status == ResultStatus.SUCCESS
        assert pr.stages_completed == 0
        assert pr.stages_total == 0

    @pytest.mark.unit
    def test_get_summary_with_stages(self):

        results = [
            TransformResult.transform_ok(100, 1, "/a"),
            TransformResult.transform_error("fail"),
            TransformResult(status=ResultStatus.PARTIAL, message="partial done"),
            TransformResult(status=ResultStatus.SKIPPED, message="skipped"),
        ]
        pr = PipelineResult.pipeline_ok(results)
        summary = pr.get_summary()
        assert "Pipeline Result:" in summary
        assert "Completion:" in summary
        assert "[OK] Stage 1" in summary
        assert "[ERROR] Stage 2" in summary
        assert "Stage 3" in summary
        assert "Stage 4" in summary

    @pytest.mark.unit
    def test_get_summary_no_data(self):

        pr = PipelineResult(
            status=ResultStatus.SUCCESS,
            message="empty",
            stages_completed=0,
            stages_total=0,
            data=None,
        )
        summary = pr.get_summary()
        assert "Pipeline Result: empty" in summary
        assert "Completion: 0.0%" in summary


class TestPipelineResultDeeper:
    """Cover PipelineResult summary generation."""

    @pytest.mark.unit
    def test_get_summary_mixed(self):

        results = [
            TransformResult.transform_ok(records=100, files=1, output="/a"),
            TransformResult.transform_error("failed stage"),
            TransformResult(status=ResultStatus.SKIPPED, message="skipped"),
            TransformResult(status=ResultStatus.PARTIAL, message="partial"),
        ]
        pr = PipelineResult.pipeline_ok(results)
        summary = pr.get_summary()
        assert "[OK]" in summary
        assert "[ERROR]" in summary
        assert "Stage 1" in summary
        assert "Stage 4" in summary

    @pytest.mark.unit
    def test_completion_rate_zero(self):

        pr = PipelineResult(
            status=MagicMock(),
            stages_completed=0,
            stages_total=0,
        )
        assert pr.completion_rate == 0.0

    @pytest.mark.unit
    def test_get_summary_empty(self):

        pr = PipelineResult.pipeline_ok([])
        summary = pr.get_summary()
        assert "0/0" in summary
