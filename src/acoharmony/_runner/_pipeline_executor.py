# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline execution component for multi-stage transformations.

 the execution of transformation pipelines that
involve multiple stages and dependencies between schemas.
"""

from datetime import datetime
from typing import Any

from .._decor8 import pipeline_method, timeit, validate_args
from .._trace.decorators import traced
from ..result import PipelineResult, ResultStatus, TransformResult
from ._registry import register_processor


@register_processor("pipeline_executor")
class PipelineExecutor:
    """
    Executes multi-stage transformation pipelines.
        - Pipeline definition and validation
        - Stage dependency resolution
        - Sequential stage execution
        - Result aggregation
    """


    def __init__(self, runner: Any):
        """
        Initialize pipeline executor.

                Args:
                    runner: Parent TransformRunner instance
        """
        self.runner = runner
        self.logger = runner.logger
        self.storage_config = runner.storage_config
        self.catalog = runner.catalog

    @pipeline_method(
        pipeline_arg="pipeline_name",
        threshold=60.0,
        track_memory=True,
    )
    def run_pipeline(self, pipeline_name: str, force: bool = False, **kwargs) -> PipelineResult:
        """
        Execute a named transformation pipeline.

                Args:
                    pipeline_name: Name of pipeline to execute
                    force: Force reprocessing of all stages
                    **kwargs: Additional arguments passed to transform_schema

                Returns:
                    PipelineResult: Aggregated results from all stages
        """
        start_time = datetime.now()

        # Check if this is a registered pipeline
        from .._pipes import PipelineRegistry

        pipeline_func = PipelineRegistry.get_pipeline(pipeline_name)

        if pipeline_func:
            # Execute registered pipeline
            self.logger.info(f"Executing pipeline: {pipeline_name}")
            try:
                results_dict = pipeline_func(self.runner, self.logger, force=force)

                # Convert dict results to PipelineResult
                stage_results = []
                for name, result_obj in results_dict.items():
                    # If result_obj is already a TransformResult, use it directly
                    if isinstance(result_obj, TransformResult):
                        stage_results.append(result_obj)
                    else:
                        # Otherwise create a success result (for backwards compatibility with LazyFrame returns)
                        result = TransformResult(
                            status=ResultStatus.SUCCESS,
                            message=f"Generated {name}",
                            records_processed=0,
                            files_processed=0,
                        )
                        stage_results.append(result)

                duration = (datetime.now() - start_time).total_seconds()
                pipeline_result = PipelineResult.pipeline_ok(stage_results)
                pipeline_result.message = f"Completed {len(results_dict)} outputs"
                pipeline_result.metadata["pipeline_name"] = pipeline_name
                pipeline_result.metadata["processing_time"] = duration
                return pipeline_result

            except (
                Exception
            ) as e:  # ALLOWED: Pipeline execution error handling - returns structured error result
                self.logger.error(f"Pipeline {pipeline_name} failed: {e}")
                error_result = TransformResult.transform_error(str(e))
                return PipelineResult.pipeline_ok([error_result])

        # Pipeline not found
        error_result = TransformResult.transform_error(f"Unknown pipeline: {pipeline_name}")
        return PipelineResult.pipeline_ok([error_result])

    def _is_stage_complete(self, stage_name: str) -> bool:
        """
        Check if a pipeline stage is already complete.

                Args:
                    stage_name: Name of stage

                Returns:
                    bool: True if stage is complete
        """
        output_path = self.storage_config.get_path("silver")
        output_file = output_path / f"{stage_name}.parquet"
        return output_file.exists()

    @classmethod
    def list_pipelines(cls) -> list[str]:
        """
        List available pipeline names.

                Returns:
                    List[str]: Available pipeline names
        """
        from .._pipes import PipelineRegistry

        # Get pipelines from PipelineRegistry
        return PipelineRegistry.list_pipelines()


    @timeit(log_level="debug")
    @traced()
    @validate_args(pipeline_name=str)
    def validate_pipeline(self, pipeline_name: str) -> dict[str, Any]:
        """
        Validate that pipeline exists in registry.

                Args:
                    pipeline_name: Name of pipeline to validate

                Returns:
                    Dict with validation results
        """
        from .._pipes import PipelineRegistry

        if PipelineRegistry.get_pipeline(pipeline_name) is None:
            return {"valid": False, "message": f"Unknown pipeline: {pipeline_name}"}

        return {"valid": True, "message": f"Pipeline '{pipeline_name}' is valid"}
