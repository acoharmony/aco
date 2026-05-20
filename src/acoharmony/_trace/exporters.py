"""Custom span exporters for ACO Harmony tracing."""

import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class FileSpanExporter(SpanExporter):
    """
    Exports spans to a JSONL file, similar to structured logging.

        Writes traces to daily files in the format:
        acoharmony_YYYYMMDD_traces.jsonl

        Each span is written as a JSON object with:
        - trace_id: Trace identifier
        - span_id: Span identifier
        - parent_span_id: Parent span identifier (if any)
        - name: Span name
        - start_time: Start timestamp (ISO format)
        - end_time: End timestamp (ISO format)
        - duration_ms: Duration in milliseconds
        - attributes: Span attributes
        - status: Span status (OK, ERROR, etc.)
        - events: Span events (if any)
    """

    def __init__(self, base_path: Path | str):
        """
        Initialize the file exporter.

                Parameters

                base_path : Path or str
                    Base directory for trace files.
        """
        self.base_path = Path(base_path) if isinstance(base_path, str) else base_path
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._current_date = datetime.now().strftime("%Y%m%d")
        self._file_handle = None

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """
        Export spans to file.

                Parameters

                spans : Sequence[ReadableSpan]
                    Spans to export.

                Returns

                SpanExportResult
                    Result of the export operation.
        """
        try:
            # Check if we need to rotate to a new file (new day)
            current_date = datetime.now().strftime("%Y%m%d")
            if current_date != self._current_date:
                if self._file_handle:
                    self._file_handle.close()
                    self._file_handle = None
                self._current_date = current_date

            # Get or create file handle
            file_path = self.base_path / f"acoharmony_{self._current_date}_traces.jsonl"
            if self._file_handle is None:
                self._file_handle = open(file_path, "a", encoding="utf-8")

            # Write each span as a JSON line
            for span in spans:
                span_dict = self._span_to_dict(span)
                json.dump(span_dict, self._file_handle)
                self._file_handle.write("\n")

            self._file_handle.flush()
            return SpanExportResult.SUCCESS

        except (
            Exception
        ) as e:  # ALLOWED: Logs error and returns, caller handles the error condition
            import logging

            logger = logging.getLogger("acoharmony.trace.exporter")
            logger.error(f"Failed to export spans to file: {e}")
            return SpanExportResult.FAILURE

    def _span_to_dict(self, span: ReadableSpan) -> dict:
        """
        Convert a span to a dictionary for JSON serialization.

                Parameters

                span : ReadableSpan
                    Span to convert.

                Returns

                dict
                    Span data as dictionary.
        """
        # Convert nanoseconds to milliseconds
        start_time_ms = span.start_time / 1_000_000
        end_time_ms = span.end_time / 1_000_000 if span.end_time else start_time_ms
        duration_ms = end_time_ms - start_time_ms

        # Convert timestamp to ISO format
        start_time_iso = datetime.fromtimestamp(start_time_ms / 1000).isoformat()
        end_time_iso = datetime.fromtimestamp(end_time_ms / 1000).isoformat()

        # Extract trace and span IDs
        trace_id = f"{span.context.trace_id:032x}"
        span_id = f"{span.context.span_id:016x}"
        parent_span_id = (
            f"{span.parent.span_id:016x}" if span.parent and span.parent.span_id else None
        )

        # Build the span dictionary
        span_dict = {
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "name": span.name,
            "start_time": start_time_iso,
            "end_time": end_time_iso,
            "duration_ms": duration_ms,
            "status": span.status.status_code.name,
            "attributes": dict(span.attributes) if span.attributes else {},
        }

        # Add events if present
        if span.events:
            span_dict["events"] = [
                {
                    "name": event.name,
                    "timestamp": datetime.fromtimestamp(
                        event.timestamp / 1_000_000_000
                    ).isoformat(),
                    "attributes": dict(event.attributes) if event.attributes else {},
                }
                for event in span.events
            ]

        # Add status description if error
        if span.status.description:
            span_dict["status_description"] = span.status.description

        return span_dict

    def shutdown(self):
        """Shutdown the exporter and close file handles."""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """
        Force flush any buffered spans.

                Parameters

                timeout_millis : int
                    Timeout in milliseconds (unused for file exporter).

                Returns

                bool
                    True if flush succeeded.
        """
        if self._file_handle:
            self._file_handle.flush()
        return True
