"""MHM core pipeline public API (compatibility re-exports)."""

from connect_summary.pipeline.context import RunContext, SummaryState, create_run_context
from connect_summary.pipeline.runner import main
from connect_summary.pipeline.spec import RunSpec, load_spec, validate_spec

__all__ = [
    "RunContext",
    "RunSpec",
    "SummaryState",
    "create_run_context",
    "load_spec",
    "main",
    "validate_spec",
]

