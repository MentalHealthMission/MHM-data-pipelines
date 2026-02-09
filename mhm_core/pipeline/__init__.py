"""Pipeline orchestration utilities for the MHM core."""

from .context import RunContext, SummaryState, create_run_context
from .spec import RunSpec, load_spec, validate_spec

__all__ = [
    "RunContext",
    "RunSpec",
    "SummaryState",
    "create_run_context",
    "load_spec",
    "main",
    "validate_spec",
]


def __getattr__(name):
    """Lazily resolve attributes that would otherwise cause circular imports."""

    if name == "main":
        from .runner import main as _main

        return _main
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
