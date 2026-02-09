"""MHM core pipeline spec compatibility wrapper."""

from connect_summary.pipeline.spec import (
    FiltersConfig,
    NotificationConfig,
    OutputsConfig,
    ProcessingConfig,
    PublishingConfig,
    RunSpec,
    SourceConfig,
    StepSpec,
    WorkspaceConfig,
    load_spec,
    validate_spec,
)

__all__ = [
    "FiltersConfig",
    "NotificationConfig",
    "OutputsConfig",
    "ProcessingConfig",
    "PublishingConfig",
    "RunSpec",
    "SourceConfig",
    "StepSpec",
    "WorkspaceConfig",
    "load_spec",
    "validate_spec",
]

