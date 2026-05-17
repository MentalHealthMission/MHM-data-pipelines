"""Base classes for pipeline steps."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
import subprocess

from ..capabilities import PipelineStepCapabilities
from ..context import RunContext
from ..spec import RunSpec


@dataclass
class PipelineStepStateDescriptor:
    """Declarative description of a durable dataset state produced by a step."""

    lineage_key: str
    data_root: str | Path | None = None
    existing_manifest_path: str | Path | None = None
    dataset_kind: str = "pipeline_step_state"
    title: str = ""
    notes: str = ""
    surface: str = ""
    domain: str = ""
    stage: str = ""
    layout: str = ""
    slice_name: str = ""
    fingerprint_mode: str = "metadata"
    parent_lineage_key: str = ""
    default_parent_manifest: str = ""
    history_event_type: str = "snapshot_pipeline_step_state"
    logical_root_overrides: Dict[str, Any] = field(default_factory=dict)
    extra_metadata: Dict[str, Any] = field(default_factory=dict)
    additional_control_documents: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class PipelineStepOperationDescriptor:
    """Declarative description of an operation performed by a step."""

    operation_kind: str = "transform"
    operation_name: str = ""
    title: str = ""
    summary: str = ""
    input_lineage_keys: list[str] = field(default_factory=list)
    output_lineage_keys: list[str] = field(default_factory=list)
    input_state_manifests: list[str | Path] = field(default_factory=list)
    input_knowledge_documents: list[tuple[str, str]] = field(default_factory=list)
    output_knowledge_documents: list[tuple[str, str]] = field(default_factory=list)
    additional_control_documents: list[tuple[str, str]] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    extra_metadata: Dict[str, Any] = field(default_factory=dict)


class PipelineStep(ABC):
    """Simple interface for a pipeline step."""

    def __init__(
        self,
        name: str,
        options: Optional[Dict[str, Any]] = None,
        *,
        run_per_participant: bool = True,
        suspend_checkpoint: str,
    ) -> None:
        self.name = name
        self.options = options or {}
        self.run_per_participant = run_per_participant
        if suspend_checkpoint not in {"participant", "batch", "step"}:
            raise ValueError(f"Invalid suspend checkpoint for step {name}: {suspend_checkpoint}")
        self.suspend_checkpoint = suspend_checkpoint

    def __call__(self, context: RunContext) -> Dict[str, Any]:
        return self.run(context)

    @abstractmethod
    def run(self, context: RunContext) -> Dict[str, Any]:
        """Execute the step and return step-specific metrics."""

    def describe_produced_states(self, context: RunContext) -> list[PipelineStepStateDescriptor]:
        """Declare durable dataset states produced by this step for provenance capture."""
        return []

    def describe_operation(self, context: RunContext) -> PipelineStepOperationDescriptor | None:
        """Declare the semantic operation performed by this step for provenance capture."""
        return None

    def describe_capabilities(self, spec: RunSpec) -> PipelineStepCapabilities:
        """Declare optional generic capabilities used by planning layers."""
        return PipelineStepCapabilities()

    def log(self, context: RunContext, message: str) -> None:
        context.logger.info("[%-9s] %s", self.name, message)

    def run_command(self, context: RunContext, cmd: list[str]) -> None:
        """Run a command, streaming stdout/stderr into the pipeline logs."""
        context.logger.debug("[%-9s] exec: %s", self.name, " ".join(cmd))
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        assert process.stdout is not None  # for type checkers
        for line in process.stdout:
            context.logger.info("[%-9s] %s", self.name, line.rstrip())

        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(f"{self.name} step failed with exit code {return_code}")


class NoOpStep(PipelineStep):
    """Intentional no-op step for smoke tests and queue barriers."""

    def __init__(self, options: Optional[Dict[str, Any]] = None) -> None:
        super().__init__("noop", options, run_per_participant=False, suspend_checkpoint="step")

    def run(self, context: RunContext) -> Dict[str, Any]:
        self.log(context, "No-op step completed.")
        return {"status": "ok"}


__all__ = [
    "PipelineStep",
    "PipelineStepOperationDescriptor",
    "PipelineStepStateDescriptor",
    "NoOpStep",
]
