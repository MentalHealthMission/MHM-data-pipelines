"""Base classes for pipeline steps."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import subprocess

from ..context import RunContext


class PipelineStep(ABC):
    """Simple interface for a pipeline step."""

    def __init__(
        self,
        name: str,
        options: Optional[Dict[str, Any]] = None,
        *,
        run_per_participant: bool = True,
    ) -> None:
        self.name = name
        self.options = options or {}
        self.run_per_participant = run_per_participant

    def __call__(self, context: RunContext) -> Dict[str, Any]:
        return self.run(context)

    @abstractmethod
    def run(self, context: RunContext) -> Dict[str, Any]:
        """Execute the step and return step-specific metrics."""

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
    """Placeholder step until full implementation lands."""

    def __init__(self, name: str, options: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(name, options)

    def run(self, context: RunContext) -> Dict[str, Any]:
        self.log(context, "Step not yet implemented; skipping.")
        return {"status": "skipped"}


__all__ = ["PipelineStep", "NoOpStep"]
