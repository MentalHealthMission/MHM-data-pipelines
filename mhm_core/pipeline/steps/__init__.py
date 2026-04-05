"""Pipeline step registry and factory."""

from __future__ import annotations

from typing import Dict, List, Type

from ..plugins import load_profile_plugins
from ..spec import RunSpec
from .base import PipelineStep

CORE_STEP_REGISTRY: Dict[str, Type[PipelineStep]] = {}


def build_step_registry(spec: RunSpec) -> Dict[str, Type[PipelineStep]]:
    registry = dict(CORE_STEP_REGISTRY)
    for plugin in load_profile_plugins(spec.profile):
        plugin.register_steps(registry)
    return registry


def build_steps(spec: RunSpec) -> List[PipelineStep]:
    registry = build_step_registry(spec)
    steps: List[PipelineStep] = []
    for index, step_spec in enumerate(spec.processing.steps, start=1):
        cls = registry.get(step_spec.type)
        if cls is None:
            raise ValueError(f"Unknown step type for profile '{spec.profile}': {step_spec.type}")
        step = cls(step_spec.options)
        setattr(step, "_step_index", index)
        setattr(step, "_step_type", step_spec.type)
        steps.append(step)
    return steps


__all__ = ["CORE_STEP_REGISTRY", "build_step_registry", "build_steps", "PipelineStep"]
