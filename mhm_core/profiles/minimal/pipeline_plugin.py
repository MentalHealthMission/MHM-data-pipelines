"""Minimal pipeline profile registrations.

This profile is the smallest supported pipeline kernel. It deliberately
excludes derived-feature, ontology, application-profile, and integration
modules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Type

from mhm_core.pipeline.plugins.base import PipelineProfilePlugin

if TYPE_CHECKING:  # pragma: no cover
    from mhm_core.pipeline.steps.base import PipelineStep


class MinimalPipelineProfile(PipelineProfilePlugin):
    profile_id = "minimal"
    include_base_profile = False

    def register_steps(self, registry: Dict[str, Type["PipelineStep"]]) -> None:
        from mhm_core.pipeline.steps.base import NoOpStep
        from mhm_core.pipeline.steps.publish import PublishStep

        steps: Dict[str, Type["PipelineStep"]] = {
            "noop": NoOpStep,
            "publish": PublishStep,
        }
        registry.update(steps)
        for name, step_cls in steps.items():
            registry[f"minimal.{name}"] = step_cls


__all__ = ["MinimalPipelineProfile"]
