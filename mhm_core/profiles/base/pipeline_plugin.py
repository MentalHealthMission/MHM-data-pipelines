"""Base pipeline profile registrations.

The base profile includes the generic step surface plus first-party optional
integration steps. Use the minimal profile when exercising the strict pipeline
kernel without derived-feature, RAPIDS, or publish integrations.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Dict, Type

from mhm_core.pipeline.plugins.base import PipelineProfilePlugin
from mhm_core.pipeline.steps.base import PipelineStep

if TYPE_CHECKING:  # pragma: no cover
    from mhm_core.pipeline.context import RunContext
    from mhm_core.pipeline.spec import RunSpec


class BasePipelineProfile(PipelineProfilePlugin):
    profile_id = "base"

    def register_steps(self, registry: Dict[str, Type["PipelineStep"]]) -> None:
        from mhm_core.pipeline.steps.base import NoOpStep
        from mhm_core.pipeline.steps.publish import PublishStep

        steps: Dict[str, Type["PipelineStep"]] = {
            "noop": NoOpStep,
            "combine_features": optional_integration_step(
                "combine_features",
                "mhm_core.pipeline.integrations.rapids:CombineFeaturesStep",
                requirement="mhm-rapids",
            ),
            "derived_features": optional_integration_step(
                "derived_features",
                "mhm_core.pipeline.integrations.derived_features:DerivedFeaturesStep",
                requirement="mhm-semantics",
            ),
            "publish": PublishStep,
        }
        registry.update(steps)
        for name, step_cls in steps.items():
            registry[f"base.{name}"] = step_cls


def optional_integration_step(
    name: str,
    target: str,
    *,
    requirement: str,
    run_per_participant: bool = False,
    suspend_checkpoint: str = "step",
) -> Type[PipelineStep]:
    """Create a lightweight proxy for an optional first-party integration."""

    module_name, _, class_name = target.partition(":")

    class OptionalIntegrationStep(PipelineStep):
        def __init__(self, options=None) -> None:
            super().__init__(
                name,
                options,
                run_per_participant=run_per_participant,
                suspend_checkpoint=suspend_checkpoint,
            )
            self._delegate: PipelineStep | None = None

        def _load_delegate(self) -> PipelineStep:
            if self._delegate is not None:
                return self._delegate
            try:
                module = import_module(module_name)
                step_cls = getattr(module, class_name)
            except (ImportError, AttributeError) as exc:
                raise RuntimeError(
                    f"Pipeline step '{name}' requires the optional {requirement} package"
                ) from exc
            self._delegate = step_cls(self.options)
            return self._delegate

        def run(self, context: "RunContext"):
            return self._load_delegate().run(context)

        def after_metrics_recorded(self, context: "RunContext", metrics):
            return self._load_delegate().after_metrics_recorded(context, metrics)

        def describe_produced_states(self, context: "RunContext"):
            return self._load_delegate().describe_produced_states(context)

        def describe_operation(self, context: "RunContext"):
            return self._load_delegate().describe_operation(context)

        def describe_capabilities(self, spec: "RunSpec"):
            return self._load_delegate().describe_capabilities(spec)

    OptionalIntegrationStep.__name__ = f"Optional{class_name}"
    OptionalIntegrationStep.__qualname__ = OptionalIntegrationStep.__name__
    return OptionalIntegrationStep


__all__ = ["BasePipelineProfile"]
