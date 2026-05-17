"""Base pipeline profile registrations.

This profile intentionally exposes only source-agnostic steps.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Type

from mhm_core.pipeline.plugins.base import PipelineProfilePlugin

if TYPE_CHECKING:  # pragma: no cover
    from mhm_core.pipeline.steps.base import PipelineStep


class BasePipelineProfile(PipelineProfilePlugin):
    profile_id = "base"

    def register_steps(self, registry: Dict[str, Type["PipelineStep"]]) -> None:
        from mhm_core.pipeline.steps.combine_features import CombineFeaturesStep
        from mhm_core.pipeline.steps.derived_features import DerivedFeaturesStep
        from mhm_core.pipeline.steps.ontology_reason import OntologyReasonStep
        from mhm_core.pipeline.steps.ontology_select import OntologySelectStep
        from mhm_core.pipeline.steps.ontology_unify import OntologyUnifyStep
        from mhm_core.pipeline.steps.publish import PublishStep

        steps: Dict[str, Type["PipelineStep"]] = {
            "combine_features": CombineFeaturesStep,
            "derived_features": DerivedFeaturesStep,
            "ontology_select": OntologySelectStep,
            "ontology_unify": OntologyUnifyStep,
            "ontology_reason": OntologyReasonStep,
            "publish": PublishStep,
        }
        registry.update(steps)
        for name, step_cls in steps.items():
            registry[f"base.{name}"] = step_cls


__all__ = ["BasePipelineProfile"]
