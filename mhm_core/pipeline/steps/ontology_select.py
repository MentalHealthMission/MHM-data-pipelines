"""Select ontology-driven feature plan."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from .base import PipelineStep
from ..context import RunContext
from ...ontology.config import load_unification_spec, select_features, write_feature_plan, UnificationFeature
from ...ontology.preference import dedupe_features_by_id, normalize_source_preference


def _render_tokens(value: object, tokens: Dict[str, object]) -> object:
    if isinstance(value, str):
        rendered = value
        for key, token in tokens.items():
            rendered = rendered.replace(f"{{{key}}}", str(token))
        return rendered
    if isinstance(value, list):
        return [_render_tokens(item, tokens) for item in value]
    if isinstance(value, dict):
        return {key: _render_tokens(val, tokens) for key, val in value.items()}
    return value


def _coerce_preference(value: object) -> List[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


class OntologySelectStep(PipelineStep):
    def __init__(self, options: Dict[str, object]) -> None:
        super().__init__("ontology_select", options, run_per_participant=False, suspend_checkpoint="step")

    def run(self, context: RunContext) -> Dict[str, object]:
        unification_path_raw = self.options.get("unification_spec") or self.options.get("unification")
        if not unification_path_raw:
            raise ValueError("ontology_select requires unification_spec")
        unification_paths = (
            [Path(str(item)).expanduser() for item in unification_path_raw]
            if isinstance(unification_path_raw, list)
            else [Path(str(unification_path_raw)).expanduser()]
        )
        for unification_path in unification_paths:
            if not unification_path.exists():
                raise FileNotFoundError(f"Unification spec not found: {unification_path}")

        mapping_path_raw = self.options.get("mapping")
        mapping_path = None
        if mapping_path_raw:
            mapping_path = Path(str(mapping_path_raw)).expanduser()

        request = dict(self.options.get("request") or {})
        categories = request.get("categories") or []
        features = request.get("features") or []
        prefer_raw = request.get("prefer")
        if prefer_raw is None:
            prefer_raw = self.options.get("prefer")
        prefer_order = normalize_source_preference(_coerce_preference(prefer_raw))

        combined = []
        for unification_path in unification_paths:
            spec = load_unification_spec(unification_path)
            combined.extend(spec.features)
        selected = select_features(
            type("CombinedSpec", (), {"features": combined}),
            categories=list(categories),
            feature_ids=list(features),
        )
        selected = dedupe_features_by_id(selected, prefer=prefer_order)
        request["prefer"] = prefer_order
        tokens = {
            "run_id": context.run_id,
            "workspace_dir": str(context.workspace_dir),
        }
        rendered: list[UnificationFeature] = []
        for feature in selected:
            rendered.append(
                UnificationFeature(
                    feature_id=feature.feature_id,
                    odim_feature=feature.odim_feature,
                    category=feature.category,
                    method=feature.method,
                    output_column=feature.output_column,
                    inputs=_render_tokens(feature.inputs, tokens),
                    params=_render_tokens(feature.params, tokens),
                    selection=_render_tokens(feature.selection, tokens),
                    computation=_render_tokens(feature.computation, tokens),
                    dimensions=list(feature.dimensions),
                    unify=bool(feature.unify),
                )
            )

        output_plan_raw = self.options.get("output_plan")
        if output_plan_raw:
            output_plan = Path(str(output_plan_raw).format(run_id=context.run_id)).expanduser()
        else:
            output_plan = context.workspace_dir / "ontology" / "feature-plan.yaml"

        write_feature_plan(
            output_plan,
            mapping_path=str(mapping_path) if mapping_path else None,
            unification_path=[str(path) for path in unification_paths],
            selected=rendered,
            request=request,
        )
        self.log(context, f"Wrote ontology feature plan to {output_plan}")
        return {"status": "ok", "features": len(selected), "plan": str(output_plan)}


__all__ = ["OntologySelectStep"]
