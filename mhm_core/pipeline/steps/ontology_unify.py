"""Unify ontology feature inputs across devices."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import yaml

from .base import PipelineStep
from ..context import RunContext, active_participants
from ...derived_features.utils import ensure_output_dir
from ...ontology.config import UnificationFeature
from ...ontology.unify import merge_unified_outputs, unify_features_for_participant


def _load_plan_features(path: Path) -> List[UnificationFeature]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    features = []
    for raw in data.get("features", []):
        features.append(
            UnificationFeature(
                feature_id=str(raw.get("id", "")).strip(),
                odim_feature=str(raw.get("odim_feature", "odim:DerivedFeature")).strip(),
                category=str(raw.get("category", "")).strip(),
                method=str(raw.get("method", "daily_sum")).strip(),
                output_column=str(raw.get("output_column", raw.get("id", ""))).strip(),
                inputs=raw.get("inputs"),
                params=dict(raw.get("params") or {}),
                selection=dict(raw.get("selection") or {}),
                computation=dict(raw.get("computation") or {}),
                dimensions=[str(dim) for dim in (raw.get("dimensions") or [])],
                unify=bool(raw.get("unify", True)),
            )
        )
    return [feat for feat in features if feat.feature_id]


class OntologyUnifyStep(PipelineStep):
    def __init__(self, options: Dict[str, object]) -> None:
        super().__init__("ontology_unify", options, run_per_participant=False, suspend_checkpoint="step")

    def run(self, context: RunContext) -> Dict[str, object]:
        plan_raw = self.options.get("plan")
        if not plan_raw:
            raise ValueError("ontology_unify requires plan path")
        plan_path = Path(str(plan_raw).format(run_id=context.run_id)).expanduser()
        if not plan_path.exists():
            raise FileNotFoundError(f"Ontology feature plan not found: {plan_path}")
        features = _load_plan_features(plan_path)
        if not features:
            return {"status": "skipped", "reason": "no features"}

        merged_dir = Path(str(self.options.get("merged_dir", context.merged_dir)).format(run_id=context.run_id)).expanduser()
        output_dir = Path(str(self.options.get("output_dir", context.workspace_dir / "ontology" / "unified")).format(run_id=context.run_id)).expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)

        participants = active_participants(context)
        processed = 0
        for participant_id in participants:
            site = context.participant_sites.get(participant_id)
            if not site:
                for candidate in merged_dir.iterdir():
                    if (candidate / participant_id).exists():
                        site = candidate.name
                        context.participant_sites[participant_id] = site
                        break
            if not site:
                context.logger.warning("Skipping %s (site unknown)", participant_id)
                continue

            outputs = unify_features_for_participant(
                features=features,
                merged_dir=merged_dir,
                site=site,
                participant_id=participant_id,
            )
            if not outputs:
                continue
            participant_out = ensure_output_dir(output_dir, participant_id)
            for output in outputs:
                target = participant_out / f"{output.feature_id}.csv"
                output.dataframe.to_csv(target, index=False)

            merged = merge_unified_outputs(outputs)
            if not merged.empty:
                merged.to_csv(participant_out / "unified_features.csv", index=False)
            processed += 1

        return {"status": "ok", "participants": processed, "output_dir": str(output_dir)}


__all__ = ["OntologyUnifyStep"]
