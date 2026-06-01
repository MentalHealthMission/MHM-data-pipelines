"""Combine RAPIDS reduced outputs with derived features."""

from __future__ import annotations

from pathlib import Path
from typing import Dict
from importlib.util import find_spec
import sys

from mhm_core.pipeline.context import RunContext, active_entities
from mhm_core.pipeline.steps.base import PipelineStep


class CombineFeaturesStep(PipelineStep):
    def __init__(self, options: Dict[str, object]) -> None:
        super().__init__("combine_features", options, run_per_participant=False, suspend_checkpoint="step")

    def run(self, context: RunContext) -> Dict[str, object]:
        rapids_dir = Path(
            str(self.options.get("rapids_dir", context.workspace_dir / "combined")).format(run_id=context.run_id)
        ).resolve()
        derived_dir = Path(
            str(self.options.get("derived_dir", context.workspace_dir / "derived_features")).format(run_id=context.run_id)
        ).resolve()
        output_dir = Path(
            str(self.options.get("output_dir", context.workspace_dir / "combined_features")).format(run_id=context.run_id)
        ).resolve()

        entities = self.options.get("entities", self.options.get("participants"))
        entities_arg = None
        if isinstance(entities, list):
            entities_arg = ",".join(str(entity_id) for entity_id in entities if str(entity_id).strip())
        elif isinstance(entities, str) and entities.strip():
            entities_arg = entities.strip()
        else:
            entities_arg = ",".join(active_entities(context))

        rapids_glob = str(self.options.get("rapids_glob", "rapids_*.csv"))
        derived_glob = str(self.options.get("derived_glob", "*.csv"))

        if find_spec("mhm_core.rapids.combine") is None:
            raise RuntimeError("combine_features requires the optional mhm-rapids package")

        cmd = [
            sys.executable,
            "-m",
            "mhm_core.rapids.combine",
            "--rapids-dir",
            str(rapids_dir),
            "--derived-dir",
            str(derived_dir),
            "--output-dir",
            str(output_dir),
            "--rapids-glob",
            rapids_glob,
            "--derived-glob",
            derived_glob,
        ]
        if entities_arg:
            cmd.extend(["--entities", entities_arg])

        self.log(context, f"Combining features into {output_dir}")
        self.run_command(context, cmd)
        return {"status": "ok", "output_dir": str(output_dir)}


__all__ = ["CombineFeaturesStep"]
