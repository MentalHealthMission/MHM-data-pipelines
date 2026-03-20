"""Combine RAPIDS reduced outputs with derived features."""

from __future__ import annotations

from pathlib import Path
from typing import Dict
import sys

from .base import PipelineStep
from ..context import RunContext, active_participants


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

        participants = self.options.get("participants")
        participants_arg = None
        if isinstance(participants, list):
            participants_arg = ",".join(str(pid) for pid in participants if str(pid).strip())
        elif isinstance(participants, str) and participants.strip():
            participants_arg = participants.strip()
        else:
            participants_arg = ",".join(active_participants(context))

        rapids_glob = str(self.options.get("rapids_glob", "rapids_*.csv"))
        derived_glob = str(self.options.get("derived_glob", "*.csv"))

        cmd = [
            sys.executable,
            str(Path(__file__).resolve().parents[2] / "combine_features.py"),
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
        if participants_arg:
            cmd.extend(["--participants", participants_arg])

        self.log(context, f"Combining features into {output_dir}")
        self.run_command(context, cmd)
        return {"status": "ok", "output_dir": str(output_dir)}


__all__ = ["CombineFeaturesStep"]
