"""Run custom derived feature extraction."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import yaml

from .base import PipelineStep
from ..context import RunContext, active_participants
from ...derived_features.runner import run_derived_features_for_participant
from ...derived_features.registry import build_derived_spec_from_root, build_derived_spec_from_registry


class DerivedFeaturesStep(PipelineStep):
    def __init__(self, options: Dict[str, object]) -> None:
        super().__init__("derived_features", options, run_per_participant=False, suspend_checkpoint="step")

    def _parse_list(self, value: object) -> Optional[List[str]]:
        if value is None:
            return None
        if isinstance(value, list):
            return [str(item) for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return None

    def _parse_bool(self, value: object) -> Optional[bool]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "y", "on"}:
                return True
            if normalized in {"false", "0", "no", "n", "off"}:
                return False
        return bool(value)

    def run(self, context: RunContext) -> Dict[str, object]:
        spec_path_raw = self.options.get("spec")
        spec_path: Optional[Path] = None
        if spec_path_raw:
            spec_path = Path(str(spec_path_raw).format(run_id=context.run_id)).expanduser()
            if not spec_path.exists():
                raise FileNotFoundError(f"derived_features spec not found: {spec_path}")
        else:
            data_book = self.options.get("data_book") or {}
            if isinstance(data_book, str):
                data_book = {"root": data_book}
            scan_root = (
                self.options.get("derived_features_root")
                or self.options.get("derived_features_scan_root")
                or self.options.get("data_book_root")
                or data_book.get("root")
            )
            if scan_root:
                root = Path(str(scan_root).format(run_id=context.run_id)).expanduser().resolve()
                if not root.exists():
                    raise FileNotFoundError(f"derived_features_root not found: {root}")

                registry_raw = (
                    self.options.get("derived_features_registry")
                    or self.options.get("data_book_registry")
                    or data_book.get("registry")
                )
                registry_paths: Optional[List[Path]] = None
                if registry_raw:
                    registry_items = self._parse_list(registry_raw) or []
                    registry_paths = [(root / Path(item)).resolve() for item in registry_items]

                include_globs = self._parse_list(
                    self.options.get("derived_features_include")
                    or self.options.get("data_book_include")
                    or data_book.get("include")
                )
                exclude_globs = self._parse_list(
                    self.options.get("derived_features_exclude")
                    or self.options.get("data_book_exclude")
                    or data_book.get("exclude")
                )
                scan_raw = self.options.get("derived_features_scan")
                if scan_raw is None:
                    scan_raw = self.options.get("data_book_scan")
                if scan_raw is None:
                    scan_raw = data_book.get("scan")
                scan_enabled = True if scan_raw is None else bool(self._parse_bool(scan_raw))

                feature_ids = self._parse_list(self.options.get("feature_ids") or self.options.get("features"))

                if not scan_enabled and not registry_paths:
                    raise ValueError("derived_features_scan disabled but no derived_features_registry provided")

                if scan_enabled:
                    spec_data = build_derived_spec_from_root(
                        root=root,
                        registry_paths=registry_paths,
                        include_globs=include_globs,
                        exclude_globs=exclude_globs,
                        feature_ids=feature_ids,
                    )
                else:
                    spec_data = build_derived_spec_from_registry(
                        registry_paths=registry_paths or [],
                        feature_ids=feature_ids,
                        base_dir=root,
                    )

                spec_dir = (context.workspace_dir / "derived_features").resolve()
                spec_dir.mkdir(parents=True, exist_ok=True)
                spec_path = spec_dir / "scanned-derived-features.yaml"
                spec_path.write_text(yaml.safe_dump(spec_data, sort_keys=False), encoding="utf-8")
                self.log(context, f"Generated derived_features spec from scan root: {spec_path}")
            else:
                self.log(context, "No derived_features spec configured; skipping.")
                return {"status": "skipped"}

        output_dir = Path(
            str(self.options.get("output_dir", context.workspace_dir / "derived_features")).format(run_id=context.run_id)
        ).resolve()

        input_dir_raw = self.options.get("input_dir")
        input_dir = None
        if input_dir_raw:
            input_dir = Path(str(input_dir_raw).format(run_id=context.run_id)).expanduser().resolve()

        participants = self.options.get("participants")
        if isinstance(participants, list):
            participant_ids = [str(pid) for pid in participants if str(pid).strip()]
        elif isinstance(participants, str) and participants.strip():
            participant_ids = [pid.strip() for pid in participants.split(",") if pid.strip()]
        else:
            participant_ids = active_participants(context)

        results: Dict[str, object] = {}
        for participant_id in participant_ids:
            self.log(context, f"Running derived_features for {participant_id}")
            results[participant_id] = run_derived_features_for_participant(
                context,
                participant_id,
                spec_path=spec_path,
                output_dir=output_dir,
                input_dir=input_dir,
            )
        return {"status": "ok", "participants": len(participant_ids), "details": results}


__all__ = ["DerivedFeaturesStep"]
