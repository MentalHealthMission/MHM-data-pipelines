"""Neutral hello-world profile for the reusable pipeline core."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Dict, Optional, Set, Type

from mhm_core.pipeline.context import RunContext, extension_state
from mhm_core.pipeline.observers import PipelineObserver
from mhm_core.pipeline.plugins.base import PipelineProfilePlugin
from mhm_core.pipeline.publishing import (
    ParticipantPublishResult,
    ParticipantPublishTarget,
    PipelinePublisher,
    PublishedArtifact,
    PublishResult,
)
from mhm_core.pipeline.steps.base import PipelineStep

HELLO_NAMESPACE = "hello_pipeline"
ARTIFACT_TARGET = "artifact"
REPORT_TARGET = "hello_report"


class HelloCollectStep(PipelineStep):
    """Create a tiny per-entity input record."""

    def __init__(self, options: Optional[Dict[str, object]] = None) -> None:
        super().__init__("hello_collect", options, run_per_participant=True, suspend_checkpoint="participant")

    def run(self, context: RunContext) -> Dict[str, object]:
        entity_id = _active_entity_id(context)
        group = _entity_group(context, entity_id)
        label = str(self.options.get("label", "world")).strip() or "world"
        record = {
            "entity_id": entity_id,
            "group": group,
            "label": label,
        }

        record_path = context.raw_dir / "hello_records" / group / entity_id / "record.json"
        record_path.parent.mkdir(parents=True, exist_ok=True)
        record_path.write_text(json.dumps(record, indent=2), encoding="utf-8")

        state = extension_state(context, HELLO_NAMESPACE)
        state.setdefault("records", {})[entity_id] = str(record_path)
        return {"status": "ok", "record_files": 1}


class HelloRenderStep(PipelineStep):
    """Render the tiny input record into a core-managed artifact and report."""

    def __init__(self, options: Optional[Dict[str, object]] = None) -> None:
        super().__init__("hello_render", options, run_per_participant=True, suspend_checkpoint="participant")

    def run(self, context: RunContext) -> Dict[str, object]:
        entity_id = _active_entity_id(context)
        group = _entity_group(context, entity_id)
        state = extension_state(context, HELLO_NAMESPACE)
        record_path = Path(state.get("records", {}).get(entity_id, ""))
        if not record_path.exists():
            raise FileNotFoundError(f"Missing hello record for entity {entity_id}")

        record = json.loads(record_path.read_text(encoding="utf-8"))
        greeting = f"Hello, {record['label']} from {entity_id} in {group}."

        artifact_dir = context.merged_dir / group / entity_id / "greeting_artifacts"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / "greeting.txt"
        artifact_path.write_text(greeting + "\n", encoding="utf-8")

        report_dir = _report_dir(context)
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"{entity_id}_hello_report.json"
        report_path.write_text(
            json.dumps(
                {
                    "entity_id": entity_id,
                    "group": group,
                    "artifact": str(artifact_path),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        state.setdefault("reports", {})[entity_id] = str(report_path)
        return {"status": "ok", "artifact_files": 1, "report_files": 1}


class HelloObserver(PipelineObserver):
    """Expose hello-profile outputs through generic observer hooks."""

    def on_run_start(self, context: RunContext) -> None:
        extension_state(context, HELLO_NAMESPACE).setdefault("run_started", True)

    def record_published_merged_artifact(
        self,
        context: RunContext,
        *,
        site: str,
        participant_id: str,
        metric: str,
        file_path: Path,
        locator: str,
    ) -> None:
        extension_state(context, HELLO_NAMESPACE).setdefault("published_artifacts", []).append(
            {
                "entity_id": participant_id,
                "group": site,
                "artifact_kind": metric,
                "file_path": str(file_path),
                "locator": locator,
            }
        )

    def participant_publish_targets(
        self,
        context: RunContext,
        *,
        participant_id: str,
        site: str,
    ) -> list[ParticipantPublishTarget]:
        targets: list[ParticipantPublishTarget] = []
        artifact_dir = context.merged_dir / site / participant_id
        if artifact_dir.exists():
            targets.append(
                ParticipantPublishTarget(
                    name=ARTIFACT_TARGET,
                    local_root=artifact_dir,
                    destination=f"hello://artifacts/{site}/{participant_id}/",
                    collect_uploads=True,
                    record_published_artifacts=True,
                    publish_entity_manifest=True,
                    primary_output=True,
                )
            )

        report_dir = _report_dir(context)
        if not (report_dir / f"{participant_id}_hello_report.json").exists():
            return targets
        targets.append(
            ParticipantPublishTarget(
                name=REPORT_TARGET,
                local_root=report_dir,
                destination=f"hello://reports/{site}/{participant_id}/",
                filter_prefix=f"{participant_id}_",
                collect_keys=True,
            )
        )
        return targets

    def after_participant_publish(
        self,
        context: RunContext,
        *,
        result: ParticipantPublishResult,
    ) -> None:
        extension_state(context, HELLO_NAMESPACE).setdefault("publish_results", {})[
            result.participant_id
        ] = {
            "report_keys": result.target_keys(REPORT_TARGET),
            "merged_uploads": [artifact.locator for artifact in result.merged_uploads],
            "participant_manifest_published": result.participant_manifest_published,
        }


class HelloLocalPublisher(PipelinePublisher):
    """Local publisher for the hello profile."""

    def publish_tree(
        self,
        context: RunContext,
        *,
        local_root: Path,
        destination: str,
        filter_prefix: str = "",
        collect_keys: bool = False,
        collect_uploads: bool = False,
        include_top_level_dirs: Optional[Set[str]] = None,
    ) -> PublishResult:
        result = PublishResult()
        if not local_root.exists():
            return result
        for file_path in sorted(local_root.rglob("*")):
            if not file_path.is_file():
                continue
            if filter_prefix and not file_path.name.startswith(filter_prefix):
                continue
            rel_path = file_path.relative_to(local_root)
            if include_top_level_dirs is not None:
                if not rel_path.parts or rel_path.parts[0] not in include_top_level_dirs:
                    continue
            locator = f"{destination.rstrip('/')}/{rel_path.as_posix()}"
            _copy_to_locator(context, file_path=file_path, locator=locator)
            result.files += 1
            result.bytes += file_path.stat().st_size
            if collect_keys:
                result.keys.append(locator)
            if collect_uploads:
                result.uploads.append(PublishedArtifact(file_path=file_path, locator=locator))
        return result

    def publish_file(self, context: RunContext, *, file_path: Path, destination: str) -> PublishResult:
        result = PublishResult()
        _copy_to_locator(context, file_path=file_path, locator=destination)
        result.files = 1
        result.bytes = file_path.stat().st_size
        return result

    def publish_participant_manifest(
        self,
        context: RunContext,
        *,
        participant_id: str,
        site: str,
        merged_local: Path,
        merged_uploads: list[PublishedArtifact],
        should_publish: bool,
    ) -> bool:
        if not should_publish:
            return False
        manifest_path = context.logs_dir / "hello_entity_manifests" / f"{participant_id}.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                {
                    "entity_id": participant_id,
                    "group": site,
                    "output_root": str(merged_local),
                    "published_artifacts": [artifact.locator for artifact in merged_uploads],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self.publish_file(
            context,
            file_path=manifest_path,
            destination=f"hello://entity-manifests/{site}/{participant_id}/manifest.json",
        )
        return True


class HelloPipelineProfile(PipelineProfilePlugin):
    profile_id = "hello"

    def register_steps(self, registry: Dict[str, Type[PipelineStep]]) -> None:
        steps: Dict[str, Type[PipelineStep]] = {
            "hello.collect": HelloCollectStep,
            "hello.render": HelloRenderStep,
        }
        registry.update(steps)

    def create_observer(self) -> PipelineObserver:
        return HelloObserver()

    def create_publisher(self) -> PipelinePublisher:
        return HelloLocalPublisher()


def _active_entity_id(context: RunContext) -> str:
    entity_id = str(
        getattr(context, "current_entity", None)
        or getattr(context, "current_participant", "")
        or ""
    ).strip()
    if not entity_id:
        raise RuntimeError("hello steps require an active entity")
    return entity_id


def _entity_group(context: RunContext, entity_id: str) -> str:
    groups = getattr(context, "entity_groups", None) or getattr(context, "participant_sites", {})
    return str(groups.get(entity_id, "default-group")).strip() or "default-group"


def _report_dir(context: RunContext) -> Path:
    return context.workspace_dir / "hello_reports"


def _copy_to_locator(context: RunContext, *, file_path: Path, locator: str) -> Path:
    target_path = _locator_path(context, locator)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(file_path, target_path)
    return target_path


def _locator_path(context: RunContext, locator: str) -> Path:
    _, _, path = locator.partition("://")
    relative = path if path else locator
    parts = [part for part in relative.split("/") if part and part not in {".", ".."}]
    return context.workspace_dir / "published" / Path(*parts)


__all__ = [
    "HELLO_NAMESPACE",
    "ARTIFACT_TARGET",
    "REPORT_TARGET",
    "HelloPipelineProfile",
]
