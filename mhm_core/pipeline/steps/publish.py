"""Publish pipeline outputs through a configured publisher."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import json
import tarfile

from .base import PipelineStep, PipelineStepOperationDescriptor, PipelineStepStateDescriptor
from ..context import (
    RunContext,
    active_entities,
    entity_group,
)
from ..publishing import (
    EntityPublishResult,
    EntityPublishTarget,
    PipelinePublisher,
    PublishResult,
    RunPublishTarget,
)


class PublishStep(PipelineStep):
    def __init__(self, options: Dict[str, object]) -> None:
        super().__init__("publish", options, run_per_participant=True, suspend_checkpoint="participant")

    def run(self, context: RunContext) -> Dict[str, object]:
        outputs = context.spec.outputs
        run_id = context.run_id
        publisher = self._publisher(context)

        target_stats: Dict[str, PublishResult] = {}
        upload_stats = {"logs": PublishResult()}

        self.log(context, "Publishing outputs")

        entity_ids = active_entities(context)

        for entity_id in entity_ids:
            group = entity_group(context, entity_id)
            if not group:
                context.logger.warning("[publish  ] Group unknown for entity %s; skipping", entity_id)
                continue

            target_results: Dict[str, PublishResult] = {}
            primary_uploads = []
            entity_manifest_published = False
            for target in context.pipeline_observer.entity_publish_targets(
                context,
                entity_id=entity_id,
                group=group,
            ):
                target_result = publisher.publish_tree(
                    context,
                    local_root=target.local_root,
                    destination=target.destination,
                    filter_prefix=target.filter_prefix,
                    collect_keys=target.collect_keys,
                    collect_uploads=target.collect_uploads,
                    include_top_level_dirs=target.include_top_level_dirs,
                )
                target_results.setdefault(target.name, PublishResult()).merge(target_result)
                target_stats.setdefault(target.name, PublishResult()).merge(target_result)
                if target.record_published_artifacts:
                    for artifact in target_result.uploads:
                        artifact_kind = artifact.file_path.parent.name
                        context.pipeline_observer.record_published_artifact(
                            context,
                            entity_id=entity_id,
                            group=group,
                            target_name=target.name,
                            artifact_kind=artifact_kind,
                            file_path=artifact.file_path,
                            locator=artifact.locator,
                        )
                if target.primary_output:
                    primary_uploads.extend(target_result.uploads)
                if target.publish_entity_manifest:
                    should_publish = context.pipeline_observer.should_publish_entity_manifest(
                        context,
                        entity_id=entity_id,
                        group=group,
                        target_name=target.name,
                        published_artifacts=target_result.uploads,
                        output_local=target.local_root,
                    )
                    if publisher.publish_entity_manifest(
                        context,
                        entity_id=entity_id,
                        group=group,
                        output_local=target.local_root,
                        published_artifacts=target_result.uploads,
                        should_publish=should_publish,
                        target_name=target.name,
                    ):
                        entity_manifest_published = True
                if target.remove_after_publish:
                    self._cleanup_publish_target(context, target=target)

            self._cleanup_entity_local(context, group, entity_id)

            context.pipeline_observer.after_entity_publish(
                context,
                result=EntityPublishResult(
                    entity_id=entity_id,
                    group=group,
                    primary_uploads=primary_uploads,
                    target_results=target_results,
                    entity_manifest_published=entity_manifest_published,
                ),
            )

        context.logs_dir.mkdir(parents=True, exist_ok=True)
        metrics_payload = {"run_id": run_id, "started_at": context.start_time.isoformat() + "Z", "metrics": context.metrics}
        metrics_path = context.logs_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics_payload, indent=2), encoding="utf-8")
        logs_prefix = _format_run_locator(outputs.logs_prefix, run_id=run_id).rstrip("/")
        upload_stats["logs"].merge(
            self._publish_run_target(
                context,
                publisher=publisher,
                target=RunPublishTarget(
                    name="metrics",
                    file_path=metrics_path,
                    destination=f"{logs_prefix}/metrics.json",
                ),
            )
        )

        for target in context.pipeline_observer.run_publish_targets(context, logs_prefix=logs_prefix):
            upload_stats["logs"].merge(self._publish_run_target(context, publisher=publisher, target=target))

        archive_opts = self.options.get("archive", {})
        if archive_opts.get("enabled"):
            archive_key = self._archive_merged(context, archive_opts, publisher=publisher)
            if archive_key:
                context.logger.info("[publish  ] Uploaded merged archive to %s", archive_key)

        entity_group_map = getattr(context, "entity_groups", None) or getattr(context, "participant_sites", {})
        manifest = {
            "run_id": run_id,
            "started_at": context.start_time.isoformat() + "Z",
            "completed_at": datetime.utcnow().isoformat() + "Z",
            "entity_count": len(entity_group_map),
            "participant_count": len(entity_group_map),
            "entities": [
                {"entity_id": eid, "group": group}
                for eid, group in entity_group_map.items()
            ],
            "participants": [
                {"participant_id": eid, "site": group}
                for eid, group in entity_group_map.items()
            ],
            "metrics": context.metrics,
        }
        manifest_path = context.logs_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        manifest_key = _format_run_locator(outputs.manifest_key, run_id=run_id)
        upload_stats["logs"].merge(
            self._publish_run_target(
                context,
                publisher=publisher,
                target=RunPublishTarget(
                    name="run_manifest",
                    file_path=manifest_path,
                    destination=manifest_key,
                ),
            )
        )

        provenance_bundle_dir = context.pipeline_observer.finalize_run(
            context,
            run_manifest_path=manifest_path,
            metrics_path=metrics_path,
        )
        if provenance_bundle_dir is not None and context.spec.provenance.upload_run_provenance:
            provenance_prefix = f"{logs_prefix}/provenance"
            upload_stats["logs"].merge(
                publisher.publish_tree(
                    context,
                    local_root=provenance_bundle_dir.parent,
                    destination=provenance_prefix,
                )
            )

        if context.spec.publishing.delete_local_workspace:
            self._cleanup_local(context)

        metrics = {
            "status": "ok",
            "published_target_files": {
                name: result.files
                for name, result in sorted(target_stats.items())
            },
        }
        for name, result in sorted(target_stats.items()):
            metrics[f"{_safe_metric_name(name)}_files"] = result.files
        return metrics

    def describe_produced_states(self, context: RunContext) -> list[PipelineStepStateDescriptor]:
        observer = getattr(context, "pipeline_observer", None)
        if observer is None:
            return []
        return observer.publish_produced_state_descriptors(context, step_name=self.name)

    def describe_operation(self, context: RunContext) -> PipelineStepOperationDescriptor | None:
        observer = getattr(context, "pipeline_observer", None)
        if observer is None:
            return None
        return observer.publish_operation_descriptor(context, step_name=self.name)

    # ------------------------------------------------------------------
    def _publisher(self, context: RunContext) -> PipelinePublisher:
        return context.pipeline_publisher

    # ------------------------------------------------------------------
    def _publish_run_target(
        self,
        context: RunContext,
        *,
        publisher: PipelinePublisher,
        target: RunPublishTarget,
    ) -> PublishResult:
        if not target.file_path.exists() or not target.file_path.is_file():
            if target.required:
                raise FileNotFoundError(f"Required run publish target missing: {target.file_path}")
            context.logger.debug("[publish  ] Skipping absent run publish target %s", target.file_path)
            return PublishResult()
        return publisher.publish_file(context, file_path=target.file_path, destination=target.destination)

    # ------------------------------------------------------------------
    def _cleanup_publish_target(self, context: RunContext, *, target: EntityPublishTarget) -> None:
        if not target.local_root.exists():
            return
        for file_path in target.local_root.rglob("*"):
            if not file_path.is_file():
                continue
            if target.include_top_level_dirs is not None:
                relative_parts = file_path.relative_to(target.local_root).parts
                if not relative_parts or relative_parts[0] not in target.include_top_level_dirs:
                    continue
            if target.filter_prefix and not file_path.name.startswith(target.filter_prefix):
                continue
            try:
                file_path.unlink()
            except OSError as exc:  # pragma: no cover
                context.logger.debug("[publish  ] Failed removing %s: %s", file_path, exc)
        for dir_path in sorted((path for path in target.local_root.rglob("*") if path.is_dir()), reverse=True):
            try:
                dir_path.rmdir()
            except OSError:
                pass
        try:
            target.local_root.rmdir()
        except OSError:
            pass

    # ------------------------------------------------------------------
    def _cleanup_entity_local(self, context: RunContext, group: str, entity_id: str) -> None:
        import shutil

        cfg = context.spec.publishing
        if cfg.remove_local_raw_after_publish:
            prefix = context.spec.source.prefix.strip("/")
            candidates = [
                context.raw_dir / prefix / group / entity_id,
                context.raw_dir / group / entity_id,
            ]
            for candidate in candidates:
                shutil.rmtree(candidate, ignore_errors=True)

    # ------------------------------------------------------------------
    def _cleanup_local(self, context: RunContext) -> None:
        import shutil

        try:
            shutil.rmtree(context.workspace_dir, ignore_errors=True)
        except Exception as exc:  # pragma: no cover
            context.logger.warning("[publish  ] Failed cleaning workspace %s: %s", context.workspace_dir, exc)

    # ------------------------------------------------------------------
    def _archive_merged(
        self,
        context: RunContext,
        archive_opts: Dict[str, object],
        *,
        publisher: PipelinePublisher,
    ) -> Optional[str]:
        """Create a tar.gz of merged outputs from this run and upload alongside logs."""
        filename = str(archive_opts.get("filename") or "merged.tar.gz")
        outputs = context.spec.outputs
        run_id = context.run_id

        archive_dir = context.logs_dir / "archives"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / filename

        with tarfile.open(archive_path, "w:gz") as tar:
            for file_path in context.merged_dir.rglob("*"):
                if not file_path.is_file():
                    continue
                rel = file_path.relative_to(context.merged_dir)
                tar.add(file_path, arcname=rel.as_posix())

        archive_prefix = _format_run_locator(outputs.logs_prefix, run_id=run_id).rstrip("/")
        custom_prefix = getattr(outputs, "archive_prefix", None)
        if custom_prefix:
            archive_prefix = _format_run_locator(str(custom_prefix), run_id=run_id).rstrip("/")
        archive_key = f"{archive_prefix}/{filename}"
        upload_result = publisher.publish_file(context, file_path=archive_path, destination=archive_key)
        return archive_key if upload_result.files else None


def _safe_metric_name(name: str) -> str:
    value = "".join(char if char.isalnum() else "_" for char in str(name).strip().lower())
    return value.strip("_") or "target"


def _format_run_locator(template: str, *, run_id: str) -> str:
    return str(template).format(
        run_id=run_id,
        group="",
        entity_id="",
        entity="",
        site="",
        participant_id="",
        participant="",
    )


__all__ = ["PublishStep"]
