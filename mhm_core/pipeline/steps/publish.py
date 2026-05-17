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
    active_participants,
    step_state_bindings,
)
from ..publishing import (
    ParticipantPublishResult,
    ParticipantPublishTarget,
    PipelinePublisher,
    PublishResult,
)


class PublishStep(PipelineStep):
    def __init__(self, options: Dict[str, object]) -> None:
        super().__init__("publish", options, run_per_participant=True, suspend_checkpoint="participant")

    def run(self, context: RunContext) -> Dict[str, object]:
        outputs = context.spec.outputs
        run_id = context.run_id
        publisher = self._publisher(context)

        upload_stats = {
            "merged": PublishResult(),
            "logs": PublishResult(),
        }
        target_stats: Dict[str, PublishResult] = {}

        self.log(context, "Publishing outputs")

        participants = active_participants(context)

        for participant_id in participants:
            site = context.participant_sites.get(participant_id)
            if not site:
                context.logger.warning("[publish  ] Site unknown for participant %s; skipping", participant_id)
                continue

            merged_metrics_to_publish = getattr(context, "merged_metrics_to_publish", {}).get(participant_id)

            merged_local = context.merged_dir / site / participant_id
            merged_prefix = outputs.merged_prefix.format(run_id=run_id, site=site, participant_id=participant_id).rstrip("/")
            merged_result = publisher.publish_tree(
                context,
                local_root=merged_local,
                destination=merged_prefix,
                collect_uploads=True,
                include_top_level_dirs=merged_metrics_to_publish,
            )
            upload_stats["merged"].merge(merged_result)
            for artifact in merged_result.uploads:
                metric = artifact.file_path.parent.name
                context.pipeline_observer.record_published_merged_artifact(
                    context,
                    site=site,
                    participant_id=participant_id,
                    metric=metric,
                    file_path=artifact.file_path,
                    locator=artifact.locator,
                )

            publish_participant_manifest = context.pipeline_observer.should_publish_participant_manifest(
                context,
                participant_id=participant_id,
                site=site,
                merged_uploads=merged_result.uploads,
                merged_local=merged_local,
            )

            target_results: Dict[str, PublishResult] = {}
            for target in context.pipeline_observer.participant_publish_targets(
                context,
                participant_id=participant_id,
                site=site,
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
                if target.remove_after_publish:
                    self._cleanup_publish_target(context, target=target)

            participant_manifest_published = publisher.publish_participant_manifest(
                context,
                participant_id=participant_id,
                site=site,
                merged_local=merged_local,
                merged_uploads=merged_result.uploads,
                should_publish=publish_participant_manifest,
            )
            self._cleanup_participant_local(context, site, participant_id)

            context.pipeline_observer.after_participant_publish(
                context,
                result=ParticipantPublishResult(
                    participant_id=participant_id,
                    site=site,
                    merged_uploads=merged_result.uploads,
                    target_results=target_results,
                    participant_manifest_published=participant_manifest_published,
                ),
            )

        context.logs_dir.mkdir(parents=True, exist_ok=True)
        metrics_payload = {"run_id": run_id, "started_at": context.start_time.isoformat() + "Z", "metrics": context.metrics}
        metrics_path = context.logs_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics_payload, indent=2), encoding="utf-8")
        logs_prefix = outputs.logs_prefix.format(run_id=run_id, site="", participant_id="").rstrip("/")
        metrics_key = f"{logs_prefix}/metrics.json"
        upload_stats["logs"].merge(publisher.publish_file(context, file_path=metrics_path, destination=metrics_key))

        for artifact in context.pipeline_observer.run_publish_artifacts(context):
            if not artifact.file_path.exists() or not artifact.file_path.is_file():
                continue
            destination_name = artifact.destination_name or artifact.file_path.name
            destination = f"{logs_prefix}/{destination_name}"
            upload_stats["logs"].merge(publisher.publish_file(context, file_path=artifact.file_path, destination=destination))

        archive_opts = self.options.get("archive", {})
        if archive_opts.get("enabled"):
            archive_key = self._archive_merged(context, archive_opts, publisher=publisher)
            if archive_key:
                context.logger.info("[publish  ] Uploaded merged archive to %s", archive_key)

        manifest = {
            "run_id": run_id,
            "started_at": context.start_time.isoformat() + "Z",
            "completed_at": datetime.utcnow().isoformat() + "Z",
            "participants": [
                {"participant_id": pid, "site": site}
                for pid, site in context.participant_sites.items()
            ],
            "metrics": context.metrics,
        }
        manifest_path = context.logs_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        manifest_key = outputs.manifest_key.format(run_id=run_id, site="", participant_id="")
        upload_stats["logs"].merge(publisher.publish_file(context, file_path=manifest_path, destination=manifest_key))

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
            "merged_files": upload_stats["merged"].files,
            "published_target_files": {
                name: result.files
                for name, result in sorted(target_stats.items())
            },
        }
        for name, result in sorted(target_stats.items()):
            metrics[f"{_safe_metric_name(name)}_files"] = result.files
        return metrics

    def describe_produced_states(self, context: RunContext) -> list[PipelineStepStateDescriptor]:
        published_manifest_path = str(getattr(context, "published_dataset_manifest_path", "") or "").strip()
        if not published_manifest_path:
            return []
        return [
            PipelineStepStateDescriptor(
                lineage_key=f"published:{context.run_id}",
                existing_manifest_path=published_manifest_path,
                title=f"Published merged output for run {context.run_id}",
                notes=f"Bound the published run-output manifest after step {self.name}.",
                surface="published_output",
                domain="passive-data",
                stage="merged",
                extra_metadata={
                    "run_id": context.run_id,
                    "step_type": getattr(self, "_step_type", self.name),
                },
            )
        ]

    def describe_operation(self, context: RunContext) -> PipelineStepOperationDescriptor | None:
        published_manifest_path = str(getattr(context, "published_dataset_manifest_path", "") or "").strip()
        if not published_manifest_path:
            return None
        merged_lineages = [
            lineage_key
            for lineage_key in sorted(step_state_bindings(context).keys())
            if lineage_key.startswith("merged:")
        ]
        return PipelineStepOperationDescriptor(
            operation_kind="publish",
            operation_name=self.name,
            title="Publish merged output",
            summary="Published the merged run outputs and attached their run-local provenance.",
            input_lineage_keys=merged_lineages,
            output_lineage_keys=[f"published:{context.run_id}"],
            extra_metadata={
                "run_id": context.run_id,
                "merged_input_count": len(merged_lineages),
            },
        )

    # ------------------------------------------------------------------
    def _publisher(self, context: RunContext) -> PipelinePublisher:
        return context.pipeline_publisher

    # ------------------------------------------------------------------
    def _cleanup_publish_target(self, context: RunContext, *, target: ParticipantPublishTarget) -> None:
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

    # ------------------------------------------------------------------
    def _cleanup_participant_local(self, context: RunContext, site: str, participant_id: str) -> None:
        import shutil

        cfg = context.spec.publishing
        if cfg.remove_local_raw_after_publish:
            prefix = context.spec.source.prefix.strip("/")
            candidates = [
                context.raw_dir / prefix / site / participant_id,
                context.raw_dir / site / participant_id,
            ]
            for candidate in candidates:
                shutil.rmtree(candidate, ignore_errors=True)

        if cfg.remove_local_merged_after_publish:
            for candidate in [
                context.merged_dir / site / participant_id,
                context.merged_dir / context.spec.source.prefix.strip("/") / site / participant_id,
            ]:
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

        archive_prefix = outputs.logs_prefix.format(run_id=run_id, site="", participant_id="").rstrip("/")
        custom_prefix = getattr(outputs, "archive_prefix", None)
        if custom_prefix:
            archive_prefix = custom_prefix.format(run_id=run_id, site="", participant_id="").rstrip("/")
        archive_key = f"{archive_prefix}/{filename}"
        upload_result = publisher.publish_file(context, file_path=archive_path, destination=archive_key)
        return archive_key if upload_result.files else None


def _safe_metric_name(name: str) -> str:
    value = "".join(char if char.isalnum() else "_" for char in str(name).strip().lower())
    return value.strip("_") or "target"


__all__ = ["PublishStep"]
