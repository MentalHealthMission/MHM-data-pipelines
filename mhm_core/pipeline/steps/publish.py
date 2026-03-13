"""Publish merged results, summaries, and manifest to S3."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import json
import tarfile

from botocore.exceptions import ClientError

from .base import PipelineStep
from ..context import RunContext, active_participants, ensure_participant_manifest, ensure_summary_manifest
from ..manifest import (
    MetricWatermark,
    ParticipantManifest,
    save_participant_manifest,
    write_local_manifest,
)
from ..summary_manifest import save_summary_manifest, write_local_summary_manifest


@dataclass
class UploadResult:
    files: int = 0
    bytes: int = 0


class PublishStep(PipelineStep):
    def __init__(self, options: Dict[str, object]) -> None:
        super().__init__("publish", options, run_per_participant=False)

    def run(self, context: RunContext) -> Dict[str, object]:
        outputs = context.spec.outputs
        run_id = context.run_id

        upload_stats = {"merged": UploadResult(), "summary": UploadResult(), "logs": UploadResult()}

        self.log(context, "Uploading outputs to S3")

        participants = active_participants(context)

        for participant_id in participants:
            site = context.participant_sites.get(participant_id)
            if not site:
                context.logger.warning("[publish  ] Site unknown for participant %s; skipping", participant_id)
                continue

            manifest = ensure_participant_manifest(context, participant_id)

            summary_state = context.summary_outputs.get(participant_id)

            merged_local = context.merged_dir / site / participant_id
            merged_prefix = outputs.merged_prefix.format(run_id=run_id, site=site, participant_id=participant_id).rstrip("/")
            upload_stats["merged"], _ = self._upload_tree(context, merged_local, merged_prefix, upload_stats["merged"])

            summary_local = context.summary_dir
            summary_prefix = outputs.summary_prefix.format(run_id=run_id, site=site, participant_id=participant_id).rstrip("/")
            upload_stats["summary"], summary_keys = self._upload_tree(
                context,
                summary_local,
                summary_prefix,
                upload_stats["summary"],
                filter_prefix=f"{participant_id}_",
                collect_keys=True,
            )

            self._refresh_manifest_metrics(manifest, merged_local)
            self._cleanup_participant_local(context, site, participant_id)

            local_manifest_dir = context.logs_dir / "participant_manifests"
            local_manifest_path = local_manifest_dir / f"{participant_id}.json"
            write_local_manifest(manifest, local_manifest_path, run_id)
            save_participant_manifest(
                context.s3_client,
                manifest,
                run_id=run_id,
                base_prefix=context.merged_base_prefix,
            )
            context.logger.info(
                "[publish  ] Updated manifest for %s/%s at %s",
                site,
                participant_id,
                context.merged_base_prefix,
            )

            if summary_state and context.summary_manifest_prefix:
                summary_manifest = ensure_summary_manifest(context, participant_id)
                if summary_keys:
                    summary_manifest.summary_files = summary_keys
                summary_manifest.source_watermarks = summary_state.source_watermarks
                local_summary_manifest_dir = context.logs_dir / "summary_manifests"
                local_summary_manifest_path = local_summary_manifest_dir / f"{participant_id}.json"
                write_local_summary_manifest(summary_manifest, local_summary_manifest_path, run_id)
                save_summary_manifest(
                    context.s3_client,
                    summary_manifest,
                    run_id=run_id,
                    manifest_prefix=context.summary_manifest_prefix,
                )
                context.logger.info(
                    "[publish  ] Updated summary manifest for %s/%s at %s",
                    site,
                    participant_id,
                    context.summary_manifest_prefix,
                )

        context.logs_dir.mkdir(parents=True, exist_ok=True)
        metrics_payload = {"run_id": run_id, "started_at": context.start_time.isoformat() + "Z", "metrics": context.metrics}
        metrics_path = context.logs_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics_payload, indent=2), encoding="utf-8")
        logs_prefix = outputs.logs_prefix.format(run_id=run_id, site="", participant_id="").rstrip("/")
        metrics_key = f"{logs_prefix}/metrics.json"
        upload_stats["logs"] = self._upload_file(context, metrics_path, metrics_key, upload_stats["logs"])

        rapids_manifest_path = context.logs_dir / "rapids_manifest.json"
        if rapids_manifest_path.exists():
            rapids_key = f"{logs_prefix}/rapids_manifest.json"
            upload_stats["logs"] = self._upload_file(context, rapids_manifest_path, rapids_key, upload_stats["logs"])

        archive_opts = self.options.get("archive", {})
        if archive_opts.get("enabled"):
            archive_key = self._archive_merged(context, archive_opts)
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
        self._upload_file(context, manifest_path, manifest_key, upload_stats["logs"])

        if context.spec.publishing.delete_local_workspace:
            self._cleanup_local(context)

        return {
            "status": "ok",
            "merged_files": upload_stats["merged"].files,
            "summary_files": upload_stats["summary"].files,
        }

    # ------------------------------------------------------------------
    def _upload_tree(
        self,
        context: RunContext,
        root: Path,
        prefix: str,
        result: UploadResult,
        *,
        filter_prefix: str = "",
        collect_keys: bool = False,
    ) -> tuple[UploadResult, List[str]]:
        s3 = context.s3_client
        keys: List[str] = []
        if not root.exists():
            return result, keys
        for file_path in root.rglob("*"):
            if file_path.is_dir():
                continue
            if filter_prefix and not file_path.name.startswith(filter_prefix):
                continue
            rel = file_path.relative_to(root)
            key = f"{prefix}/{rel.as_posix()}"
            result = self._upload_file(context, file_path, key, result)
            if collect_keys:
                if not key.startswith("s3://"):
                    key = f"s3://{key}"
                keys.append(key)
        return result, keys

    # ------------------------------------------------------------------
    def _refresh_manifest_metrics(self, manifest: ParticipantManifest, merged_root: Path) -> None:
        if not merged_root.exists():
            return

        for metric_dir in merged_root.iterdir():
            if not metric_dir.is_dir():
                continue
            metric = metric_dir.name
            files = list(metric_dir.glob("*.csv.gz"))
            if not files:
                continue
            latest_file = max(files, key=lambda p: p.name)
            existing = manifest.metrics.get(metric, MetricWatermark())
            existing.bytes_merged = latest_file.stat().st_size
            existing.files_merged = sum(1 for _ in metric_dir.glob("*.csv.gz"))
            manifest.metrics[metric] = existing

    # ------------------------------------------------------------------
    def _upload_file(self, context: RunContext, path: Path, key: str, result: UploadResult) -> UploadResult:
        s3 = context.s3_client
        if not key.startswith("s3://"):
            raise ValueError(f"S3 key must be an s3:// URI, got {key}")
        _, remainder = key.split("s3://", 1)
        bucket, _, s3_key = remainder.partition("/")
        try:
            s3.upload_file(str(path), bucket, s3_key)
            result.files += 1
            result.bytes += path.stat().st_size
        except ClientError as exc:
            context.logger.error("[publish  ] Failed uploading %s -> s3://%s/%s: %s", path, bucket, s3_key, exc)
        return result

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

        if cfg.remove_local_summary_after_publish:
            for file_path in context.summary_dir.glob(f"{participant_id}_*.json"):
                try:
                    file_path.unlink()
                except OSError as exc:  # pragma: no cover
                    context.logger.debug("[publish  ] Failed removing %s: %s", file_path, exc)

    # ------------------------------------------------------------------
    def _cleanup_local(self, context: RunContext) -> None:
        import shutil

        try:
            shutil.rmtree(context.workspace_dir, ignore_errors=True)
        except Exception as exc:  # pragma: no cover
            context.logger.warning("[publish  ] Failed cleaning workspace %s: %s", context.workspace_dir, exc)

    # ------------------------------------------------------------------
    def _archive_merged(self, context: RunContext, archive_opts: Dict[str, object]) -> Optional[str]:
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
        upload_result = UploadResult()
        upload_result = self._upload_file(context, archive_path, archive_key, upload_result)
        return archive_key if upload_result.files else None


__all__ = ["PublishStep"]
