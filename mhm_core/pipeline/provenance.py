"""Pipeline-run provenance helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from connect_summary.provenance.manifests import (
    build_dataset_snapshot_from_inventory,
    build_parent_document_refs,
    document_reference_from_path,
    write_dataset_manifest_bundle,
)
from connect_summary.provenance.model import LogicalAddress, file_mtime_iso
from connect_summary.provenance.source import artifacts_and_coverage, snapshot_source_state
from connect_summary.provenance.specs import snapshot_pipeline_spec


def initialize_run_provenance(context) -> None:
    if not getattr(context.spec.provenance, "enabled", True):
        return
    provenance_dir = context.provenance_dir
    if provenance_dir is None:
        return
    provenance_dir.mkdir(parents=True, exist_ok=True)

    source_state_manifest = context.spec.source.source_state_manifest or ""
    if getattr(context.spec.provenance, "snapshot_source_state", False):
        source_state_dir = provenance_dir / "source_state"
        source_uri = f"s3://{context.spec.source.bucket}/{context.spec.source.prefix.strip('/')}"
        result = snapshot_source_state(
            source=source_uri,
            manifest_root=source_state_dir,
            source_id=f"{context.run_id}-source-state",
            title=f"Observed source state for {context.run_id}",
            surface="source",
            domain="passive-data",
            stage="captured",
            layout="raw_source_v1",
            notes=f"Captured for pipeline run {context.run_id}",
        )
        context.source_state_manifest_path = str(result["paths"]["dataset_manifest"])
        source_state_manifest = context.source_state_manifest_path
    elif source_state_manifest:
        context.source_state_manifest_path = source_state_manifest

    if context.spec_locator:
        spec_manifest_path = provenance_dir / "pipeline_spec_manifest.json"
        snapshot_pipeline_spec(
            spec_path=context.spec_locator,
            output_path=spec_manifest_path,
            source_state_manifest=source_state_manifest,
            parent_dataset_manifest=context.spec.provenance.parent_dataset_manifest,
        )
        context.pipeline_spec_manifest_path = spec_manifest_path


def record_published_merged_artifact(
    context,
    *,
    site: str,
    participant_id: str,
    metric: str,
    file_path: Path,
    s3_uri: str,
) -> None:
    address = LogicalAddress(
        surface="run-output",
        domain="passive-data",
        stage="merged",
        dataset_id=context.run_id,
        site=site,
        participant_id=participant_id,
        stream=metric,
        artifact=file_path.name,
    )
    observed = {
        "fingerprint_mode": "metadata",
        "size_bytes": file_path.stat().st_size,
        "modified_at": file_mtime_iso(file_path),
    }
    record = {
        "artifact_type": "published_file",
        "logical_address": address.to_dict(),
        "logical_address_display": address.display(),
        "observed": observed,
        "physical_bindings": [
            {
                "binding_type": "posix_path",
                "locator": str(file_path),
            },
            {
                "binding_type": "s3_object",
                "locator": s3_uri,
            },
        ],
    }
    from connect_summary.provenance.manifests import build_artifact_hash

    record["artifact_hash"] = build_artifact_hash(record)
    context.published_merged_artifacts.append(record)


def finalize_run_provenance(
    context,
    *,
    run_manifest_path: Path,
    metrics_path: Path,
) -> Optional[Path]:
    if not getattr(context.spec.provenance, "enabled", True):
        return None
    provenance_dir = context.provenance_dir
    if provenance_dir is None:
        return None
    if not context.published_merged_artifacts:
        return None

    artifacts, coverage_summary = artifacts_and_coverage(context.published_merged_artifacts)
    parents: List[Dict[str, str]] = []
    if context.source_state_manifest_path:
        ref = document_reference_from_path(str(context.source_state_manifest_path), role="source_state_manifest", relation="derived_from")
        if ref is not None:
            parents.append(ref.to_dict())
    if context.spec.provenance.parent_dataset_manifest:
        parents.extend(build_parent_document_refs([context.spec.provenance.parent_dataset_manifest]))
    control_documents: List[Dict[str, str]] = []
    for locator, role in [
        (str(context.pipeline_spec_manifest_path) if context.pipeline_spec_manifest_path else "", "pipeline_spec_manifest"),
        (str(run_manifest_path), "run_manifest"),
        (str(metrics_path), "run_metrics"),
    ]:
        ref = document_reference_from_path(locator, role=role)
        if ref is not None:
            control_documents.append(ref.to_dict())

    snapshot = build_dataset_snapshot_from_inventory(
        data_locator=context.merged_base_prefix,
        dataset_kind="published_run_output",
        logical_root=LogicalAddress(
            surface="run-output",
            domain="passive-data",
            stage="merged",
            dataset_id=context.run_id,
        ).to_dict(),
        layout="site_participant_stream_v1",
        fingerprint_mode="metadata",
        artifacts=artifacts,
        coverage_summary=coverage_summary,
        dataset_id=f"{context.run_id}-published-merged",
        title=f"Published merged output for run {context.run_id}",
        notes=f"Automatically emitted provenance for run {context.run_id}",
        extra_metadata={
            "run_id": context.run_id,
            "merged_base_prefix": context.merged_base_prefix,
        },
        parents=parents,
        control_documents=control_documents,
    )
    bundle_dir = provenance_dir / "published_merged_dataset"
    write_dataset_manifest_bundle(
        manifest_root=bundle_dir,
        snapshot=snapshot,
        history_event={
            "event_type": "publish_run_output_dataset",
            "generated_at": snapshot["generated_at"],
            "dataset_id": snapshot["dataset_id"],
            "dataset_kind": snapshot["dataset_kind"],
            "data_root": snapshot["data_root"],
            "logical_root": snapshot["logical_root"],
            "run_id": context.run_id,
        },
    )
    return bundle_dir
