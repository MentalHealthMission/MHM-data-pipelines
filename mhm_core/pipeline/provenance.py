"""Pipeline-run provenance helpers."""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Dict, List, Optional

from connect_summary.provenance.manifests import (
    append_history_event,
    build_dataset_snapshot,
    build_dataset_snapshot_from_inventory,
    build_parent_document_refs,
    document_reference_from_path,
    write_dataset_manifest_bundle,
)
from connect_summary.provenance.dataset_refresh import advance_current_dataset_state_from_inventory_overlay
from connect_summary.provenance.hashing import add_document_hash
from connect_summary.provenance.model import LogicalAddress, PROVENANCE_SCHEMA_VERSION, file_mtime_iso
from connect_summary.provenance.operations import build_operation_event, write_operation_event
from connect_summary.provenance.source import artifacts_and_coverage, snapshot_source_state
from connect_summary.provenance.specs import snapshot_pipeline_spec
from connect_summary.path_utils import normalize_user_path

from .steps.base import PipelineStep, PipelineStepOperationDescriptor, PipelineStepStateDescriptor


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
        source_scope_prefixes = _source_state_scope_prefixes(context)
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
            include_relative_prefixes=source_scope_prefixes,
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


def capture_declared_step_states(
    context,
    *,
    step: PipelineStep,
    step_index: int,
    metrics: Optional[Dict[str, object]] = None,
) -> List[Path]:
    if not getattr(context.spec.provenance, "enabled", True):
        return []
    provenance_dir = getattr(context, "provenance_dir", None)
    if provenance_dir is None:
        return []

    captured: List[Path] = []
    for descriptor in step.describe_produced_states(context):
        existing_manifest_path = str(descriptor.existing_manifest_path or "").strip()
        if existing_manifest_path:
            manifest_path = normalize_user_path(Path(existing_manifest_path).expanduser())
            if not manifest_path.exists() or not manifest_path.is_file():
                continue
            context.step_state_bindings[descriptor.lineage_key] = str(manifest_path)
            captured.append(manifest_path)
            continue

        data_root_value = descriptor.data_root
        if data_root_value is None:
            continue
        data_root = normalize_user_path(Path(data_root_value).expanduser())
        if not data_root.exists() or not data_root.is_dir():
            continue

        parent_manifest = _resolve_step_state_parent_manifest(context, descriptor)
        snapshot = build_dataset_snapshot(
            data_root=data_root,
            dataset_kind=descriptor.dataset_kind,
            title=descriptor.title or f"{step.name} state",
            source_dataset_manifest=parent_manifest,
            source_state_manifest=str(context.source_state_manifest_path or ""),
            notes=descriptor.notes,
            surface=descriptor.surface,
            domain=descriptor.domain,
            stage=descriptor.stage,
            layout=descriptor.layout,
            slice_name=descriptor.slice_name,
            fingerprint_mode=descriptor.fingerprint_mode,
            logical_root_overrides=dict(descriptor.logical_root_overrides or {}),
            extra_metadata={
                "run_id": context.run_id,
                "step_name": step.name,
                "step_index": step_index,
                "lineage_key": descriptor.lineage_key,
                **dict(descriptor.extra_metadata or {}),
            },
        )
        snapshot["control_documents"] = _merge_step_control_documents(
            snapshot.get("control_documents", []),
            _build_step_control_documents(context, descriptor),
        )

        manifest_root = _step_state_manifest_root(
            provenance_dir=provenance_dir,
            lineage_key=descriptor.lineage_key,
            step_index=step_index,
            step_name=step.name,
        )
        paths = write_dataset_manifest_bundle(
            manifest_root=manifest_root,
            snapshot=snapshot,
            history_event={
                "event_type": descriptor.history_event_type,
                "generated_at": snapshot["generated_at"],
                "dataset_id": snapshot["dataset_id"],
                "dataset_kind": snapshot["dataset_kind"],
                "data_root": snapshot["data_root"],
                "logical_root": snapshot["logical_root"],
                "run_id": context.run_id,
                "step_name": step.name,
                "step_index": step_index,
                "lineage_key": descriptor.lineage_key,
                "participant_id": getattr(context, "current_participant", "") or "",
                "metrics": metrics or {},
            },
        )
        context.step_state_bindings[descriptor.lineage_key] = str(paths["dataset_manifest"])
        captured.append(paths["dataset_manifest"])
    return captured


def capture_declared_step_operations(
    context,
    *,
    step: PipelineStep,
    step_index: int,
    metrics: Optional[Dict[str, object]] = None,
    pre_step_state_bindings: Optional[Dict[str, str]] = None,
) -> List[Path]:
    if not getattr(context.spec.provenance, "enabled", True):
        return []
    provenance_dir = getattr(context, "provenance_dir", None)
    if provenance_dir is None:
        return []

    state_descriptors = step.describe_produced_states(context)
    descriptor = step.describe_operation(context) or _infer_step_operation_descriptor(
        context=context,
        step=step,
        state_descriptors=state_descriptors,
    )
    if descriptor is None:
        return []

    pre_bindings = dict(pre_step_state_bindings or {})
    output_refs = _operation_output_state_refs(
        descriptor=descriptor,
        state_descriptors=state_descriptors,
        current_bindings=getattr(context, "step_state_bindings", {}),
    )
    if not output_refs:
        return []

    input_refs = _operation_input_state_refs(
        descriptor=descriptor,
        state_descriptors=state_descriptors,
        pre_step_bindings=pre_bindings,
    )
    input_knowledge_refs = _operation_document_refs(
        descriptor.input_knowledge_documents,
        relation="uses",
    )
    output_knowledge_refs = _operation_document_refs(
        descriptor.output_knowledge_documents,
        relation="produces",
    )
    control_documents = _merge_step_control_documents(
        _build_step_control_documents(context, _state_descriptor_control_union(state_descriptors)),
        _operation_document_refs(descriptor.additional_control_documents, relation="supports_operation"),
    )

    site = ""
    participant_id = str(getattr(context, "current_participant", "") or "").strip()
    if participant_id:
        site = str(getattr(context, "participant_sites", {}).get(participant_id, "")).strip()

    operation_payload = build_operation_event(
        operation_kind=str(descriptor.operation_kind or "transform"),
        operation_name=str(descriptor.operation_name or step.name),
        title=str(descriptor.title or step.name.replace("_", " ").title()),
        summary=str(descriptor.summary or ""),
        input_state_refs=input_refs,
        input_knowledge_refs=input_knowledge_refs,
        output_state_refs=output_refs,
        output_knowledge_refs=output_knowledge_refs,
        control_documents=control_documents,
        parameters=dict(descriptor.parameters or {}),
        execution_context={
            "run_id": context.run_id,
            "step_name": step.name,
            "step_index": step_index,
            "participant_id": participant_id,
            "site": site,
        },
        metrics=metrics or {},
        extra_metadata={
            "run_id": context.run_id,
            "step_name": step.name,
            "step_index": step_index,
            "participant_id": participant_id,
            "site": site,
            **dict(descriptor.extra_metadata or {}),
        },
    )

    written: List[Path] = []
    seen_roots: set[Path] = set()
    for ref in output_refs:
        locator = str(ref.get("locator", "")).strip()
        if not locator:
            continue
        manifest_path = normalize_user_path(Path(locator).expanduser())
        manifest_root = manifest_path.parent
        if manifest_root in seen_roots:
            continue
        seen_roots.add(manifest_root)
        operation_path = manifest_root / "operation_event.json"
        write_operation_event(operation_path, operation_payload)
        written.append(operation_path)
    return written


def record_published_merged_artifact(
    context,
    *,
    site: str,
    participant_id: str,
    metric: str,
    file_path: Path,
    s3_uri: str,
) -> None:
    relative_locator = "/".join([site, participant_id, metric, file_path.name])
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
                "relative_locator": relative_locator,
            },
            {
                "binding_type": "s3_object",
                "locator": s3_uri,
                "relative_locator": relative_locator,
            },
        ],
    }
    from connect_summary.provenance.manifests import build_artifact_hash

    record["artifact_hash"] = build_artifact_hash(record)
    context.published_merged_artifacts.append(record)


def _resolve_step_state_parent_manifest(context, descriptor: PipelineStepStateDescriptor) -> str:
    current_binding = str(context.step_state_bindings.get(descriptor.lineage_key, "")).strip()
    if current_binding:
        return current_binding
    parent_lineage = str(descriptor.parent_lineage_key or "").strip()
    if parent_lineage:
        parent_binding = str(context.step_state_bindings.get(parent_lineage, "")).strip()
        if parent_binding:
            return parent_binding
    return str(descriptor.default_parent_manifest or "").strip()


def _infer_step_operation_descriptor(
    *,
    context,
    step: PipelineStep,
    state_descriptors: List[PipelineStepStateDescriptor],
) -> PipelineStepOperationDescriptor | None:
    if not state_descriptors:
        return None
    return PipelineStepOperationDescriptor(
        operation_kind="transform",
        operation_name=step.name,
        title=step.name.replace("_", " ").title(),
        summary="Recorded generic pipeline operation that produced the linked state.",
        output_lineage_keys=[descriptor.lineage_key for descriptor in state_descriptors],
    )


def _state_descriptor_control_union(
    descriptors: List[PipelineStepStateDescriptor],
) -> PipelineStepStateDescriptor:
    merged = PipelineStepStateDescriptor(lineage_key="")
    seen: set[tuple[str, str]] = set()
    for descriptor in descriptors:
        for locator, role in list(descriptor.additional_control_documents or []):
            key = (str(locator), str(role))
            if key in seen:
                continue
            seen.add(key)
            merged.additional_control_documents.append((str(locator), str(role)))
    return merged


def _operation_input_state_refs(
    *,
    descriptor: PipelineStepOperationDescriptor,
    state_descriptors: List[PipelineStepStateDescriptor],
    pre_step_bindings: Dict[str, str],
) -> List[Dict[str, str]]:
    refs: List[Dict[str, str]] = []
    seen: set[str] = set()

    for lineage_key in descriptor.input_lineage_keys:
        locator = str(pre_step_bindings.get(lineage_key, "")).strip()
        _append_operation_state_ref(refs, seen, locator, role="input_state_manifest", relation="uses")

    for locator in descriptor.input_state_manifests:
        _append_operation_state_ref(
            refs,
            seen,
            str(locator),
            role="input_state_manifest",
            relation="uses",
        )

    if refs:
        return refs

    for state_descriptor in state_descriptors:
        locator = str(pre_step_bindings.get(state_descriptor.lineage_key, "")).strip()
        if not locator and state_descriptor.parent_lineage_key:
            locator = str(pre_step_bindings.get(state_descriptor.parent_lineage_key, "")).strip()
        if not locator:
            locator = str(state_descriptor.default_parent_manifest or "").strip()
        _append_operation_state_ref(refs, seen, locator, role="input_state_manifest", relation="uses")
    return refs


def _operation_output_state_refs(
    *,
    descriptor: PipelineStepOperationDescriptor,
    state_descriptors: List[PipelineStepStateDescriptor],
    current_bindings: Dict[str, str],
) -> List[Dict[str, str]]:
    refs: List[Dict[str, str]] = []
    seen: set[str] = set()

    lineage_keys = list(descriptor.output_lineage_keys or [])
    if not lineage_keys:
        lineage_keys = [state_descriptor.lineage_key for state_descriptor in state_descriptors]

    for lineage_key in lineage_keys:
        locator = str(current_bindings.get(lineage_key, "")).strip()
        _append_operation_state_ref(refs, seen, locator, role="output_state_manifest", relation="produces")
    return refs


def _operation_document_refs(
    entries: List[tuple[str, str]],
    *,
    relation: str,
) -> List[Dict[str, str]]:
    refs: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for locator, role in entries:
        key = (str(locator), str(role))
        if key in seen:
            continue
        seen.add(key)
        ref = document_reference_from_path(str(locator), role=str(role), relation=relation)
        if ref is not None:
            refs.append(ref.to_dict())
    return refs


def _append_operation_state_ref(
    refs: List[Dict[str, str]],
    seen: set[str],
    locator: str,
    *,
    role: str,
    relation: str,
) -> None:
    path = str(locator or "").strip()
    if not path or path in seen:
        return
    ref = document_reference_from_path(path, role=role, relation=relation)
    if ref is None:
        return
    seen.add(path)
    refs.append(ref.to_dict())


def _source_state_scope_prefixes(context) -> list[str]:
    participants = list(getattr(context.spec.source, "participants", []) or [])
    participant_sites = dict(getattr(context, "participant_sites", {}) or {})
    scoped_participants: list[str] = []
    for participant_id in participants:
        site = str(participant_sites.get(participant_id, "")).strip()
        if not site:
            continue
        scoped_participants.append(f"{site}/{participant_id}")
    if scoped_participants:
        return sorted(dict.fromkeys(scoped_participants))
    sites = [str(site).strip() for site in getattr(context.spec.source, "sites", []) if str(site).strip()]
    if sites:
        return sorted(dict.fromkeys(sites))
    return []


def _build_step_control_documents(
    context,
    descriptor: PipelineStepStateDescriptor,
) -> List[Dict[str, str]]:
    refs: List[Dict[str, str]] = []
    for locator, role in [
        (str(context.pipeline_spec_manifest_path) if context.pipeline_spec_manifest_path else "", "pipeline_spec_manifest"),
        (str(context.source_state_manifest_path or ""), "source_state_manifest"),
        *list(descriptor.additional_control_documents or []),
    ]:
        ref = document_reference_from_path(locator, role=role)
        if ref is not None:
            refs.append(ref.to_dict())
    return refs


def _merge_step_control_documents(
    existing: object,
    additional: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    merged: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for entry in list(existing or []) + additional:
        if not isinstance(entry, dict):
            continue
        locator = str(entry.get("locator", "")).strip()
        role = str(entry.get("role", "")).strip()
        key = (locator, role)
        if not locator or key in seen:
            continue
        seen.add(key)
        merged.append(entry)
    return merged


def _step_state_manifest_root(
    *,
    provenance_dir: Path,
    lineage_key: str,
    step_index: int,
    step_name: str,
) -> Path:
    safe_lineage = _safe_segment(lineage_key)
    safe_step = _safe_segment(step_name)
    return provenance_dir / "step_states" / safe_lineage / f"{step_index:02d}-{safe_step}"


def _safe_segment(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9._-]+", "-", text)
    return text.strip("-") or "state"


def _pipeline_step_state_parent_refs(context) -> List[Dict[str, str]]:
    refs: List[Dict[str, str]] = []
    seen: set[str] = set()
    for lineage_key in sorted(getattr(context, "step_state_bindings", {}).keys()):
        if not lineage_key.startswith("merged:"):
            continue
        locator = str(context.step_state_bindings.get(lineage_key, "")).strip()
        if not locator or locator in seen:
            continue
        ref = document_reference_from_path(
            locator,
            role="pipeline_step_state_manifest",
            relation="derived_from",
        )
        if ref is None:
            continue
        seen.add(locator)
        refs.append(ref.to_dict())
    return refs


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
    parents.extend(_pipeline_step_state_parent_refs(context))
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
        source_state_documents=_source_state_documents(context),
        source_state_as_of=_source_state_as_of(context),
        built_at=str(_load_json_document(run_manifest_path).get("generated_at", "")) or "",
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
    _write_canonical_dataset_update_event(
        context,
        bundle_dir=bundle_dir,
        coverage_summary=coverage_summary,
        run_manifest_path=run_manifest_path,
        metrics_path=metrics_path,
    )
    context.published_dataset_manifest_path = str(bundle_dir / "dataset_manifest.json")
    return bundle_dir


def _write_canonical_dataset_update_event(
    context,
    *,
    bundle_dir: Path,
    coverage_summary: Dict[str, object],
    run_manifest_path: Path,
    metrics_path: Path,
) -> Optional[Path]:
    parent_manifest_locator = str(context.spec.provenance.parent_dataset_manifest or "").strip()
    if not parent_manifest_locator:
        return None

    target_ref = document_reference_from_path(
        parent_manifest_locator,
        role="target_dataset_manifest",
        relation="updates",
    )
    if target_ref is None:
        return None

    published_dataset_manifest_path = bundle_dir / "dataset_manifest.json"
    published_ref = document_reference_from_path(
        str(published_dataset_manifest_path),
        role="published_output_dataset_manifest",
        relation="delta_for",
    )
    if published_ref is None:
        return None
    published_payload = _load_json_document(published_dataset_manifest_path)
    target_payload = _load_json_document(Path(parent_manifest_locator).expanduser())
    target_logical_root = dict(target_payload.get("logical_root", {})) or {
        "dataset_id": target_ref.dataset_id,
    }

    control_documents: List[Dict[str, str]] = []
    for locator, role in [
        (str(context.pipeline_spec_manifest_path) if context.pipeline_spec_manifest_path else "", "pipeline_spec_manifest"),
        (str(run_manifest_path), "run_manifest"),
        (str(metrics_path), "run_metrics"),
        (str(context.source_state_manifest_path or ""), "source_state_manifest"),
    ]:
        ref = document_reference_from_path(locator, role=role)
        if ref is not None:
            control_documents.append(ref.to_dict())

    event_payload = add_document_hash(
        {
            "manifest_type": "canonical_dataset_update_event",
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "event_type": "canonical_dataset_update",
            "generated_at": str(published_payload.get("generated_at", "")),
            "run_id": context.run_id,
            "update_strategy": "append_only_history",
            "target_dataset": target_ref.to_dict(),
            "published_output_dataset": published_ref.to_dict(),
            "logical_root": target_logical_root,
            "update_scope": coverage_summary,
            "control_documents": control_documents,
            "extra_metadata": {
                "apply_parent_history_update": bool(context.spec.provenance.apply_parent_history_update),
                "merged_base_prefix": context.merged_base_prefix,
            },
        }
    )
    event_path = bundle_dir.parent / "canonical_dataset_update_event.json"
    event_path.write_text(json.dumps(event_payload, indent=2, sort_keys=True), encoding="utf-8")

    if context.spec.provenance.apply_parent_history_update:
        _append_parent_dataset_history_event(
            parent_manifest_locator=parent_manifest_locator,
            event_path=event_path,
            run_id=context.run_id,
            target_ref=target_ref.to_dict(),
            published_ref=published_ref.to_dict(),
            coverage_summary=coverage_summary,
        )
        _advance_parent_dataset_current_state(
            context,
            parent_manifest_locator=parent_manifest_locator,
            published_dataset_manifest_path=published_dataset_manifest_path,
            event_path=event_path,
            run_manifest_path=run_manifest_path,
            metrics_path=metrics_path,
        )
    return event_path


def _append_parent_dataset_history_event(
    *,
    parent_manifest_locator: str,
    event_path: Path,
    run_id: str,
    target_ref: Dict[str, str],
    published_ref: Dict[str, str],
    coverage_summary: Dict[str, object],
) -> Optional[Path]:
    parent_manifest_path = Path(parent_manifest_locator).expanduser()
    if not parent_manifest_path.exists() or not parent_manifest_path.is_file():
        return None

    update_event_ref = document_reference_from_path(
        str(event_path),
        role="canonical_dataset_update_event",
        relation="applied_to",
    )
    if update_event_ref is None:
        return None

    history_log_path = parent_manifest_path.resolve().parent / "history.jsonl"
    append_history_event(
        history_log_path,
        {
            "event_type": "apply_canonical_dataset_update",
            "generated_at": str(_load_json_document(event_path).get("generated_at", "")),
            "run_id": run_id,
            "target_dataset": target_ref,
            "published_output_dataset": published_ref,
            "update_event": update_event_ref.to_dict(),
            "update_scope": coverage_summary,
        },
    )
    return history_log_path


def _advance_parent_dataset_current_state(
    context,
    *,
    parent_manifest_locator: str,
    published_dataset_manifest_path: Path,
    event_path: Path,
    run_manifest_path: Path,
    metrics_path: Path,
) -> Optional[Path]:
    logger = getattr(context, "logger", None)
    parent_manifest_path = Path(parent_manifest_locator).expanduser()
    if not parent_manifest_path.exists() or not parent_manifest_path.is_file():
        if logger is not None:
            logger.info("[provenance] Parent dataset manifest is not a local file; skipping canonical state advance")
        return None

    published_inventory_path = published_dataset_manifest_path.parent / "artifact_inventory.jsonl"
    if not published_inventory_path.exists():
        if logger is not None:
            logger.info("[provenance] Published artifact inventory missing; skipping canonical state advance")
        return None

    try:
        result = advance_current_dataset_state_from_inventory_overlay(
            dataset_manifest_path=parent_manifest_path,
            overlay_artifact_inventory_path=published_inventory_path,
            title=f"Advance canonical dataset for {context.run_id}",
            summary=f"Advanced the canonical dataset state from published run output {context.run_id}.",
            transition_kind="canonical_dataset_update",
            supporting_documents={
                "published_output_artifact_inventory": published_inventory_path,
                "canonical_dataset_update_event": event_path,
                "run_manifest": run_manifest_path,
                "run_metrics": metrics_path,
            },
            additional_control_documents=[
                (str(event_path), "canonical_dataset_update_event"),
                (str(published_dataset_manifest_path), "published_output_dataset_manifest"),
                (str(run_manifest_path), "run_manifest"),
                (str(metrics_path), "run_metrics"),
                (str(context.pipeline_spec_manifest_path), "pipeline_spec_manifest"),
                (str(context.source_state_manifest_path), "source_state_manifest"),
            ],
        )
    except Exception as exc:
        if logger is not None:
            logger.warning("[provenance] Failed to advance parent canonical dataset state: %s", exc)
        return None
    if logger is not None:
        logger.info("[provenance] Advanced parent canonical dataset state at %s", result["event_root"])
    return Path(result["event_root"])


def _load_json_document(path: Path) -> Dict[str, object]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _source_state_documents(context) -> List[Dict[str, str]]:
    locator = str(context.source_state_manifest_path or "").strip()
    if not locator:
        return []
    ref = document_reference_from_path(locator, role="source_state_manifest", relation="observed_from")
    return [ref.to_dict()] if ref is not None else []


def _source_state_as_of(context) -> str:
    locator = str(context.source_state_manifest_path or "").strip()
    if not locator:
        return ""
    payload = _load_json_document(Path(locator).expanduser())
    if payload.get("state_coordinates", {}).get("source_state_as_of"):
        return str(payload["state_coordinates"]["source_state_as_of"])
    return str(payload.get("generated_at", ""))
