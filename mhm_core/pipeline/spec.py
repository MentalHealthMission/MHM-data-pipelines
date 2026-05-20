"""Specification parsing and validation for the core pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional
import json
import re
import uuid

import yaml

from .object_store import (
    ObjectStore,
    create_object_store_for_locator,
    locator_needs_object_store,
    object_store_from_client,
    split_s3_uri,
)

DEFAULT_PIPELINE_PROFILE = "base"

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)


@dataclass
class SourceConfig:
    bucket: str
    prefix: str
    entities: List[str]
    discover_all: bool = False
    groups: List[str] = field(default_factory=list)
    source_state_manifest: str = ""
    entity_group_map: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SourceConfig":
        bucket = str(data.get("bucket", "")).strip()
        prefix = str(data.get("prefix", "")).strip().strip("/")
        raw_entities = data.get("entities", data.get("participants", []))
        entities = [str(entity).strip() for entity in raw_entities if str(entity).strip()]
        discover_all = bool(data.get("discover_all", False))
        raw_groups = data.get("groups", data.get("sites", []))
        groups = [str(group).strip() for group in raw_groups if str(group).strip()]
        raw_entity_group_map = data.get("entity_group_map", data.get("site_map", {}))
        entity_group_map = {
            str(entity).strip(): str(group).strip()
            for entity, group in getattr(raw_entity_group_map, "items", lambda: [])()
            if str(entity).strip() and str(group).strip()
        }
        source_state_manifest = str(data.get("source_state_manifest", "")).strip()
        return cls(
            bucket=bucket,
            prefix=prefix,
            entities=entities,
            discover_all=discover_all,
            groups=groups,
            source_state_manifest=source_state_manifest,
            entity_group_map=entity_group_map,
        )

    @property
    def participants(self) -> List[str]:
        return self.entities

    @participants.setter
    def participants(self, value: Iterable[str]) -> None:
        self.entities = [str(entity).strip() for entity in value if str(entity).strip()]

    @property
    def sites(self) -> List[str]:
        return self.groups

    @sites.setter
    def sites(self, value: Iterable[str]) -> None:
        self.groups = [str(group).strip() for group in value if str(group).strip()]

    @property
    def site_map(self) -> Dict[str, str]:
        return self.entity_group_map

    @site_map.setter
    def site_map(self, value: Mapping[str, str]) -> None:
        self.entity_group_map = {
            str(entity).strip(): str(group).strip()
            for entity, group in value.items()
            if str(entity).strip() and str(group).strip()
        }


@dataclass
class FiltersConfig:
    include_metrics: List[str] = field(default_factory=list)
    exclude_metrics: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Optional[Mapping[str, Any]]) -> "FiltersConfig":
        if not data:
            return cls()
        raw_exclude = data.get("exclude_metrics", [])
        raw_include = data.get("include_metrics", [])
        exclude_metrics = sorted({str(item).strip() for item in raw_exclude if str(item).strip()})
        include_metrics = sorted({str(item).strip() for item in raw_include if str(item).strip()})
        return cls(include_metrics=include_metrics, exclude_metrics=exclude_metrics)


@dataclass
class WorkspaceConfig:
    root: Path
    run_subdir: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WorkspaceConfig":
        root = Path(str(data.get("root", "/tmp")))
        run_subdir = str(data.get("run_subdir", "runs/{run_id}"))
        return cls(root=root, run_subdir=run_subdir)

    def resolve_run_path(self, run_id: str) -> Path:
        subdir = self.run_subdir.format(run_id=run_id)
        return (self.root / subdir).resolve()


@dataclass
class BatchingConfig:
    strategy: str = "none"
    max_entities: Optional[int] = None
    resume_completed: bool = False

    @classmethod
    def from_dict(cls, data: Optional[Mapping[str, Any]]) -> "BatchingConfig":
        if not data:
            return cls()
        strategy = str(data.get("strategy", "none")).strip().lower() or "none"
        if strategy == "participant_count":
            strategy = "entity_count"
        max_entities_raw = data.get("max_entities", data.get("max_participants"))
        try:
            max_entities = int(max_entities_raw) if max_entities_raw is not None else None
        except (TypeError, ValueError):
            max_entities = None
        resume_completed = bool(data.get("resume_completed", False))
        return cls(
            strategy=strategy,
            max_entities=max_entities,
            resume_completed=resume_completed,
        )

    @property
    def max_participants(self) -> Optional[int]:
        return self.max_entities

    @max_participants.setter
    def max_participants(self, value: Optional[int]) -> None:
        self.max_entities = value


@dataclass
class ProvenanceConfig:
    enabled: bool = True
    snapshot_source_state: bool = False
    upload_run_provenance: bool = True
    parent_dataset_manifest: str = ""
    apply_parent_history_update: bool = False

    @classmethod
    def from_dict(cls, data: Optional[Mapping[str, Any]]) -> "ProvenanceConfig":
        if not data:
            return cls()
        return cls(
            enabled=bool(data.get("enabled", True)),
            snapshot_source_state=bool(data.get("snapshot_source_state", False)),
            upload_run_provenance=bool(data.get("upload_run_provenance", True)),
            parent_dataset_manifest=str(data.get("parent_dataset_manifest", "")).strip(),
            apply_parent_history_update=bool(data.get("apply_parent_history_update", False)),
        )


@dataclass
class OutputsConfig:
    merged_prefix: str
    summary_prefix: str
    manifest_key: str
    logs_prefix: str
    archive_prefix: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OutputsConfig":
        return cls(
            merged_prefix=str(data.get("merged_prefix", "")).strip(),
            summary_prefix=str(data.get("summary_prefix", "")).strip(),
            manifest_key=str(data.get("manifest_key", "")).strip(),
            logs_prefix=str(data.get("logs_prefix", "")).strip(),
            archive_prefix=str(data["archive_prefix"]).strip() if data.get("archive_prefix") else None,
        )


@dataclass
class StepSpec:
    type: str
    options: Dict[str, Any]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "StepSpec":
        step_type = str(data.get("type", "")).strip()
        options = {k: v for k, v in data.items() if k != "type"}
        return cls(type=step_type, options=options)


@dataclass
class ProcessingConfig:
    steps: List[StepSpec]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ProcessingConfig":
        raw_steps = data.get("steps", [])
        steps = [StepSpec.from_dict(item) for item in raw_steps]
        return cls(steps=steps)


@dataclass
class NotificationConfig:
    channel: str
    recipients: List[str]
    subject: str

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "NotificationConfig":
        return cls(
            channel=str(data.get("channel", "")).strip(),
            recipients=[str(item).strip() for item in data.get("recipients", [])],
            subject=str(data.get("subject", "")).strip(),
        )


@dataclass
class PublishingConfig:
    delete_local_workspace: bool = True
    retain_local_logs: str = "7d"
    remove_local_raw_after_publish: bool = True
    remove_local_merged_after_publish: bool = True
    remove_local_summary_after_publish: bool = True
    remove_local_latest_measurement_after_publish: bool = True
    notifications: List[NotificationConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PublishingConfig":
        notifications = [NotificationConfig.from_dict(item) for item in data.get("notifications", [])]
        return cls(
            delete_local_workspace=bool(data.get("delete_local_workspace", True)),
            retain_local_logs=str(data.get("retain_local_logs", "7d")),
            remove_local_raw_after_publish=bool(data.get("remove_local_raw_after_publish", True)),
            remove_local_merged_after_publish=bool(data.get("remove_local_merged_after_publish", True)),
            remove_local_summary_after_publish=bool(data.get("remove_local_summary_after_publish", True)),
            remove_local_latest_measurement_after_publish=bool(
                data.get("remove_local_latest_measurement_after_publish", True)
            ),
            notifications=notifications,
        )


@dataclass
class RunSpec:
    run_id: str
    profile: str
    created_by: str
    created_at: str
    priority: str
    source: SourceConfig
    filters: FiltersConfig
    workspace: WorkspaceConfig
    batching: BatchingConfig
    provenance: ProvenanceConfig
    outputs: OutputsConfig
    processing: ProcessingConfig
    publishing: PublishingConfig

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        default_profile: str = DEFAULT_PIPELINE_PROFILE,
    ) -> "RunSpec":
        resolved_default_profile = str(default_profile or DEFAULT_PIPELINE_PROFILE).strip() or DEFAULT_PIPELINE_PROFILE
        return cls(
            run_id=str(data.get("run_id", "")).strip(),
            profile=str(data.get("profile", resolved_default_profile)).strip() or resolved_default_profile,
            created_by=str(data.get("created_by", "")).strip(),
            created_at=str(data.get("created_at", "")).strip(),
            priority=str(data.get("priority", "medium")).strip().lower() or "medium",
            source=SourceConfig.from_dict(data.get("source", {})),
            filters=FiltersConfig.from_dict(data.get("filters")),
            workspace=WorkspaceConfig.from_dict(data.get("workspace", {})),
            batching=BatchingConfig.from_dict(data.get("batching")),
            provenance=ProvenanceConfig.from_dict(data.get("provenance")),
            outputs=OutputsConfig.from_dict(data.get("outputs", {})),
            processing=ProcessingConfig.from_dict(data.get("processing", {})),
            publishing=PublishingConfig.from_dict(data.get("publishing", {})),
        )

    def iter_participants(self) -> Iterable[str]:
        return list(self.iter_entities())

    def iter_entities(self) -> Iterable[str]:
        return list(self.source.entities)


def load_spec(
    path: str,
    *,
    s3_client: Optional[Any] = None,
    object_store: Optional[ObjectStore] = None,
    default_profile: str = DEFAULT_PIPELINE_PROFILE,
) -> RunSpec:
    """Load a specification from a local path or object-store URI."""

    if locator_needs_object_store(path):
        if object_store is None:
            object_store = object_store_from_client(s3_client) if s3_client is not None else create_object_store_for_locator(path)
        payload = object_store.read_bytes(path)
        data = yaml.safe_load(payload)
    else:
        with Path(path).expanduser().open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    if not isinstance(data, MutableMapping):
        raise ValueError("Specification root must be a mapping/dictionary")
    if object_store is None and s3_client is not None:
        object_store = object_store_from_client(s3_client)
    _resolve_manifest_native_inputs(data, spec_locator=path, object_store=object_store)
    return RunSpec.from_dict(data, default_profile=default_profile)


def validate_spec(spec: RunSpec) -> List[str]:
    """Validate the specification and return a list of human-readable errors."""

    errors: List[str] = []

    if not spec.run_id:
        errors.append("run_id must be provided")
    if not spec.profile:
        errors.append("profile must be provided")
    if spec.priority not in {"low", "medium", "high", "urgent", "ludicrous"}:
        errors.append("priority must be one of: low, medium, high, urgent, ludicrous")
    entities = list(spec.iter_entities())
    if not entities and not spec.source.discover_all:
        errors.append("source.entities must contain at least one entity ID (or set discover_all=true)")
    else:
        duplicates = _find_duplicates(entities)
        if duplicates:
            errors.append(f"duplicate entity IDs detected: {sorted(duplicates)}")
        invalid = [entity for entity in entities if not _is_safe_identifier(entity)]
        if invalid:
            errors.append(f"entity IDs must be non-empty path-safe identifiers: {invalid}")
    invalid_groups = [group for group in spec.source.groups if not group.strip()]
    if invalid_groups:
        errors.append("source.groups must contain non-empty group names")

    if not spec.outputs.manifest_key:
        errors.append("outputs.manifest_key must be configured")

    if not spec.processing.steps:
        errors.append("processing.steps must define at least one step")
    else:
        for step in spec.processing.steps:
            if not step.type:
                errors.append("processing step missing type")

    shared = set(spec.filters.include_metrics) & set(spec.filters.exclude_metrics)
    if shared:
        errors.append(f"metrics cannot be both included and excluded: {sorted(shared)}")

    batching_strategy = spec.batching.strategy
    if batching_strategy not in {"none", "group", "site", "entity_count", "participant_count"}:
        errors.append("batching.strategy must be one of: none, group, entity_count")
    if batching_strategy in {"entity_count", "participant_count"}:
        if spec.batching.max_entities is None or spec.batching.max_entities <= 0:
            errors.append("batching.max_entities must be a positive integer when batching.strategy=entity_count")
    if spec.batching.max_entities is not None and spec.batching.max_entities <= 0:
        errors.append("batching.max_entities must be positive when provided")

    return errors


def _split_s3_uri(uri: str) -> tuple[str, str]:
    return split_s3_uri(uri)


def _resolve_manifest_native_inputs(
    data: MutableMapping[str, Any],
    *,
    spec_locator: str,
    object_store: Optional[ObjectStore],
) -> None:
    source = data.get("source")
    if isinstance(source, MutableMapping):
        source_state_manifest = str(source.get("source_state_manifest", "")).strip()
        if source_state_manifest:
            _apply_source_state_manifest(
                source,
                manifest_locator=source_state_manifest,
                spec_locator=spec_locator,
                object_store=object_store,
            )


def _apply_source_state_manifest(
    source: MutableMapping[str, Any],
    *,
    manifest_locator: str,
    spec_locator: str,
    object_store: Optional[ObjectStore],
) -> None:
    manifest = _load_json_document(manifest_locator, base_locator=spec_locator, object_store=object_store)
    binding = manifest.get("data_root_binding", {}) if isinstance(manifest, dict) else {}
    locator = str(getattr(binding, "get", lambda *_: "")("locator") if binding else "")
    if not locator and isinstance(binding, dict):
        locator = str(binding.get("locator", ""))
    if locator.startswith("s3://"):
        bucket, prefix = _split_s3_uri(locator)
        if not source.get("bucket"):
            source["bucket"] = bucket
        if not source.get("prefix"):
            source["prefix"] = prefix

    if not _source_has_groups(source):
        coverage_locator = _linked_document_locator(manifest, "coverage_summary", base_locator=manifest_locator)
        if coverage_locator:
            coverage = _load_json_document(coverage_locator, base_locator=manifest_locator, object_store=object_store)
            groups = [group for group, _entities in _coverage_group_rows(coverage)]
            if groups:
                source["groups"] = sorted(dict.fromkeys(groups))

    if (not _source_has_entities(source)) and not bool(source.get("discover_all", False)):
        coverage_locator = _linked_document_locator(manifest, "coverage_summary", base_locator=manifest_locator)
        if coverage_locator:
            coverage = _load_json_document(coverage_locator, base_locator=manifest_locator, object_store=object_store)
            entity_ids: list[str] = []
            for _group, row_entities in _coverage_group_rows(coverage):
                entity_ids.extend(row_entities)
            if entity_ids:
                source["entities"] = sorted(dict.fromkeys(entity_ids))


def _source_has_groups(source: Mapping[str, Any]) -> bool:
    for key in ("groups", "sites"):
        value = source.get(key)
        if isinstance(value, list) and any(str(item).strip() for item in value):
            return True
    return False


def _source_has_entities(source: Mapping[str, Any]) -> bool:
    for key in ("entities", "participants"):
        value = source.get(key)
        if isinstance(value, list) and any(str(item).strip() for item in value):
            return True
    return False


def _coverage_group_rows(coverage: Mapping[str, Any]) -> list[tuple[str, list[str]]]:
    coverage_payload = coverage.get("coverage", coverage)
    if not isinstance(coverage_payload, Mapping):
        return []
    group_rows = coverage_payload.get("group_summary", [])
    if isinstance(group_rows, list) and group_rows:
        rows: list[tuple[str, list[str]]] = []
        for row in group_rows:
            if not isinstance(row, Mapping):
                continue
            group = str(row.get("group", row.get("site", ""))).strip()
            raw_entities = row.get("entities", row.get("participants", []))
            entities = [str(item).strip() for item in raw_entities if str(item).strip()]
            if group:
                rows.append((group, entities))
        return rows

    site_rows = coverage_payload.get("site_summary", [])
    rows = []
    for row in site_rows if isinstance(site_rows, list) else []:
        if not isinstance(row, Mapping):
            continue
        site = str(row.get("site", "")).strip()
        raw_participants = row.get("participants", [])
        participants = [str(item).strip() for item in raw_participants if str(item).strip()]
        if site:
            rows.append((site, participants))
    return rows


def _linked_document_locator(manifest: Mapping[str, Any], document_name: str, *, base_locator: str) -> str:
    documents = manifest.get("documents", {}) if isinstance(manifest, Mapping) else {}
    if not isinstance(documents, Mapping):
        return ""
    entry = documents.get(document_name, {})
    if not isinstance(entry, Mapping):
        return ""
    locator = str(entry.get("locator", "")).strip()
    if not locator:
        return ""
    return _resolve_relative_locator(locator, base_locator=base_locator)


def _load_json_document(locator: str, *, base_locator: str, object_store: Optional[ObjectStore]) -> Dict[str, Any]:
    resolved = _resolve_relative_locator(locator, base_locator=base_locator)
    if locator_needs_object_store(resolved):
        if object_store is None:
            object_store = create_object_store_for_locator(resolved)
        payload = object_store.read_bytes(resolved)
        return json.loads(payload)
    return json.loads(Path(resolved).expanduser().read_text(encoding="utf-8"))


def _resolve_relative_locator(locator: str, *, base_locator: str) -> str:
    if locator.startswith("s3://"):
        return locator
    path = Path(locator).expanduser()
    if path.is_absolute():
        return str(path)
    if base_locator.startswith("s3://"):
        bucket, key = _split_s3_uri(base_locator)
        key_prefix = key.rsplit("/", 1)[0] if "/" in key else ""
        joined = f"{key_prefix}/{locator}".strip("/")
        return f"s3://{bucket}/{joined}"
    base_path = Path(base_locator).expanduser()
    return str((base_path.parent / locator).resolve())


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return False
    return bool(UUID_RE.match(value))


def is_uuid_identifier(value: str) -> bool:
    return _is_uuid(value)


def _is_safe_identifier(value: str) -> bool:
    text = str(value).strip()
    return bool(text) and "/" not in text and "\\" not in text and text not in {".", ".."}


def _find_duplicates(values: Iterable[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        else:
            seen.add(value)
    return duplicates


__all__ = [
    "DEFAULT_PIPELINE_PROFILE",
    "RunSpec",
    "SourceConfig",
    "FiltersConfig",
    "WorkspaceConfig",
    "BatchingConfig",
    "OutputsConfig",
    "ProcessingConfig",
    "PublishingConfig",
    "StepSpec",
    "is_uuid_identifier",
    "load_spec",
    "validate_spec",
]
