"""Specification parsing and validation for the core pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional
import json
import re
import uuid

import boto3
import yaml

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
    participants: List[str]
    discover_all: bool = False
    sites: List[str] = field(default_factory=list)
    source_state_manifest: str = ""

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SourceConfig":
        bucket = str(data.get("bucket", "")).strip()
        prefix = str(data.get("prefix", "")).strip().strip("/")
        raw_participants = data.get("participants", [])
        participants = [str(pid).strip() for pid in raw_participants if str(pid).strip()]
        discover_all = bool(data.get("discover_all", False))
        raw_sites = data.get("sites", [])
        sites = [str(site).strip() for site in raw_sites if str(site).strip()]
        source_state_manifest = str(data.get("source_state_manifest", "")).strip()
        return cls(
            bucket=bucket,
            prefix=prefix,
            participants=participants,
            discover_all=discover_all,
            sites=sites,
            source_state_manifest=source_state_manifest,
        )


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
    max_participants: Optional[int] = None
    resume_completed: bool = False

    @classmethod
    def from_dict(cls, data: Optional[Mapping[str, Any]]) -> "BatchingConfig":
        if not data:
            return cls()
        strategy = str(data.get("strategy", "none")).strip().lower() or "none"
        max_participants_raw = data.get("max_participants")
        try:
            max_participants = int(max_participants_raw) if max_participants_raw is not None else None
        except (TypeError, ValueError):
            max_participants = None
        resume_completed = bool(data.get("resume_completed", False))
        return cls(
            strategy=strategy,
            max_participants=max_participants,
            resume_completed=resume_completed,
        )


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
        return list(self.source.participants)


def load_spec(
    path: str,
    *,
    s3_client: Optional[boto3.client] = None,
    default_profile: str = DEFAULT_PIPELINE_PROFILE,
) -> RunSpec:
    """Load a specification from a local path or an S3 URI."""

    if path.startswith("s3://"):
        if s3_client is None:
            s3_client = boto3.client("s3")
        bucket, key = _split_s3_uri(path)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        payload = obj["Body"].read()
        data = yaml.safe_load(payload)
    else:
        with Path(path).expanduser().open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    if not isinstance(data, MutableMapping):
        raise ValueError("Specification root must be a mapping/dictionary")
    _resolve_manifest_native_inputs(data, spec_locator=path, s3_client=s3_client)
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
    if not spec.source.bucket:
        errors.append("source.bucket must be provided (or resolvable via source.source_state_manifest)")
    if not spec.source.prefix:
        errors.append("source.prefix must be provided (or resolvable via source.source_state_manifest)")

    participants = spec.source.participants
    if not participants and not spec.source.discover_all:
        errors.append("source.participants must contain at least one participant ID (or set discover_all=true)")
    else:
        duplicates = _find_duplicates(participants)
        if duplicates:
            errors.append(f"duplicate participant IDs detected: {sorted(duplicates)}")
        invalid = [pid for pid in participants if not _is_uuid(pid)]
        if invalid:
            errors.append(f"participant IDs must be valid UUIDs: {invalid}")
    invalid_sites = [site for site in spec.source.sites if not site.strip()]
    if invalid_sites:
        errors.append("source.sites must contain non-empty site names")

    if not spec.outputs.merged_prefix:
        errors.append("outputs.merged_prefix must be configured")
    if not spec.outputs.summary_prefix:
        errors.append("outputs.summary_prefix must be configured")
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
    if batching_strategy not in {"none", "site", "participant_count"}:
        errors.append("batching.strategy must be one of: none, site, participant_count")
    if batching_strategy == "participant_count":
        if spec.batching.max_participants is None or spec.batching.max_participants <= 0:
            errors.append("batching.max_participants must be a positive integer when batching.strategy=participant_count")
    if spec.batching.max_participants is not None and spec.batching.max_participants <= 0:
        errors.append("batching.max_participants must be positive when provided")

    return errors


def _split_s3_uri(uri: str) -> tuple[str, str]:
    _, remainder = uri.split("s3://", 1)
    bucket, _, key = remainder.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return bucket, key


def _resolve_manifest_native_inputs(
    data: MutableMapping[str, Any],
    *,
    spec_locator: str,
    s3_client,
) -> None:
    source = data.get("source")
    if isinstance(source, MutableMapping):
        source_state_manifest = str(source.get("source_state_manifest", "")).strip()
        if source_state_manifest:
            _apply_source_state_manifest(
                source,
                manifest_locator=source_state_manifest,
                spec_locator=spec_locator,
                s3_client=s3_client,
            )


def _apply_source_state_manifest(
    source: MutableMapping[str, Any],
    *,
    manifest_locator: str,
    spec_locator: str,
    s3_client,
) -> None:
    manifest = _load_json_document(manifest_locator, base_locator=spec_locator, s3_client=s3_client)
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

    if not source.get("sites") or (isinstance(source.get("sites"), list) and not any(str(item).strip() for item in source.get("sites", []))):
        coverage_locator = _linked_document_locator(manifest, "coverage_summary", base_locator=manifest_locator)
        if coverage_locator:
            coverage = _load_json_document(coverage_locator, base_locator=manifest_locator, s3_client=s3_client)
            sites = []
            for row in coverage.get("coverage", {}).get("site_summary", []):
                site = str(row.get("site", "")).strip()
                if site:
                    sites.append(site)
            if sites:
                source["sites"] = sorted(dict.fromkeys(sites))

    participants = source.get("participants")
    if (not participants) and not bool(source.get("discover_all", False)):
        coverage_locator = _linked_document_locator(manifest, "coverage_summary", base_locator=manifest_locator)
        if coverage_locator:
            coverage = _load_json_document(coverage_locator, base_locator=manifest_locator, s3_client=s3_client)
            participant_ids: list[str] = []
            for row in coverage.get("coverage", {}).get("site_summary", []):
                participant_ids.extend(str(item).strip() for item in row.get("participants", []) if str(item).strip())
            if participant_ids:
                source["participants"] = sorted(dict.fromkeys(participant_ids))


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


def _load_json_document(locator: str, *, base_locator: str, s3_client) -> Dict[str, Any]:
    resolved = _resolve_relative_locator(locator, base_locator=base_locator)
    if resolved.startswith("s3://"):
        if s3_client is None:
            s3_client = boto3.client("s3")
        bucket, key = _split_s3_uri(resolved)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        payload = obj["Body"].read()
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
    "load_spec",
    "validate_spec",
]
