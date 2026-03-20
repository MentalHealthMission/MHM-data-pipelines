"""Specification parsing and validation for the core pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional
import re
import uuid

import boto3
import yaml

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

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SourceConfig":
        bucket = str(data.get("bucket", "")).strip()
        prefix = str(data.get("prefix", "")).strip().strip("/")
        raw_participants = data.get("participants", [])
        participants = [str(pid).strip() for pid in raw_participants if str(pid).strip()]
        discover_all = bool(data.get("discover_all", False))
        raw_sites = data.get("sites", [])
        sites = [str(site).strip() for site in raw_sites if str(site).strip()]
        return cls(bucket=bucket, prefix=prefix, participants=participants, discover_all=discover_all, sites=sites)


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
    outputs: OutputsConfig
    processing: ProcessingConfig
    publishing: PublishingConfig

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RunSpec":
        return cls(
            run_id=str(data.get("run_id", "")).strip(),
            profile=str(data.get("profile", "connect")).strip() or "connect",
            created_by=str(data.get("created_by", "")).strip(),
            created_at=str(data.get("created_at", "")).strip(),
            priority=str(data.get("priority", "medium")).strip().lower() or "medium",
            source=SourceConfig.from_dict(data.get("source", {})),
            filters=FiltersConfig.from_dict(data.get("filters")),
            workspace=WorkspaceConfig.from_dict(data.get("workspace", {})),
            batching=BatchingConfig.from_dict(data.get("batching")),
            outputs=OutputsConfig.from_dict(data.get("outputs", {})),
            processing=ProcessingConfig.from_dict(data.get("processing", {})),
            publishing=PublishingConfig.from_dict(data.get("publishing", {})),
        )

    def iter_participants(self) -> Iterable[str]:
        return list(self.source.participants)


def load_spec(path: str, *, s3_client: Optional[boto3.client] = None) -> RunSpec:
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
    return RunSpec.from_dict(data)


def validate_spec(spec: RunSpec) -> List[str]:
    """Validate the specification and return a list of human-readable errors."""

    errors: List[str] = []

    if not spec.run_id:
        errors.append("run_id must be provided")
    if not spec.profile:
        errors.append("profile must be provided")
    if spec.priority not in {"low", "medium", "high", "urgent"}:
        errors.append("priority must be one of: low, medium, high, urgent")
    if not spec.source.bucket:
        errors.append("source.bucket must be provided")
    if not spec.source.prefix:
        errors.append("source.prefix must be provided")

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
