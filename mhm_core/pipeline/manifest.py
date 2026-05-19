"""Helpers for loading and storing per-entity merge manifests."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from .object_store import is_client_error, is_missing_key_error

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass
class MetricWatermark:
    latest_object_key: Optional[str] = None
    latest_timestamp: Optional[str] = None
    files_merged: int = 0
    bytes_merged: int = 0
    refresh_mode: Optional[str] = None
    refresh_start_timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "latest_object_key": self.latest_object_key,
            "latest_timestamp": self.latest_timestamp,
            "files_merged": self.files_merged,
            "bytes_merged": self.bytes_merged,
            "refresh_mode": self.refresh_mode,
            "refresh_start_timestamp": self.refresh_start_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "MetricWatermark":
        return cls(
            latest_object_key=data.get("latest_object_key"),
            latest_timestamp=data.get("latest_timestamp"),
            files_merged=int(data.get("files_merged", 0) or 0),
            bytes_merged=int(data.get("bytes_merged", 0) or 0),
            refresh_mode=data.get("refresh_mode"),
            refresh_start_timestamp=data.get("refresh_start_timestamp"),
        )


@dataclass
class EntityManifest:
    entity_id: str
    group: str
    metrics: Dict[str, MetricWatermark] = field(default_factory=dict)
    updated_at: Optional[str] = None
    last_run_id: Optional[str] = None

    @property
    def participant_id(self) -> str:
        """Compatibility alias for existing CONNECT callers."""

        return self.entity_id

    @property
    def site(self) -> str:
        """Compatibility alias for existing CONNECT callers."""

        return self.group

    def to_dict(self) -> Dict[str, object]:
        return {
            "entity_id": self.entity_id,
            "group": self.group,
            # Compatibility fields retained for current CONNECT manifests.
            "participant_id": self.entity_id,
            "site": self.group,
            "metrics": {metric: watermark.to_dict() for metric, watermark in self.metrics.items()},
            "updated_at": self.updated_at,
            "last_run_id": self.last_run_id,
        }

    @classmethod
    def from_dict(cls, entity_id: str, group: str, data: Dict[str, object]) -> "EntityManifest":
        raw_metrics = data.get("metrics", {}) or {}
        metrics = {
            metric: MetricWatermark.from_dict(values or {})
            for metric, values in raw_metrics.items()
        }
        return cls(
            entity_id=str(data.get("entity_id") or data.get("participant_id") or entity_id),
            group=str(data.get("group") or data.get("site") or group),
            metrics=metrics,
            updated_at=data.get("updated_at"),
            last_run_id=data.get("last_run_id"),
        )


class ParticipantManifest(EntityManifest):
    """Compatibility manifest constructor using CONNECT participant/site names."""

    def __init__(
        self,
        participant_id: str,
        site: str,
        metrics: Dict[str, MetricWatermark] | None = None,
        updated_at: Optional[str] = None,
        last_run_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            entity_id=participant_id,
            group=site,
            metrics=metrics or {},
            updated_at=updated_at,
            last_run_id=last_run_id,
        )

    @classmethod
    def from_dict(cls, participant_id: str, site: str, data: Dict[str, object]) -> "ParticipantManifest":
        manifest = EntityManifest.from_dict(participant_id, site, data)
        return cls(
            participant_id=manifest.entity_id,
            site=manifest.group,
            metrics=manifest.metrics,
            updated_at=manifest.updated_at,
            last_run_id=manifest.last_run_id,
        )


def _require_base_prefix(base_prefix: str) -> str:
    prefix = str(base_prefix or "").strip().rstrip("/")
    if not prefix:
        raise ValueError("base_prefix must be provided")
    return prefix


def entity_manifest_object_key(group: str, entity_id: str, *, base_prefix: str) -> str:
    base_prefix = _require_base_prefix(base_prefix)
    base_prefix = base_prefix.rstrip("/")
    return f"{base_prefix}/{group}/{entity_id}/manifest.json"


def manifest_s3_key(site: str, participant_id: str, *, base_prefix: str) -> str:
    """Compatibility wrapper for the CONNECT participant/site key layout."""

    return entity_manifest_object_key(site, participant_id, base_prefix=base_prefix)


def load_entity_manifest(
    s3_client,
    *,
    group: str,
    entity_id: str,
    base_prefix: str,
) -> EntityManifest:
    key = entity_manifest_object_key(group, entity_id, base_prefix=base_prefix)
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
        payload = obj["Body"].read()
        data = json.loads(payload)
        if isinstance(data, dict):
            return EntityManifest.from_dict(entity_id, group, data)
    except Exception as exc:
        if is_client_error(exc) and not is_missing_key_error(exc):
            raise
        # malformed or absent manifest; fall through to default
    return EntityManifest(entity_id=entity_id, group=group)


def save_entity_manifest(
    s3_client,
    manifest: EntityManifest,
    *,
    run_id: str,
    base_prefix: str,
) -> None:
    manifest.updated_at = datetime.utcnow().strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    doc = json.dumps(manifest.to_dict(), indent=2, sort_keys=True)

    key = entity_manifest_object_key(manifest.group, manifest.entity_id, base_prefix=base_prefix)
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=doc.encode("utf-8"))


def load_participant_manifest(
    s3_client,
    *,
    site: str,
    participant_id: str,
    base_prefix: str,
) -> ParticipantManifest:
    """Compatibility wrapper for participant/site manifests."""

    manifest = load_entity_manifest(
        s3_client,
        group=site,
        entity_id=participant_id,
        base_prefix=base_prefix,
    )
    return ParticipantManifest(
        participant_id=manifest.entity_id,
        site=manifest.group,
        metrics=manifest.metrics,
        updated_at=manifest.updated_at,
        last_run_id=manifest.last_run_id,
    )


def save_participant_manifest(
    s3_client,
    manifest: ParticipantManifest,
    *,
    run_id: str,
    base_prefix: str,
) -> None:
    """Compatibility wrapper for participant/site manifests."""

    save_entity_manifest(s3_client, manifest, run_id=run_id, base_prefix=base_prefix)


def write_local_manifest(manifest: EntityManifest, path: Path, run_id: str) -> None:
    manifest.updated_at = datetime.utcnow().strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


__all__ = [
    "EntityManifest",
    "MetricWatermark",
    "ParticipantManifest",
    "entity_manifest_object_key",
    "load_entity_manifest",
    "load_participant_manifest",
    "save_entity_manifest",
    "save_participant_manifest",
    "manifest_s3_key",
    "write_local_manifest",
]
