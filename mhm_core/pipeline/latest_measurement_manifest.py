"""Latest-measurement manifest helpers for caching and reuse."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .object_store import is_client_error, is_missing_key_error

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass
class EntityLatestMeasurementManifest:
    entity_id: str
    group: str
    source_watermarks: Dict[str, str] = field(default_factory=dict)
    measurement_files: List[str] = field(default_factory=list)
    results: Dict[str, object] = field(default_factory=dict)
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
            "participant_id": self.entity_id,
            "site": self.group,
            "source_watermarks": self.source_watermarks,
            "measurement_files": self.measurement_files,
            "results": self.results,
            "updated_at": self.updated_at,
            "last_run_id": self.last_run_id,
        }

    @classmethod
    def from_dict(cls, entity_id: str, group: str, data: Dict[str, object]) -> "EntityLatestMeasurementManifest":
        return cls(
            entity_id=str(data.get("entity_id") or data.get("participant_id") or entity_id),
            group=str(data.get("group") or data.get("site") or group),
            source_watermarks=dict(data.get("source_watermarks", {})),
            measurement_files=list(data.get("measurement_files", [])),
            results=dict(data.get("results", {})),
            updated_at=data.get("updated_at"),
            last_run_id=data.get("last_run_id"),
        )


class LatestMeasurementManifest(EntityLatestMeasurementManifest):
    """Compatibility manifest constructor using participant/site names."""

    def __init__(
        self,
        participant_id: str,
        site: str,
        source_watermarks: Dict[str, str] | None = None,
        measurement_files: List[str] | None = None,
        results: Dict[str, object] | None = None,
        updated_at: Optional[str] = None,
        last_run_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            entity_id=participant_id,
            group=site,
            source_watermarks=source_watermarks or {},
            measurement_files=measurement_files or [],
            results=results or {},
            updated_at=updated_at,
            last_run_id=last_run_id,
        )

    @classmethod
    def from_dict(cls, participant_id: str, site: str, data: Dict[str, object]) -> "LatestMeasurementManifest":
        manifest = EntityLatestMeasurementManifest.from_dict(participant_id, site, data)
        return cls(
            participant_id=manifest.entity_id,
            site=manifest.group,
            source_watermarks=manifest.source_watermarks,
            measurement_files=manifest.measurement_files,
            results=manifest.results,
            updated_at=manifest.updated_at,
            last_run_id=manifest.last_run_id,
        )


def entity_latest_measurement_manifest_object_key(
    group: str,
    entity_id: str,
    *,
    manifest_prefix: str,
) -> str:
    manifest_prefix = manifest_prefix.rstrip("/")
    return f"{manifest_prefix}/{group}/{entity_id}/manifest.json"


def latest_measurement_manifest_s3_key(
    site: str,
    participant_id: str,
    *,
    manifest_prefix: str,
) -> str:
    """Compatibility wrapper for the participant/site manifest key layout."""

    return entity_latest_measurement_manifest_object_key(site, participant_id, manifest_prefix=manifest_prefix)


def load_entity_latest_measurement_manifest(
    s3_client,
    *,
    group: str,
    entity_id: str,
    manifest_prefix: str,
) -> EntityLatestMeasurementManifest:
    key = entity_latest_measurement_manifest_object_key(group, entity_id, manifest_prefix=manifest_prefix)
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
        payload = obj["Body"].read()
        data = json.loads(payload)
        if isinstance(data, dict):
            return EntityLatestMeasurementManifest.from_dict(entity_id, group, data)
    except Exception as exc:
        if is_client_error(exc) and not is_missing_key_error(exc):
            raise
    return EntityLatestMeasurementManifest(entity_id=entity_id, group=group)


def save_entity_latest_measurement_manifest(
    s3_client,
    manifest: EntityLatestMeasurementManifest,
    *,
    run_id: str,
    manifest_prefix: str,
) -> None:
    manifest.updated_at = datetime.now(timezone.utc).strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    key = entity_latest_measurement_manifest_object_key(
        manifest.group,
        manifest.entity_id,
        manifest_prefix=manifest_prefix,
    )
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    body = json.dumps(manifest.to_dict(), indent=2, sort_keys=True).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=body)


def load_latest_measurement_manifest(
    s3_client,
    *,
    site: str,
    participant_id: str,
    manifest_prefix: str,
) -> LatestMeasurementManifest:
    """Compatibility wrapper for participant/site latest-measurement manifests."""

    manifest = load_entity_latest_measurement_manifest(
        s3_client,
        group=site,
        entity_id=participant_id,
        manifest_prefix=manifest_prefix,
    )
    return LatestMeasurementManifest(
        participant_id=manifest.entity_id,
        site=manifest.group,
        source_watermarks=manifest.source_watermarks,
        measurement_files=manifest.measurement_files,
        results=manifest.results,
        updated_at=manifest.updated_at,
        last_run_id=manifest.last_run_id,
    )


def save_latest_measurement_manifest(
    s3_client,
    manifest: LatestMeasurementManifest,
    *,
    run_id: str,
    manifest_prefix: str,
) -> None:
    """Compatibility wrapper for participant/site latest-measurement manifests."""

    save_entity_latest_measurement_manifest(s3_client, manifest, run_id=run_id, manifest_prefix=manifest_prefix)


def write_local_latest_measurement_manifest(
    manifest: EntityLatestMeasurementManifest,
    path: Path,
    run_id: str,
) -> None:
    manifest.updated_at = datetime.now(timezone.utc).strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


__all__ = [
    "EntityLatestMeasurementManifest",
    "LatestMeasurementManifest",
    "entity_latest_measurement_manifest_object_key",
    "latest_measurement_manifest_s3_key",
    "load_entity_latest_measurement_manifest",
    "load_latest_measurement_manifest",
    "save_entity_latest_measurement_manifest",
    "save_latest_measurement_manifest",
    "write_local_latest_measurement_manifest",
]
