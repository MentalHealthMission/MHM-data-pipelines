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
class LatestMeasurementManifest:
    participant_id: str
    site: str
    source_watermarks: Dict[str, str] = field(default_factory=dict)
    measurement_files: List[str] = field(default_factory=list)
    results: Dict[str, object] = field(default_factory=dict)
    updated_at: Optional[str] = None
    last_run_id: Optional[str] = None

    @property
    def entity_id(self) -> str:
        return self.participant_id

    @property
    def group(self) -> str:
        return self.site

    def to_dict(self) -> Dict[str, object]:
        return {
            "entity_id": self.entity_id,
            "group": self.group,
            "participant_id": self.participant_id,
            "site": self.site,
            "source_watermarks": self.source_watermarks,
            "measurement_files": self.measurement_files,
            "results": self.results,
            "updated_at": self.updated_at,
            "last_run_id": self.last_run_id,
        }

    @classmethod
    def from_dict(cls, participant_id: str, site: str, data: Dict[str, object]) -> "LatestMeasurementManifest":
        return cls(
            participant_id=str(data.get("entity_id") or data.get("participant_id") or participant_id),
            site=str(data.get("group") or data.get("site") or site),
            source_watermarks=dict(data.get("source_watermarks", {})),
            measurement_files=list(data.get("measurement_files", [])),
            results=dict(data.get("results", {})),
            updated_at=data.get("updated_at"),
            last_run_id=data.get("last_run_id"),
        )


def latest_measurement_manifest_s3_key(
    site: str,
    participant_id: str,
    *,
    manifest_prefix: str,
) -> str:
    manifest_prefix = manifest_prefix.rstrip("/")
    return f"{manifest_prefix}/{site}/{participant_id}/manifest.json"


def load_latest_measurement_manifest(
    s3_client,
    *,
    site: str,
    participant_id: str,
    manifest_prefix: str,
) -> LatestMeasurementManifest:
    key = latest_measurement_manifest_s3_key(site, participant_id, manifest_prefix=manifest_prefix)
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
        payload = obj["Body"].read()
        data = json.loads(payload)
        if isinstance(data, dict):
            return LatestMeasurementManifest.from_dict(participant_id, site, data)
    except Exception as exc:
        if is_client_error(exc) and not is_missing_key_error(exc):
            raise
    return LatestMeasurementManifest(participant_id=participant_id, site=site)


def save_latest_measurement_manifest(
    s3_client,
    manifest: LatestMeasurementManifest,
    *,
    run_id: str,
    manifest_prefix: str,
) -> None:
    manifest.updated_at = datetime.now(timezone.utc).strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    key = latest_measurement_manifest_s3_key(
        manifest.site,
        manifest.participant_id,
        manifest_prefix=manifest_prefix,
    )
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    body = json.dumps(manifest.to_dict(), indent=2, sort_keys=True).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=body)


def write_local_latest_measurement_manifest(
    manifest: LatestMeasurementManifest,
    path: Path,
    run_id: str,
) -> None:
    manifest.updated_at = datetime.now(timezone.utc).strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


__all__ = [
    "LatestMeasurementManifest",
    "latest_measurement_manifest_s3_key",
    "load_latest_measurement_manifest",
    "save_latest_measurement_manifest",
    "write_local_latest_measurement_manifest",
]
