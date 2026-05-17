"""Helpers for loading and storing per-participant merge manifests."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from botocore.exceptions import ClientError

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
class ParticipantManifest:
    participant_id: str
    site: str
    metrics: Dict[str, MetricWatermark] = field(default_factory=dict)
    updated_at: Optional[str] = None
    last_run_id: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "participant_id": self.participant_id,
            "site": self.site,
            "metrics": {metric: watermark.to_dict() for metric, watermark in self.metrics.items()},
            "updated_at": self.updated_at,
            "last_run_id": self.last_run_id,
        }

    @classmethod
    def from_dict(cls, participant_id: str, site: str, data: Dict[str, object]) -> "ParticipantManifest":
        raw_metrics = data.get("metrics", {}) or {}
        metrics = {
            metric: MetricWatermark.from_dict(values or {})
            for metric, values in raw_metrics.items()
        }
        return cls(
            participant_id=participant_id,
            site=site,
            metrics=metrics,
            updated_at=data.get("updated_at"),
            last_run_id=data.get("last_run_id"),
        )


def _require_base_prefix(base_prefix: str) -> str:
    prefix = str(base_prefix or "").strip().rstrip("/")
    if not prefix:
        raise ValueError("base_prefix must be provided")
    return prefix


def manifest_s3_key(site: str, participant_id: str, *, base_prefix: str) -> str:
    base_prefix = _require_base_prefix(base_prefix)
    base_prefix = base_prefix.rstrip("/")
    return f"{base_prefix}/{site}/{participant_id}/manifest.json"


def load_participant_manifest(
    s3_client,
    *,
    site: str,
    participant_id: str,
    base_prefix: str,
) -> ParticipantManifest:
    key = manifest_s3_key(site, participant_id, base_prefix=base_prefix)
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
        payload = obj["Body"].read()
        data = json.loads(payload)
        if isinstance(data, dict):
            return ParticipantManifest.from_dict(participant_id, site, data)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code != "NoSuchKey":
            raise
    except Exception:
        # malformed manifest; fall through to default
        pass
    return ParticipantManifest(participant_id=participant_id, site=site)


def save_participant_manifest(
    s3_client,
    manifest: ParticipantManifest,
    *,
    run_id: str,
    base_prefix: str,
) -> None:
    manifest.updated_at = datetime.utcnow().strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    doc = json.dumps(manifest.to_dict(), indent=2, sort_keys=True)

    key = manifest_s3_key(manifest.site, manifest.participant_id, base_prefix=base_prefix)
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=doc.encode("utf-8"))


def write_local_manifest(manifest: ParticipantManifest, path: Path, run_id: str) -> None:
    manifest.updated_at = datetime.utcnow().strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


__all__ = [
    "MetricWatermark",
    "ParticipantManifest",
    "load_participant_manifest",
    "save_participant_manifest",
    "manifest_s3_key",
    "write_local_manifest",
]
