"""Summary manifest helpers for caching and reuse."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from botocore.exceptions import ClientError

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass
class SummaryManifest:
    participant_id: str
    site: str
    source_watermarks: Dict[str, str] = field(default_factory=dict)
    summary_files: List[str] = field(default_factory=list)
    updated_at: Optional[str] = None
    last_run_id: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "participant_id": self.participant_id,
            "site": self.site,
            "source_watermarks": self.source_watermarks,
            "summary_files": self.summary_files,
            "updated_at": self.updated_at,
            "last_run_id": self.last_run_id,
        }

    @classmethod
    def from_dict(cls, participant_id: str, site: str, data: Dict[str, object]) -> "SummaryManifest":
        return cls(
            participant_id=participant_id,
            site=site,
            source_watermarks=dict(data.get("source_watermarks", {})),
            summary_files=list(data.get("summary_files", [])),
            updated_at=data.get("updated_at"),
            last_run_id=data.get("last_run_id"),
        )


def summary_manifest_s3_key(
    site: str,
    participant_id: str,
    *,
    manifest_prefix: str,
) -> str:
    manifest_prefix = manifest_prefix.rstrip("/")
    return f"{manifest_prefix}/{site}/{participant_id}/manifest.json"


def load_summary_manifest(
    s3_client,
    *,
    site: str,
    participant_id: str,
    manifest_prefix: str,
) -> SummaryManifest:
    key = summary_manifest_s3_key(site, participant_id, manifest_prefix=manifest_prefix)
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
        payload = obj["Body"].read()
        data = json.loads(payload)
        if isinstance(data, dict):
            return SummaryManifest.from_dict(participant_id, site, data)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code != "NoSuchKey":
            raise
    except Exception:
        pass
    return SummaryManifest(participant_id=participant_id, site=site)


def save_summary_manifest(
    s3_client,
    manifest: SummaryManifest,
    *,
    run_id: str,
    manifest_prefix: str,
) -> None:
    manifest.updated_at = datetime.utcnow().strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    key = summary_manifest_s3_key(manifest.site, manifest.participant_id, manifest_prefix=manifest_prefix)
    bucket, _, s3_key = key[len("s3://") :].partition("/")
    body = json.dumps(manifest.to_dict(), indent=2, sort_keys=True).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=body)


def write_local_summary_manifest(manifest: SummaryManifest, path: Path, run_id: str) -> None:
    manifest.updated_at = datetime.utcnow().strftime(ISO_FORMAT)
    manifest.last_run_id = run_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


__all__ = [
    "SummaryManifest",
    "load_summary_manifest",
    "save_summary_manifest",
    "summary_manifest_s3_key",
    "write_local_summary_manifest",
]
