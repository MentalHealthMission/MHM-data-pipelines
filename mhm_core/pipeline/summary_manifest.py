"""Summary manifest helpers for caching and reuse."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .object_store import ObjectStore, is_client_error, is_missing_key_error, object_store_from_client

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass
class EntitySummaryManifest:
    entity_id: str
    group: str
    source_watermarks: Dict[str, str] = field(default_factory=dict)
    summary_files: List[str] = field(default_factory=list)
    updated_at: Optional[str] = None
    last_run_id: Optional[str] = None

    @property
    def participant_id(self) -> str:
        """Compatibility alias for participant/site callers."""

        return self.entity_id

    @property
    def site(self) -> str:
        """Compatibility alias for participant/site callers."""

        return self.group

    def to_dict(self) -> Dict[str, object]:
        return {
            "entity_id": self.entity_id,
            "group": self.group,
            "participant_id": self.entity_id,
            "site": self.group,
            "source_watermarks": self.source_watermarks,
            "summary_files": self.summary_files,
            "updated_at": self.updated_at,
            "last_run_id": self.last_run_id,
        }

    @classmethod
    def from_dict(cls, entity_id: str, group: str, data: Dict[str, object]) -> "EntitySummaryManifest":
        return cls(
            entity_id=str(data.get("entity_id") or data.get("participant_id") or entity_id),
            group=str(data.get("group") or data.get("site") or group),
            source_watermarks=dict(data.get("source_watermarks", {})),
            summary_files=list(data.get("summary_files", [])),
            updated_at=data.get("updated_at"),
            last_run_id=data.get("last_run_id"),
        )


class SummaryManifest(EntitySummaryManifest):
    """Compatibility manifest constructor using participant/site names."""

    def __init__(
        self,
        participant_id: str,
        site: str,
        source_watermarks: Dict[str, str] | None = None,
        summary_files: List[str] | None = None,
        updated_at: Optional[str] = None,
        last_run_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            entity_id=participant_id,
            group=site,
            source_watermarks=source_watermarks or {},
            summary_files=summary_files or [],
            updated_at=updated_at,
            last_run_id=last_run_id,
        )

    @classmethod
    def from_dict(cls, participant_id: str, site: str, data: Dict[str, object]) -> "SummaryManifest":
        manifest = EntitySummaryManifest.from_dict(participant_id, site, data)
        return cls(
            participant_id=manifest.entity_id,
            site=manifest.group,
            source_watermarks=manifest.source_watermarks,
            summary_files=manifest.summary_files,
            updated_at=manifest.updated_at,
            last_run_id=manifest.last_run_id,
        )


def entity_summary_manifest_object_key(
    group: str,
    entity_id: str,
    *,
    manifest_prefix: str,
) -> str:
    manifest_prefix = manifest_prefix.rstrip("/")
    return f"{manifest_prefix}/{group}/{entity_id}/manifest.json"


def summary_manifest_s3_key(
    site: str,
    participant_id: str,
    *,
    manifest_prefix: str,
) -> str:
    """Compatibility wrapper for the participant/site manifest key layout."""

    return entity_summary_manifest_object_key(site, participant_id, manifest_prefix=manifest_prefix)


def load_entity_summary_manifest(
    s3_client,
    *,
    group: str,
    entity_id: str,
    manifest_prefix: str,
) -> EntitySummaryManifest:
    return load_entity_summary_manifest_from_store(
        object_store_from_client(s3_client),
        group=group,
        entity_id=entity_id,
        manifest_prefix=manifest_prefix,
    )


def load_entity_summary_manifest_from_store(
    object_store: ObjectStore,
    *,
    group: str,
    entity_id: str,
    manifest_prefix: str,
) -> EntitySummaryManifest:
    locator = entity_summary_manifest_object_key(group, entity_id, manifest_prefix=manifest_prefix)
    try:
        payload = object_store.read_bytes(locator)
        data = json.loads(payload)
        if isinstance(data, dict):
            return EntitySummaryManifest.from_dict(entity_id, group, data)
    except Exception as exc:
        if is_client_error(exc) and not is_missing_key_error(exc):
            raise
    return EntitySummaryManifest(entity_id=entity_id, group=group)


def save_entity_summary_manifest(
    s3_client,
    manifest: EntitySummaryManifest,
    *,
    run_id: str,
    manifest_prefix: str,
) -> None:
    save_entity_summary_manifest_to_store(
        object_store_from_client(s3_client),
        manifest,
        run_id=run_id,
        manifest_prefix=manifest_prefix,
    )


def save_entity_summary_manifest_to_store(
    object_store: ObjectStore,
    manifest: EntitySummaryManifest,
    *,
    run_id: str,
    manifest_prefix: str,
) -> None:
    manifest.updated_at = utc_now()
    manifest.last_run_id = run_id
    locator = entity_summary_manifest_object_key(manifest.group, manifest.entity_id, manifest_prefix=manifest_prefix)
    body = json.dumps(manifest.to_dict(), indent=2, sort_keys=True).encode("utf-8")
    object_store.write_bytes(locator, body, content_type="application/json")


def load_summary_manifest(
    s3_client,
    *,
    site: str,
    participant_id: str,
    manifest_prefix: str,
) -> SummaryManifest:
    """Compatibility wrapper for participant/site summary manifests."""

    manifest = load_entity_summary_manifest(
        s3_client,
        group=site,
        entity_id=participant_id,
        manifest_prefix=manifest_prefix,
    )
    return SummaryManifest(
        participant_id=manifest.entity_id,
        site=manifest.group,
        source_watermarks=manifest.source_watermarks,
        summary_files=manifest.summary_files,
        updated_at=manifest.updated_at,
        last_run_id=manifest.last_run_id,
    )


def save_summary_manifest(
    s3_client,
    manifest: SummaryManifest,
    *,
    run_id: str,
    manifest_prefix: str,
) -> None:
    """Compatibility wrapper for participant/site summary manifests."""

    save_entity_summary_manifest(s3_client, manifest, run_id=run_id, manifest_prefix=manifest_prefix)


def write_local_summary_manifest(manifest: EntitySummaryManifest, path: Path, run_id: str) -> None:
    manifest.updated_at = utc_now()
    manifest.last_run_id = run_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FORMAT)


__all__ = [
    "EntitySummaryManifest",
    "SummaryManifest",
    "entity_summary_manifest_object_key",
    "load_entity_summary_manifest",
    "load_entity_summary_manifest_from_store",
    "load_summary_manifest",
    "save_entity_summary_manifest",
    "save_entity_summary_manifest_to_store",
    "save_summary_manifest",
    "summary_manifest_s3_key",
    "write_local_summary_manifest",
]
