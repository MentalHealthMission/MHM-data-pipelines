"""S3-backed pipeline backend adapters."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

import yaml

from ..queue import QueueEntry, normalize_priority


def create_boto3_session(*, profile_name: str | None = None) -> Any:
    """Create a boto3 session only when an S3-backed path needs one."""

    import boto3

    return boto3.session.Session(profile_name=profile_name) if profile_name else boto3.session.Session()


def create_s3_client(*, session: Any = None, profile_name: str | None = None) -> Any:
    """Create an S3 client using a supplied session or a lazily-created session."""

    if session is None:
        session = create_boto3_session(profile_name=profile_name)
    return session.client("s3")


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Split an S3 URI into bucket and key."""

    text = str(uri)
    if not text.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got {uri}")
    remainder = text[len("s3://") :]
    bucket, _, key = remainder.partition("/")
    if not bucket:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return bucket, key


class S3QueueBackend:
    """Queue backend that reads YAML specs from an S3 prefix."""

    def __init__(self, s3_client: Any, queue_prefix: str) -> None:
        self.s3_client = s3_client
        self.queue_prefix = queue_prefix

    def list_specs(self, *, states: Iterable[str]) -> list[QueueEntry]:
        bucket, prefix = split_s3_uri(self.queue_prefix)
        entries: list[QueueEntry] = []
        for state in states:
            state_prefix = f"{prefix.rstrip('/')}/{state}/"
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=state_prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key", "")
                    if not key or key.endswith("/"):
                        continue
                    payload = self.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
                    try:
                        data = yaml.safe_load(payload) or {}
                    except Exception:
                        data = {}
                    priority = normalize_priority(getattr(data, "get", lambda *_: None)("priority"))
                    entries.append(
                        QueueEntry(
                            state=str(state),
                            key=key.split("/", 2)[-1],
                            priority=priority,
                            last_modified=_ensure_utc(obj["LastModified"]),
                        )
                    )
        return entries


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


__all__ = [
    "S3QueueBackend",
    "create_boto3_session",
    "create_s3_client",
    "split_s3_uri",
]
