"""S3-backed pipeline backend adapters."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
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


class S3ObjectStore:
    """Object-store adapter that hides S3 client details behind locator methods."""

    scheme = "s3"

    def __init__(self, s3_client: Any) -> None:
        self.s3_client = s3_client

    def read_bytes(self, locator: str) -> bytes:
        bucket, key = split_s3_uri(locator)
        body = self.s3_client.get_object(Bucket=bucket, Key=key)["Body"]
        return body.read()

    def write_bytes(self, locator: str, payload: bytes, *, content_type: str | None = None) -> None:
        bucket, key = split_s3_uri(locator)
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key, "Body": payload}
        if content_type:
            kwargs["ContentType"] = content_type
        try:
            self.s3_client.put_object(**kwargs)
        except TypeError:
            if "ContentType" not in kwargs:
                raise
            kwargs.pop("ContentType")
            self.s3_client.put_object(**kwargs)

    def upload_file(self, source_path: str | Path, locator: str) -> None:
        bucket, key = split_s3_uri(locator)
        self.s3_client.upload_file(str(source_path), bucket, key)

    def download_file(self, locator: str, destination_path: str | Path) -> None:
        bucket, key = split_s3_uri(locator)
        self.s3_client.download_file(bucket, key, str(destination_path))

    def iter_object_locators(self, prefix_locator: str) -> Iterable[str]:
        bucket, prefix = split_s3_uri(prefix_locator)
        prefix = _prefix_key(prefix)
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                if key and not key.endswith("/"):
                    yield f"s3://{bucket}/{key}"

    def iter_child_prefix_locators(self, prefix_locator: str) -> Iterable[str]:
        bucket, prefix = split_s3_uri(prefix_locator)
        prefix = _prefix_key(prefix)
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for child in page.get("CommonPrefixes", []):
                child_prefix = str(child.get("Prefix", "")).rstrip("/")
                if child_prefix:
                    yield f"s3://{bucket}/{child_prefix}"

    def prefix_has_objects(self, prefix_locator: str) -> bool:
        bucket, prefix = split_s3_uri(prefix_locator)
        prefix = _prefix_key(prefix)
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        except AttributeError:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                return bool(page.get("Contents"))
            return False
        return bool(response.get("KeyCount") or response.get("Contents"))


class S3QueueBackend:
    """Queue backend that reads YAML specs from an S3 prefix."""

    def __init__(self, s3_client: Any, queue_prefix: str) -> None:
        self.s3_client = s3_client
        self.queue_prefix = queue_prefix

    def list_specs(self, *, states: Iterable[str]) -> list[QueueEntry]:
        bucket, prefix = split_s3_uri(self.queue_prefix)
        queue_root = prefix.strip("/")
        entries: list[QueueEntry] = []
        for state in states:
            state_prefix = f"{queue_root}/{state}/" if queue_root else f"{state}/"
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
                    display_key = key[len(state_prefix) :] if key.startswith(state_prefix) else key.rsplit("/", 1)[-1]
                    entries.append(
                        QueueEntry(
                            state=str(state),
                            key=display_key,
                            priority=priority,
                            last_modified=_ensure_utc(obj["LastModified"]),
                        )
                    )
        return entries


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _prefix_key(key: str) -> str:
    text = str(key or "").strip("/")
    return f"{text}/" if text else ""


__all__ = [
    "S3ObjectStore",
    "S3QueueBackend",
    "create_boto3_session",
    "create_s3_client",
    "split_s3_uri",
]
