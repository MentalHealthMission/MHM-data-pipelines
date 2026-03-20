"""Priority queue helpers for S3-backed pipeline specs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

import yaml

PRIORITY_RANK = {
    "low": 0,
    "medium": 1,
    "high": 2,
    "urgent": 3,
    "ludicrous": 4,
}
STATE_RANK = {
    "pending": 0,
    "suspended": 1,
}


@dataclass
class QueueEntry:
    state: str
    key: str
    priority: str
    last_modified: datetime


def normalize_priority(value: object) -> str:
    priority = str(value or "medium").strip().lower() or "medium"
    if priority not in PRIORITY_RANK:
        return "medium"
    return priority


def list_specs(s3_client, queue_prefix: str, *, states: Iterable[str]) -> list[QueueEntry]:
    bucket, prefix = split_s3_uri(queue_prefix)
    entries: list[QueueEntry] = []
    for state in states:
        state_prefix = f"{prefix.rstrip('/')}/{state}/"
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=state_prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                if not key or key.endswith("/"):
                    continue
                payload = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
                try:
                    data = yaml.safe_load(payload) or {}
                except Exception:
                    data = {}
                priority = normalize_priority(getattr(data, "get", lambda *_: None)("priority"))
                entries.append(
                    QueueEntry(
                        state=state,
                        key=key.split("/", 2)[-1],
                        priority=priority,
                        last_modified=_ensure_utc(obj["LastModified"]),
                    )
                )
    return entries


def select_next_spec(
    s3_client,
    queue_prefix: str,
    *,
    states: Iterable[str] = ("pending", "suspended"),
    minimum_priority: str | None = None,
) -> Optional[QueueEntry]:
    entries = list_specs(s3_client, queue_prefix, states=states)
    if minimum_priority:
        threshold = PRIORITY_RANK[normalize_priority(minimum_priority)]
        entries = [entry for entry in entries if PRIORITY_RANK[entry.priority] >= threshold]
    if not entries:
        return None
    return _sort_entries(entries)[0]


def has_pending_urgent(s3_client, queue_prefix: str) -> bool:
    return has_pending_priority(s3_client, queue_prefix, minimum_priority="urgent")


def has_pending_priority(s3_client, queue_prefix: str, *, minimum_priority: str) -> bool:
    pending = list_specs(s3_client, queue_prefix, states=("pending",))
    threshold = PRIORITY_RANK[normalize_priority(minimum_priority)]
    return any(PRIORITY_RANK[entry.priority] >= threshold for entry in pending)


def split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got {uri}")
    remainder = uri[len("s3://") :]
    bucket, _, key = remainder.partition("/")
    if not bucket:
        raise ValueError(f"Missing bucket in S3 URI: {uri}")
    return bucket, key


def _sort_entries(entries: list[QueueEntry]) -> list[QueueEntry]:
    return sorted(
        entries,
        key=lambda entry: (
            -PRIORITY_RANK[entry.priority],
            entry.last_modified,
            STATE_RANK.get(entry.state, 99),
            entry.key,
        ),
    )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


__all__ = [
    "PRIORITY_RANK",
    "QueueEntry",
    "has_pending_priority",
    "has_pending_urgent",
    "list_specs",
    "normalize_priority",
    "select_next_spec",
    "split_s3_uri",
]
