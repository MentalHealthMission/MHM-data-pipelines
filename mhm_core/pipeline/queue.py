"""Priority queue selection helpers for pipeline specs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional, Protocol

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


class QueueBackend(Protocol):
    """Backend contract for listing queued pipeline specifications."""

    def list_specs(self, *, states: Iterable[str]) -> list[QueueEntry]:
        """Return queue entries for the requested states."""


def normalize_priority(value: object) -> str:
    priority = str(value or "medium").strip().lower() or "medium"
    if priority not in PRIORITY_RANK:
        return "medium"
    return priority


def list_queue_specs(backend: QueueBackend, *, states: Iterable[str]) -> list[QueueEntry]:
    """List queue entries through a backend-neutral queue contract."""

    return backend.list_specs(states=states)


def select_next_queue_spec(
    backend: QueueBackend,
    *,
    states: Iterable[str] = ("pending", "suspended"),
    minimum_priority: str | None = None,
) -> Optional[QueueEntry]:
    """Select the next queue entry using generic priority rules."""

    entries = list_queue_specs(backend, states=states)
    if minimum_priority:
        threshold = PRIORITY_RANK[normalize_priority(minimum_priority)]
        entries = [entry for entry in entries if PRIORITY_RANK[entry.priority] >= threshold]
    if not entries:
        return None
    return _sort_entries(entries)[0]


def has_pending_queue_priority(backend: QueueBackend, *, minimum_priority: str) -> bool:
    pending = list_queue_specs(backend, states=("pending",))
    threshold = PRIORITY_RANK[normalize_priority(minimum_priority)]
    return any(PRIORITY_RANK[entry.priority] >= threshold for entry in pending)


def has_pending_urgent_queue_entry(backend: QueueBackend) -> bool:
    return has_pending_queue_priority(backend, minimum_priority="urgent")


def list_specs(s3_client, queue_prefix: str, *, states: Iterable[str]) -> list[QueueEntry]:
    """Compatibility wrapper for the historical S3-backed queue API."""

    from .backends.s3 import S3QueueBackend

    return list_queue_specs(S3QueueBackend(s3_client, queue_prefix), states=states)


def select_next_spec(
    s3_client,
    queue_prefix: str,
    *,
    states: Iterable[str] = ("pending", "suspended"),
    minimum_priority: str | None = None,
) -> Optional[QueueEntry]:
    """Compatibility wrapper for selecting from an S3-backed queue."""

    from .backends.s3 import S3QueueBackend

    return select_next_queue_spec(
        S3QueueBackend(s3_client, queue_prefix),
        states=states,
        minimum_priority=minimum_priority,
    )


def has_pending_urgent(s3_client, queue_prefix: str) -> bool:
    return has_pending_priority(s3_client, queue_prefix, minimum_priority="urgent")


def has_pending_priority(s3_client, queue_prefix: str, *, minimum_priority: str) -> bool:
    """Compatibility wrapper for the historical S3-backed queue API."""

    from .backends.s3 import S3QueueBackend

    return has_pending_queue_priority(S3QueueBackend(s3_client, queue_prefix), minimum_priority=minimum_priority)


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Compatibility wrapper for S3 queue URI parsing."""

    from .backends.s3 import split_s3_uri as _split_s3_uri

    return _split_s3_uri(uri)


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
    "QueueBackend",
    "has_pending_queue_priority",
    "has_pending_urgent_queue_entry",
    "has_pending_priority",
    "has_pending_urgent",
    "list_specs",
    "list_queue_specs",
    "normalize_priority",
    "select_next_spec",
    "select_next_queue_spec",
    "split_s3_uri",
]
