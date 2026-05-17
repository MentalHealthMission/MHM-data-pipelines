"""Generic publish lifecycle payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class ParticipantPublishResult:
    """Outputs uploaded for one participant during a publish step."""

    participant_id: str
    site: str
    merged_uploads: List[tuple[Path, str]] = field(default_factory=list)
    summary_keys: List[str] = field(default_factory=list)
    latest_measurement_keys: List[str] = field(default_factory=list)
    participant_manifest_published: bool = False


__all__ = ["ParticipantPublishResult"]
