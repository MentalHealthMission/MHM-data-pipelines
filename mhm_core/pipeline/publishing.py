"""Generic publish contracts and lifecycle payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set


@dataclass(frozen=True)
class PublishedArtifact:
    """A local file published to an external or downstream locator."""

    file_path: Path
    locator: str


@dataclass
class PublishResult:
    """Aggregate result for one publish operation."""

    files: int = 0
    bytes: int = 0
    keys: List[str] = field(default_factory=list)
    uploads: List[PublishedArtifact] = field(default_factory=list)

    def merge(self, other: "PublishResult") -> "PublishResult":
        self.files += other.files
        self.bytes += other.bytes
        self.keys.extend(other.keys)
        self.uploads.extend(other.uploads)
        return self


@dataclass(frozen=True)
class RunPublishArtifact:
    """Additional run-level artifact exposed by a profile or observer."""

    file_path: Path
    destination_name: Optional[str] = None


@dataclass(frozen=True)
class ParticipantPublishTarget:
    """A profile-declared participant-level output target."""

    name: str
    local_root: Path
    destination: str
    filter_prefix: str = ""
    collect_keys: bool = False
    collect_uploads: bool = False
    include_top_level_dirs: Optional[Set[str]] = None
    remove_after_publish: bool = False


@dataclass(frozen=True)
class ParticipantPublishResult:
    """Outputs uploaded for one participant during a publish step."""

    participant_id: str
    site: str
    merged_uploads: List[PublishedArtifact] = field(default_factory=list)
    target_results: Dict[str, PublishResult] = field(default_factory=dict)
    participant_manifest_published: bool = False

    def target_keys(self, target_name: str) -> List[str]:
        result = self.target_results.get(target_name)
        return list(result.keys) if result else []


class PipelinePublisher:
    """Destination adapter used by the generic publish step.

    The core publish step decides what logical outputs exist. Concrete
    publishers decide how those outputs are materialized, uploaded, or ignored.
    """

    def publish_tree(
        self,
        context,
        *,
        local_root: Path,
        destination: str,
        filter_prefix: str = "",
        collect_keys: bool = False,
        collect_uploads: bool = False,
        include_top_level_dirs: Optional[Set[str]] = None,
    ) -> PublishResult:
        return PublishResult()

    def publish_file(self, context, *, file_path: Path, destination: str) -> PublishResult:
        return PublishResult()

    def publish_participant_manifest(
        self,
        context,
        *,
        participant_id: str,
        site: str,
        merged_local: Path,
        merged_uploads: List[PublishedArtifact],
        should_publish: bool,
    ) -> bool:
        return False


class NoOpPipelinePublisher(PipelinePublisher):
    """Publisher for core-only runs that do not configure an output backend."""


__all__ = [
    "NoOpPipelinePublisher",
    "ParticipantPublishResult",
    "ParticipantPublishTarget",
    "PipelinePublisher",
    "PublishResult",
    "PublishedArtifact",
    "RunPublishArtifact",
]
