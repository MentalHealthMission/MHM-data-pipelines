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
class RunPublishTarget:
    """Profile-declared run-level file publication target."""

    name: str
    file_path: Path
    destination: str
    required: bool = True


@dataclass(frozen=True)
class RunPublishArtifact:
    """Compatibility shape for additional run-level artifacts.

    New observers should prefer `RunPublishTarget`, which makes destination
    ownership explicit at the declaration site.
    """

    file_path: Path
    destination_name: Optional[str] = None


@dataclass(frozen=True)
class EntityPublishTarget:
    """A profile-declared entity-level output target."""

    name: str
    local_root: Path
    destination: str
    filter_prefix: str = ""
    collect_keys: bool = False
    collect_uploads: bool = False
    include_top_level_dirs: Optional[Set[str]] = None
    remove_after_publish: bool = False
    publish_entity_manifest: bool = False
    record_published_artifacts: bool = False
    primary_output: bool = False


@dataclass(frozen=True)
class EntityPublishResult:
    """Outputs uploaded for one entity during a publish step."""

    entity_id: str
    group: str
    primary_uploads: List[PublishedArtifact] = field(default_factory=list)
    target_results: Dict[str, PublishResult] = field(default_factory=dict)
    entity_manifest_published: bool = False

    def target_keys(self, target_name: str) -> List[str]:
        result = self.target_results.get(target_name)
        return list(result.keys) if result else []

    @property
    def participant_id(self) -> str:
        return self.entity_id

    @property
    def site(self) -> str:
        return self.group

    @property
    def merged_uploads(self) -> List[PublishedArtifact]:
        return self.primary_uploads

    @property
    def participant_manifest_published(self) -> bool:
        return self.entity_manifest_published


class PipelinePublisher:
    """Destination adapter used by the generic publish step.

    Profile observers decide what logical outputs exist. Concrete publishers
    decide how those outputs are materialized, uploaded, or ignored.
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

    def publish_entity_manifest(
        self,
        context,
        *,
        entity_id: str,
        group: str,
        output_local: Path,
        published_artifacts: List[PublishedArtifact],
        should_publish: bool,
        target_name: str,
    ) -> bool:
        return self.publish_participant_manifest(
            context,
            participant_id=entity_id,
            site=group,
            merged_local=output_local,
            merged_uploads=published_artifacts,
            should_publish=should_publish,
        )

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


ParticipantPublishResult = EntityPublishResult
ParticipantPublishTarget = EntityPublishTarget


__all__ = [
    "EntityPublishResult",
    "EntityPublishTarget",
    "NoOpPipelinePublisher",
    "ParticipantPublishResult",
    "ParticipantPublishTarget",
    "PipelinePublisher",
    "PublishResult",
    "PublishedArtifact",
    "RunPublishArtifact",
    "RunPublishTarget",
]
