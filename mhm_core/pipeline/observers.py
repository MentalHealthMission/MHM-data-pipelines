"""Observer hooks for pipeline lifecycle side effects."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .publishing import (
        EntityPublishResult,
        EntityPublishTarget,
        ParticipantPublishResult,
        ParticipantPublishTarget,
        PublishedArtifact,
        RunPublishArtifact,
        RunPublishTarget,
    )
    from .steps.base import PipelineStep


class PipelineObserver:
    """No-op lifecycle observer for pipeline execution.

    Subclasses can attach side effects such as provenance capture without the
    runner depending on their implementation package.
    """

    def on_run_start(self, context) -> None:
        return None

    def before_step(
        self,
        context,
        *,
        step: "PipelineStep",
        step_index: int,
    ) -> object:
        return None

    def after_step(
        self,
        context,
        *,
        step: "PipelineStep",
        step_index: int,
        metrics: Optional[Dict[str, object]] = None,
        pre_step_state: object = None,
    ) -> None:
        return None

    def record_published_merged_artifact(
        self,
        context,
        *,
        site: str,
        participant_id: str,
        metric: str,
        file_path: Path,
        locator: str,
    ) -> None:
        return None

    def record_published_artifact(
        self,
        context,
        *,
        entity_id: str,
        group: str,
        target_name: str,
        artifact_kind: str,
        file_path: Path,
        locator: str,
    ) -> None:
        self.record_published_merged_artifact(
            context,
            site=group,
            participant_id=entity_id,
            metric=artifact_kind,
            file_path=file_path,
            locator=locator,
        )

    def should_publish_entity_manifest(
        self,
        context,
        *,
        entity_id: str,
        group: str,
        target_name: str,
        published_artifacts: List["PublishedArtifact"],
        output_local: Path,
    ) -> bool:
        return self.should_publish_participant_manifest(
            context,
            participant_id=entity_id,
            site=group,
            merged_uploads=published_artifacts,
            merged_local=output_local,
        )

    def should_publish_participant_manifest(
        self,
        context,
        *,
        participant_id: str,
        site: str,
        merged_uploads: List["PublishedArtifact"],
        merged_local: Path,
    ) -> bool:
        return True

    def run_publish_artifacts(self, context) -> List["RunPublishArtifact"]:
        return []

    def run_publish_targets(self, context, *, logs_prefix: str) -> List["RunPublishTarget"]:
        from .publishing import RunPublishTarget

        targets: List["RunPublishTarget"] = []
        for artifact in self.run_publish_artifacts(context):
            destination_name = artifact.destination_name or artifact.file_path.name
            targets.append(
                RunPublishTarget(
                    name=Path(destination_name).stem or "run_artifact",
                    file_path=artifact.file_path,
                    destination=f"{logs_prefix.rstrip('/')}/{destination_name}",
                    required=False,
                )
            )
        return targets

    def participant_publish_targets(
        self,
        context,
        *,
        participant_id: str,
        site: str,
    ) -> List["ParticipantPublishTarget"]:
        return []

    def entity_publish_targets(
        self,
        context,
        *,
        entity_id: str,
        group: str,
    ) -> List["EntityPublishTarget"]:
        return self.participant_publish_targets(
            context,
            participant_id=entity_id,
            site=group,
        )

    def finalize_run(
        self,
        context,
        *,
        run_manifest_path: Path,
        metrics_path: Path,
    ) -> Optional[Path]:
        return None

    def after_participant_publish(
        self,
        context,
        *,
        result: "ParticipantPublishResult",
    ) -> None:
        return None

    def after_entity_publish(
        self,
        context,
        *,
        result: "EntityPublishResult",
    ) -> None:
        self.after_participant_publish(context, result=result)


class NoOpPipelineObserver(PipelineObserver):
    """Explicit no-op observer used by generic pipeline execution."""


class CompositePipelineObserver(PipelineObserver):
    """Fan out lifecycle hooks to multiple observers."""

    def __init__(self, observers: Iterable[PipelineObserver]) -> None:
        self._observers: List[PipelineObserver] = list(observers)

    @property
    def observers(self) -> tuple[PipelineObserver, ...]:
        return tuple(self._observers)

    def on_run_start(self, context) -> None:
        for observer in self._observers:
            observer.on_run_start(context)

    def before_step(
        self,
        context,
        *,
        step: "PipelineStep",
        step_index: int,
    ) -> object:
        return [
            observer.before_step(context, step=step, step_index=step_index)
            for observer in self._observers
        ]

    def after_step(
        self,
        context,
        *,
        step: "PipelineStep",
        step_index: int,
        metrics: Optional[Dict[str, object]] = None,
        pre_step_state: object = None,
    ) -> None:
        states = pre_step_state if isinstance(pre_step_state, list) else []
        for index, observer in enumerate(self._observers):
            observer_state = states[index] if index < len(states) else None
            observer.after_step(
                context,
                step=step,
                step_index=step_index,
                metrics=metrics,
                pre_step_state=observer_state,
            )

    def record_published_merged_artifact(
        self,
        context,
        *,
        site: str,
        participant_id: str,
        metric: str,
        file_path: Path,
        locator: str,
    ) -> None:
        for observer in self._observers:
            observer.record_published_merged_artifact(
                context,
                site=site,
                participant_id=participant_id,
                metric=metric,
                file_path=file_path,
                locator=locator,
            )

    def record_published_artifact(
        self,
        context,
        *,
        entity_id: str,
        group: str,
        target_name: str,
        artifact_kind: str,
        file_path: Path,
        locator: str,
    ) -> None:
        for observer in self._observers:
            observer.record_published_artifact(
                context,
                entity_id=entity_id,
                group=group,
                target_name=target_name,
                artifact_kind=artifact_kind,
                file_path=file_path,
                locator=locator,
            )

    def finalize_run(
        self,
        context,
        *,
        run_manifest_path: Path,
        metrics_path: Path,
    ) -> Optional[Path]:
        result: Optional[Path] = None
        for observer in self._observers:
            observer_result = observer.finalize_run(
                context,
                run_manifest_path=run_manifest_path,
                metrics_path=metrics_path,
            )
            if observer_result is not None:
                result = observer_result
        return result

    def should_publish_participant_manifest(
        self,
        context,
        *,
        participant_id: str,
        site: str,
        merged_uploads: List["PublishedArtifact"],
        merged_local: Path,
    ) -> bool:
        return all(
            observer.should_publish_participant_manifest(
                context,
                participant_id=participant_id,
                site=site,
                merged_uploads=merged_uploads,
                merged_local=merged_local,
            )
            for observer in self._observers
        )

    def should_publish_entity_manifest(
        self,
        context,
        *,
        entity_id: str,
        group: str,
        target_name: str,
        published_artifacts: List["PublishedArtifact"],
        output_local: Path,
    ) -> bool:
        return all(
            observer.should_publish_entity_manifest(
                context,
                entity_id=entity_id,
                group=group,
                target_name=target_name,
                published_artifacts=published_artifacts,
                output_local=output_local,
            )
            for observer in self._observers
        )

    def run_publish_artifacts(self, context) -> List["RunPublishArtifact"]:
        artifacts: List["RunPublishArtifact"] = []
        for observer in self._observers:
            artifacts.extend(observer.run_publish_artifacts(context))
        return artifacts

    def run_publish_targets(self, context, *, logs_prefix: str) -> List["RunPublishTarget"]:
        targets: List["RunPublishTarget"] = []
        for observer in self._observers:
            targets.extend(observer.run_publish_targets(context, logs_prefix=logs_prefix))
        return targets

    def participant_publish_targets(
        self,
        context,
        *,
        participant_id: str,
        site: str,
    ) -> List["ParticipantPublishTarget"]:
        targets: List["ParticipantPublishTarget"] = []
        for observer in self._observers:
            targets.extend(
                observer.participant_publish_targets(
                    context,
                    participant_id=participant_id,
                    site=site,
                )
            )
        return targets

    def entity_publish_targets(
        self,
        context,
        *,
        entity_id: str,
        group: str,
    ) -> List["EntityPublishTarget"]:
        targets: List["EntityPublishTarget"] = []
        for observer in self._observers:
            targets.extend(
                observer.entity_publish_targets(
                    context,
                    entity_id=entity_id,
                    group=group,
                )
            )
        return targets

    def after_participant_publish(
        self,
        context,
        *,
        result: "ParticipantPublishResult",
    ) -> None:
        for observer in self._observers:
            observer.after_participant_publish(context, result=result)

    def after_entity_publish(
        self,
        context,
        *,
        result: "EntityPublishResult",
    ) -> None:
        for observer in self._observers:
            observer.after_entity_publish(context, result=result)


__all__ = [
    "CompositePipelineObserver",
    "NoOpPipelineObserver",
    "PipelineObserver",
]
