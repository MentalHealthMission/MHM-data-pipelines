"""Observer hooks for pipeline lifecycle side effects."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .publishing import ParticipantPublishResult
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
        s3_uri: str,
    ) -> None:
        return None

    def should_publish_participant_manifest(
        self,
        context,
        *,
        participant_id: str,
        site: str,
        merged_uploads: List[tuple[Path, str]],
        merged_local: Path,
    ) -> bool:
        return True

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
        s3_uri: str,
    ) -> None:
        for observer in self._observers:
            observer.record_published_merged_artifact(
                context,
                site=site,
                participant_id=participant_id,
                metric=metric,
                file_path=file_path,
                s3_uri=s3_uri,
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
        merged_uploads: List[tuple[Path, str]],
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

    def after_participant_publish(
        self,
        context,
        *,
        result: "ParticipantPublishResult",
    ) -> None:
        for observer in self._observers:
            observer.after_participant_publish(context, result=result)


__all__ = [
    "CompositePipelineObserver",
    "NoOpPipelineObserver",
    "PipelineObserver",
]
