"""Generic pipeline capability declarations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Set


@dataclass(frozen=True)
class RefreshSourceCapability:
    """Declare that a step controls source refresh policy."""

    refresh_options: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CacheRefreshCapability:
    """Declare that cached outputs can drive input refresh requirements."""

    cache_policy_options: Mapping[str, Any] = field(default_factory=dict)
    metric_names: Set[str] = field(default_factory=set)


@dataclass(frozen=True)
class EntitySelectionCapability:
    """Declare entity selection requirements for a step."""

    required_source_metrics: Set[str] = field(default_factory=set)
    skip_completed_resume: bool = False
    label: str = "source-metric"


@dataclass(frozen=True)
class PipelineStepCapabilities:
    """Optional capabilities exposed by a pipeline step."""

    refresh_source: Optional[RefreshSourceCapability] = None
    cache_refresh: Optional[CacheRefreshCapability] = None
    entity_selection: Optional[EntitySelectionCapability] = None
    participant_selection: Optional[EntitySelectionCapability] = None

    def __post_init__(self) -> None:
        if self.entity_selection is None and self.participant_selection is not None:
            object.__setattr__(self, "entity_selection", self.participant_selection)
        elif self.participant_selection is None and self.entity_selection is not None:
            object.__setattr__(self, "participant_selection", self.entity_selection)


ParticipantSelectionCapability = EntitySelectionCapability


__all__ = [
    "CacheRefreshCapability",
    "EntitySelectionCapability",
    "ParticipantSelectionCapability",
    "PipelineStepCapabilities",
    "RefreshSourceCapability",
]
