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
class ParticipantSelectionCapability:
    """Declare participant selection requirements for a step."""

    required_source_metrics: Set[str] = field(default_factory=set)
    skip_completed_resume: bool = False
    label: str = "source-metric"


@dataclass(frozen=True)
class PipelineStepCapabilities:
    """Optional capabilities exposed by a pipeline step."""

    refresh_source: Optional[RefreshSourceCapability] = None
    cache_refresh: Optional[CacheRefreshCapability] = None
    participant_selection: Optional[ParticipantSelectionCapability] = None


__all__ = [
    "CacheRefreshCapability",
    "ParticipantSelectionCapability",
    "PipelineStepCapabilities",
    "RefreshSourceCapability",
]
