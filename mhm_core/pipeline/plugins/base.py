"""Profile plugin contract for source-specific pipeline behavior."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, Type

if TYPE_CHECKING:  # pragma: no cover
    from ..observers import PipelineObserver
    from ..publishing import PipelinePublisher
    from ..steps.base import PipelineStep


class PipelineProfilePlugin(ABC):
    """Register source-specific step implementations."""

    profile_id: str
    include_base_profile: bool = True

    @abstractmethod
    def register_steps(self, registry: Dict[str, Type["PipelineStep"]]) -> None:
        """Populate or override step handlers in the supplied registry."""

    def create_observer(self) -> "PipelineObserver | None":
        """Return an optional observer for profile-specific side effects."""
        return None

    def create_publisher(self) -> "PipelinePublisher | None":
        """Return an optional publisher for profile-specific output handling."""
        return None

    def validate_spec(self, spec) -> list[str]:
        """Return profile-specific validation errors for a loaded run spec."""
        return []
