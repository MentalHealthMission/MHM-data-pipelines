"""Profile plugin contract for source-specific pipeline behavior."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, Type

if TYPE_CHECKING:  # pragma: no cover
    from ..steps.base import PipelineStep


class PipelineProfilePlugin(ABC):
    """Register source-specific step implementations."""

    profile_id: str

    @abstractmethod
    def register_steps(self, registry: Dict[str, Type["PipelineStep"]]) -> None:
        """Populate or override step handlers in the supplied registry."""
