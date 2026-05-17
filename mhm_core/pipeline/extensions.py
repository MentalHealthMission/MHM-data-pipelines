"""Structured extension state for pipeline runtimes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, MutableMapping


@dataclass
class PipelineExtensionRegistry:
    """Namespace-indexed extension state.

    This keeps profile-specific runtime state visible and movable without
    adding a new top-level RunContext field for every integration.
    """

    namespaces: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def namespace(self, name: str) -> MutableMapping[str, Any]:
        key = str(name).strip()
        if not key:
            raise ValueError("extension namespace must be non-empty")
        return self.namespaces.setdefault(key, {})


__all__ = ["PipelineExtensionRegistry"]
