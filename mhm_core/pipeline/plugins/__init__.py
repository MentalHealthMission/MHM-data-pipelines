"""Profile plugin loader."""

from __future__ import annotations

from importlib import import_module
from typing import Dict, List

from ..observers import CompositePipelineObserver, NoOpPipelineObserver, PipelineObserver
from .base import PipelineProfilePlugin

_PLUGIN_CLASS_MAP: Dict[str, str] = {
    "base": "mhm_core.profiles.base.pipeline_plugin:BasePipelineProfile",
    "connect": "connect_summary.profiles.connect.pipeline_plugin:ConnectPipelineProfile",
}


def _load_plugin(profile: str) -> PipelineProfilePlugin:
    target = _PLUGIN_CLASS_MAP.get(profile)
    if not target:
        raise ValueError(f"Unknown pipeline profile '{profile}'")
    module_name, _, class_name = target.partition(":")
    module = import_module(module_name)
    plugin_cls = getattr(module, class_name, None)
    if plugin_cls is None:
        raise ValueError(f"Invalid profile plugin target '{target}'")
    plugin = plugin_cls()
    if not isinstance(plugin, PipelineProfilePlugin):
        raise TypeError(f"Profile plugin '{target}' must implement PipelineProfilePlugin")
    return plugin


def load_profile_plugins(profile: str | None) -> List[PipelineProfilePlugin]:
    selected = str(profile or "connect").strip().lower() or "connect"
    if selected == "base":
        return [_load_plugin("base")]
    return [_load_plugin("base"), _load_plugin(selected)]


def load_profile_plugin(profile: str | None) -> PipelineProfilePlugin:
    """Backward-compatible single-plugin accessor."""
    plugins = load_profile_plugins(profile)
    return plugins[-1]


def load_pipeline_observer(profile: str | None) -> PipelineObserver:
    observers = [
        observer
        for plugin in load_profile_plugins(profile)
        if (observer := plugin.create_observer()) is not None
    ]
    if not observers:
        return NoOpPipelineObserver()
    if len(observers) == 1:
        return observers[0]
    return CompositePipelineObserver(observers)


__all__ = [
    "PipelineProfilePlugin",
    "load_pipeline_observer",
    "load_profile_plugin",
    "load_profile_plugins",
]
