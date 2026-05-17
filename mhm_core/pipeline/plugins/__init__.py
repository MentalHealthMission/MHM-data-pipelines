"""Profile plugin loader."""

from __future__ import annotations

from importlib import import_module
from typing import Dict, List, Type

from ..observers import CompositePipelineObserver, NoOpPipelineObserver, PipelineObserver
from .base import PipelineProfilePlugin

PluginTarget = str | Type[PipelineProfilePlugin]

_BUILTIN_PLUGIN_CLASS_MAP: Dict[str, PluginTarget] = {
    "base": "mhm_core.profiles.base.pipeline_plugin:BasePipelineProfile",
    "ontology": "mhm_core.profiles.ontology.pipeline_plugin:OntologyPipelineProfile",
}

_PLUGIN_CLASS_MAP: Dict[str, PluginTarget] = dict(_BUILTIN_PLUGIN_CLASS_MAP)


def register_profile_plugin(profile: str, target: PluginTarget, *, replace: bool = True) -> None:
    """Register an application or integration profile plugin.

    Generic pipeline code owns the registry mechanism, but application packages
    own registration of their profiles. This keeps `mhm_core.pipeline` from
    hard-coding CONNECT or other downstream applications.
    """

    profile_id = _normalize_profile(profile)
    if not profile_id:
        raise ValueError("profile must be non-empty")
    if not replace and profile_id in _PLUGIN_CLASS_MAP:
        raise ValueError(f"Pipeline profile '{profile_id}' is already registered")
    _PLUGIN_CLASS_MAP[profile_id] = target


def registered_profile_plugins() -> Dict[str, PluginTarget]:
    """Return the currently registered profile plugin targets."""

    return dict(_PLUGIN_CLASS_MAP)


def _normalize_profile(profile: str | None) -> str:
    return str(profile or "").strip().lower()


def _load_plugin(profile: str) -> PipelineProfilePlugin:
    target = _PLUGIN_CLASS_MAP.get(profile)
    if not target:
        raise ValueError(f"Unknown pipeline profile '{profile}'")
    if isinstance(target, str):
        module_name, _, class_name = target.partition(":")
        module = import_module(module_name)
        plugin_cls = getattr(module, class_name, None)
    else:
        plugin_cls = target
    if plugin_cls is None:
        raise ValueError(f"Invalid profile plugin target '{target}'")
    plugin = plugin_cls()
    if not isinstance(plugin, PipelineProfilePlugin):
        raise TypeError(f"Profile plugin '{target}' must implement PipelineProfilePlugin")
    return plugin


def load_profile_plugins(profile: str | None, *, default_profile: str | None = None) -> List[PipelineProfilePlugin]:
    selected = _normalize_profile(profile) or _normalize_profile(default_profile) or "base"
    if selected == "base":
        return [_load_plugin("base")]
    return [_load_plugin("base"), _load_plugin(selected)]


def load_profile_plugin(profile: str | None, *, default_profile: str | None = None) -> PipelineProfilePlugin:
    """Backward-compatible single-plugin accessor."""
    plugins = load_profile_plugins(profile, default_profile=default_profile)
    return plugins[-1]


def load_pipeline_observer(profile: str | None, *, default_profile: str | None = None) -> PipelineObserver:
    observers = [
        observer
        for plugin in load_profile_plugins(profile, default_profile=default_profile)
        if (observer := plugin.create_observer()) is not None
    ]
    if not observers:
        return NoOpPipelineObserver()
    if len(observers) == 1:
        return observers[0]
    return CompositePipelineObserver(observers)


__all__ = [
    "PipelineProfilePlugin",
    "PluginTarget",
    "load_pipeline_observer",
    "load_profile_plugin",
    "load_profile_plugins",
    "registered_profile_plugins",
    "register_profile_plugin",
]
