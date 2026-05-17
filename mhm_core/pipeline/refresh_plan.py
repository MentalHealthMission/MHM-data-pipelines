"""Refresh plan helpers for orchestrating download/cache policies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional

from .capabilities import CacheRefreshCapability, RefreshSourceCapability
from .spec import RunSpec


@dataclass
class RefreshRule:
    """Declarative instruction describing how far back to fetch data."""

    mode: str = "incremental"  # incremental | full | relative
    anchor: Optional[str] = None  # e.g. start_of_month
    relative_days: Optional[int] = None  # rewind window relative to last watermark

    def priority(self) -> int:
        if self.mode == "full":
            return 3
        if self.mode == "relative":
            return 2
        return 1


@dataclass
class RefreshPlan:
    """Resolved plan for a run specifying refresh rules per scope."""

    default_rule: RefreshRule = field(default_factory=RefreshRule)
    metric_rules: Dict[str, RefreshRule] = field(default_factory=dict)
    participant_rules: Dict[str, RefreshRule] = field(default_factory=dict)


@dataclass
class SummaryCachePolicy:
    manifest_prefix: Optional[str] = None
    reuse_enabled: bool = False
    refresh_rule: Optional[RefreshRule] = None


def parse_refresh_rule(options: Optional[Mapping[str, Any]]) -> RefreshRule:
    if not options:
        return RefreshRule()
    mode = str(options.get("mode", "incremental")).lower()
    anchor = options.get("anchor")
    relative_days = options.get("relative_days")
    try:
        relative_days = int(relative_days) if relative_days is not None else None
    except (TypeError, ValueError):
        relative_days = None
    return RefreshRule(mode=mode, anchor=anchor, relative_days=relative_days)


def merge_rules(current: RefreshRule, new_rule: RefreshRule) -> RefreshRule:
    if new_rule.priority() > current.priority():
        return new_rule
    if new_rule.priority() == current.priority():
        if new_rule.mode == "relative":
            if _relative_days(new_rule) > _relative_days(current):
                return new_rule
            if new_rule.anchor and new_rule.anchor != current.anchor:
                # Prefer explicit anchor overrides.
                return new_rule
        if new_rule.mode == "incremental" and new_rule.anchor and not current.anchor:
            return new_rule
    return current


def build_refresh_plan(spec: RunSpec, *, steps: Optional[Iterable[Any]] = None) -> tuple[RefreshPlan, SummaryCachePolicy]:
    plan = RefreshPlan()
    summary_policy = SummaryCachePolicy()

    for step in steps or ():
        capabilities = step.describe_capabilities(spec)
        if capabilities.refresh_source:
            _apply_refresh_source(plan, capabilities.refresh_source)
        if capabilities.cache_refresh:
            _apply_cache_refresh(plan, summary_policy, capabilities.cache_refresh)

    return plan, summary_policy


def _apply_refresh_source(plan: RefreshPlan, capability: RefreshSourceCapability) -> None:
    refresh_options = capability.refresh_options or {}
    if not isinstance(refresh_options, Mapping):
        refresh_options = {}
    plan.default_rule = merge_rules(plan.default_rule, parse_refresh_rule(refresh_options))
    metric_overrides = refresh_options.get("per_metric", {}) if isinstance(refresh_options, Mapping) else {}
    if isinstance(metric_overrides, Mapping):
        for metric, opts in metric_overrides.items():
            if isinstance(opts, Mapping):
                metric_key = str(metric)
                existing = plan.metric_rules.get(metric_key, plan.default_rule)
                plan.metric_rules[metric_key] = merge_rules(existing, parse_refresh_rule(opts))
    participant_overrides = refresh_options.get("per_participant", {}) if isinstance(refresh_options, Mapping) else {}
    if isinstance(participant_overrides, Mapping):
        for participant, opts in participant_overrides.items():
            if isinstance(opts, Mapping):
                participant_key = str(participant)
                existing = plan.participant_rules.get(participant_key, plan.default_rule)
                plan.participant_rules[participant_key] = merge_rules(existing, parse_refresh_rule(opts))


def _apply_cache_refresh(
    plan: RefreshPlan,
    summary_policy: SummaryCachePolicy,
    capability: CacheRefreshCapability,
) -> None:
    cache_options = capability.cache_policy_options or {}
    if not isinstance(cache_options, Mapping):
        cache_options = {}
    summary_policy.manifest_prefix = str(cache_options.get("manifest_prefix", "") or "") or None
    summary_policy.reuse_enabled = bool(cache_options.get("reuse", False))
    refresh_options = cache_options.get("refresh") if isinstance(cache_options, Mapping) else None
    summary_policy.refresh_rule = parse_refresh_rule(refresh_options if isinstance(refresh_options, Mapping) else None)
    if summary_policy.refresh_rule.mode == "incremental":
        return
    for metric in capability.metric_names:
        existing = plan.metric_rules.get(metric, plan.default_rule)
        plan.metric_rules[metric] = merge_rules(existing, summary_policy.refresh_rule)


def _relative_days(rule: RefreshRule) -> int:
    if rule.relative_days is not None:
        return rule.relative_days
    # start_of_month etc treated as large window to favour override
    if rule.anchor == "start_of_month":
        return 32
    if rule.anchor == "start_of_week":
        return 7
    return 0


__all__ = ["RefreshPlan", "RefreshRule", "SummaryCachePolicy", "build_refresh_plan"]
