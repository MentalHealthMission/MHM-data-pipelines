"""Refresh plan helpers for orchestrating download/cache policies."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Optional

from connect_summary.summary_v2 import load_summary_spec

from .spec import RunSpec, StepSpec

SUMMARY_STEP_TYPES = ("summary", "connect.summary", "summary_v2", "connect.summary_v2")
DOWNLOAD_STEP_TYPES = ("download", "connect.download")


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


def parse_refresh_rule(options: Optional[dict]) -> RefreshRule:
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


def build_refresh_plan(spec: RunSpec) -> tuple[RefreshPlan, SummaryCachePolicy]:
    download_step = _locate_first_step(spec.processing.steps, DOWNLOAD_STEP_TYPES)
    summary_step = _locate_first_step(spec.processing.steps, SUMMARY_STEP_TYPES)

    download_refresh = parse_refresh_rule(download_step.options.get("refresh") if download_step else None)
    plan = RefreshPlan(default_rule=download_refresh)

    if download_step:
        metric_overrides = download_step.options.get("refresh", {}).get("per_metric", {})
        for metric, opts in metric_overrides.items():
            plan.metric_rules[metric] = parse_refresh_rule(opts)
        participant_overrides = download_step.options.get("refresh", {}).get("per_participant", {})
        for participant, opts in participant_overrides.items():
            plan.participant_rules[participant] = parse_refresh_rule(opts)

    summary_policy = SummaryCachePolicy()
    if summary_step:
        cache_opts = summary_step.options.get("cache_policy", {})
        summary_policy.manifest_prefix = cache_opts.get("manifest_prefix")
        summary_policy.reuse_enabled = bool(cache_opts.get("reuse", False))
        summary_policy.refresh_rule = parse_refresh_rule(cache_opts.get("refresh"))

        if summary_policy.refresh_rule and summary_policy.refresh_rule.mode != "incremental":
            metrics = _collect_summary_metrics(summary_step, run_id=spec.run_id)
            for metric in metrics:
                existing = plan.metric_rules.get(metric, plan.default_rule)
                plan.metric_rules[metric] = merge_rules(existing, summary_policy.refresh_rule)

    return plan, summary_policy


def _locate_first_step(steps: Iterable[StepSpec], step_types: Iterable[str]) -> Optional[StepSpec]:
    wanted = set(step_types)
    for step in steps:
        if step.type in wanted:
            return step
    return None


def _collect_summary_metrics(summary_step: StepSpec, *, run_id: str) -> set[str]:
    if summary_step.type in {"summary_v2", "connect.summary_v2"}:
        return _collect_summary_v2_metrics(summary_step, run_id=run_id)

    metrics: set[str] = set()
    for collection in ("features", "questionnaires", "questionnaire_sliders", "questionnaire_histograms"):
        for item in summary_step.options.get(collection, []):
            flag = item.get("flag") if isinstance(item, dict) else str(item)
            if not flag:
                continue
            parts = flag.split(":")
            if len(parts) >= 2:
                metrics.add(parts[1])
    return metrics


def _collect_summary_v2_metrics(summary_step: StepSpec, *, run_id: str) -> set[str]:
    spec_path_raw = summary_step.options.get("spec")
    if not spec_path_raw:
        return set()
    spec_path = Path(str(spec_path_raw).format(run_id=run_id)).expanduser()
    if not spec_path.exists():
        return set()

    overrides: Dict[str, object] = {}
    if "input_dir" in summary_step.options:
        overrides["input_dir"] = str(summary_step.options["input_dir"]).format(run_id=run_id)
    if "output_dir" in summary_step.options:
        overrides["output_dir"] = str(summary_step.options["output_dir"]).format(run_id=run_id)
    if "time_resolution" in summary_step.options:
        overrides["time_resolution"] = summary_step.options["time_resolution"]
    if "participants" in summary_step.options:
        overrides["participants"] = summary_step.options["participants"]

    try:
        loaded = load_summary_spec(spec_path, overrides=overrides)
    except Exception:
        return set()

    metrics: set[str] = set()
    for feature in loaded.get("feature_defs", []):
        source = str((feature or {}).get("source") or "").strip()
        if source:
            metrics.add(source)
    for questionnaire in loaded.get("questionnaire_defs", []):
        file_filter = str((questionnaire or {}).get("file_filter") or "").strip()
        if file_filter:
            metrics.add(file_filter)
    for slider in loaded.get("slider_defs", []):
        file_filter = str((slider or {}).get("file_filter") or "").strip()
        if file_filter:
            metrics.add(file_filter)
    for histogram in loaded.get("histogram_defs", []):
        file_filter = str((histogram or {}).get("file_filter") or "").strip()
        if file_filter:
            metrics.add(file_filter)
    return metrics


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
