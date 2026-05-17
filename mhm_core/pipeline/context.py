"""Execution context helpers for the core pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
import logging
from typing import Any

import boto3

from .spec import RunSpec
from .discovery import discover_participants
from .extensions import PipelineExtensionRegistry
from .manifest import ParticipantManifest, load_participant_manifest
from .observers import NoOpPipelineObserver, PipelineObserver
from .publishing import NoOpPipelinePublisher, PipelinePublisher
from .refresh_plan import RefreshPlan, SummaryCachePolicy
from .latest_measurement_manifest import LatestMeasurementManifest, load_latest_measurement_manifest
from .summary_manifest import SummaryManifest, load_summary_manifest


@dataclass
class SummaryState:
    reused: bool
    local_files: List[Path]
    source_watermarks: Dict[str, str]


@dataclass
class LatestMeasurementState:
    reused: bool
    local_files: List[Path]
    source_watermarks: Dict[str, str]
    results: Dict[str, object]


@dataclass
class RunContext:
    spec: RunSpec
    run_id: str
    workspace_dir: Path
    raw_dir: Path
    merged_dir: Path
    summary_dir: Path
    latest_measurement_dir: Path
    logs_dir: Path
    s3_client: boto3.client
    start_time: datetime = field(default_factory=datetime.utcnow)
    metrics: Dict[str, object] = field(default_factory=dict)
    entity_groups: Dict[str, str] = field(default_factory=dict)
    batch_entities: Optional[List[str]] = None
    current_entity: Optional[str] = None
    participant_sites: Dict[str, str] = field(default_factory=dict)
    batch_participants: Optional[List[str]] = None
    current_participant: Optional[str] = None
    participant_manifests: Dict[str, ParticipantManifest] = field(default_factory=dict)
    merged_base_prefix: str = ""
    refresh_plan: Optional[RefreshPlan] = None
    summary_cache_policy: Optional[SummaryCachePolicy] = None
    summary_manifest_prefix: Optional[str] = None
    summary_manifests: Dict[str, SummaryManifest] = field(default_factory=dict)
    summary_outputs: Dict[str, SummaryState] = field(default_factory=dict)
    latest_measurement_manifest_prefix: Optional[str] = None
    latest_measurement_output_prefix: Optional[str] = None
    latest_measurement_manifests: Dict[str, LatestMeasurementManifest] = field(default_factory=dict)
    latest_measurement_outputs: Dict[str, LatestMeasurementState] = field(default_factory=dict)
    extensions: PipelineExtensionRegistry = field(default_factory=PipelineExtensionRegistry)
    spec_locator: str = ""
    provenance_dir: Optional[Path] = None
    pipeline_spec_manifest_path: Optional[Path] = None
    source_state_manifest_path: Optional[str] = None
    published_merged_artifacts: List[Dict[str, Any]] = field(default_factory=list)
    step_state_bindings: Dict[str, str] = field(default_factory=dict)
    published_dataset_manifest_path: Optional[str] = None
    merged_metrics_to_publish: Dict[str, Set[str]] = field(default_factory=dict)
    pipeline_observer: PipelineObserver = field(default_factory=NoOpPipelineObserver)
    pipeline_publisher: PipelinePublisher = field(default_factory=NoOpPipelinePublisher)
    logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        self.logger = logging.getLogger(f"mhm_core.pipeline.{self.run_id}")
        self.logger.setLevel(logging.INFO)
        if self.participant_sites and not self.entity_groups:
            self.entity_groups.update(self.participant_sites)
        if self.entity_groups and not self.participant_sites:
            self.participant_sites.update(self.entity_groups)
        _bind_compat_extension_state(self)

    def ensure_directories(self) -> None:
        for path in (
            self.workspace_dir,
            self.raw_dir,
            self.merged_dir,
            self.summary_dir,
            self.latest_measurement_dir,
            self.logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        if self.provenance_dir is not None:
            self.provenance_dir.mkdir(parents=True, exist_ok=True)


def create_run_context(
    spec: RunSpec,
    *,
    boto3_session: Optional[boto3.session.Session] = None,
    spec_locator: str = "",
) -> RunContext:
    session = boto3_session or boto3.session.Session()
    s3_client = session.client("s3")

    run_dir = spec.workspace.resolve_run_path(spec.run_id)
    raw_dir = run_dir / "raw"
    merged_dir = run_dir / "merged"
    summary_dir = run_dir / "summary"
    latest_measurement_dir = run_dir / "latest_measurement_dates"
    logs_dir = run_dir / "logs"

    merged_base_prefix = resolve_output_base_prefix(spec.outputs.merged_prefix, run_id=spec.run_id)

    context = RunContext(
        spec=spec,
        run_id=spec.run_id,
        workspace_dir=run_dir,
        raw_dir=raw_dir,
        merged_dir=merged_dir,
        summary_dir=summary_dir,
        latest_measurement_dir=latest_measurement_dir,
        logs_dir=logs_dir,
        s3_client=s3_client,
        merged_base_prefix=merged_base_prefix,
        spec_locator=spec_locator,
        provenance_dir=logs_dir / "provenance",
    )
    context.ensure_directories()

    log_file = context.logs_dir / "pipeline.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
    context.logger.addHandler(file_handler)

    group_map = getattr(spec.source, "entity_group_map", None)
    if isinstance(group_map, dict):
        set_entity_groups(context, group_map)
    if spec.source.discover_all and not spec.source.participants:
        participants, discovered_map = discover_participants(
            s3_client,
            bucket=spec.source.bucket,
            prefix=spec.source.prefix,
            logger=context.logger,
            sites=getattr(spec.source, "sites", None),
        )
        spec.source.participants = participants
        spec.source.entity_group_map.update(discovered_map)
        set_entity_groups(context, discovered_map)
        context.logger.info("[spec] Discovered %d participants via discover_all", len(participants))

    return context


def resolve_output_base_prefix(template: str, *, run_id: str) -> str:
    rendered = template.format(
        run_id=run_id,
        site="{site}",
        participant_id="{participant_id}",
        participant="{participant}",
        group="{group}",
        entity_id="{entity_id}",
        entity="{entity}",
    )
    if "{site}" in rendered:
        rendered = rendered.split("{site}", 1)[0]
    if "{group}" in rendered:
        rendered = rendered.split("{group}", 1)[0]
    return rendered.rstrip("/") or rendered


def resolve_output_prefix(template: str, *, run_id: str) -> str:
    return template.format(
        run_id=run_id,
        site="",
        participant_id="",
        participant="",
        group="",
        entity_id="",
        entity="",
    ).rstrip("/")


def ensure_participant_manifest(context: RunContext, participant_id: str) -> ParticipantManifest:
    if participant_id not in context.participant_manifests:
        site = context.participant_sites.get(participant_id)
        if not site:
            raise KeyError(f"Site unknown for participant {participant_id}; cannot load manifest")
        manifest = load_participant_manifest(
            context.s3_client,
            site=site,
            participant_id=participant_id,
            base_prefix=context.merged_base_prefix,
        )
        context.participant_manifests[participant_id] = manifest
    return context.participant_manifests[participant_id]


def set_entity_groups(context: RunContext, mapping: Dict[str, str]) -> None:
    cleaned = {
        str(entity_id).strip(): str(group).strip()
        for entity_id, group in mapping.items()
        if str(entity_id).strip() and str(group).strip()
    }
    if not hasattr(context, "entity_groups"):
        setattr(context, "entity_groups", {})
    if not hasattr(context, "participant_sites"):
        setattr(context, "participant_sites", {})
    context.entity_groups.update(cleaned)
    context.participant_sites.update(cleaned)


def entity_group(context: RunContext, entity_id: str) -> str:
    entity_groups_map = getattr(context, "entity_groups", {})
    participant_sites_map = getattr(context, "participant_sites", {})
    if entity_id in entity_groups_map:
        return entity_groups_map[entity_id]
    if entity_id in participant_sites_map:
        group = participant_sites_map[entity_id]
        if hasattr(context, "entity_groups"):
            context.entity_groups[entity_id] = group
        return group
    return ""


def extension_state(context: RunContext, namespace: str) -> Dict[str, Any]:
    registry = getattr(context, "extensions", None)
    if registry is None:
        registry = PipelineExtensionRegistry()
        setattr(context, "extensions", registry)
    return registry.namespace(namespace)  # type: ignore[return-value]


def summary_outputs(context: RunContext) -> Dict[str, SummaryState]:
    return extension_state(context, "summary").setdefault("outputs", context.summary_outputs)


def latest_measurement_outputs(context: RunContext) -> Dict[str, LatestMeasurementState]:
    return extension_state(context, "latest_measurement").setdefault("outputs", context.latest_measurement_outputs)


def step_state_bindings(context: RunContext) -> Dict[str, str]:
    return extension_state(context, "provenance").setdefault("step_state_bindings", context.step_state_bindings)


def published_merged_artifacts(context: RunContext) -> List[Dict[str, Any]]:
    return extension_state(context, "provenance").setdefault(
        "published_merged_artifacts",
        context.published_merged_artifacts,
    )


def _bind_compat_extension_state(context: RunContext) -> None:
    extension_state(context, "summary").setdefault("manifests", context.summary_manifests)
    extension_state(context, "summary").setdefault("outputs", context.summary_outputs)
    extension_state(context, "latest_measurement").setdefault("manifests", context.latest_measurement_manifests)
    extension_state(context, "latest_measurement").setdefault("outputs", context.latest_measurement_outputs)
    extension_state(context, "provenance").setdefault("published_merged_artifacts", context.published_merged_artifacts)
    extension_state(context, "provenance").setdefault("step_state_bindings", context.step_state_bindings)


def ensure_summary_manifest(context: RunContext, participant_id: str) -> SummaryManifest:
    if participant_id not in context.summary_manifests:
        site = context.participant_sites.get(participant_id)
        if not site:
            raise KeyError(f"Site unknown for participant {participant_id}; cannot load summary manifest")
        prefix = context.summary_manifest_prefix
        if not prefix:
            context.summary_manifests[participant_id] = SummaryManifest(participant_id=participant_id, site=site)
        else:
            manifest = load_summary_manifest(
                context.s3_client,
                site=site,
                participant_id=participant_id,
                manifest_prefix=prefix,
            )
            context.summary_manifests[participant_id] = manifest
    return context.summary_manifests[participant_id]


def ensure_latest_measurement_manifest(context: RunContext, participant_id: str) -> LatestMeasurementManifest:
    if participant_id not in context.latest_measurement_manifests:
        site = context.participant_sites.get(participant_id)
        if not site:
            raise KeyError(f"Site unknown for participant {participant_id}; cannot load latest-measurement manifest")
        prefix = context.latest_measurement_manifest_prefix
        if not prefix:
            context.latest_measurement_manifests[participant_id] = LatestMeasurementManifest(
                participant_id=participant_id,
                site=site,
            )
        else:
            manifest = load_latest_measurement_manifest(
                context.s3_client,
                site=site,
                participant_id=participant_id,
                manifest_prefix=prefix,
            )
            context.latest_measurement_manifests[participant_id] = manifest
    return context.latest_measurement_manifests[participant_id]


def active_participants(context: RunContext) -> List[str]:
    return active_entities(context)


def active_entities(context: RunContext) -> List[str]:
    current_entity = getattr(context, "current_entity", None)
    current_participant = getattr(context, "current_participant", None)
    batch_entities = getattr(context, "batch_entities", None)
    batch_participants = getattr(context, "batch_participants", None)
    entity_groups_map = getattr(context, "entity_groups", {})
    participant_sites_map = getattr(context, "participant_sites", {})
    if current_entity:
        return [current_entity]
    if current_participant:
        return [current_participant]
    if batch_entities:
        return list(batch_entities)
    if batch_participants:
        return list(batch_participants)
    if entity_groups_map:
        return list(entity_groups_map.keys())
    if participant_sites_map:
        return list(participant_sites_map.keys())
    if hasattr(context.spec, "iter_entities"):
        return list(context.spec.iter_entities())
    return list(context.spec.iter_participants())


__all__ = [
    "RunContext",
    "SummaryState",
    "LatestMeasurementState",
    "active_entities",
    "create_run_context",
    "active_participants",
    "entity_group",
    "ensure_participant_manifest",
    "ensure_summary_manifest",
    "ensure_latest_measurement_manifest",
    "extension_state",
    "latest_measurement_outputs",
    "published_merged_artifacts",
    "resolve_output_base_prefix",
    "resolve_output_prefix",
    "set_entity_groups",
    "step_state_bindings",
    "summary_outputs",
]
