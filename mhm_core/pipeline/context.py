"""Execution context helpers for the core pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import logging
from typing import Any

import boto3

from .spec import RunSpec
from .discovery import discover_participants
from .manifest import ParticipantManifest, load_participant_manifest
from .refresh_plan import RefreshPlan, SummaryCachePolicy
from .summary_manifest import SummaryManifest, load_summary_manifest


@dataclass
class SummaryState:
    reused: bool
    local_files: List[Path]
    source_watermarks: Dict[str, str]


@dataclass
class RunContext:
    spec: RunSpec
    run_id: str
    workspace_dir: Path
    raw_dir: Path
    merged_dir: Path
    summary_dir: Path
    logs_dir: Path
    s3_client: boto3.client
    start_time: datetime = field(default_factory=datetime.utcnow)
    metrics: Dict[str, object] = field(default_factory=dict)
    participant_sites: Dict[str, str] = field(default_factory=dict)
    batch_participants: Optional[List[str]] = None
    current_participant: Optional[str] = None
    participant_manifests: Dict[str, ParticipantManifest] = field(default_factory=dict)
    merged_base_prefix: str = "s3://connect-uom/merged-data"
    refresh_plan: Optional[RefreshPlan] = None
    summary_cache_policy: Optional[SummaryCachePolicy] = None
    summary_manifest_prefix: Optional[str] = None
    summary_manifests: Dict[str, SummaryManifest] = field(default_factory=dict)
    summary_outputs: Dict[str, SummaryState] = field(default_factory=dict)
    spec_locator: str = ""
    provenance_dir: Optional[Path] = None
    pipeline_spec_manifest_path: Optional[Path] = None
    source_state_manifest_path: Optional[str] = None
    published_merged_artifacts: List[Dict[str, Any]] = field(default_factory=list)
    step_state_bindings: Dict[str, str] = field(default_factory=dict)
    logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        self.logger = logging.getLogger(f"mhm_core.pipeline.{self.run_id}")
        self.logger.setLevel(logging.INFO)

    def ensure_directories(self) -> None:
        for path in (self.workspace_dir, self.raw_dir, self.merged_dir, self.summary_dir, self.logs_dir):
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
    logs_dir = run_dir / "logs"

    merged_base_prefix = resolve_output_base_prefix(spec.outputs.merged_prefix, run_id=spec.run_id)

    context = RunContext(
        spec=spec,
        run_id=spec.run_id,
        workspace_dir=run_dir,
        raw_dir=raw_dir,
        merged_dir=merged_dir,
        summary_dir=summary_dir,
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

    site_map = getattr(spec.source, "site_map", None)
    if isinstance(site_map, dict):
        context.participant_sites.update(site_map)
    if spec.source.discover_all and not spec.source.participants:
        participants, discovered_map = discover_participants(
            s3_client,
            bucket=spec.source.bucket,
            prefix=spec.source.prefix,
            logger=context.logger,
            sites=getattr(spec.source, "sites", None),
        )
        spec.source.participants = participants
        context.participant_sites.update(discovered_map)
        context.logger.info("[spec] Discovered %d participants via discover_all", len(participants))

    return context


def resolve_output_base_prefix(template: str, *, run_id: str) -> str:
    rendered = template.format(
        run_id=run_id,
        site="{site}",
        participant_id="{participant_id}",
        participant="{participant}",
    )
    if "{site}" in rendered:
        rendered = rendered.split("{site}", 1)[0]
    return rendered.rstrip("/") or rendered


def resolve_output_prefix(template: str, *, run_id: str) -> str:
    return template.format(
        run_id=run_id,
        site="",
        participant_id="",
        participant="",
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


def active_participants(context: RunContext) -> List[str]:
    if context.current_participant:
        return [context.current_participant]
    if context.batch_participants:
        return list(context.batch_participants)
    if context.participant_sites:
        return list(context.participant_sites.keys())
    return list(context.spec.iter_participants())


__all__ = [
    "RunContext",
    "SummaryState",
    "create_run_context",
    "active_participants",
    "ensure_participant_manifest",
    "ensure_summary_manifest",
    "resolve_output_base_prefix",
    "resolve_output_prefix",
]
