"""Command-line entry point for the MHM-core pipeline runner."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from typing import Optional
from collections import defaultdict

from .capabilities import EntitySelectionCapability
from .context import (
    create_run_context,
    object_store_locator_for_spec,
    resolve_output_prefix,
    set_cache_refresh_policy,
    source_root_locator,
    spec_needs_object_store,
)
from .discovery import discover_entities_from_store
from .object_store import (
    ObjectStore,
    client_error_code,
    create_boto3_session,
    create_object_store_for_locator,
    locator_needs_object_store,
    locator_scheme,
    object_store_client,
    object_store_from_client,
)
from .plugins import load_pipeline_observer, load_pipeline_publisher, validate_profile_spec
from .queue import PRIORITY_RANK, QueueBackend, select_next_queue_spec
from .refresh_plan import build_refresh_plan
from .spec import DEFAULT_PIPELINE_PROFILE, RunSpec, load_spec, validate_spec
from .steps import build_steps

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"
SUSPEND_EXIT_CODE = 75
SUSPEND_CHECK_INTERVAL_SECONDS = 15.0


def main(
    argv: Optional[list[str]] = None,
    *,
    default_pipeline_profile: str = DEFAULT_PIPELINE_PROFILE,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    if args.command == "validate":
        return cmd_validate(args, default_pipeline_profile=default_pipeline_profile)
    if args.command == "run":
        return cmd_run(args, default_pipeline_profile=default_pipeline_profile)
    parser.print_help()
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MHM core pipeline runner")
    sub = parser.add_subparsers(dest="command")

    validate = sub.add_parser("validate", help="Validate a run specification")
    validate.add_argument("spec", help="Path or s3:// URI to the run spec YAML")

    run = sub.add_parser("run", help="Execute the pipeline for a run spec")
    run.add_argument("spec", help="Path or s3:// URI to the run spec YAML")
    run.add_argument("--profile", help="AWS profile name", default=None)
    run.add_argument(
        "--clean-workspace",
        action="store_true",
        help="Remove existing workspace for the run_id before executing",
    )

    return parser


def cmd_validate(
    args: argparse.Namespace,
    *,
    default_pipeline_profile: str = DEFAULT_PIPELINE_PROFILE,
) -> int:
    session = None
    s3_client = None
    object_store = None
    if _locator_needs_s3_client(args.spec):
        session = _boto3_session()
        object_store = create_object_store_for_locator(args.spec, session=session)
        s3_client = object_store_client(object_store)
    spec = load_spec(args.spec, s3_client=s3_client, object_store=object_store, default_profile=default_pipeline_profile)
    if spec.source.discover_all and not spec.source.entities and object_store is None:
        session = _boto3_session()
        object_store = create_object_store_for_locator(object_store_locator_for_spec(spec), session=session)
        s3_client = object_store_client(object_store)
    _maybe_discover_entities(spec, object_store, logger=logging.getLogger("mhm_core.pipeline.validate"))
    errors = validate_spec(spec) + validate_profile_spec(spec, default_profile=default_pipeline_profile)
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1
    entity_count = len(list(spec.iter_entities()))
    print(
        json.dumps(
            {
                "status": "ok",
                "run_id": spec.run_id,
                "entities": entity_count,
                "participants": entity_count,
            },
            indent=2,
        )
    )
    return 0


def cmd_run(
    args: argparse.Namespace,
    *,
    default_pipeline_profile: str = DEFAULT_PIPELINE_PROFILE,
) -> int:
    session = None
    s3_client = None
    object_store = None
    if _locator_needs_s3_client(args.spec):
        session = _boto3_session(profile_name=args.profile)
        object_store = create_object_store_for_locator(args.spec, session=session)
        s3_client = object_store_client(object_store)
    spec = load_spec(args.spec, s3_client=s3_client, object_store=object_store, default_profile=default_pipeline_profile)
    if spec_needs_object_store(spec) and object_store is None:
        session = _boto3_session(profile_name=args.profile)
        object_store = create_object_store_for_locator(object_store_locator_for_spec(spec), session=session)
        s3_client = object_store_client(object_store)
    _maybe_discover_entities(spec, object_store, logger=logging.getLogger("mhm_core.pipeline.discovery"))
    errors = validate_spec(spec) + validate_profile_spec(spec, default_profile=default_pipeline_profile)
    if errors:
        for err in errors:
            logging.error(err)
        return 1

    if args.clean_workspace:
        run_dir = spec.workspace.resolve_run_path(spec.run_id)
        shutil.rmtree(run_dir, ignore_errors=True)

    context = create_run_context(
        spec,
        boto3_session=session,
        s3_client=s3_client,
        object_store=object_store,
        spec_locator=args.spec,
    )
    context.pipeline_observer = load_pipeline_observer(spec.profile, default_profile=default_pipeline_profile)
    context.pipeline_publisher = load_pipeline_publisher(spec.profile, default_profile=default_pipeline_profile)
    context.logger.info("Starting pipeline run %s", spec.run_id)
    context.entity_groups.update(getattr(spec.source, "entity_group_map", {}))
    context.participant_sites.update(context.entity_groups)
    context.pipeline_observer.on_run_start(context)

    steps = build_steps(spec)
    per_participant_steps = [step for step in steps if getattr(step, "run_per_participant", True)]
    run_once_steps = [step for step in steps if not getattr(step, "run_per_participant", True)]
    refresh_plan, cache_policy = build_refresh_plan(spec, steps=steps)
    context.refresh_plan = refresh_plan
    set_cache_refresh_policy(context, cache_policy)
    if cache_policy.manifest_prefix:
        context.summary_manifest_prefix = resolve_output_prefix(
            cache_policy.manifest_prefix,
            run_id=spec.run_id,
        )
    else:
        context.summary_manifest_prefix = None
    entities = list(spec.iter_entities())
    entity_selection = _entity_selection_capabilities(spec, steps)
    entities = _filter_entities_for_required_source_metrics(context, entities, entity_selection)
    entities = _filter_completed_entities(
        context,
        entities,
        skip_completed_resume=any(capability.skip_completed_resume for capability in entity_selection),
    )
    spec.source.entities = list(entities)
    context.metrics = {step.name: {} for step in steps}
    batches = _build_entity_batches(spec, entities, context.entity_groups)
    total_batches = len(batches)

    if not batches:
        context.logger.info("No entities remain after resume filtering; nothing to do.")
        return 0

    queue_prefix = os.environ.get("QUEUE_PREFIX", "").strip()
    last_suspend_probe = 0.0

    for batch_idx, batch in enumerate(batches, start=1):
        context.batch_entities = list(batch)
        context.batch_participants = list(batch)
        batch_label = f"batch_{batch_idx:03d}"
        context.logger.info(
            "Starting batch %d/%d (%d entities)",
            batch_idx,
            total_batches,
            len(batch),
        )
        for entity_id in batch:
            context.current_entity = entity_id
            context.current_participant = entity_id
            context.logger.info("Processing entity %s", entity_id)
            for step in per_participant_steps:
                step_index = int(getattr(step, "_step_index", 0) or 0)
                pre_step_state = context.pipeline_observer.before_step(
                    context,
                    step=step,
                    step_index=step_index,
                )
                metrics = _run_step_with_timing(context, step)
                context.metrics[step.name][entity_id] = metrics
                context.pipeline_observer.after_step(
                    context,
                    step=step,
                    step_index=step_index,
                    metrics=metrics,
                    pre_step_state=pre_step_state,
                )
            if queue_prefix and _should_suspend(context, queue_prefix, last_suspend_probe):
                return SUSPEND_EXIT_CODE
            last_suspend_probe = time.monotonic()

        context.current_entity = None
        context.current_participant = None
        for step in run_once_steps:
            context.logger.info(
                "Running step %s for batch %d/%d (%d entities)",
                step.name,
                batch_idx,
                total_batches,
                len(batch),
            )
            step_index = int(getattr(step, "_step_index", 0) or 0)
            pre_step_state = context.pipeline_observer.before_step(
                context,
                step=step,
                step_index=step_index,
            )
            metrics = _run_step_with_timing(context, step)
            context.pipeline_observer.after_step(
                context,
                step=step,
                step_index=step_index,
                metrics=metrics,
                pre_step_state=pre_step_state,
            )
            key = "all" if total_batches == 1 else batch_label
            if total_batches == 1:
                context.metrics[step.name][key] = metrics
            else:
                context.metrics[step.name][key] = {
                    "entities": list(batch),
                    "participants": list(batch),
                    "metrics": metrics,
                }
            if (
                queue_prefix
                and step.suspend_checkpoint in {"step", "batch"}
                and _should_suspend(context, queue_prefix, last_suspend_probe)
            ):
                return SUSPEND_EXIT_CODE
            last_suspend_probe = time.monotonic()

    context.current_entity = None
    context.current_participant = None
    context.batch_entities = None
    context.batch_participants = None

    context.logger.info("Pipeline run %s completed.", spec.run_id)
    return 0


def _run_step_with_timing(context, step):
    started = time.monotonic()
    metrics = step.run(context)
    duration_seconds = round(time.monotonic() - started, 3)
    context.logger.info("[timing  ] Step %s completed in %.3fs", step.name, duration_seconds)
    if isinstance(metrics, dict):
        metrics = dict(metrics)
        metrics.setdefault("duration_seconds", duration_seconds)
        return metrics
    return {"status": "ok", "result": metrics, "duration_seconds": duration_seconds}


def _maybe_discover_entities(spec: RunSpec, object_store: ObjectStore | None, *, logger: logging.Logger) -> None:
    if not getattr(spec.source, "discover_all", False):
        return
    if spec.source.entities:
        return
    if object_store is None:
        raise RuntimeError("source.discover_all requires an object-store backend")
    object_store = _coerce_object_store(object_store)
    entities, entity_group_map = discover_entities_from_store(
        object_store,
        source_locator=source_root_locator(spec.source),
        logger=logger,
        groups=getattr(spec.source, "groups", None),
    )
    spec.source.entities = entities
    spec.source.entity_group_map = entity_group_map
    logger.info("Discovered %d entities across %d groups", len(entities), len(set(entity_group_map.values())))


def _maybe_discover_participants(spec: RunSpec, object_store: ObjectStore | None, *, logger: logging.Logger) -> None:
    """Compatibility wrapper for the historical runner helper name."""

    _maybe_discover_entities(spec, object_store, logger=logger)


def _build_entity_batches(spec: RunSpec, entities: list[str], entity_groups: dict[str, str]) -> list[list[str]]:
    strategy = getattr(spec.batching, "strategy", "none")
    max_entities = getattr(spec.batching, "max_entities", getattr(spec.batching, "max_participants", None))

    if strategy in {"group", "site"}:
        grouped: dict[str, list[str]] = defaultdict(list)
        for entity_id in entities:
            group = entity_groups.get(entity_id, "")
            grouped[group].append(entity_id)

        ordered_groups: list[str] = []
        for group in getattr(spec.source, "groups", getattr(spec.source, "sites", [])):
            if group in grouped and group not in ordered_groups:
                ordered_groups.append(group)
        for group in sorted(grouped):
            if group not in ordered_groups:
                ordered_groups.append(group)

        group_batches = [sorted(grouped[group]) for group in ordered_groups if grouped.get(group)]
        return _chunk_batches(group_batches, max_entities=max_entities)

    if strategy in {"entity_count", "participant_count"} and max_entities:
        return [
            entities[idx : idx + max_entities]
            for idx in range(0, len(entities), max_entities)
        ]

    return [entities]


def _build_batches(spec: RunSpec, participants: list[str], participant_sites: dict[str, str]) -> list[list[str]]:
    return _build_entity_batches(spec, participants, participant_sites)


def _chunk_batches(batches: list[list[str]], *, max_entities: int | None) -> list[list[str]]:
    if not max_entities or max_entities <= 0:
        return batches
    chunked: list[list[str]] = []
    for batch in batches:
        for idx in range(0, len(batch), max_entities):
            chunked.append(batch[idx : idx + max_entities])
    return chunked


def _filter_completed_entities(
    context,
    entities: list[str],
    *,
    skip_completed_resume: bool = False,
) -> list[str]:
    if not getattr(context.spec.batching, "resume_completed", False):
        return entities
    if not entities:
        return entities
    if skip_completed_resume:
        context.logger.info("[resume   ] Skipping merged-data resume filter for selected step capability")
        return entities
    if not locator_needs_object_store(context.merged_base_prefix):
        context.logger.info("[resume   ] merged base prefix is not S3-backed; skipping resume filter")
        return entities

    entities_by_group: dict[str, set[str]] = defaultdict(set)
    unknown_group_count = 0
    entity_groups = getattr(context, "entity_groups", None) or getattr(context, "participant_sites", {})
    for entity_id in entities:
        group = entity_groups.get(entity_id)
        if not group:
            unknown_group_count += 1
            continue
        entities_by_group[group].add(entity_id)

    object_store = _context_object_store(context)
    completed: set[str] = set()

    for group, group_entities in entities_by_group.items():
        group_prefix = f"{context.merged_base_prefix.rstrip('/')}/{group}/"
        try:
            for locator in object_store.iter_object_locators(group_prefix):
                if not locator.endswith("/manifest.json"):
                    continue
                rel = locator[len(group_prefix) :]
                entity_id = rel.split("/", 1)[0].strip("/")
                if entity_id in group_entities:
                    completed.add(entity_id)
        except Exception as exc:
            if not client_error_code(exc):
                raise
            context.logger.warning(
                "[resume   ] Failed listing published manifests under %s: %s",
                group_prefix,
                exc,
            )

    if unknown_group_count:
        context.logger.info(
            "[resume   ] %d entities have unknown group mapping and will not be skipped",
            unknown_group_count,
        )

    if completed:
        context.logger.info(
            "[resume   ] Skipping %d entities with published manifests under %s",
            len(completed),
            context.merged_base_prefix,
        )
    else:
        context.logger.info(
            "[resume   ] No published participant manifests found under %s",
            context.merged_base_prefix,
        )

    remaining = [entity_id for entity_id in entities if entity_id not in completed]
    context.logger.info("[resume   ] %d entities remain to process", len(remaining))
    return remaining


def _filter_completed_participants(
    context,
    participants: list[str],
    *,
    skip_completed_resume: bool = False,
) -> list[str]:
    return _filter_completed_entities(context, participants, skip_completed_resume=skip_completed_resume)


def _entity_selection_capabilities(spec: RunSpec, steps) -> list[EntitySelectionCapability]:
    capabilities: list[EntitySelectionCapability] = []
    for step in steps:
        step_capabilities = step.describe_capabilities(spec)
        if step_capabilities.entity_selection:
            capabilities.append(step_capabilities.entity_selection)
    return capabilities


def _participant_selection_capabilities(spec: RunSpec, steps) -> list[EntitySelectionCapability]:
    return _entity_selection_capabilities(spec, steps)


def _filter_entities_for_required_source_metrics(
    context,
    entities: list[str],
    capabilities: list[EntitySelectionCapability],
) -> list[str]:
    if not entities:
        return entities
    metrics = sorted(
        {
            str(metric).strip()
            for capability in capabilities
            for metric in capability.required_source_metrics
            if str(metric).strip()
        }
    )
    if not metrics:
        return entities
    labels = sorted({str(capability.label).strip() for capability in capabilities if str(capability.label).strip()})
    label = ", ".join(labels) if labels else "source-metric"

    source_locator = source_root_locator(context.spec.source).rstrip("/")
    kept: list[str] = []
    unknown_group_count = 0
    checked_prefixes = 0
    entity_groups = getattr(context, "entity_groups", None) or getattr(context, "participant_sites", {})

    for entity_id in entities:
        group = entity_groups.get(entity_id)
        if not group:
            unknown_group_count += 1
            kept.append(entity_id)
            continue
        for metric in metrics:
            metric_prefix = "/".join(
                part.strip("/")
                for part in [source_locator, group, entity_id, metric]
                if str(part).strip("/")
            )
            checked_prefixes += 1
            if _object_prefix_has_objects(
                _context_object_store(context),
                f"{metric_prefix}/",
            ):
                kept.append(entity_id)
                break

    removed = len(entities) - len(kept)
    context.logger.info(
        "[select  ] Kept %d entities with source data for %d required %s metric(s); filtered %d without those metrics",
        len(kept),
        len(metrics),
        label,
        removed,
    )
    context.logger.debug("[select  ] Checked %d source metric prefixes", checked_prefixes)
    if unknown_group_count:
        context.logger.info(
            "[select  ] %d entities have unknown group mapping and were kept for normal validation",
            unknown_group_count,
        )
    return kept


def _filter_participants_for_required_source_metrics(
    context,
    participants: list[str],
    capabilities: list[EntitySelectionCapability],
) -> list[str]:
    return _filter_entities_for_required_source_metrics(context, participants, capabilities)


def _s3_prefix_has_objects(s3_client, bucket: str, prefix: str) -> bool:
    """Compatibility wrapper for historical tests and callers."""

    return object_store_from_client(s3_client).prefix_has_objects(f"s3://{bucket}/{prefix.strip('/')}/")


def _object_prefix_has_objects(object_store: ObjectStore, prefix_locator: str) -> bool:
    return object_store.prefix_has_objects(prefix_locator)


def _context_object_store(context) -> ObjectStore:
    object_store = getattr(context, "object_store", None)
    if object_store is not None:
        return _coerce_object_store(object_store)
    return object_store_from_client(context.s3_client)


def _coerce_object_store(candidate) -> ObjectStore:
    if hasattr(candidate, "iter_child_prefix_locators") and hasattr(candidate, "read_bytes"):
        return candidate
    return object_store_from_client(candidate)


def _locator_needs_s3_client(locator: str) -> bool:
    return locator_needs_object_store(locator)


def _boto3_session(*, profile_name: str | None = None):
    return create_boto3_session(profile_name=profile_name)


def _should_suspend(context, queue_prefix: str, last_suspend_probe: float) -> bool:
    if context.spec.priority == "ludicrous":
        return False
    now = time.monotonic()
    if now - last_suspend_probe < SUSPEND_CHECK_INTERVAL_SECONDS:
        return False
    next_entry = select_next_queue_spec(_queue_backend(context.s3_client, queue_prefix), states=("pending",))
    if next_entry is None:
        return False
    if next_entry.priority not in {"urgent", "ludicrous"}:
        return False
    current_rank = PRIORITY_RANK[context.spec.priority]
    next_rank = PRIORITY_RANK[next_entry.priority]
    if next_rank <= current_rank:
        return False
    if next_entry.priority == "ludicrous":
        context.logger.info(
            "[suspend  ] Pending ludicrous run detected; suspending %s at safe checkpoint",
            context.run_id,
        )
        return True
    if next_entry.priority == "urgent":
        context.logger.info(
            "[suspend  ] Pending urgent run detected; suspending %s at safe checkpoint",
            context.run_id,
        )
        return True
    return False


def _queue_backend(object_store_client, queue_prefix: str) -> QueueBackend:
    scheme = locator_scheme(queue_prefix)
    if scheme == "s3":
        from .backends.s3 import S3QueueBackend

        return S3QueueBackend(object_store_client, queue_prefix)
    raise ValueError(f"Unsupported queue backend for locator scheme '{scheme or 'local'}': {queue_prefix}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
