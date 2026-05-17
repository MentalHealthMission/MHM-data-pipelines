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

import boto3
from botocore.exceptions import ClientError

from .context import create_run_context, resolve_output_prefix
from .discovery import discover_participants
from .plugins import load_pipeline_observer
from .queue import PRIORITY_RANK, select_next_spec
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
    session = boto3.session.Session()
    s3_client = session.client("s3")
    spec = load_spec(args.spec, s3_client=s3_client, default_profile=default_pipeline_profile)
    _maybe_discover_participants(spec, s3_client, logger=logging.getLogger("mhm_core.pipeline.validate"))
    errors = validate_spec(spec)
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1
    print(json.dumps({"status": "ok", "run_id": spec.run_id, "participants": len(spec.source.participants)}, indent=2))
    return 0


def cmd_run(
    args: argparse.Namespace,
    *,
    default_pipeline_profile: str = DEFAULT_PIPELINE_PROFILE,
) -> int:
    session = boto3.session.Session(profile_name=args.profile) if args.profile else boto3.session.Session()
    spec = load_spec(args.spec, s3_client=session.client("s3"), default_profile=default_pipeline_profile)
    _maybe_discover_participants(spec, session.client("s3"), logger=logging.getLogger("mhm_core.pipeline.discovery"))
    errors = validate_spec(spec)
    if errors:
        for err in errors:
            logging.error(err)
        return 1

    if args.clean_workspace:
        run_dir = spec.workspace.resolve_run_path(spec.run_id)
        shutil.rmtree(run_dir, ignore_errors=True)

    context = create_run_context(spec, boto3_session=session, spec_locator=args.spec)
    context.pipeline_observer = load_pipeline_observer(spec.profile, default_profile=default_pipeline_profile)
    context.logger.info("Starting pipeline run %s", spec.run_id)
    context.participant_sites.update(getattr(spec.source, "site_map", {}))
    context.pipeline_observer.on_run_start(context)

    steps = build_steps(spec)
    per_participant_steps = [step for step in steps if getattr(step, "run_per_participant", True)]
    run_once_steps = [step for step in steps if not getattr(step, "run_per_participant", True)]
    refresh_plan, summary_policy = build_refresh_plan(spec)
    context.refresh_plan = refresh_plan
    context.summary_cache_policy = summary_policy
    if summary_policy.manifest_prefix:
        context.summary_manifest_prefix = resolve_output_prefix(
            summary_policy.manifest_prefix,
            run_id=spec.run_id,
        )
    else:
        context.summary_manifest_prefix = None
    participants = list(spec.iter_participants())
    participants = _filter_participants_for_latest_measurement_metrics(context, participants)
    participants = _filter_completed_participants(context, participants)
    spec.source.participants = list(participants)
    context.metrics = {step.name: {} for step in steps}
    batches = _build_batches(spec, participants, context.participant_sites)
    total_batches = len(batches)

    if not batches:
        context.logger.info("No participants remain after resume filtering; nothing to do.")
        return 0

    queue_prefix = os.environ.get("QUEUE_PREFIX", "").strip()
    last_suspend_probe = 0.0

    for batch_idx, batch in enumerate(batches, start=1):
        context.batch_participants = list(batch)
        batch_label = f"batch_{batch_idx:03d}"
        context.logger.info(
            "Starting batch %d/%d (%d participants)",
            batch_idx,
            total_batches,
            len(batch),
        )
        for participant_id in batch:
            context.current_participant = participant_id
            context.logger.info("Processing participant %s", participant_id)
            for step in per_participant_steps:
                step_index = int(getattr(step, "_step_index", 0) or 0)
                pre_step_state = context.pipeline_observer.before_step(
                    context,
                    step=step,
                    step_index=step_index,
                )
                metrics = _run_step_with_timing(context, step)
                context.metrics[step.name][participant_id] = metrics
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

        context.current_participant = None
        for step in run_once_steps:
            context.logger.info(
                "Running step %s for batch %d/%d (%d participants)",
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

    context.current_participant = None
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


def _maybe_discover_participants(spec: RunSpec, s3_client, *, logger: logging.Logger) -> None:
    if not getattr(spec.source, "discover_all", False):
        return
    if spec.source.participants:
        return
    participants, site_map = discover_participants(
        s3_client,
        bucket=spec.source.bucket,
        prefix=spec.source.prefix,
        logger=logger,
        sites=getattr(spec.source, "sites", None),
    )
    spec.source.participants = participants
    spec.source.site_map = site_map  # type: ignore[attr-defined]
    logger.info("Discovered %d participants across %d sites", len(participants), len(set(site_map.values())))


def _build_batches(spec: RunSpec, participants: list[str], participant_sites: dict[str, str]) -> list[list[str]]:
    strategy = getattr(spec.batching, "strategy", "none")
    max_participants = getattr(spec.batching, "max_participants", None)

    if strategy == "site":
        grouped: dict[str, list[str]] = defaultdict(list)
        for participant_id in participants:
            site = participant_sites.get(participant_id, "")
            grouped[site].append(participant_id)

        ordered_sites: list[str] = []
        for site in getattr(spec.source, "sites", []):
            if site in grouped and site not in ordered_sites:
                ordered_sites.append(site)
        for site in sorted(grouped):
            if site not in ordered_sites:
                ordered_sites.append(site)

        site_batches = [sorted(grouped[site]) for site in ordered_sites if grouped.get(site)]
        return _chunk_batches(site_batches, max_participants=max_participants)

    if strategy == "participant_count" and max_participants:
        return [
            participants[idx : idx + max_participants]
            for idx in range(0, len(participants), max_participants)
        ]

    return [participants]


def _chunk_batches(batches: list[list[str]], *, max_participants: int | None) -> list[list[str]]:
    if not max_participants or max_participants <= 0:
        return batches
    chunked: list[list[str]] = []
    for batch in batches:
        for idx in range(0, len(batch), max_participants):
            chunked.append(batch[idx : idx + max_participants])
    return chunked


def _filter_completed_participants(context, participants: list[str]) -> list[str]:
    if not getattr(context.spec.batching, "resume_completed", False):
        return participants
    if not participants:
        return participants
    if _is_latest_measurement_only_spec(context.spec):
        context.logger.info(
            "[resume   ] Skipping merged-data resume filter for latest-measurement run"
        )
        return participants
    if not context.merged_base_prefix.startswith("s3://"):
        context.logger.info("[resume   ] merged base prefix is not S3-backed; skipping resume filter")
        return participants

    participants_by_site: dict[str, set[str]] = defaultdict(set)
    unknown_site_count = 0
    for participant_id in participants:
        site = context.participant_sites.get(participant_id)
        if not site:
            unknown_site_count += 1
            continue
        participants_by_site[site].add(participant_id)

    bucket, key_prefix = _split_s3_uri(context.merged_base_prefix)
    paginator = context.s3_client.get_paginator("list_objects_v2")
    completed: set[str] = set()

    for site, site_participants in participants_by_site.items():
        site_prefix = f"{key_prefix.rstrip('/')}/{site}/"
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=site_prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key", "")
                    if not key.endswith("/manifest.json"):
                        continue
                    rel = key[len(site_prefix) :]
                    participant_id = rel.split("/", 1)[0].strip("/")
                    if participant_id in site_participants:
                        completed.add(participant_id)
        except ClientError as exc:
            context.logger.warning(
                "[resume   ] Failed listing published manifests under s3://%s/%s: %s",
                bucket,
                site_prefix,
                exc,
            )

    if unknown_site_count:
        context.logger.info(
            "[resume   ] %d participants have unknown site mapping and will not be skipped",
            unknown_site_count,
        )

    if completed:
        context.logger.info(
            "[resume   ] Skipping %d participants with published manifests under %s",
            len(completed),
            context.merged_base_prefix,
        )
    else:
        context.logger.info(
            "[resume   ] No published participant manifests found under %s",
            context.merged_base_prefix,
        )

    remaining = [participant_id for participant_id in participants if participant_id not in completed]
    context.logger.info("[resume   ] %d participants remain to process", len(remaining))
    return remaining


def _is_latest_measurement_only_spec(spec: RunSpec) -> bool:
    step_types = {str(step.type).strip() for step in getattr(spec.processing, "steps", [])}
    if "latest_measurement_dates" not in step_types:
        return False
    data_transform_steps = {"merge", "redact", "summary", "rapids"}
    return not bool(step_types & data_transform_steps)


def _latest_measurement_metrics(spec: RunSpec) -> list[str]:
    metrics: set[str] = set()
    for step in getattr(spec.processing, "steps", []):
        if str(step.type).strip() != "latest_measurement_dates":
            continue
        for measure in step.options.get("measures", []):
            if not isinstance(measure, dict):
                continue
            metric = str(measure.get("metric", "")).strip()
            if metric:
                metrics.add(metric)
    if not metrics:
        metrics.update(getattr(spec.filters, "include_metrics", []))
    return sorted(metrics)


def _filter_participants_for_latest_measurement_metrics(context, participants: list[str]) -> list[str]:
    if not participants:
        return participants
    if not _is_latest_measurement_only_spec(context.spec):
        return participants

    metrics = _latest_measurement_metrics(context.spec)
    if not metrics:
        return participants

    source_prefix = str(context.spec.source.prefix).strip().strip("/")
    kept: list[str] = []
    unknown_site_count = 0
    checked_prefixes = 0

    for participant_id in participants:
        site = context.participant_sites.get(participant_id)
        if not site:
            unknown_site_count += 1
            kept.append(participant_id)
            continue
        for metric in metrics:
            metric_prefix = "/".join(
                part.strip("/")
                for part in [source_prefix, site, participant_id, metric]
                if str(part).strip("/")
            )
            checked_prefixes += 1
            if _s3_prefix_has_objects(context.s3_client, context.spec.source.bucket, f"{metric_prefix}/"):
                kept.append(participant_id)
                break

    removed = len(participants) - len(kept)
    context.logger.info(
        "[latest  ] Kept %d participants with source data for %d latest-measurement metric(s); filtered %d without those metrics",
        len(kept),
        len(metrics),
        removed,
    )
    context.logger.debug("[latest  ] Checked %d source metric prefixes", checked_prefixes)
    if unknown_site_count:
        context.logger.info(
            "[latest  ] %d participants have unknown site mapping and were kept for normal validation",
            unknown_site_count,
        )
    return kept


def _s3_prefix_has_objects(s3_client, bucket: str, prefix: str) -> bool:
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    except AttributeError:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            return bool(page.get("Contents"))
        return False
    return bool(response.get("KeyCount") or response.get("Contents"))


def _split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got {uri}")
    remainder = uri[len("s3://") :]
    bucket, _, key = remainder.partition("/")
    if not bucket:
        raise ValueError(f"Missing bucket in S3 URI: {uri}")
    return bucket, key


def _should_suspend(context, queue_prefix: str, last_suspend_probe: float) -> bool:
    if context.spec.priority == "ludicrous":
        return False
    now = time.monotonic()
    if now - last_suspend_probe < SUSPEND_CHECK_INTERVAL_SECONDS:
        return False
    next_entry = select_next_spec(context.s3_client, queue_prefix, states=("pending",))
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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
